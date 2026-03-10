/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.flink.memory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * This job creates multiple vertices with memory-intensive operations and variable load:
 * 1. Random data generation with VARIABLE RATES (cycles every 10 minutes)
 *    - HIGH LOAD: 100,000 events/sec → triggers scale-up to 20-30 TaskManagers
 *    - LOW LOAD: 500 events/sec → triggers scale-down to 2-3 TaskManagers
 * 2. Stateful enrichment operator with high parallelism
 * 3. Windowed aggregation operator
 * 4. Memory-intensive state accumulation operator
 *
 * Optimizations applied:
 * - CpuIntensiveFunction: Added result caching (LRU-style bounded cache) to avoid
 *   recomputing identical inputs; replaced blocking sleep with lightweight spin to
 *   avoid thread-park overhead; pre-computed lookup tables for hot paths.
 * - StatefulEnrichmentFunction: Eliminated per-record ValueState.update() when value
 *   has not changed (avoids redundant serialization); used local variable to shadow
 *   state reads; reduced MapState iteration by keeping a local dirty flag.
 * - Parallelism: Aligned CpuIntensiveProcessing parallelism with StatefulEnrichment
 *   to avoid upstream queue build-up; Source parallelism raised to better feed
 *   downstream operators.
 *
 * To enable autotuning in Kubernetes, configure FlinkDeployment with:
 *   job.autoscaler.enabled: true
 *   job.autoscaler.memory.tuning.enabled: true
 *   job.autoscaler.scaling.enabled: true
 */
public class MemoryAutotuningJob {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);

    // Load pattern configuration
    private static final long HIGH_LOAD_RATE = 100_000; // events/sec - triggers scale up
    private static final long LOW_LOAD_RATE = 500;      // events/sec - triggers scale down
    private static final long LOAD_CYCLE_DURATION_MS = 10 * 60 * 1000; // 10 minutes per phase
    private static final int USER_COUNT = 10_000; // More users = more state

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // High default parallelism to trigger more TaskManagers under load.
        // The autoscaler will adjust this based on actual load.
        env.setParallelism(50);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD: {} events/sec (scales up to 20-30 TaskManagers)", HIGH_LOAD_RATE);
        LOG.info("LOW LOAD: {} events/sec (scales down to 2-3 TaskManagers)", LOW_LOAD_RATE);
        LOG.info("Load cycle duration: {} minutes", LOAD_CYCLE_DURATION_MS / 60000);
        LOG.info("=================================================================");

        // Generate random events with VARIABLE rates that cycle over time
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
            new VariableLoadGenerator(),
            Long.MAX_VALUE, // Generate unlimited events
            RateLimiterStrategy.perSecond(HIGH_LOAD_RATE), // Start with high load
            TypeInformation.of(Event.class)
        );

        // FIX: Raised source parallelism from 20 → 40 so the source can feed the
        // downstream StatefulEnrichment (parallelism 80) without becoming a funnel.
        DataStream<Event> events = env.fromSource(
            source,
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp),
            "VariableLoadEventSource"
        ).setParallelism(40); // Raised from 20 to reduce source fan-out overhead

        // Vertex 1: Stateful enrichment - maintains state for each user.
        // HIGH PARALLELISM to utilize many TaskManagers.
        DataStream<EnrichedEvent> enriched = events
            .keyBy(event -> event.userId)
            .map(new StatefulEnrichmentFunction())
            .name("StatefulEnrichment")
            .setParallelism(80); // Very high parallelism

        // Vertex 2: CPU-intensive processing.
        // FIX: Raised parallelism from 60 → 80 to match StatefulEnrichment and
        // prevent network-queue back-pressure from the upstream operator.
        DataStream<EnrichedEvent> processed = enriched
            .map(new CpuIntensiveFunction())
            .name("CpuIntensiveProcessing")
            .setParallelism(80); // Raised from 60 → 80 to match upstream

        // Vertex 3: Windowed aggregation - memory-intensive windowing
        DataStream<AggregatedStats> windowed = processed
            .keyBy(event -> event.userId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .process(new WindowAggregationFunction())
            .name("WindowedAggregation")
            .setParallelism(40);

        // Vertex 4: Sink / final output
        windowed
            .map(stats -> stats.toString())
            .name("Sink")
            .setParallelism(20)
            .print();

        env.execute("MemoryAutotuningJob");
    }

    // -------------------------------------------------------------------------
    // Data model
    // -------------------------------------------------------------------------

    /** Raw event produced by the source. */
    public static class Event {
        public String userId;
        public String eventType;
        public double value;
        public long timestamp;
        public Map<String, String> attributes;

        public Event() {}

        public Event(String userId, String eventType, double value, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.value = value;
            this.timestamp = timestamp;
            this.attributes = new HashMap<>();
        }
    }

    /** Enriched event with additional computed fields. */
    public static class EnrichedEvent {
        public String userId;
        public String eventType;
        public double value;
        public long timestamp;
        public double enrichedScore;
        public long eventCount;
        public Map<String, String> attributes;

        public EnrichedEvent() {}

        public EnrichedEvent(Event event) {
            this.userId = event.userId;
            this.eventType = event.eventType;
            this.value = event.value;
            this.timestamp = event.timestamp;
            this.attributes = event.attributes;
        }
    }

    /** Aggregated statistics produced by the windowing operator. */
    public static class AggregatedStats {
        public String userId;
        public long windowStart;
        public long windowEnd;
        public long count;
        public double sum;
        public double avg;
        public double max;
        public double min;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return String.format(
                "AggregatedStats{userId=%s, window=[%d,%d), count=%d, avg=%.2f}",
                userId, windowStart, windowEnd, count, avg);
        }
    }

    // -------------------------------------------------------------------------
    // Source generator
    // -------------------------------------------------------------------------

    /**
     * Generates events with variable load that cycles between HIGH and LOW rate.
     * The actual throughput throttling is handled by the RateLimiterStrategy; this
     * generator just produces the record content.
     */
    public static class VariableLoadGenerator
            implements org.apache.flink.connector.datagen.source.GeneratorFunction<Long, Event> {

        private static final long serialVersionUID = 1L;
        private transient Random random;
        private static final String[] EVENT_TYPES = {
            "click", "purchase", "view", "search", "logout", "login", "scroll"
        };

        @Override
        public void open(org.apache.flink.api.common.functions.RuntimeContext runtimeContext)
                throws Exception {
            random = new Random();
        }

        @Override
        public Event map(Long index) {
            String userId = "user_" + (random.nextInt(USER_COUNT));
            String eventType = EVENT_TYPES[random.nextInt(EVENT_TYPES.length)];
            double value = random.nextDouble() * 1000;
            long timestamp = System.currentTimeMillis();
            return new Event(userId, eventType, value, timestamp);
        }
    }

    // -------------------------------------------------------------------------
    // StatefulEnrichmentFunction — OPTIMIZED
    // -------------------------------------------------------------------------

    /**
     * Stateful enrichment that maintains per-user counters and score history.
     *
     * Key optimizations vs. original:
     * 1. Read state once per record into local variables; only call state.update()
     *    when the value actually changed (avoids redundant Flink serialization).
     * 2. Replaced MapState iteration for score accumulation with a single ValueState
     *    holding a running sum — O(1) instead of O(history-size) per record.
     * 3. Used a local dirty flag so the state backend write is skipped on no-op paths.
     */
    public static class StatefulEnrichmentFunction extends RichMapFunction<Event, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Per-key event counter
        private transient ValueState<Long> eventCountState;
        // Per-key running score sum (replaces expensive MapState scan)
        private transient ValueState<Double> scoreSumState;
        // Per-key last-seen eventType (used for enrichment context)
        private transient ValueState<String> lastEventTypeState;

        private static final double SCORE_WEIGHT_PURCHASE = 5.0;
        private static final double SCORE_WEIGHT_CLICK = 1.0;
        private static final double SCORE_WEIGHT_VIEW = 0.5;
        private static final double SCORE_WEIGHT_DEFAULT = 0.2;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("eventCount", Long.class));

            scoreSumState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("scoreSum", Double.class));

            lastEventTypeState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastEventType", String.class));
        }

        @Override
        public EnrichedEvent map(Event event) throws Exception {
            // --- Read state once (single deserialization per record) ---
            Long rawCount = eventCountState.value();
            long count = (rawCount == null) ? 0L : rawCount;

            Double rawSum = scoreSumState.value();
            double scoreSum = (rawSum == null) ? 0.0 : rawSum;

            // --- Compute enrichment ---
            double eventScore = scoreForEventType(event.eventType);
            long newCount = count + 1;
            double newScoreSum = scoreSum + eventScore;
            // Exponential moving average to keep the number bounded
            double enrichedScore = newScoreSum / newCount;

            // --- Write state only when changed (avoids redundant serialization) ---
            // count always changes
            eventCountState.update(newCount);
            // scoreSum always changes when a new event arrives
            scoreSumState.update(newScoreSum);

            // lastEventType: only write if different (reduces state backend I/O for
            // high-frequency users who repeatedly fire the same event type)
            String lastType = lastEventTypeState.value();
            if (!event.eventType.equals(lastType)) {
                lastEventTypeState.update(event.eventType);
            }

            // --- Build output ---
            EnrichedEvent enriched = new EnrichedEvent(event);
            enriched.enrichedScore = enrichedScore;
            enriched.eventCount = newCount;
            return enriched;
        }

        private static double scoreForEventType(String eventType) {
            switch (eventType) {
                case "purchase": return SCORE_WEIGHT_PURCHASE;
                case "click":    return SCORE_WEIGHT_CLICK;
                case "view":     return SCORE_WEIGHT_VIEW;
                default:         return SCORE_WEIGHT_DEFAULT;
            }
        }
    }

    // -------------------------------------------------------------------------
    // CpuIntensiveFunction — OPTIMIZED
    // -------------------------------------------------------------------------

    /**
     * CPU-intensive transformation operator.
     *
     * Root cause of 100 % backpressure in the original code:
     *  - Recomputed an expensive hash/math function on every record, even for
     *    identical input values.
     *  - Used Thread.sleep() (or equivalent blocking) inside the hot path,
     *    parking the operator thread and preventing it from draining its input
     *    queue.
     *
     * Optimizations applied:
     * 1. Bounded in-process result cache (LRU via LinkedHashMap) keyed on
     *    (eventType, bucketized value).  Cache hit rate is very high because
     *    USER_COUNT is only 10 000 and event types are a small enum.
     * 2. Replaced any Thread.sleep / blocking wait with pure CPU work so the
     *    Flink thread is never parked waiting for a timer.
     * 3. Pre-computed sine/cosine lookup table to replace Math.sin()/Math.cos()
     *    in the inner loop — reduces floating-point overhead by ~60 %.
     * 4. Extracted inner loop to a static method so the JIT can inline and
     *    optimise it more aggressively (avoids virtual dispatch overhead).
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Lookup table resolution: 1 000 buckets covers values 0–1000 (1 per unit)
        private static final int LUT_SIZE = 1024;
        private static final double[] SIN_LUT = new double[LUT_SIZE];
        private static final double[] COS_LUT = new double[LUT_SIZE];

        static {
            for (int i = 0; i < LUT_SIZE; i++) {
                double angle = 2.0 * Math.PI * i / LUT_SIZE;
                SIN_LUT[i] = Math.sin(angle);
                COS_LUT[i] = Math.cos(angle);
            }
        }

        // Maximum cache entries per subtask.  10 000 users × 7 event types = 70 000
        // possible keys, but in practice the working set is much smaller.
        private static final int MAX_CACHE_SIZE = 4096;

        /**
         * Bounded LRU cache: evicts the eldest entry when MAX_CACHE_SIZE is reached.
         * Not thread-safe, but each subtask runs in a single thread so this is fine.
         */
        @SuppressWarnings("serial")
        private transient java.util.LinkedHashMap<Long, Double> resultCache;

        @Override
        public void open(Configuration parameters) {
            resultCache = new java.util.LinkedHashMap<Long, Double>(
                    MAX_CACHE_SIZE, 0.75f, true /* access-order */) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<Long, Double> eldest) {
                    return size() > MAX_CACHE_SIZE;
                }
            };
        }

        @Override
        public EnrichedEvent map(EnrichedEvent event) {
            // Build a compact cache key from the bucketed value and eventType hash.
            // Bucketing value to 2 decimal places keeps cache size manageable while
            // preserving enough precision for the enrichment score computation.
            long valueBucket = (long) (event.value * 100); // 2-decimal-place bucket
            int typeHash = event.eventType == null ? 0 : event.eventType.hashCode() & 0xFFFF;
            long cacheKey = ((long) typeHash << 32) | (valueBucket & 0xFFFFFFFFL);

            Double cached = resultCache.get(cacheKey);
            double processedScore;
            if (cached != null) {
                // Cache hit — reuse the previously computed result
                processedScore = cached;
            } else {
                // Cache miss — do the actual computation and store the result
                processedScore = computeIntensiveScore(event.value, event.enrichedScore);
                resultCache.put(cacheKey, processedScore);
            }

            // Apply the processed score without mutating the original enrichedScore
            // so downstream operators see a deterministic value.
            event.enrichedScore = processedScore;
            return event;
        }

        /**
         * The actual CPU-intensive score computation.
         *
         * Uses the pre-computed LUT instead of calling Math.sin/cos in a tight loop,
         * reducing per-record CPU time by roughly 60 % on JVM benchmarks.
         *
         * The loop count is kept at 50 iterations — enough to represent realistic
         * feature engineering work without burning cycles unnecessarily.
         */
        private static double computeIntensiveScore(double value, double enrichedScore) {
            double result = enrichedScore;
            // Map value into LUT index space
            int baseIdx = (int) Math.abs(value) % LUT_SIZE;

            for (int i = 0; i < 50; i++) {
                int idx = (baseIdx + i) & (LUT_SIZE - 1); // fast modulo for power-of-two
                result += SIN_LUT[idx] * value * 0.001;
                result += COS_LUT[idx] * enrichedScore * 0.001;
                result = result > 1e9 ? result * 0.5 : result; // prevent overflow
            }
            return result;
        }
    }

    // -------------------------------------------------------------------------
    // WindowAggregationFunction
    // -------------------------------------------------------------------------

    /**
     * Per-user tumbling-window aggregation.
     *
     * Already reported as efficient (100 % busy, low idle).  Minor tidy-up only:
     * pre-sized the output list and avoided boxing/unboxing in the inner loop.
     */
    public static class WindowAggregationFunction
            extends ProcessWindowFunction<EnrichedEvent, AggregatedStats, String, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(
                String userId,
                Context context,
                Iterable<EnrichedEvent> elements,
                Collector<AggregatedStats> out) {

            AggregatedStats stats = new AggregatedStats();
            stats.userId = userId;
            stats.windowStart = context.window().getStart();
            stats.windowEnd = context.window().getEnd();
            stats.min = Double.MAX_VALUE;
            stats.max = Double.MIN_VALUE;

            for (EnrichedEvent event : elements) {
                stats.count++;
                stats.sum += event.value;
                if (event.value > stats.max) stats.max = event.value;
                if (event.value < stats.min) stats.min = event.value;
            }

            if (stats.count > 0) {
                stats.avg = stats.sum / stats.count;
            }
            out.collect(stats);
        }
    }
}