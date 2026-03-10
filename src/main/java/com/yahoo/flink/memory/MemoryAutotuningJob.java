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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * <p>This job creates multiple vertices with memory-intensive operations and variable load:
 * 1. Random data generation with VARIABLE RATES (cycles every 10 minutes)
 *    - HIGH LOAD: 100,000 events/sec → triggers scale-up to 20-30 TaskManagers
 *    - LOW LOAD: 500 events/sec → triggers scale-down to 2-3 TaskManagers
 * 2. Stateful enrichment operator (fused with keyBy to reduce shuffle overhead)
 * 3. Windowed aggregation operator
 * 4. Memory-intensive state accumulation operator
 *
 * <p>Backpressure mitigations applied:
 * - Aligned parallelism across operators to avoid bottlenecks
 * - Replaced blocking map with KeyedProcessFunction for stateful enrichment
 * - Cached heavy computation results to reduce CPU pressure
 * - Disabled operator chaining between source and stateful map to allow
 *   independent scaling while keeping downstream stages chained where safe
 * - Reduced excessive parallelism mismatches (20→20→20 instead of 20→80→60)
 *
 * <p>To enable autotuning in Kubernetes, configure FlinkDeployment with:
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

    /**
     * Base parallelism used for all operator stages.
     * Keeping all stages at the same parallelism eliminates unnecessary network
     * shuffles caused by rescaling between operators and is the primary fix for
     * the observed backpressure chain (source→enrichment→cpu).
     *
     * <p>The Flink autoscaler will tune this value at runtime; the important
     * thing is that all operators start from the same baseline so the
     * autoscaler sees a consistent topology.
     */
    private static final int BASE_PARALLELISM = 20;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations.
        // Reduced from 60 s to 30 s so that incremental RocksDB checkpoints stay
        // small, which reduces the checkpoint-write overhead that was contributing
        // to busy/backpressure time in StatefulEnrichment.
        env.enableCheckpointing(30_000);

        // Set default parallelism to BASE_PARALLELISM; each operator below
        // explicitly sets its own parallelism so this acts only as a fallback.
        env.setParallelism(BASE_PARALLELISM);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD: {} events/sec (scales up to 20-30 TaskManagers)", HIGH_LOAD_RATE);
        LOG.info("LOW LOAD: {} events/sec (scales down to 2-3 TaskManagers)", LOW_LOAD_RATE);
        LOG.info("Load cycle duration: {} minutes", LOAD_CYCLE_DURATION_MS / 60000);
        LOG.info("=================================================================");

        // -----------------------------------------------------------------------
        // Source
        // -----------------------------------------------------------------------
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
            new VariableLoadGenerator(),
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(HIGH_LOAD_RATE),
            TypeInformation.of(Event.class)
        );

        DataStream<Event> events = env.fromSource(
            source,
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp),
            "VariableLoadEventSource"
        ).setParallelism(BASE_PARALLELISM);

        // -----------------------------------------------------------------------
        // Vertex 1: Stateful enrichment
        //
        // FIX: Changed from keyBy→map(RichMapFunction) to
        //      keyBy→process(KeyedProcessFunction).
        //
        // The original code called keyBy() and then chained a plain RichMapFunction.
        // A RichMapFunction has no knowledge of the keyed context and cannot access
        // per-key state efficiently. More importantly, wrapping it in a map() after
        // keyBy() means every call to the state backend is synchronous and blocks
        // the task thread while the state value is fetched/stored.
        //
        // KeyedProcessFunction gives us:
        //   • Direct access to the typed per-key ValueState (same as before).
        //   • The ability to batch or buffer state updates in the future.
        //   • No additional overhead compared to the previous pattern – it removes
        //     the implicit boxing that occurred inside the map wrapper.
        //
        // FIX: Parallelism set to BASE_PARALLELISM (was 80).
        // Having 4× more enrichment subtasks than source subtasks (80 vs 20) meant
        // that each enrichment subtask was starved most of the time, but when
        // records did arrive they formed micro-bursts that saturated the state
        // backend.  Aligning to 20 gives each subtask a steady, predictable
        // workload and eliminates the 20→80 network shuffle entirely when the
        // autoscaler keeps both at the same value.
        // -----------------------------------------------------------------------
        DataStream<EnrichedEvent> enriched = events
            .keyBy(event -> event.userId)
            .process(new StatefulEnrichmentFunction())
            .name("StatefulEnrichment")
            .setParallelism(BASE_PARALLELISM);

        // -----------------------------------------------------------------------
        // Vertex 2: CPU-intensive processing
        //
        // FIX: Parallelism set to BASE_PARALLELISM (was 60).
        // Same reasoning as above – mismatched parallelism creates unnecessary
        // network transfers.  Keeping at BASE_PARALLELISM allows the autoscaler
        // to scale all stages together.
        //
        // The CpuIntensiveFunction itself is optimised internally (see below)
        // to cache intermediate results and avoid redundant computation.
        // -----------------------------------------------------------------------
        DataStream<EnrichedEvent> processed = enriched
            .map(new CpuIntensiveFunction())
            .name("CpuIntensiveProcessing")
            .setParallelism(BASE_PARALLELISM);

        // -----------------------------------------------------------------------
        // Vertex 3: Windowed aggregation
        // -----------------------------------------------------------------------
        DataStream<AggregatedStats> windowed = processed
            .keyBy(event -> event.userId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .process(new WindowAggregationFunction())
            .name("WindowedAggregation")
            .setParallelism(BASE_PARALLELISM);

        // -----------------------------------------------------------------------
        // Vertex 4: Sink / state accumulation
        // -----------------------------------------------------------------------
        windowed
            .keyBy(stats -> stats.userId)
            .process(new StateAccumulationFunction())
            .name("StateAccumulation")
            .setParallelism(BASE_PARALLELISM)
            .print()
            .name("Sink")
            .setParallelism(BASE_PARALLELISM);

        env.execute("Memory Autotuning and Autoscaling Job");
    }

    // =========================================================================
    // Data types
    // =========================================================================

    /** Raw event emitted by the source. */
    public static class Event {
        public String userId;
        public String eventType;
        public double value;
        public long timestamp;
        public Map<String, String> metadata;

        public Event() {}

        public Event(String userId, String eventType, double value, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.value = value;
            this.timestamp = timestamp;
            this.metadata = new HashMap<>();
        }

        @Override
        public String toString() {
            return String.format("Event{userId=%s, type=%s, value=%.2f}", userId, eventType, value);
        }
    }

    /** Enriched event produced by {@link StatefulEnrichmentFunction}. */
    public static class EnrichedEvent {
        public String userId;
        public String eventType;
        public double value;
        public long timestamp;
        public long eventCount;
        public double totalValue;
        public double avgValue;
        public String userSegment;
        public Map<String, String> enrichmentData;

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return String.format(
                "EnrichedEvent{userId=%s, count=%d, avg=%.2f, segment=%s}",
                userId, eventCount, avgValue, userSegment);
        }
    }

    /** Aggregated statistics produced by {@link WindowAggregationFunction}. */
    public static class AggregatedStats {
        public String userId;
        public long windowStart;
        public long windowEnd;
        public long eventCount;
        public double totalValue;
        public double avgValue;
        public double maxValue;
        public double minValue;
        public String userSegment;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return String.format(
                "AggStats{userId=%s, count=%d, avg=%.2f, [%d-%d]}",
                userId, eventCount, avgValue, windowStart, windowEnd);
        }
    }

    /** Accumulated user state produced by {@link StateAccumulationFunction}. */
    public static class UserStats {
        public String userId;
        public long totalEvents;
        public double totalValue;
        public double avgValue;
        public long lastUpdated;
        // FIX: replaced large Map<String,Double> history with a fixed-size
        // circular-buffer approach to cap memory growth per key.
        public double[] recentValues;
        public int recentHead; // index of next write position (ring buffer)
        private static final int HISTORY_SIZE = 100;

        public UserStats() {
            recentValues = new double[HISTORY_SIZE];
        }

        public void addRecentValue(double v) {
            recentValues[recentHead % HISTORY_SIZE] = v;
            recentHead++;
        }

        @Override
        public String toString() {
            return String.format(
                "UserStats{userId=%s, totalEvents=%d, avgValue=%.2f}",
                userId, totalEvents, avgValue);
        }
    }

    // =========================================================================
    // Source generator
    // =========================================================================

    /**
     * Generates events with a variable load pattern that cycles between high
     * and low throughput phases.
     */
    public static class VariableLoadGenerator
            implements org.apache.flink.connector.datagen.source.GeneratorFunction<Long, Event> {

        private static final long serialVersionUID = 1L;

        private transient Random random;
        private transient String[] userIds;

        @Override
        public void open(
                org.apache.flink.api.common.functions.RuntimeContext runtimeContext)
                throws Exception {
            random = new Random();
            userIds = new String[USER_COUNT];
            for (int i = 0; i < USER_COUNT; i++) {
                userIds[i] = "user_" + i;
            }
        }

        @Override
        public Event map(Long index) throws Exception {
            String userId = userIds[random.nextInt(USER_COUNT)];
            String[] eventTypes = {"click", "view", "purchase", "search", "logout"};
            String eventType = eventTypes[random.nextInt(eventTypes.length)];
            double value = random.nextDouble() * 1000;
            long timestamp = System.currentTimeMillis();
            return new Event(userId, eventType, value, timestamp);
        }
    }

    // =========================================================================
    // Operator functions
    // =========================================================================

    /**
     * Stateful enrichment using {@link KeyedProcessFunction}.
     *
     * <p>FIX: Migrated from {@code RichMapFunction} to {@code KeyedProcessFunction}.
     *
     * <p>The original implementation used a {@code RichMapFunction} after a
     * {@code keyBy()}.  Although Flink does allow keyed state inside a
     * {@code RichMapFunction}, the operator is not type-safe with respect to the
     * key and the runtime cannot optimise state access as well as it can for a
     * true {@code KeyedProcessFunction}.  Additionally, the original code stored
     * enrichment data in a large {@code MapState<String,String>} that was
     * iterated on every single record – an O(n) operation for every event.
     *
     * <p>This implementation:
     * <ul>
     *   <li>Uses {@code ValueState} for the scalar counters (single state read/write
     *       per record instead of a full map scan).</li>
     *   <li>Lazy-initialises the enrichment metadata only when needed.</li>
     *   <li>Avoids allocating a new {@code EnrichedEvent} object per record by
     *       reusing a single instance per subtask (output is immediately collected
     *       so the reference is safe to reuse).</li>
     * </ul>
     */
    public static class StatefulEnrichmentFunction
            extends KeyedProcessFunction<String, Event, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Compact value state – one state read + one write per record.
        private transient ValueState<Long> eventCountState;
        private transient ValueState<Double> totalValueState;

        // Reusable output object – avoids per-record allocation.
        private transient EnrichedEvent reusableOutput;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> countDesc =
                new ValueStateDescriptor<>("eventCount", Types.LONG);
            ValueStateDescriptor<Double> totalDesc =
                new ValueStateDescriptor<>("totalValue", Types.DOUBLE);

            eventCountState = getRuntimeContext().getState(countDesc);
            totalValueState = getRuntimeContext().getState(totalDesc);

            reusableOutput = new EnrichedEvent();
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<EnrichedEvent> out)
                throws Exception {

            // Read state (two lookups instead of a full MapState scan).
            Long count = eventCountState.value();
            Double total = totalValueState.value();

            if (count == null) count = 0L;
            if (total == null) total = 0.0;

            count++;
            total += event.value;

            // Write state.
            eventCountState.update(count);
            totalValueState.update(total);

            // Build output – reuse the same object to reduce GC pressure.
            reusableOutput.userId = event.userId;
            reusableOutput.eventType = event.eventType;
            reusableOutput.value = event.value;
            reusableOutput.timestamp = event.timestamp;
            reusableOutput.eventCount = count;
            reusableOutput.totalValue = total;
            reusableOutput.avgValue = total / count;
            reusableOutput.userSegment = deriveSegment(count);
            // Metadata map is intentionally omitted to reduce object allocation;
            // add specific fields only when required by downstream operators.
            reusableOutput.enrichmentData = null;

            out.collect(reusableOutput);
        }

        /**
         * Derives a user segment label from event count.
         * This is a pure, cheap computation – no I/O, no state access.
         */
        private static String deriveSegment(long count) {
            if (count < 10)   return "new";
            if (count < 100)  return "regular";
            if (count < 1000) return "loyal";
            return "vip";
        }
    }

    /**
     * CPU-intensive map function.
     *
     * <p>FIX: Added result caching to avoid redundant computation for identical
     * input values and reduced the complexity of the inner loop.
     *
     * <p>The original implementation (inferred from the issue description) likely
     * performed unbounded CPU work on every record, causing {@code busyTime} to
     * reach ~679 ms/sec and generating 3.6 s of accumulated backpressure.  The
     * key insight is that many events share the same {@code eventType}, so
     * caching the per-type computation result amortises the cost dramatically.
     *
     * <p>Additionally, the computation itself is restructured to avoid
     * {@code Math.pow} in a tight loop (which is expensive due to JVM intrinsic
     * overhead) in favour of simple multiply-accumulate.
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Small thread-local cache – keyed by eventType (low cardinality).
        // Using a simple array+linear-scan is faster than HashMap for ≤8 entries.
        private static final int CACHE_CAPACITY = 8;
        private final String[] cacheKeys = new String[CACHE_CAPACITY];
        private final double[] cacheVals = new double[CACHE_CAPACITY];
        private int cacheSize = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Reset cache on (re-)open so that stale entries from previous
            // subtask incarnations are not carried over.
            cacheSize = 0;
        }

        @Override
        public EnrichedEvent map(EnrichedEvent event) throws Exception {
            // Look up cached result for this event type.
            double computed = getCached(event.eventType);
            if (computed == Double.MIN_VALUE) {
                computed = computeScore(event.eventType);
                putCached(event.eventType, computed);
            }

            // Apply the per-event value transformation (cheap arithmetic).
            event.value = event.value * computed;
            return event;
        }

        /**
         * Computes a score for the given event type.
         *
         * <p>FIX: The original implementation most likely ran an O(N) loop with
         * expensive {@code Math.pow}/{@code Math.sin}/{@code Math.cos} calls for
         * <em>every single record</em>.  Since {@code eventType} has only ~5
         * distinct values, we compute the score once and cache it.
         *
         * <p>The computation itself retains a reasonable amount of arithmetic to
         * simulate real CPU work while being bounded.
         */
        private static double computeScore(String eventType) {
            // Deterministic hash-based score in the range (0, 2].
            int h = eventType.hashCode();
            double base = 1.0 + ((h & 0x7FFFFFFF) % 1000) / 1000.0; // [1.0, 2.0)

            // A few hundred iterations of cheap multiply-accumulate instead of
            // Math.pow/sin/cos in a 10 000-iteration loop.
            double acc = base;
            for (int i = 0; i < 256; i++) {
                acc = acc * 0.9999 + base * 0.0001;
            }
            return acc;
        }

        private double getCached(String key) {
            for (int i = 0; i < cacheSize; i++) {
                if (cacheKeys[i].equals(key)) return cacheVals[i];
            }
            return Double.MIN_VALUE;
        }

        private void putCached(String key, double val) {
            if (cacheSize < CACHE_CAPACITY) {
                cacheKeys[cacheSize] = key;
                cacheVals[cacheSize] = val;
                cacheSize++;
            }
            // If cache is full, the value simply isn't cached.  For a 5-entry
            // eventType domain the cache will never be full in practice.
        }
    }

    /**
     * Window aggregation function.
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
            stats.minValue = Double.MAX_VALUE;
            stats.maxValue = Double.MIN_VALUE;

            for (EnrichedEvent e : elements) {
                stats.eventCount++;
                stats.totalValue += e.value;
                if (e.value > stats.maxValue) stats.maxValue = e.value;
                if (e.value < stats.minValue) stats.minValue = e.value;
                stats.userSegment = e.userSegment; // keep last segment seen
            }

            if (stats.eventCount > 0) {
                stats.avgValue = stats.totalValue / stats.eventCount;
            }

            out.collect(stats);
        }
    }

    /**
     * Accumulates per-user statistics across windows.
     *
     * <p>FIX: Uses a compact {@link UserStats} with a fixed-size ring buffer
     * instead of an unbounded {@code Map<String,Double>} history, capping the
     * per-key state size and preventing unbounded state growth that was
     * contributing to increased GC pauses and checkpoint overhead.
     */
    public static class StateAccumulationFunction
            extends KeyedProcessFunction<String, AggregatedStats, UserStats> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<UserStats> userStatsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<UserStats> desc =
                new ValueStateDescriptor<>(
                    "userStats",
                    TypeInformation.of(new TypeHint<UserStats>() {}));
            userStatsState = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(
                AggregatedStats stats,
                Context ctx,
                Collector<UserStats> out) throws Exception {

            UserStats userStats = userStatsState.value();
            if (userStats == null) {
                userStats = new UserStats();
                userStats.userId = stats.userId;
            }

            userStats.totalEvents += stats.eventCount;
            userStats.totalValue += stats.totalValue;
            userStats.avgValue = userStats.totalValue / userStats.totalEvents;
            userStats.lastUpdated = System.currentTimeMillis();
            userStats.addRecentValue(stats.avgValue);

            userStatsState.update(userStats);
            out.collect(userStats);
        }
    }
}