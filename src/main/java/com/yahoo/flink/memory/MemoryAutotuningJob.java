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
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * <p>This job creates multiple vertices with memory-intensive operations and variable load:
 *
 * <ol>
 *   <li>Random data generation cycling between HIGH and LOW rates every {@code LOAD_CYCLE_DURATION_MS}
 *       <ul>
 *         <li>HIGH LOAD: {@value #HIGH_LOAD_RATE} events/sec → triggers scale-up
 *         <li>LOW LOAD: {@value #LOW_LOAD_RATE} events/sec → triggers scale-down
 *       </ul>
 *   <li>Stateful enrichment operator (keyed, maintains per-user state)
 *   <li>Windowed aggregation operator (TumblingEventTimeWindows of 1 minute)
 *   <li>Memory-intensive state accumulation operator
 * </ol>
 *
 * <p>To enable autotuning in Kubernetes, configure FlinkDeployment with:
 *
 * <pre>
 *   job.autoscaler.enabled: "true"
 *   job.autoscaler.memory.tuning.enabled: "true"
 *   job.autoscaler.scaling.enabled: "true"
 * </pre>
 */
public class MemoryAutotuningJob {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);

    // Load pattern configuration
    static final long HIGH_LOAD_RATE = 100_000L; // events/sec - triggers scale up
    static final long LOW_LOAD_RATE = 500L;       // events/sec - triggers scale down
    static final long LOAD_CYCLE_DURATION_MS = 10L * 60 * 1_000; // 10 minutes per phase

    /** Number of distinct users – directly controls the size of keyed state. */
    static final int USER_COUNT = 10_000;

    // Parallelism constants – kept in one place so they are easy to tune.
    private static final int SOURCE_PARALLELISM      = 4;
    private static final int ENRICHMENT_PARALLELISM  = 8;
    private static final int CPU_PARALLELISM         = 8;
    private static final int WINDOW_PARALLELISM      = 8;
    private static final int SINK_PARALLELISM        = 4;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations (60 s interval).
        env.enableCheckpointing(60_000L);

        // Default parallelism; individual operators override as needed.
        // Keep this reasonable – the autoscaler will scale up from here.
        env.setParallelism(4);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD : {} events/sec", HIGH_LOAD_RATE);
        LOG.info("LOW  LOAD : {} events/sec", LOW_LOAD_RATE);
        LOG.info("Cycle duration: {} minutes", LOAD_CYCLE_DURATION_MS / 60_000);
        LOG.info("=================================================================");

        // Source: generate events at HIGH_LOAD_RATE; the generator itself throttles
        // internally to simulate load cycles so the DataGen source rate cap is the ceiling.
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new VariableLoadGenerator(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(HIGH_LOAD_RATE),
                TypeInformation.of(Event.class));

        DataStream<Event> events = env
                .fromSource(
                        source,
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, ts) -> event.timestamp),
                        "VariableLoadEventSource")
                .setParallelism(SOURCE_PARALLELISM);

        // Vertex 1: Stateful enrichment – maintains per-user ValueState + MapState.
        DataStream<EnrichedEvent> enriched = events
                .keyBy(event -> event.userId)
                .process(new StatefulEnrichmentFunction())
                .name("StatefulEnrichment")
                .uid("stateful-enrichment")
                .setParallelism(ENRICHMENT_PARALLELISM);

        // Vertex 2: CPU-intensive processing to exercise back-pressure handling.
        DataStream<EnrichedEvent> processed = enriched
                .map(new CpuIntensiveFunction())
                .name("CpuIntensiveProcessing")
                .uid("cpu-intensive-processing")
                .setParallelism(CPU_PARALLELISM);

        // Vertex 3: Windowed aggregation – 1-minute tumbling event-time windows.
        DataStream<AggregatedStats> windowed = processed
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new WindowAggregationFunction())
                .name("WindowedAggregation")
                .uid("windowed-aggregation")
                .setParallelism(WINDOW_PARALLELISM);

        // Vertex 4: Memory-intensive state accumulation (keyed on userId).
        windowed
                .keyBy(stats -> stats.userId)
                .process(new MemoryIntensiveStateFunction())
                .name("MemoryIntensiveState")
                .uid("memory-intensive-state")
                .setParallelism(SINK_PARALLELISM)
                // Discard results – replace with a real sink in production.
                .print()
                .name("Sink")
                .uid("sink")
                .setParallelism(SINK_PARALLELISM);

        env.execute("MemoryAutotuningJob");
    }

    // -------------------------------------------------------------------------
    // Domain model
    // -------------------------------------------------------------------------

    /** Raw event produced by the source. */
    public static class Event implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long   timestamp;
        public double value;
        public String category;

        public Event() {}

        public Event(String userId, long timestamp, double value, String category) {
            this.userId    = userId;
            this.timestamp = timestamp;
            this.value     = value;
            this.category  = category;
        }

        @Override
        public String toString() {
            return "Event{userId='" + userId + "', ts=" + timestamp
                    + ", value=" + value + ", category='" + category + "'}";
        }
    }

    /** Event after stateful enrichment. */
    public static class EnrichedEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long   timestamp;
        public double value;
        public String category;
        public long   eventCount;   // number of events seen for this user so far
        public double runningAvg;   // running average value for this user

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return "EnrichedEvent{userId='" + userId + "', count=" + eventCount
                    + ", avg=" + runningAvg + '}';
        }
    }

    /** Aggregated window result. */
    public static class AggregatedStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long   windowStart;
        public long   windowEnd;
        public long   count;
        public double sum;
        public double min;
        public double max;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return "AggregatedStats{userId='" + userId + "', count=" + count
                    + ", sum=" + sum + ", min=" + min + ", max=" + max + '}';
        }
    }

    // -------------------------------------------------------------------------
    // Source generator
    // -------------------------------------------------------------------------

    /**
     * Generates {@link Event}s while internally simulating high/low load cycles.
     *
     * <p>During a LOW-load phase the generator simply sleeps between records so
     * that – together with the DataGen source's rate limiter ceiling – the
     * effective throughput matches {@link #LOW_LOAD_RATE}.
     */
    public static class VariableLoadGenerator implements GeneratorFunction<Long, Event> {

        private static final long serialVersionUID = 1L;

        private transient Random  random;
        private transient long    jobStartMs;

        /** Extra sleep (ms) added per record during low-load phases; 0 during high load. */
        private transient long sleepPerRecordMs;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            random     = new Random();
            jobStartMs = System.currentTimeMillis();
            updateLoadPhase();
        }

        @Override
        public Event map(Long index) throws Exception {
            updateLoadPhase();

            if (sleepPerRecordMs > 0) {
                Thread.sleep(sleepPerRecordMs);
            }

            String userId   = "user-" + (random.nextInt(USER_COUNT));
            String category = pickCategory(random);
            double value    = random.nextDouble() * 1_000.0;
            long   ts       = System.currentTimeMillis();

            return new Event(userId, ts, value, category);
        }

        // ------------------------------------------------------------------ //

        private void updateLoadPhase() {
            long elapsed  = System.currentTimeMillis() - jobStartMs;
            long phase    = (elapsed / LOAD_CYCLE_DURATION_MS) % 2; // 0 = high, 1 = low
            boolean high  = (phase == 0);

            if (high) {
                sleepPerRecordMs = 0;
                LOG.debug("Load phase: HIGH ({} events/sec)", HIGH_LOAD_RATE);
            } else {
                // The DataGen source's rate limiter already caps at HIGH_LOAD_RATE.
                // To achieve LOW_LOAD_RATE we sleep between records.
                // sleep ≈ (1/LOW - 1/HIGH) seconds per record  →  convert to ms.
                double sleepSec  = (1.0 / LOW_LOAD_RATE) - (1.0 / HIGH_LOAD_RATE);
                sleepPerRecordMs = Math.max(0L, (long) (sleepSec * 1_000));
                LOG.debug("Load phase: LOW  ({} events/sec, sleep={}ms)",
                        LOW_LOAD_RATE, sleepPerRecordMs);
            }
        }

        private static String pickCategory(Random rng) {
            switch (rng.nextInt(5)) {
                case 0:  return "purchases";
                case 1:  return "views";
                case 2:  return "clicks";
                case 3:  return "searches";
                default: return "other";
            }
        }
    }

    // -------------------------------------------------------------------------
    // Operators
    // -------------------------------------------------------------------------

    /**
     * Stateful enrichment using keyed state.
     *
     * <p>Maintains:
     * <ul>
     *   <li>{@code eventCountState} – total events seen for this key
     *   <li>{@code runningSumState} – running value sum for moving-average computation
     *   <li>{@code categoryCountState} – per-category event counts (MapState)
     * </ul>
     */
    public static class StatefulEnrichmentFunction
            extends KeyedProcessFunction<String, Event, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<Long>              eventCountState;
        private transient ValueState<Double>            runningSumState;
        private transient MapState<String, Long>        categoryCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("eventCount", Long.class));

            runningSumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("runningSum", Double.class));

            categoryCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("categoryCount",
                            TypeInformation.of(String.class),
                            TypeInformation.of(Long.class)));
        }

        @Override
        public void processElement(Event event,
                                   Context ctx,
                                   Collector<EnrichedEvent> out) throws Exception {

            // Read state (null-safe defaults).
            long   count = eventCountState.value() == null ? 0L   : eventCountState.value();
            double sum   = runningSumState.value()  == null ? 0.0  : runningSumState.value();

            count += 1;
            sum   += event.value;

            eventCountState.update(count);
            runningSumState.update(sum);

            // Update per-category count.
            Long catCount = categoryCountState.get(event.category);
            categoryCountState.put(event.category, catCount == null ? 1L : catCount + 1L);

            EnrichedEvent enriched = new EnrichedEvent();
            enriched.userId     = event.userId;
            enriched.timestamp  = event.timestamp;
            enriched.value      = event.value;
            enriched.category   = event.category;
            enriched.eventCount = count;
            enriched.runningAvg = sum / count;

            out.collect(enriched);
        }
    }

    /**
     * CPU-intensive map function that performs a configurable amount of work per record
     * to simulate compute pressure and exercise back-pressure propagation.
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        /** Number of hash iterations per record – tune to adjust CPU load. */
        private static final int HASH_ITERATIONS = 500;

        @Override
        public EnrichedEvent map(EnrichedEvent event) {
            // Simulate CPU work: repeated hashing.
            int hash = event.userId.hashCode();
            for (int i = 0; i < HASH_ITERATIONS; i++) {
                hash = Integer.hashCode(hash ^ (int) event.value ^ i);
            }
            // Use result to prevent JIT from eliminating the loop.
            event.value += (hash & 0xFF) * 1e-9;
            return event;
        }
    }

    /**
     * Window function that computes count, sum, min, and max per user per window.
     */
    public static class WindowAggregationFunction
            extends ProcessWindowFunction<EnrichedEvent, AggregatedStats, String, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(String userId,
                            Context context,
                            Iterable<EnrichedEvent> elements,
                            Collector<AggregatedStats> out) {

            long   count = 0L;
            double sum   = 0.0;
            double min   = Double.MAX_VALUE;
            double max   = Double.MIN_VALUE;

            for (EnrichedEvent e : elements) {
                count++;
                sum += e.value;
                if (e.value < min) min = e.value;
                if (e.value > max) max = e.value;
            }

            AggregatedStats stats = new AggregatedStats();
            stats.userId      = userId;
            stats.windowStart = context.window().getStart();
            stats.windowEnd   = context.window().getEnd();
            stats.count       = count;
            stats.sum         = sum;
            stats.min         = (count > 0) ? min : 0.0;
            stats.max         = (count > 0) ? max : 0.0;

            out.collect(stats);
        }
    }

    /**
     * Accumulates historical window results in a {@link MapState} to create
     * realistic memory pressure that the autotuner can observe and react to.
     *
     * <p>State is bounded per key: only the most recent {@value #MAX_HISTORY} windows
     * are retained to avoid unbounded growth.
     */
    public static class MemoryIntensiveStateFunction
            extends KeyedProcessFunction<String, AggregatedStats, String> {

        private static final long serialVersionUID = 1L;

        /** Maximum number of historical windows kept per user key. */
        private static final int MAX_HISTORY = 100;

        private transient MapState<Long, AggregatedStats> historyState;
        private transient ValueState<Long>                oldestWindowState;

        @Override
        public void open(Configuration parameters) throws Exception {
            historyState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "windowHistory",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(AggregatedStats.class)));

            oldestWindowState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("oldestWindow", Long.class));
        }

        @Override
        public void processElement(AggregatedStats stats,
                                   Context ctx,
                                   Collector<String> out) throws Exception {

            historyState.put(stats.windowStart, stats);

            // Evict oldest entry when history exceeds the cap.
            Long oldest = oldestWindowState.value();
            if (oldest == null) {
                oldest = stats.windowStart;
                oldestWindowState.update(oldest);
            }

            // Count entries (MapState does not expose size directly).
            long count = 0;
            for (Map.Entry<Long, AggregatedStats> ignored : historyState.entries()) {
                count++;
            }

            if (count > MAX_HISTORY) {
                historyState.remove(oldest);
                // Advance oldest pointer – find the next smallest key.
                long nextOldest = Long.MAX_VALUE;
                for (Long key : historyState.keys()) {
                    if (key < nextOldest) nextOldest = key;
                }
                oldestWindowState.update(nextOldest == Long.MAX_VALUE ? stats.windowStart : nextOldest);
            }

            out.collect("User " + stats.userId + " has " + count + " window(s) in history; "
                    + "latest window sum=" + stats.sum);
        }
    }
}