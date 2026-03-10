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
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.configuration.Configuration;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * <p>This job creates multiple vertices with memory-intensive operations and variable load:
 * <ol>
 *   <li>Random data generation with HIGH load rate</li>
 *   <li>Stateful enrichment operator (keyed, maintains per-user history and metrics)</li>
 *   <li>CPU-intensive processing to create realistic backpressure</li>
 *   <li>Windowed aggregation operator</li>
 *   <li>Memory-intensive state accumulation operator</li>
 * </ol>
 *
 * <p>To enable autotuning in Kubernetes, configure FlinkDeployment with:
 * <pre>
 *   job.autoscaler.enabled: "true"
 *   job.autoscaler.memory.tuning.enabled: "true"
 *   job.autoscaler.scaling.enabled: "true"
 * </pre>
 *
 * <p>Load pattern (HIGH_LOAD_RATE events/sec) is intended to demonstrate scale-up behaviour.
 * Reduce HIGH_LOAD_RATE or restart the job at LOW_LOAD_RATE to demonstrate scale-down.
 */
public class MemoryAutotuningJob {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);

    // -----------------------------------------------------------------------
    // Load pattern configuration
    // -----------------------------------------------------------------------
    /** High-load events/sec – intended to trigger scale-up. */
    private static final long HIGH_LOAD_RATE = 100_000L;

    /** Low-load events/sec – intended to trigger scale-down. */
    private static final long LOW_LOAD_RATE = 500L;

    /** Number of distinct user IDs used for keyed state partitioning. */
    private static final int USER_COUNT = 10_000;

    /** Checkpoint interval in milliseconds. */
    private static final long CHECKPOINT_INTERVAL_MS = 60_000L;

    // -----------------------------------------------------------------------
    // Parallelism configuration (tune to your cluster size)
    // -----------------------------------------------------------------------
    private static final int SOURCE_PARALLELISM      = 20;
    private static final int ENRICHMENT_PARALLELISM  = 40;
    private static final int CPU_PARALLELISM         = 40;
    private static final int WINDOW_PARALLELISM      = 20;
    private static final int SINK_PARALLELISM        = 10;

    // -----------------------------------------------------------------------
    // main
    // -----------------------------------------------------------------------
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        // Default parallelism – the autoscaler will adjust per vertex based on load.
        env.setParallelism(SOURCE_PARALLELISM);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD : {} events/sec", HIGH_LOAD_RATE);
        LOG.info("LOW  LOAD : {} events/sec", LOW_LOAD_RATE);
        LOG.info("=================================================================");

        // ------------------------------------------------------------------
        // Source: generate events at HIGH_LOAD_RATE
        // ------------------------------------------------------------------
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new EventGeneratorFunction(),
                Long.MAX_VALUE,                               // unlimited events
                RateLimiterStrategy.perSecond(HIGH_LOAD_RATE),
                TypeInformation.of(Event.class)
        );

        DataStream<Event> events = env
                .fromSource(
                        source,
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, ts) -> event.timestamp),
                        "VariableLoadEventSource"
                )
                .setParallelism(SOURCE_PARALLELISM);

        // ------------------------------------------------------------------
        // Vertex 1: Stateful enrichment – maintains per-user keyed state
        // Uses KeyedProcessFunction so keyed state APIs are available.
        // ------------------------------------------------------------------
        DataStream<EnrichedEvent> enriched = events
                .keyBy(event -> event.userId)
                .process(new StatefulEnrichmentFunction())
                .name("StatefulEnrichment")
                .setParallelism(ENRICHMENT_PARALLELISM);

        // ------------------------------------------------------------------
        // Vertex 2: CPU-intensive processing to create realistic backpressure
        // ------------------------------------------------------------------
        DataStream<EnrichedEvent> processed = enriched
                .map(new CpuIntensiveFunction())
                .name("CpuIntensiveProcessing")
                .setParallelism(CPU_PARALLELISM);

        // ------------------------------------------------------------------
        // Vertex 3: Windowed aggregation – memory-intensive windowing
        // ------------------------------------------------------------------
        DataStream<AggregatedStats> windowed = processed
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new WindowAggregationFunction())
                .name("WindowedAggregation")
                .setParallelism(WINDOW_PARALLELISM);

        // ------------------------------------------------------------------
        // Vertex 4: Memory-intensive state accumulation (sink-like operator)
        // ------------------------------------------------------------------
        windowed
                .keyBy(stats -> stats.userId)
                .process(new MemoryIntensiveAccumulatorFunction())
                .name("MemoryIntensiveAccumulator")
                .setParallelism(SINK_PARALLELISM)
                .print()
                .name("ConsoleSink")
                .setParallelism(1);

        env.execute("MemoryAutotuningJob");
    }

    // =======================================================================
    // Domain model
    // =======================================================================

    /** Raw event emitted by the source. */
    public static class Event implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public String sessionId;
        public double value;
        public long   timestamp;

        public Event() {}

        public Event(String userId, String sessionId, double value, long timestamp) {
            this.userId    = userId;
            this.sessionId = sessionId;
            this.value     = value;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Event{userId='%s', sessionId='%s', value=%.2f, ts=%d}",
                    userId, sessionId, value, timestamp);
        }
    }

    /** Event enriched with per-user statistics. */
    public static class EnrichedEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public String sessionId;
        public double value;
        public long   timestamp;
        public long   eventCount;
        public double runningAverage;

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return String.format(
                    "EnrichedEvent{userId='%s', value=%.2f, count=%d, avg=%.2f}",
                    userId, value, eventCount, runningAverage);
        }
    }

    /** Per-window aggregated statistics. */
    public static class AggregatedStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long   windowStart;
        public long   windowEnd;
        public long   eventCount;
        public double totalValue;
        public double maxValue;
        public double minValue;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return String.format(
                    "AggStats{userId='%s', window=[%d,%d), count=%d, total=%.2f}",
                    userId, windowStart, windowEnd, eventCount, totalValue);
        }
    }

    // =======================================================================
    // Source generator
    // =======================================================================

    /**
     * Generates {@link Event} instances with random userId / value pairs.
     *
     * <p>Implements {@link GeneratorFunction} so it can be used directly inside
     * {@link DataGeneratorSource}.  The index argument is the global sequence
     * number of the record being generated.
     */
    public static class EventGeneratorFunction
            implements GeneratorFunction<Long, Event>, Serializable {

        private static final long serialVersionUID = 1L;

        // Random is not serializable – initialise lazily per task instance.
        private transient Random random;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
        }

        @Override
        public Event map(Long index) {
            String userId    = "user_" + (random.nextInt(USER_COUNT));
            String sessionId = "session_" + (random.nextInt(USER_COUNT * 10));
            double value     = random.nextDouble() * 1_000.0;
            long   timestamp = System.currentTimeMillis();
            return new Event(userId, sessionId, value, timestamp);
        }
    }

    // =======================================================================
    // Operators
    // =======================================================================

    /**
     * Keyed process function that maintains per-user event count and running sum
     * in Flink managed keyed state.
     *
     * <p>Using {@link KeyedProcessFunction} (rather than {@link RichMapFunction})
     * gives access to the full keyed-state API and timers.
     */
    public static class StatefulEnrichmentFunction
            extends KeyedProcessFunction<String, Event, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        private transient ValueState<Long>   eventCountState;
        private transient ValueState<Double> runningSumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("eventCount", Long.class));

            runningSumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("runningSum", Double.class));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<EnrichedEvent> out)
                throws Exception {

            // Read current state (defaulting nulls to zero)
            long   count = eventCountState.value() != null ? eventCountState.value() : 0L;
            double sum   = runningSumState.value()  != null ? runningSumState.value()  : 0.0;

            // Update state
            count += 1;
            sum   += event.value;
            eventCountState.update(count);
            runningSumState.update(sum);

            // Emit enriched event
            EnrichedEvent enriched = new EnrichedEvent();
            enriched.userId         = event.userId;
            enriched.sessionId      = event.sessionId;
            enriched.value          = event.value;
            enriched.timestamp      = event.timestamp;
            enriched.eventCount     = count;
            enriched.runningAverage = sum / count;

            out.collect(enriched);
        }
    }

    /**
     * CPU-intensive map function that performs dummy computation to simulate
     * realistic processing load and create measurable backpressure.
     */
    public static class CpuIntensiveFunction
            extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        /** Number of dummy hash iterations per event. */
        private static final int HASH_ITERATIONS = 500;

        @Override
        public EnrichedEvent map(EnrichedEvent event) {
            // Simulate CPU work: repeated hashing
            int hash = event.userId.hashCode();
            for (int i = 0; i < HASH_ITERATIONS; i++) {
                hash = Integer.hashCode(hash ^ (int) event.value ^ i);
            }
            // Touch the value slightly so the compiler cannot optimise the loop away
            event.value += (hash & 0x1) * 1e-15;
            return event;
        }
    }

    /**
     * Window function that aggregates all events in a tumbling window into a
     * single {@link AggregatedStats} record per user per window.
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
            stats.userId      = userId;
            stats.windowStart = context.window().getStart();
            stats.windowEnd   = context.window().getEnd();
            stats.minValue    = Double.MAX_VALUE;
            stats.maxValue    = Double.MIN_VALUE;

            for (EnrichedEvent e : elements) {
                stats.eventCount++;
                stats.totalValue += e.value;
                if (e.value > stats.maxValue) stats.maxValue = e.value;
                if (e.value < stats.minValue) stats.minValue = e.value;
            }

            if (stats.eventCount == 0) {
                // Guard: emit nothing for an empty window (should not happen)
                return;
            }

            out.collect(stats);
        }
    }

    /**
     * Keyed process function that accumulates a capped history of
     * {@link AggregatedStats} windows in {@link MapState} – deliberately
     * memory-intensive to exercise Flink's managed memory and autotuning.
     *
     * <p>State is bounded to {@code MAX_HISTORY_ENTRIES} to avoid unbounded growth.
     */
    public static class MemoryIntensiveAccumulatorFunction
            extends KeyedProcessFunction<String, AggregatedStats, String> {

        private static final long serialVersionUID = 1L;

        /** Maximum number of window entries retained per user key. */
        private static final int MAX_HISTORY_ENTRIES = 100;

        // MapState: windowEnd → AggregatedStats
        private transient MapState<Long, AggregatedStats> historyState;

        // ValueState tracking a simple counter for ordered eviction
        private transient ValueState<Long> oldestWindowState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Long, AggregatedStats> historyDesc =
                    new MapStateDescriptor<>(
                            "windowHistory",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(AggregatedStats.class));
            historyState = getRuntimeContext().getMapState(historyDesc);

            oldestWindowState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("oldestWindow", Long.class));
        }

        @Override
        public void processElement(
                AggregatedStats stats,
                Context ctx,
                Collector<String> out) throws Exception {

            historyState.put(stats.windowEnd, stats);

            // Evict the oldest entry when history exceeds the cap
            long size = sizeOf(historyState);
            if (size > MAX_HISTORY_ENTRIES) {
                Long oldest = oldestWindowState.value();
                if (oldest != null) {
                    historyState.remove(oldest);
                }
            }
            // Track the current window as a candidate for future eviction
            Long prev = oldestWindowState.value();
            if (prev == null || stats.windowEnd < prev) {
                oldestWindowState.update(stats.windowEnd);
            }

            // Emit a summary string
            out.collect(String.format(
                    "User %s | windows retained: %d | latest window total: %.2f",
                    stats.userId, size, stats.totalValue));
        }

        /** Counts entries in the MapState without materialising them all into a list. */
        private static long sizeOf(MapState<Long, AggregatedStats> state) throws Exception {
            long count = 0;
            for (Map.Entry<Long, AggregatedStats> ignored : state.entries()) {
                count++;
            }
            return count;
        }
    }
}