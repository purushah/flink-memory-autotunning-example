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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * <p>This job creates multiple vertices with memory-intensive operations and variable load:
 * <ol>
 *   <li>Random data generation with VARIABLE RATES (cycles every 10 minutes)
 *     <ul>
 *       <li>HIGH LOAD: 100,000 events/sec → triggers scale-up to 20-30 TaskManagers</li>
 *       <li>LOW LOAD: 500 events/sec → triggers scale-down to 2-3 TaskManagers</li>
 *     </ul>
 *   </li>
 *   <li>Stateful enrichment operator with configurable parallelism</li>
 *   <li>Windowed aggregation operator</li>
 *   <li>Memory-intensive state accumulation operator</li>
 * </ol>
 *
 * <p>To enable autotuning in Kubernetes, configure FlinkDeployment with:
 * <pre>
 *   job.autoscaler.enabled: true
 *   job.autoscaler.memory.tuning.enabled: true
 *   job.autoscaler.scaling.enabled: true
 * </pre>
 *
 * <p>Note: The DataGeneratorSource uses a fixed rate set at job startup. For true dynamic
 * rate changes, consider using a Kafka source with variable producers, or implement a
 * custom source with internally variable throughput via sleep/burst patterns.
 */
public class MemoryAutotuningJob {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);

    // Load pattern configuration
    private static final long HIGH_LOAD_RATE = 100_000L; // events/sec - triggers scale up
    private static final long LOW_LOAD_RATE = 500L;       // events/sec - triggers scale down
    private static final long LOAD_CYCLE_DURATION_MS = 10 * 60 * 1000L; // 10 minutes per phase

    // State / cardinality configuration
    private static final int USER_COUNT = 10_000; // More users = more state

    // Parallelism configuration — kept moderate; let the autoscaler adjust upward
    private static final int SOURCE_PARALLELISM = 4;
    private static final int ENRICHMENT_PARALLELISM = 8;
    private static final int CPU_PARALLELISM = 8;
    private static final int WINDOW_PARALLELISM = 8;
    private static final int SINK_PARALLELISM = 4;

    // -------------------------------------------------------------------------
    // Data model
    // -------------------------------------------------------------------------

    /**
     * Raw event emitted by the source.
     */
    public static class Event implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public String eventType;
        public double value;
        public long timestamp;

        public Event() {}

        public Event(String userId, String eventType, double value, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Event{userId=%s, type=%s, value=%.2f, ts=%d}",
                    userId, eventType, value, timestamp);
        }
    }

    /**
     * Event enriched with per-user state from the enrichment operator.
     */
    public static class EnrichedEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public String eventType;
        public double value;
        public long timestamp;
        public long eventCount;
        public double runningAverage;
        public Map<String, Long> eventTypeCounts;

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return String.format(
                    "EnrichedEvent{userId=%s, count=%d, avg=%.2f}", userId, eventCount, runningAverage);
        }
    }

    /**
     * Aggregated statistics produced by the windowed operator.
     */
    public static class AggregatedStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long windowStart;
        public long windowEnd;
        public long eventCount;
        public double totalValue;
        public double avgValue;
        public double maxValue;
        public double minValue;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return String.format(
                    "AggregatedStats{userId=%s, window=[%d,%d), count=%d, avg=%.2f}",
                    userId, windowStart, windowEnd, eventCount, avgValue);
        }
    }

    // -------------------------------------------------------------------------
    // Source generator
    // -------------------------------------------------------------------------

    /**
     * Generates {@link Event} records with a built-in variable-rate pattern.
     *
     * <p>Because {@link DataGeneratorSource} fixes the external rate at construction time,
     * variable throughput is achieved here by internally sleeping during low-load phases,
     * effectively producing fewer records per wall-clock second even though the source
     * "slot" rate is set to {@code HIGH_LOAD_RATE}.
     */
    public static class VariableLoadGenerator implements GeneratorFunction<Long, Event>, Serializable {
        private static final long serialVersionUID = 1L;

        // Not serialized — re-created in open() on the task side
        private transient Random random;
        private transient String[] eventTypes;
        private transient long jobStartMs;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
            eventTypes = new String[]{"click", "view", "purchase", "scroll", "hover"};
            jobStartMs = System.currentTimeMillis();
        }

        @Override
        public Event map(Long index) throws Exception {
            long elapsedMs = System.currentTimeMillis() - jobStartMs;
            long phaseMs = elapsedMs % (2 * LOAD_CYCLE_DURATION_MS);
            boolean isHighLoad = phaseMs < LOAD_CYCLE_DURATION_MS;

            if (!isHighLoad) {
                // Throttle by sleeping to approximate LOW_LOAD_RATE
                // ratio = HIGH / LOW → sleep for (ratio-1) slots of time per record
                long ratioMinus1 = (HIGH_LOAD_RATE / LOW_LOAD_RATE) - 1;
                if (ratioMinus1 > 0) {
                    // Cap sleep to avoid stalling checkpoints
                    long sleepMs = Math.min(ratioMinus1, 200L);
                    Thread.sleep(sleepMs);
                }
            }

            String userId = "user-" + (Math.abs(random.nextInt()) % USER_COUNT);
            String eventType = eventTypes[random.nextInt(eventTypes.length)];
            double value = random.nextDouble() * 1000.0;

            return new Event(userId, eventType, value, System.currentTimeMillis());
        }
    }

    // -------------------------------------------------------------------------
    // Operators
    // -------------------------------------------------------------------------

    /**
     * Stateful enrichment operator.
     *
     * <p>Must be used after {@code keyBy} so that Flink state backends are available.
     * Uses {@link RichMapFunction} which has access to {@link org.apache.flink.api.common.functions.RuntimeContext}
     * and keyed state when placed in a keyed stream context.
     *
     * <p><b>Important:</b> keyed state (ValueState / MapState) is only valid inside a keyed
     * operator. Calling {@code keyBy(...).map(new StatefulEnrichmentFunction())} is valid
     * because Flink preserves the keyed context through map operators.
     */
    public static class StatefulEnrichmentFunction
            extends RichMapFunction<Event, EnrichedEvent> {
        private static final long serialVersionUID = 1L;

        // Per-key state
        private transient ValueState<Long> eventCountState;
        private transient ValueState<Double> runningSumState;
        private transient MapState<String, Long> eventTypeCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("eventCount", Long.class));

            runningSumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("runningSum", Double.class));

            eventTypeCountState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "eventTypeCounts",
                            TypeInformation.of(new TypeHint<String>() {}),
                            TypeInformation.of(new TypeHint<Long>() {})));
        }

        @Override
        public EnrichedEvent map(Event event) throws Exception {
            // Read current state (handle null for first record per key)
            long count = eventCountState.value() == null ? 0L : eventCountState.value();
            double sum = runningSumState.value() == null ? 0.0 : runningSumState.value();

            count++;
            sum += event.value;

            // Update state
            eventCountState.update(count);
            runningSumState.update(sum);

            Long typeCount = eventTypeCountState.get(event.eventType);
            eventTypeCountState.put(event.eventType, typeCount == null ? 1L : typeCount + 1L);

            // Build enriched event
            EnrichedEvent enriched = new EnrichedEvent();
            enriched.userId = event.userId;
            enriched.eventType = event.eventType;
            enriched.value = event.value;
            enriched.timestamp = event.timestamp;
            enriched.eventCount = count;
            enriched.runningAverage = sum / count;

            // Snapshot of event-type distribution (shallow copy for safety)
            enriched.eventTypeCounts = new HashMap<>();
            eventTypeCountState.entries().forEach(
                    e -> enriched.eventTypeCounts.put(e.getKey(), e.getValue()));

            return enriched;
        }
    }

    /**
     * CPU-intensive transformation that simulates non-trivial processing work,
     * creating measurable backpressure so the autoscaler has something to react to.
     */
    public static class CpuIntensiveFunction
            extends RichMapFunction<EnrichedEvent, EnrichedEvent> {
        private static final long serialVersionUID = 1L;

        // Instance-level Random is safe: each parallel instance has its own object
        private transient Random random;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
        }

        @Override
        public EnrichedEvent map(EnrichedEvent event) throws Exception {
            // Simulate CPU work: compute a small hash chain
            long hash = event.userId.hashCode();
            int iterations = 500 + random.nextInt(500); // 500–999 iterations
            for (int i = 0; i < iterations; i++) {
                hash = Long.hashCode(hash * 6364136223846793005L + 1442695040888963407L);
            }
            // Prevent dead-code elimination
            if (hash == 0) {
                event.value += 0.000001;
            }
            return event;
        }
    }

    /**
     * Windowed aggregation function that computes per-user statistics over a tumbling window.
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

            long count = 0;
            double total = 0.0;
            double max = Double.NEGATIVE_INFINITY;
            double min = Double.POSITIVE_INFINITY;

            for (EnrichedEvent e : elements) {
                count++;
                total += e.value;
                if (e.value > max) max = e.value;
                if (e.value < min) min = e.value;
            }

            if (count == 0) {
                return; // Safety guard — should not happen with Flink windowing
            }

            AggregatedStats stats = new AggregatedStats();
            stats.userId = userId;
            stats.windowStart = context.window().getStart();
            stats.windowEnd = context.window().getEnd();
            stats.eventCount = count;
            stats.totalValue = total;
            stats.avgValue = total / count;
            stats.maxValue = max;
            stats.minValue = min;

            out.collect(stats);
        }
    }

    /**
     * Memory-intensive sink operator that accumulates recent stats in heap state,
     * giving the memory autotuner something to measure and respond to.
     */
    public static class MemoryIntensiveSinkFunction
            extends KeyedProcessFunction<String, AggregatedStats, String> {
        private static final long serialVersionUID = 1L;

        private static final int MAX_HISTORY = 100; // cap per key to avoid unbounded growth

        private transient ValueState<List<AggregatedStats>> historyState;

        @Override
        public void open(Configuration parameters) throws Exception {
            historyState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "statsHistory",
                            TypeInformation.of(new TypeHint<List<AggregatedStats>>() {})));
        }

        @Override
        public void processElement(
                AggregatedStats stats,
                Context ctx,
                Collector<String> out) throws Exception {

            List<AggregatedStats> history = historyState.value();
            if (history == null) {
                history = new ArrayList<>();
            }

            history.add(stats);

            // Cap history to avoid unbounded state growth per key
            if (history.size() > MAX_HISTORY) {
                history = history.subList(history.size() - MAX_HISTORY, history.size());
            }

            historyState.update(history);

            // Compute summary across retained history
            double avgOfAvgs = history.stream()
                    .mapToDouble(s -> s.avgValue)
                    .average()
                    .orElse(0.0);

            out.collect(String.format(
                    "User %s | history=%d | overallAvg=%.2f | latest=%s",
                    stats.userId, history.size(), avgOfAvgs, stats));
        }
    }

    // -------------------------------------------------------------------------
    // Job wiring
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations (60 s interval)
        env.enableCheckpointing(60_000L);

        // Use a conservative default parallelism; the autoscaler will scale up as needed.
        // Avoid setting this excessively high — it wastes resources before the first scale event.
        env.setParallelism(4);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD: {} events/sec (triggers scale-up)", HIGH_LOAD_RATE);
        LOG.info("LOW LOAD:  {} events/sec (triggers scale-down)", LOW_LOAD_RATE);
        LOG.info("Load cycle duration: {} minutes", LOAD_CYCLE_DURATION_MS / 60_000L);
        LOG.info("User cardinality: {}", USER_COUNT);
        LOG.info("=================================================================");

        // --- Source ---
        // The external rate cap is set to HIGH_LOAD_RATE; the generator internally throttles
        // during low-load phases via sleep so the autoscaler observes real throughput drops.
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new VariableLoadGenerator(),
                Long.MAX_VALUE,                                 // unlimited events
                RateLimiterStrategy.perSecond(HIGH_LOAD_RATE), // outer rate cap
                TypeInformation.of(Event.class));

        DataStream<Event> events = env
                .fromSource(
                        source,
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, timestamp) -> event.timestamp),
                        "VariableLoadEventSource")
                .setParallelism(SOURCE_PARALLELISM);

        // --- Vertex 1: Stateful enrichment (keyed) ---
        DataStream<EnrichedEvent> enriched = events
                .keyBy(event -> event.userId)
                .map(new StatefulEnrichmentFunction())
                .name("StatefulEnrichment")
                .uid("stateful-enrichment")      // stable UID for savepoint compatibility
                .setParallelism(ENRICHMENT_PARALLELISM);

        // --- Vertex 2: CPU-intensive processing ---
        DataStream<EnrichedEvent> processed = enriched
                .map(new CpuIntensiveFunction())
                .name("CpuIntensiveProcessing")
                .uid("cpu-intensive-processing")
                .setParallelism(CPU_PARALLELISM);

        // --- Vertex 3: Windowed aggregation (1-minute tumbling window) ---
        DataStream<AggregatedStats> windowed = processed
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new WindowAggregationFunction())
                .name("WindowedAggregation")
                .uid("windowed-aggregation")
                .setParallelism(WINDOW_PARALLELISM);

        // --- Vertex 4: Memory-intensive keyed sink ---
        windowed
                .keyBy(stats -> stats.userId)
                .process(new MemoryIntensiveSinkFunction())
                .name("MemoryIntensiveSink")
                .uid("memory-intensive-sink")
                .setParallelism(SINK_PARALLELISM)
                .print()
                .name("ConsolePrint")
                .setParallelism(1);

        env.execute("MemoryAutotuningJob");
    }
}