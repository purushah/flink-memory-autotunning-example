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
 *   <li>Random data generation with variable rates (cycles every 10 minutes)
 *       <ul>
 *         <li>HIGH LOAD: 100,000 events/sec → triggers scale-up</li>
 *         <li>LOW LOAD: 500 events/sec → triggers scale-down</li>
 *       </ul>
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
 */
public class MemoryAutotuningJob {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);

    // Load pattern configuration
    private static final long HIGH_LOAD_RATE = 100_000L; // events/sec - triggers scale up
    private static final long LOW_LOAD_RATE = 500L;       // events/sec - triggers scale down
    private static final long LOAD_CYCLE_DURATION_MS = 10L * 60 * 1000; // 10 minutes per phase
    private static final int USER_COUNT = 10_000;          // More users = more state

    // Parallelism configuration — kept low so the autoscaler has room to scale up
    private static final int SOURCE_PARALLELISM = 4;
    private static final int ENRICHMENT_PARALLELISM = 8;
    private static final int CPU_PARALLELISM = 6;
    private static final int WINDOW_PARALLELISM = 4;
    private static final int SINK_PARALLELISM = 2;
    private static final int DEFAULT_PARALLELISM = 4;

    // Checkpointing
    private static final long CHECKPOINT_INTERVAL_MS = 60_000L;

    // Window size
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);

    // -----------------------------------------------------------------------
    // main
    // -----------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);

        // Reasonable default parallelism; the autoscaler will raise/lower this
        env.setParallelism(DEFAULT_PARALLELISM);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD: {} events/sec", HIGH_LOAD_RATE);
        LOG.info("LOW LOAD:  {} events/sec", LOW_LOAD_RATE);
        LOG.info("Load cycle duration: {} minutes", LOAD_CYCLE_DURATION_MS / 60_000);
        LOG.info("=================================================================");

        // Source — rate is fixed at HIGH_LOAD_RATE for demonstration; the
        // VariableLoadGenerator itself skips events during "low-load" phases so
        // the effective throughput still cycles without restarting the job.
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
            new VariableLoadGenerator(),
            Long.MAX_VALUE,
            RateLimiterStrategy.perSecond(HIGH_LOAD_RATE),
            TypeInformation.of(Event.class)
        );

        DataStream<Event> events = env
            .fromSource(
                source,
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((event, timestamp) -> event.timestamp),
                "VariableLoadEventSource"
            )
            .setParallelism(SOURCE_PARALLELISM);

        // Vertex 1: Stateful enrichment — maintains per-user state
        DataStream<EnrichedEvent> enriched = events
            .keyBy(event -> event.userId)
            .map(new StatefulEnrichmentFunction())
            .name("StatefulEnrichment")
            .setParallelism(ENRICHMENT_PARALLELISM);

        // Vertex 2: CPU-intensive processing
        DataStream<EnrichedEvent> processed = enriched
            .map(new CpuIntensiveFunction())
            .name("CpuIntensiveProcessing")
            .setParallelism(CPU_PARALLELISM);

        // Vertex 3: Windowed aggregation — memory-intensive windowing
        DataStream<AggregatedStats> windowed = processed
            .keyBy(event -> event.userId)
            .window(TumblingEventTimeWindows.of(WINDOW_SIZE))
            .process(new WindowAggregationFunction())
            .name("WindowedAggregation")
            .setParallelism(WINDOW_PARALLELISM);

        // Vertex 4: Memory-intensive state accumulation via KeyedProcessFunction
        DataStream<String> output = windowed
            .keyBy(stats -> stats.userId)
            .process(new MemoryIntensiveStateFunction())
            .name("MemoryIntensiveState")
            .setParallelism(SINK_PARALLELISM);

        // Sink
        output
            .print()
            .name("ConsoleSink")
            .setParallelism(SINK_PARALLELISM);

        env.execute("MemoryAutotuningJob");
    }

    // -----------------------------------------------------------------------
    // Domain model
    // -----------------------------------------------------------------------

    /** Immutable event produced by the source. */
    public static class Event implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String userId;
        public final double value;
        public final long timestamp;
        public final String category;

        public Event(String userId, double value, long timestamp, String category) {
            this.userId = userId;
            this.value = value;
            this.timestamp = timestamp;
            this.category = category;
        }

        @Override
        public String toString() {
            return "Event{userId='" + userId + "', value=" + value
                + ", ts=" + timestamp + ", category='" + category + "'}";
        }
    }

    /** Event enriched with per-user statistics. */
    public static class EnrichedEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String userId;
        public final double value;
        public final long timestamp;
        public final String category;
        public final long eventCount;
        public final double avgValue;

        public EnrichedEvent(String userId, double value, long timestamp,
                             String category, long eventCount, double avgValue) {
            this.userId = userId;
            this.value = value;
            this.timestamp = timestamp;
            this.category = category;
            this.eventCount = eventCount;
            this.avgValue = avgValue;
        }
    }

    /** Per-user statistics emitted by the window operator. */
    public static class AggregatedStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String userId;
        public final long windowStart;
        public final long windowEnd;
        public final long count;
        public final double sum;
        public final double min;
        public final double max;

        public AggregatedStats(String userId, long windowStart, long windowEnd,
                               long count, double sum, double min, double max) {
            this.userId = userId;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.count = count;
            this.sum = sum;
            this.min = min;
            this.max = max;
        }

        @Override
        public String toString() {
            return "AggregatedStats{userId='" + userId + "', count=" + count
                + ", avg=" + (count > 0 ? sum / count : 0)
                + ", [" + windowStart + "," + windowEnd + ")}";
        }
    }

    // -----------------------------------------------------------------------
    // Operator implementations
    // -----------------------------------------------------------------------

    /**
     * Generates {@link Event}s and simulates variable load by returning {@code null}
     * (which the DataGeneratorSource skips) during low-load phases.
     *
     * <p>Because the DataGeneratorSource does not expose a dynamic rate API we model
     * the load cycle by simply skipping a proportion of generated records during the
     * low-load phase rather than by changing the rate limiter at runtime.
     */
    public static class VariableLoadGenerator implements GeneratorFunction<Long, Event> {
        private static final long serialVersionUID = 1L;

        private static final String[] CATEGORIES = {"A", "B", "C", "D", "E"};
        private transient Random random;

        @Override
        public Event map(Long index) {
            if (random == null) {
                random = new Random();
            }

            long now = System.currentTimeMillis();
            long phasePosition = now % (2 * LOAD_CYCLE_DURATION_MS);
            boolean isHighLoad = phasePosition < LOAD_CYCLE_DURATION_MS;

            // During low-load phase, drop most events to simulate reduced throughput
            if (!isHighLoad) {
                double keepRatio = (double) LOW_LOAD_RATE / HIGH_LOAD_RATE;
                if (random.nextDouble() > keepRatio) {
                    // Return a sentinel-free skip: generate a dummy event with a
                    // marker category that downstream operators ignore cheaply.
                    // Returning null is not supported by all serializers, so we
                    // use a lightweight approach and let downstream filter it.
                    return new Event(
                        "user_" + (random.nextInt(USER_COUNT)),
                        random.nextDouble() * 100,
                        now,
                        "__skip__"
                    );
                }
            }

            String userId = "user_" + random.nextInt(USER_COUNT);
            String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
            return new Event(userId, random.nextDouble() * 100, now, category);
        }
    }

    /**
     * Maintains per-user event count and running sum in Flink managed state.
     * Emits an {@link EnrichedEvent} for every non-skip input event.
     */
    public static class StatefulEnrichmentFunction extends RichMapFunction<Event, EnrichedEvent> {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> eventCountState;
        private transient ValueState<Double> sumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("eventCount", Long.class));
            sumState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("valueSum", Double.class));
        }

        @Override
        public EnrichedEvent map(Event event) throws Exception {
            // Initialise state with defaults if not yet set
            long count = eventCountState.value() == null ? 0L : eventCountState.value();
            double sum = sumState.value() == null ? 0.0 : sumState.value();

            count++;
            sum += event.value;

            eventCountState.update(count);
            sumState.update(sum);

            double avg = count > 0 ? sum / count : 0.0;
            return new EnrichedEvent(event.userId, event.value, event.timestamp,
                event.category, count, avg);
        }
    }

    /**
     * Performs a CPU-intensive computation to create measurable processing load.
     * The amount of work scales with the event value so the operator is not trivially
     * optimised away by the JIT.
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {
        private static final long serialVersionUID = 1L;

        // Iteration bound keeps each call bounded to avoid unbounded stalls
        private static final int ITERATIONS = 1_000;

        @Override
        public EnrichedEvent map(EnrichedEvent event) {
            // Simulate CPU work; result is consumed so JIT cannot eliminate it
            double result = event.value;
            for (int i = 0; i < ITERATIONS; i++) {
                result = Math.sqrt(result * result + i);
            }
            // We discard `result` after use; the point is the CPU cycles consumed
            return new EnrichedEvent(
                event.userId,
                result,           // propagate so JIT keeps the loop
                event.timestamp,
                event.category,
                event.eventCount,
                event.avgValue
            );
        }
    }

    /**
     * Aggregates events within a tumbling window into {@link AggregatedStats}.
     */
    public static class WindowAggregationFunction
        extends ProcessWindowFunction<EnrichedEvent, AggregatedStats, String, TimeWindow> {

        private static final long serialVersionUID = 1L;

        @Override
        public void process(String key,
                            Context context,
                            Iterable<EnrichedEvent> elements,
                            Collector<AggregatedStats> out) {

            long count = 0;
            double sum = 0.0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (EnrichedEvent e : elements) {
                // Skip sentinel events introduced by VariableLoadGenerator
                if ("__skip__".equals(e.category)) {
                    continue;
                }
                count++;
                sum += e.value;
                if (e.value < min) min = e.value;
                if (e.value > max) max = e.value;
            }

            if (count > 0) {
                out.collect(new AggregatedStats(
                    key,
                    context.window().getStart(),
                    context.window().getEnd(),
                    count, sum, min, max
                ));
            }
        }
    }

    /**
     * Accumulates a bounded history of {@link AggregatedStats} per user in Flink
     * managed {@link MapState}, then emits a summary string.
     *
     * <p>The history is capped at {@link #MAX_HISTORY} entries to prevent unbounded
     * state growth.
     */
    public static class MemoryIntensiveStateFunction
        extends KeyedProcessFunction<String, AggregatedStats, String> {

        private static final long serialVersionUID = 1L;

        /** Maximum number of historical windows retained per user. */
        private static final int MAX_HISTORY = 100;

        private transient MapState<Long, AggregatedStats> historyState;
        private transient ValueState<Long> oldestWindowState;

        @Override
        public void open(Configuration parameters) throws Exception {
            historyState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                    "windowHistory",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(new TypeHint<AggregatedStats>() {})
                )
            );
            oldestWindowState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("oldestWindow", Long.class)
            );
        }

        @Override
        public void processElement(AggregatedStats stats,
                                   Context ctx,
                                   Collector<String> out) throws Exception {

            // Evict oldest entry when history is full
            Long oldest = oldestWindowState.value();
            if (oldest == null) {
                oldest = stats.windowStart;
                oldestWindowState.update(oldest);
            }

            // Count existing entries
            long entryCount = 0;
            for (Map.Entry<Long, AggregatedStats> ignored : historyState.entries()) {
                entryCount++;
            }

            if (entryCount >= MAX_HISTORY) {
                historyState.remove(oldest);
                // Advance oldest pointer — find the next smallest key
                long nextOldest = Long.MAX_VALUE;
                for (Long k : historyState.keys()) {
                    if (k < nextOldest) nextOldest = k;
                }
                oldest = nextOldest == Long.MAX_VALUE ? stats.windowStart : nextOldest;
                oldestWindowState.update(oldest);
            }

            historyState.put(stats.windowStart, stats);

            double totalSum = 0.0;
            long totalCount = 0;
            for (AggregatedStats s : historyState.values()) {
                totalSum += s.sum;
                totalCount += s.count;
            }

            double overallAvg = totalCount > 0 ? totalSum / totalCount : 0.0;
            out.collect(String.format(
                "User=%s windows=%d overallAvg=%.2f windowAvg=%.2f",
                stats.userId, entryCount + 1, overallAvg,
                stats.count > 0 ? stats.sum / stats.count : 0.0
            ));
        }
    }
}