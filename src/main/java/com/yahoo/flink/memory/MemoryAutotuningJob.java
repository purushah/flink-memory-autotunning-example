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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
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
 *   <li>Random data generation with variable rates (cycles every {@code LOAD_CYCLE_DURATION_MS} ms)
 *       <ul>
 *         <li>HIGH LOAD: {@code HIGH_LOAD_RATE} events/sec → triggers scale-up</li>
 *         <li>LOW LOAD: {@code LOW_LOAD_RATE} events/sec → triggers scale-down</li>
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
    private static final long HIGH_LOAD_RATE = 100_000; // events/sec - triggers scale up
    private static final long LOW_LOAD_RATE = 500;      // events/sec - triggers scale down
    private static final long LOAD_CYCLE_DURATION_MS = 10 * 60 * 1000L; // 10 minutes per phase
    private static final int USER_COUNT = 10_000; // More users = more state

    // Parallelism settings - kept reasonable to allow autoscaler room to scale up/down.
    // The autoscaler will tune these at runtime; initial values should not be excessively large.
    private static final int DEFAULT_PARALLELISM = 4;
    private static final int SOURCE_PARALLELISM = 4;
    private static final int ENRICHMENT_PARALLELISM = 4;
    private static final int CPU_PARALLELISM = 4;
    private static final int SINK_PARALLELISM = 2;

    // State TTL to prevent unbounded state growth
    private static final Duration STATE_TTL = Duration.ofHours(1);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations
        env.enableCheckpointing(60_000); // Checkpoint every 60 seconds

        // Set a conservative default parallelism; the autoscaler will adjust at runtime.
        env.setParallelism(DEFAULT_PARALLELISM);

        LOG.info("=================================================================");
        LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
        LOG.info("HIGH LOAD: {} events/sec (autoscaler scales up)", HIGH_LOAD_RATE);
        LOG.info("LOW LOAD:  {} events/sec (autoscaler scales down)", LOW_LOAD_RATE);
        LOG.info("Load cycle duration: {} minutes", LOAD_CYCLE_DURATION_MS / 60_000);
        LOG.info("=================================================================");

        // The DataGeneratorSource uses VariableLoadGenerator which internally alternates
        // between HIGH and LOW load rates based on wall-clock time.  The outer
        // RateLimiterStrategy is set to HIGH_LOAD_RATE so Flink never throttles below the
        // generator's own pacing; actual throughput is controlled inside the generator.
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
        ).setParallelism(SOURCE_PARALLELISM);

        // Vertex 1: Stateful enrichment - maintains per-user state with TTL
        DataStream<EnrichedEvent> enriched = events
                .keyBy(event -> event.userId)
                .map(new StatefulEnrichmentFunction())
                .name("StatefulEnrichment")
                .setParallelism(ENRICHMENT_PARALLELISM);

        // Vertex 2: CPU-intensive processing to create measurable load
        DataStream<EnrichedEvent> processed = enriched
                .map(new CpuIntensiveFunction())
                .name("CpuIntensiveProcessing")
                .setParallelism(CPU_PARALLELISM);

        // Vertex 3: Windowed aggregation - memory-intensive windowing
        DataStream<AggregatedStats> windowed = processed
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new WindowAggregationFunction())
                .name("WindowedAggregation")
                .setParallelism(ENRICHMENT_PARALLELISM);

        // Vertex 4: Memory-intensive state accumulation with TTL-guarded state
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

    // -------------------------------------------------------------------------
    // Domain model
    // -------------------------------------------------------------------------

    /** A raw event emitted by the source generator. */
    public static class Event implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long timestamp;
        public double value;
        public String category;

        public Event() {}

        public Event(String userId, long timestamp, double value, String category) {
            this.userId = userId;
            this.timestamp = timestamp;
            this.value = value;
            this.category = category;
        }

        @Override
        public String toString() {
            return "Event{userId='" + userId + "', timestamp=" + timestamp
                    + ", value=" + value + ", category='" + category + "'}";
        }
    }

    /** An enriched event produced by {@link StatefulEnrichmentFunction}. */
    public static class EnrichedEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long timestamp;
        public double value;
        public String category;
        public long eventCount;      // how many events seen for this user so far
        public double runningAvg;    // running average value for this user

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return "EnrichedEvent{userId='" + userId + "', eventCount=" + eventCount
                    + ", runningAvg=" + runningAvg + '}';
        }
    }

    /** Per-window aggregated statistics produced by {@link WindowAggregationFunction}. */
    public static class AggregatedStats implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long windowStart;
        public long windowEnd;
        public long count;
        public double sum;
        public double min;
        public double max;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return "AggregatedStats{userId='" + userId + "', count=" + count
                    + ", avg=" + (count > 0 ? sum / count : 0)
                    + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd + '}';
        }
    }

    // -------------------------------------------------------------------------
    // Generator
    // -------------------------------------------------------------------------

    /**
     * Generates {@link Event} instances at a variable rate.
     *
     * <p>The generator alternates between HIGH and LOW load phases based on wall-clock time so
     * that the autoscaler can observe real throughput changes and react accordingly.
     * During the LOW phase, the generator introduces a sleep to reduce throughput even though
     * the outer {@link RateLimiterStrategy} is set to the high-load ceiling.
     */
    public static class VariableLoadGenerator implements GeneratorFunction<Long, Event> {
        private static final long serialVersionUID = 1L;

        private final Random random = new Random();

        @Override
        public Event map(Long index) throws Exception {
            long now = System.currentTimeMillis();
            long phaseMs = now % (2 * LOAD_CYCLE_DURATION_MS);
            boolean highLoad = phaseMs < LOAD_CYCLE_DURATION_MS;

            if (!highLoad) {
                // Throttle artificially during the low-load phase.
                // Target LOW_LOAD_RATE events/sec → sleep ~(1000/LOW_LOAD_RATE) ms per event.
                long sleepMs = 1_000L / LOW_LOAD_RATE;
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }

            String userId = "user-" + random.nextInt(USER_COUNT);
            return new Event(
                    userId,
                    System.currentTimeMillis(),
                    random.nextDouble() * 1000,
                    highLoad ? "high" : "low"
            );
        }
    }

    // -------------------------------------------------------------------------
    // Operators
    // -------------------------------------------------------------------------

    /**
     * Maintains per-user event count and running average in Flink keyed state.
     * State has a TTL to prevent unbounded growth.
     */
    public static class StatefulEnrichmentFunction extends RichMapFunction<Event, EnrichedEvent> {
        private static final long serialVersionUID = 1L;

        private transient ValueState<Long> countState;
        private transient ValueState<Double> sumState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(STATE_TTL)
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            ValueStateDescriptor<Long> countDescriptor =
                    new ValueStateDescriptor<>("count", Long.class);
            countDescriptor.enableTimeToLive(ttlConfig);
            countState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Double> sumDescriptor =
                    new ValueStateDescriptor<>("sum", Double.class);
            sumDescriptor.enableTimeToLive(ttlConfig);
            sumState = getRuntimeContext().getState(sumDescriptor);
        }

        @Override
        public EnrichedEvent map(Event event) throws Exception {
            Long count = countState.value();
            Double sum = sumState.value();

            if (count == null) {
                count = 0L;
            }
            if (sum == null) {
                sum = 0.0;
            }

            count += 1;
            sum += event.value;

            countState.update(count);
            sumState.update(sum);

            EnrichedEvent enriched = new EnrichedEvent();
            enriched.userId = event.userId;
            enriched.timestamp = event.timestamp;
            enriched.value = event.value;
            enriched.category = event.category;
            enriched.eventCount = count;
            enriched.runningAvg = sum / count;
            return enriched;
        }
    }

    /**
     * Simulates CPU-intensive processing by performing light mathematical work.
     * The work is kept intentionally modest so it creates measurable CPU load without
     * causing artificial back-pressure that would distort autoscaler metrics.
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public EnrichedEvent map(EnrichedEvent event) throws Exception {
            // Simulate moderate CPU work: a few hundred arithmetic ops per record.
            double accumulator = event.value;
            for (int i = 0; i < 500; i++) {
                accumulator = Math.sqrt(accumulator * i + 1);
            }
            // Prevent dead-code elimination; result intentionally discarded.
            if (Double.isNaN(accumulator)) {
                LOG.warn("Unexpected NaN for userId={}", event.userId);
            }
            return event;
        }
    }

    /**
     * Aggregates events within a tumbling event-time window per user.
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

            for (EnrichedEvent e : elements) {
                stats.count++;
                stats.sum += e.value;
                if (e.value < stats.min) stats.min = e.value;
                if (e.value > stats.max) stats.max = e.value;
            }

            out.collect(stats);
        }
    }

    /**
     * Accumulates windowed statistics in keyed MapState to exercise the state backend.
     * State is bounded by TTL to prevent unbounded memory growth.
     */
    public static class MemoryIntensiveAccumulatorFunction
            extends KeyedProcessFunction<String, AggregatedStats, String> {
        private static final long serialVersionUID = 1L;

        /** Maps windowStart → AggregatedStats for recent windows. */
        private transient MapState<Long, AggregatedStats> windowHistory;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(STATE_TTL)
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            MapStateDescriptor<Long, AggregatedStats> descriptor = new MapStateDescriptor<>(
                    "windowHistory",
                    TypeInformation.of(new TypeHint<Long>() {}),
                    TypeInformation.of(new TypeHint<AggregatedStats>() {})
            );
            descriptor.enableTimeToLive(ttlConfig);
            windowHistory = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(
                AggregatedStats stats,
                Context ctx,
                Collector<String> out) throws Exception {

            windowHistory.put(stats.windowStart, stats);

            // Produce a summary so the operator has observable output.
            long totalCount = 0;
            double totalSum = 0;
            for (AggregatedStats s : windowHistory.values()) {
                totalCount += s.count;
                totalSum += s.sum;
            }

            out.collect(String.format(
                    "user=%s windows=%d totalCount=%d avgValue=%.2f",
                    stats.userId,
                    countEntries(),
                    totalCount,
                    totalCount > 0 ? totalSum / totalCount : 0.0
            ));
        }

        /** Counts entries in the MapState without materialising the full collection. */
        private long countEntries() throws Exception {
            long n = 0;
            for (Map.Entry<Long, AggregatedStats> ignored : windowHistory.entries()) {
                n++;
            }
            return n;
        }
    }
}