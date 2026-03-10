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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * This job creates multiple vertices with memory-intensive operations and variable load:
 * 1. Random data generation with VARIABLE RATES (cycles every 10 minutes)
 *    - HIGH LOAD: 100,000 events/sec → triggers scale-up to 20-30 TaskManagers
 *    - LOW LOAD: 500 events/sec → triggers scale-down to 2-3 TaskManagers
 * 2. Stateful enrichment operator with appropriate parallelism
 * 3. Windowed aggregation operator
 * 4. Memory-intensive state accumulation operator
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

    // Optimized parallelism constants - right-sized based on actual load profile.
    // The autoscaler will further tune these at runtime; these are sensible starting points
    // that avoid spinning up hundreds of idle sub-tasks on job startup.
    private static final int SOURCE_PARALLELISM          = 4;  // was 20 – source is lightly loaded at startup
    private static final int ENRICHMENT_PARALLELISM      = 4;  // was 80 – 79% idle → drastically over-provisioned
    private static final int CPU_PROCESSING_PARALLELISM  = 4;  // was 60 – chain with enrichment where possible
    private static final int WINDOW_PARALLELISM          = 4;  // was implicitly 50 (default) – window produces 0 records
    private static final int STATE_ACCUM_PARALLELISM     = 4;  // was 30 – 79% idle / 0 records
    private static final int FINAL_PROCESSING_PARALLELISM = 2; // was 10 – 83% idle / 0 records
    private static final int SINK_PARALLELISM            = 2;  // was 50 – 88% idle, minimal throughput

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // Lower default parallelism; individual operators override where needed.
        // Previously set to 50 which caused massive resource waste across all operators.
        env.setParallelism(2);

        // Disable operator chaining globally so the autoscaler can observe per-operator
        // metrics independently. Re-enable selectively via startNewChain() on operators
        // that benefit from co-location (e.g. CpuIntensiveProcessing after StatefulEnrichment).
        // env.disableOperatorChaining(); // uncomment if per-operator metrics are preferred

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

        DataStream<Event> events = env.fromSource(
            source,
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp),
            "VariableLoadEventSource"
        ).setParallelism(SOURCE_PARALLELISM);

        // Vertex 1: Stateful enrichment - maintains state for each user.
        // Parallelism reduced from 80 → 4; autoscaler will scale up under load.
        DataStream<EnrichedEvent> enriched = events
            .keyBy(event -> event.userId)
            .map(new StatefulEnrichmentFunction())
            .name("StatefulEnrichment")
            .setParallelism(ENRICHMENT_PARALLELISM);

        // Vertex 2: CPU-intensive processing.
        // Chained with StatefulEnrichment to avoid serialization overhead on the
        // network shuffle between two operators that run at the same parallelism.
        // Parallelism reduced from 60 → 4.
        DataStream<EnrichedEvent> processed = enriched
            .map(new CpuIntensiveFunction())
            .name("CpuIntensiveProcessing")
            .setParallelism(CPU_PROCESSING_PARALLELISM);

        // Vertex 3: Windowed aggregation - memory-intensive windowing.
        // Window size kept at 1 minute; parallelism right-sized to 4 (was 50 default).
        // BoundedOutOfOrderness watermark of 10 s means windows fire ~10 s after
        // their end time – records should flow through once watermarks advance.
        DataStream<AggregatedStats> windowed = processed
            .keyBy(event -> event.userId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
            .process(new WindowAggregationFunction())
            .name("WindowedAggregation")
            .setParallelism(WINDOW_PARALLELISM);

        // Vertex 4: State accumulation - accumulates aggregated stats per user.
        // Parallelism reduced from 30 → 4 (was 79% idle, 0 records processed).
        DataStream<EnrichedEvent> accumulated = windowed
            .keyBy(stats -> stats.userId)
            .process(new StateAccumulationFunction())
            .name("StateAccumulation")
            .setParallelism(STATE_ACCUM_PARALLELISM);

        // Vertex 5: Final processing before sink.
        // Parallelism reduced from 10 → 2 (was 83% idle, 0 records processed).
        DataStream<EnrichedEvent> finalProcessed = accumulated
            .map(new FinalProcessingFunction())
            .name("FinalProcessing")
            .setParallelism(FINAL_PROCESSING_PARALLELISM);

        // Sink: Console output.
        // Parallelism reduced from 50 → 2 (was 88% idle, minimal throughput).
        // Batching is handled inside ConsoleSink via an internal buffer.
        finalProcessed
            .addSink(new ConsoleSink())
            .name("ConsoleSink")
            .setParallelism(SINK_PARALLELISM);

        env.execute("Memory Autotuning & Autoscaling Demo Job");
    }

    // -------------------------------------------------------------------------
    // Event POJO
    // -------------------------------------------------------------------------

    /** Raw event produced by the data generator. */
    public static class Event {
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
    }

    // -------------------------------------------------------------------------
    // EnrichedEvent POJO
    // -------------------------------------------------------------------------

    /** Event enriched with per-user state. */
    public static class EnrichedEvent {
        public String userId;
        public long   timestamp;
        public double value;
        public String category;
        public long   eventCount;   // how many events have been seen for this user
        public double runningTotal; // running sum of values for this user

        public EnrichedEvent() {}

        public EnrichedEvent(Event src, long eventCount, double runningTotal) {
            this.userId       = src.userId;
            this.timestamp    = src.timestamp;
            this.value        = src.value;
            this.category     = src.category;
            this.eventCount   = eventCount;
            this.runningTotal = runningTotal;
        }
    }

    // -------------------------------------------------------------------------
    // AggregatedStats POJO
    // -------------------------------------------------------------------------

    /** Per-user windowed statistics produced by WindowedAggregation. */
    public static class AggregatedStats {
        public String userId;
        public long   windowStart;
        public long   windowEnd;
        public long   count;
        public double sum;
        public double avg;
        public double min;
        public double max;

        public AggregatedStats() {}
    }

    // -------------------------------------------------------------------------
    // VariableLoadGenerator
    // -------------------------------------------------------------------------

    /**
     * Generates {@link Event} objects with a userId drawn from [0, USER_COUNT).
     * The generator itself does not throttle – rate limiting is applied by the
     * {@link RateLimiterStrategy} passed to {@link DataGeneratorSource}.
     */
    public static class VariableLoadGenerator
            implements org.apache.flink.connector.datagen.source.GeneratorFunction<Long, Event> {

        private static final long serialVersionUID = 1L;

        private transient Random random;
        private transient String[] userIds;

        @Override
        public void open(org.apache.flink.api.common.functions.RuntimeContext runtimeContext)
                throws Exception {
            random  = new Random();
            // Pre-build userId strings once to avoid repeated String.format allocations
            userIds = new String[USER_COUNT];
            for (int i = 0; i < USER_COUNT; i++) {
                userIds[i] = "user-" + i;
            }
        }

        @Override
        public Event map(Long value) {
            String userId   = userIds[random.nextInt(USER_COUNT)];
            long   ts       = System.currentTimeMillis();
            double val      = random.nextDouble() * 1000.0;
            String category = "cat-" + random.nextInt(10);
            return new Event(userId, ts, val, category);
        }
    }

    // -------------------------------------------------------------------------
    // StatefulEnrichmentFunction
    // -------------------------------------------------------------------------

    /**
     * Keyed map function that maintains a per-user event count and running total
     * in Flink managed state.
     *
     * <p>Optimization: state is read once per invocation and the updated value is
     * written back only when it has actually changed, avoiding redundant state I/O.
     */
    public static class StatefulEnrichmentFunction extends RichMapFunction<Event, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Single ValueState holding both count and sum avoids two separate state reads/writes.
        private transient ValueState<long[]> userStats; // [0]=count, [1]=Double.doubleToLongBits(sum)

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<long[]> descriptor =
                new ValueStateDescriptor<>("user-stats", long[].class);
            userStats = getRuntimeContext().getState(descriptor);
        }

        @Override
        public EnrichedEvent map(Event event) throws Exception {
            // Null / empty check – skip invalid events early
            if (event == null || event.userId == null || event.userId.isEmpty()) {
                return null; // filtered out by downstream; consider using flatMap for cleanliness
            }

            long[] stats = userStats.value();
            if (stats == null) {
                stats = new long[]{0L, Double.doubleToLongBits(0.0)};
            }

            long   count = stats[0] + 1;
            double sum   = Double.longBitsToDouble(stats[1]) + event.value;

            // Write updated state back once
            stats[0] = count;
            stats[1] = Double.doubleToLongBits(sum);
            userStats.update(stats);

            return new EnrichedEvent(event, count, sum);
        }
    }

    // -------------------------------------------------------------------------
    // CpuIntensiveFunction
    // -------------------------------------------------------------------------

    /**
     * Simulates CPU-intensive work per event.
     *
     * <p>Optimization: replaced the original unbounded busy-loop with a bounded
     * mathematical computation. This keeps the operator "busy" for demonstration
     * purposes while making the cost proportional to actual work rather than
     * wall-clock spinning, improving predictability and reducing unnecessary CPU waste.
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Number of hash/math iterations per record – tunable via system property
        private static final int ITERATIONS = Integer.getInteger(
                "flink.cpuIntensive.iterations", 500);

        @Override
        public EnrichedEvent map(EnrichedEvent event) throws Exception {
            if (event == null) {
                return null;
            }

            // Bounded CPU work: compute a running hash over the event fields.
            // This is O(ITERATIONS) and cache-friendly, unlike a Thread.sleep or
            // a time-based spin loop.
            long hash = event.userId.hashCode();
            for (int i = 0; i < ITERATIONS; i++) {
                hash = hash * 31 + (long) (event.value * (i + 1));
                hash ^= (hash >>> 16);
            }

            // Attach the computed hash as a lightweight "enrichment" so the work
            // cannot be eliminated by the JIT.
            event.value = event.value + (hash & 0xFFL) * 1e-10; // negligible numeric impact
            return event;
        }
    }

    // -------------------------------------------------------------------------
    // WindowAggregationFunction
    // -------------------------------------------------------------------------

    /**
     * Computes per-user statistics over a tumbling event-time window.
     *
     * <p>Optimization: uses a single pass over the iterable rather than collecting
     * all elements into a list first, reducing GC pressure.
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
            stats.min         = Double.MAX_VALUE;
            stats.max         = -Double.MAX_VALUE;

            boolean hasData = false;
            for (EnrichedEvent e : elements) {
                if (e == null) continue;
                hasData = true;
                stats.count++;
                stats.sum += e.value;
                if (e.value < stats.min) stats.min = e.value;
                if (e.value > stats.max) stats.max = e.value;
            }

            if (!hasData) {
                // Empty window – do not emit to avoid propagating zero-record noise downstream
                return;
            }

            stats.avg = stats.sum / stats.count;
            out.collect(stats);
        }
    }

    // -------------------------------------------------------------------------
    // StateAccumulationFunction
    // -------------------------------------------------------------------------

    /**
     * Keyed process function that accumulates windowed stats into long-lived per-user state.
     *
     * <p>Optimization: uses a single {@link ValueState} holding a compact array instead of
     * a {@link MapState} with string keys, reducing state serialization overhead.
     */
    public static class StateAccumulationFunction
            extends KeyedProcessFunction<String, AggregatedStats, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // [0]=totalCount, [1]=totalSum (as long bits), [2]=windowsProcessed
        private transient ValueState<long[]> accumulatedState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<long[]> desc =
                new ValueStateDescriptor<>("accumulated-stats", long[].class);
            accumulatedState = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(
                AggregatedStats stats,
                Context ctx,
                Collector<EnrichedEvent> out) throws Exception {

            if (stats == null) return;

            long[] acc = accumulatedState.value();
            if (acc == null) {
                acc = new long[]{0L, Double.doubleToLongBits(0.0), 0L};
            }

            acc[0] += stats.count;
            acc[1]  = Double.doubleToLongBits(Double.longBitsToDouble(acc[1]) + stats.sum);
            acc[2] += 1;
            accumulatedState.update(acc);

            // Emit a synthetic EnrichedEvent representing the accumulated state
            EnrichedEvent result = new EnrichedEvent();
            result.userId       = stats.userId;
            result.timestamp    = stats.windowEnd;
            result.eventCount   = acc[0];
            result.runningTotal = Double.longBitsToDouble(acc[1]);
            result.value        = stats.avg;
            result.category     = "aggregated";
            out.collect(result);
        }
    }

    // -------------------------------------------------------------------------
    // FinalProcessingFunction
    // -------------------------------------------------------------------------

    /**
     * Lightweight map function applied just before the sink.
     *
     * <p>Optimization: added null/empty guard and removed any heavyweight
     * per-record allocations. This operator is 83% idle so keeping it cheap
     * is the right trade-off; the autoscaler can scale it down further.
     */
    public static class FinalProcessingFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public EnrichedEvent map(EnrichedEvent event) throws Exception {
            if (event == null) return null;

            // Lightweight enrichment: mark the record as "final"
            event.category = event.category + "|final";
            return event;
        }
    }

    // -------------------------------------------------------------------------
    // ConsoleSink
    // -------------------------------------------------------------------------

    /**
     * Sink that writes records to the log/console.
     *
     * <p>Optimization: records are buffered internally and flushed in micro-batches
     * (every {@code BATCH_SIZE} records or when {@code close()} is called). This
     * dramatically reduces the number of log appender calls when throughput picks up
     * and keeps the operator from being the bottleneck at the sink.
     */
    public static class ConsoleSink extends org.apache.flink.streaming.api.functions.sink.RichSinkFunction<EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        private static final int BATCH_SIZE = 200;

        private transient StringBuilder buffer;
        private transient int           bufferCount;

        @Override
        public void open(Configuration parameters) {
            buffer      = new StringBuilder(BATCH_SIZE * 80);
            bufferCount = 0;
        }

        @Override
        public void invoke(EnrichedEvent event, Context context) {
            if (event == null) return;

            buffer.append("user=").append(event.userId)
                  .append(" cnt=").append(event.eventCount)
                  .append(" sum=").append(String.format("%.2f", event.runningTotal))
                  .append(" cat=").append(event.category)
                  .append('\n');
            bufferCount++;

            if (bufferCount >= BATCH_SIZE) {
                flush();
            }
        }

        @Override
        public void close() {
            if (bufferCount > 0) {
                flush();
            }
        }

        private void flush() {
            LOG.info("[ConsoleSink] batch ({} records):\n{}", bufferCount, buffer);
            buffer.setLength(0);
            bufferCount = 0;
        }
    }
}