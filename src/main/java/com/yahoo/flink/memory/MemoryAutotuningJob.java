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
import java.util.List;
import java.util.Random;

/**
 * Flink job demonstrating memory autotuning and autoscaling with Kubernetes Operator.
 *
 * <p>This job creates multiple vertices with memory-intensive operations and variable load:
 * <ol>
 *   <li>Random data generation at a configurable rate</li>
 *   <li>Stateful enrichment operator using keyed state (ValueState + MapState)</li>
 *   <li>CPU-intensive processing to simulate real workloads</li>
 *   <li>Windowed aggregation operator (TumblingEventTimeWindows)</li>
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
 * <p>Load rates can be controlled via the following environment variables (events/sec):
 * <ul>
 *   <li>{@code SOURCE_RATE_PER_SEC} – default {@value MemoryAutotuningJob#DEFAULT_SOURCE_RATE}</li>
 * </ul>
 */
public class MemoryAutotuningJob {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);

    // ---------- tuneable constants ----------
    /** Default source rate (events / second). Override via env var SOURCE_RATE_PER_SEC. */
    static final long DEFAULT_SOURCE_RATE = 10_000L;

    /** Number of distinct user keys – controls state fan-out. */
    static final int USER_COUNT = 10_000;

    /** Number of attribute entries stored per user in MapState. */
    static final int ATTRIBUTES_PER_USER = 20;

    // ----------------------------------------

    public static void main(String[] args) throws Exception {

        final long sourceRate = parseSourceRate();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing so that state backends are actually exercised.
        env.enableCheckpointing(60_000L); // every 60 s

        // Keep default parallelism low; let the autoscaler drive it up/down.
        env.setParallelism(4);

        LOG.info("=================================================================");
        LOG.info("Starting MemoryAutotuningJob");
        LOG.info("Source rate : {} events/sec", sourceRate);
        LOG.info("User count  : {}", USER_COUNT);
        LOG.info("=================================================================");

        // ── Source ────────────────────────────────────────────────────────────
        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new EventGeneratorFunction(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(sourceRate),
                TypeInformation.of(Event.class));

        DataStream<Event> events = env
                .fromSource(
                        source,
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner((event, ts) -> event.timestamp),
                        "EventSource")
                .setParallelism(2);

        // ── Vertex 1 : Stateful enrichment ────────────────────────────────────
        // keyBy is required so that keyed state (ValueState / MapState) is valid.
        DataStream<EnrichedEvent> enriched = events
                .keyBy(event -> event.userId)
                .process(new StatefulEnrichmentFunction())
                .name("StatefulEnrichment")
                .setParallelism(4);

        // ── Vertex 2 : CPU-intensive processing ───────────────────────────────
        DataStream<EnrichedEvent> processed = enriched
                .map(new CpuIntensiveFunction())
                .name("CpuIntensiveProcessing")
                .setParallelism(4);

        // ── Vertex 3 : Windowed aggregation ───────────────────────────────────
        DataStream<AggregatedStats> windowed = processed
                .keyBy(e -> e.userId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .process(new WindowAggregationFunction())
                .name("WindowedAggregation")
                .setParallelism(4);

        // ── Vertex 4 : Memory-intensive state accumulation ────────────────────
        windowed
                .keyBy(s -> s.userId)
                .process(new MemoryIntensiveAccumulatorFunction())
                .name("MemoryIntensiveAccumulator")
                .setParallelism(4)
                .print()
                .name("Sink");

        env.execute("MemoryAutotuningJob");
    }

    // ── Helper ────────────────────────────────────────────────────────────────

    private static long parseSourceRate() {
        String env = System.getenv("SOURCE_RATE_PER_SEC");
        if (env != null && !env.isEmpty()) {
            try {
                long rate = Long.parseLong(env.trim());
                if (rate > 0) {
                    return rate;
                }
                LOG.warn("SOURCE_RATE_PER_SEC must be positive, using default {}", DEFAULT_SOURCE_RATE);
            } catch (NumberFormatException e) {
                LOG.warn("Cannot parse SOURCE_RATE_PER_SEC='{}', using default {}", env, DEFAULT_SOURCE_RATE);
            }
        }
        return DEFAULT_SOURCE_RATE;
    }

    // =========================================================================
    // Data model
    // =========================================================================

    /** Raw event emitted by the source. */
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

    /** Event enriched with per-user state. */
    public static class EnrichedEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        public String userId;
        public long   timestamp;
        public double value;
        public String category;
        public long   eventCount;       // running count for this user
        public double runningAverage;   // running average value for this user
        public String enrichmentTag;    // extra string data to increase memory pressure

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return "EnrichedEvent{userId='" + userId + "', count=" + eventCount
                    + ", avg=" + runningAverage + '}';
        }
    }

    /** Per-window aggregation result. */
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
            return "AggregatedStats{userId='" + userId + "', window=[" + windowStart
                    + "," + windowEnd + "), count=" + count + ", sum=" + sum + '}';
        }
    }

    // =========================================================================
    // Operator implementations
    // =========================================================================

    /**
     * Generates {@link Event} instances with a uniformly random userId chosen from
     * {@code [user-0, user-(USER_COUNT-1)]}.
     */
    public static class EventGeneratorFunction
            implements GeneratorFunction<Long, Event>, Serializable {

        private static final long serialVersionUID = 1L;

        private static final String[] CATEGORIES = {"A", "B", "C", "D", "E"};

        // Lazily initialised; Random is not serializable so we create it on first use.
        private transient Random random;

        @Override
        public Event map(Long index) {
            if (random == null) {
                random = new Random();
            }
            String userId   = "user-" + random.nextInt(USER_COUNT);
            long   ts       = System.currentTimeMillis();
            double value    = random.nextDouble() * 1_000.0;
            String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
            return new Event(userId, ts, value, category);
        }
    }

    /**
     * Stateful enrichment that tracks per-user event count and running average using
     * {@link ValueState} and {@link MapState}.
     *
     * <p>Must be used on a <em>keyed</em> stream – here keyed by {@code userId}.
     */
    public static class StatefulEnrichmentFunction
            extends KeyedProcessFunction<String, Event, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        // Running count and sum for computing the average
        private transient ValueState<Long>   countState;
        private transient ValueState<Double> sumState;

        // Attribute map – simulates enrichment look-up data stored in state
        private transient MapState<String, String> attributeState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("event-count", Long.class));

            sumState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("value-sum", Double.class));

            attributeState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("user-attributes", String.class, String.class));
        }

        @Override
        public void processElement(Event event, Context ctx, Collector<EnrichedEvent> out)
                throws Exception {

            // ── update count ──────────────────────────────────────────────────
            Long count = countState.value();
            if (count == null) count = 0L;
            count++;
            countState.update(count);

            // ── update running sum ────────────────────────────────────────────
            Double sum = sumState.value();
            if (sum == null) sum = 0.0;
            sum += event.value;
            sumState.update(sum);

            // ── populate attribute map (lazy initialisation) ──────────────────
            // We only write attributes once per user to avoid unbounded growth
            // while still exercising MapState memory pressure.
            if (count == 1L) {
                for (int i = 0; i < ATTRIBUTES_PER_USER; i++) {
                    attributeState.put("attr-" + i, event.userId + "-val-" + i);
                }
            }

            // ── build enriched event ──────────────────────────────────────────
            EnrichedEvent enriched = new EnrichedEvent();
            enriched.userId         = event.userId;
            enriched.timestamp      = event.timestamp;
            enriched.value          = event.value;
            enriched.category       = event.category;
            enriched.eventCount     = count;
            enriched.runningAverage = sum / count;
            enriched.enrichmentTag  = attributeState.get("attr-0"); // read one entry

            out.collect(enriched);
        }
    }

    /**
     * Simulates CPU-intensive work by performing hash iterations on the event payload.
     * This creates back-pressure so the autoscaler has something to react to.
     */
    public static class CpuIntensiveFunction
            extends RichMapFunction<EnrichedEvent, EnrichedEvent> {

        private static final long serialVersionUID = 1L;

        /** Number of hash rounds – keep low enough not to block checkpointing. */
        private static final int HASH_ROUNDS = 500;

        @Override
        public EnrichedEvent map(EnrichedEvent event) {
            // Burn a small, deterministic amount of CPU per record.
            int hash = event.userId.hashCode();
            for (int i = 0; i < HASH_ROUNDS; i++) {
                hash = Integer.hashCode(hash ^ (int) event.value ^ (int) event.eventCount);
            }
            // Embed the hash in the tag so the work is not optimised away.
            event.enrichmentTag = (event.enrichmentTag == null ? "" : event.enrichmentTag)
                    + "-h" + hash;
            return event;
        }
    }

    /**
     * Tumbling-window aggregation that computes count / sum / min / max per user per window.
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

            long   count = 0;
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
            stats.min         = min;
            stats.max         = max;

            out.collect(stats);
        }
    }

    /**
     * Accumulates the last N {@link AggregatedStats} windows per user in a {@link MapState},
     * intentionally exercising heap / managed-memory pressure so the autoscaler's memory
     * tuning is observable.
     */
    public static class MemoryIntensiveAccumulatorFunction
            extends KeyedProcessFunction<String, AggregatedStats, String> {

        private static final long serialVersionUID = 1L;

        /** Maximum number of historical windows retained per user. */
        private static final int MAX_HISTORY = 50;

        private transient MapState<Long, AggregatedStats> historyState;

        @Override
        public void open(Configuration parameters) throws Exception {
            historyState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "window-history",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(new TypeHint<AggregatedStats>() {})));
        }

        @Override
        public void processElement(
                AggregatedStats stats,
                Context ctx,
                Collector<String> out) throws Exception {

            // Store this window's result keyed by windowStart.
            historyState.put(stats.windowStart, stats);

            // Prune oldest entries to cap state size.
            List<Long> keys = new ArrayList<>();
            historyState.keys().forEach(keys::add);

            if (keys.size() > MAX_HISTORY) {
                keys.sort(Long::compareTo);
                int toRemove = keys.size() - MAX_HISTORY;
                for (int i = 0; i < toRemove; i++) {
                    historyState.remove(keys.get(i));
                }
            }

            out.collect("User " + stats.userId + " has " + keys.size()
                    + " windows, latest count=" + stats.count);
        }
    }
}