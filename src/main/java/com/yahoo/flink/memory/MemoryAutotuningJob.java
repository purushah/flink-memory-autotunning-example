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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * 2. Stateful enrichment operator with high parallelism
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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable checkpointing for stateful operations
        env.enableCheckpointing(60000); // Checkpoint every 60 seconds

        // High default parallelism to trigger more TaskManagers under load
        // The autoscaler will adjust this based on actual load
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

        DataStream<Event> events = env.fromSource(
            source,
            WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp),
            "VariableLoadEventSource"
        ).setParallelism(20); // High parallelism for source

        // Vertex 1: Stateful enrichment - maintains state for each user
        // HIGH PARALLELISM to utilize many TaskManagers
        DataStream<EnrichedEvent> enriched = events
            .keyBy(event -> event.userId)
            .map(new StatefulEnrichmentFunction())
            .name("StatefulEnrichment")
            .setParallelism(80); // Very high parallelism

        // Vertex 2: CPU-intensive processing to create backpressure
        DataStream<EnrichedEvent> processed = enriched
            .map(new CpuIntensiveFunction())
            .name("CpuIntensiveProcessing")
            .setParallelism(60);

        // Vertex 3: Windowed aggregation - memory-intensive windowing
        DataStream<AggregatedStats> windowed = processed
            .keyBy(event -> event.userId)
            .window(TumblingEventTimeWindows.of(Duration.ofMinutes(3)))
            .process(new WindowedAggregationFunction())
            .name("WindowedAggregation")
            .setParallelism(40);

        // Vertex 4: State accumulation - grows state over time
        DataStream<UserProfile> profiles = windowed
            .keyBy(stats -> stats.userId)
            .process(new StateAccumulationFunction())
            .name("StateAccumulation")
            .setParallelism(30);

        // Vertex 5: Final processing and sink
        profiles
            .map(new FinalProcessingFunction())
            .name("FinalProcessing")
            .setParallelism(10)
            .print()
            .name("ConsoleSink");

        LOG.info("Starting Flink Memory Autotuning and Autoscaling Example Job");
        LOG.info("This job demonstrates variable load for autoscaling demonstration");

        env.execute("Flink Memory Autotuning and Autoscaling Example");
    }

    /**
     * Variable load generator that cycles between high and low rates.
     * This simulates real-world traffic patterns and demonstrates autoscaling.
     */
    public static class VariableLoadGenerator implements org.apache.flink.connector.datagen.source.GeneratorFunction<Long, Event> {
        private final Random random = new Random();
        private long startTime = 0;

        @Override
        public Event map(Long index) {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();
            }

            long elapsed = System.currentTimeMillis() - startTime;
            long cyclePosition = elapsed % (LOAD_CYCLE_DURATION_MS * 2); // Full cycle = high + low

            // Determine if we're in high or low load phase
            boolean isHighLoad = cyclePosition < LOAD_CYCLE_DURATION_MS;

            // Log phase transitions (every 30 seconds)
            if (index % 30000 == 0) {
                String phase = isHighLoad ? "HIGH LOAD" : "LOW LOAD";
                long rate = isHighLoad ? HIGH_LOAD_RATE : LOW_LOAD_RATE;
                long minutesInPhase = (cyclePosition % LOAD_CYCLE_DURATION_MS) / 60000;
                LOG.info(">>> LOAD PATTERN: {} - {} events/sec - {} min into phase",
                    phase, rate, minutesInPhase);
            }

            // Generate event
            String userId = "user_" + (random.nextInt(USER_COUNT) + 1);
            String eventType = random.nextBoolean() ? "CLICK" : "PURCHASE";
            double amount = random.nextDouble() * 1000;
            long timestamp = System.currentTimeMillis();

            return new Event(userId, eventType, amount, timestamp);
        }
    }

    /**
     * CPU-intensive function to create computational load.
     * This helps trigger CPU-based autoscaling.
     */
    public static class CpuIntensiveFunction extends RichMapFunction<EnrichedEvent, EnrichedEvent> {
        @Override
        public EnrichedEvent map(EnrichedEvent event) throws Exception {
            // Simulate CPU-intensive computation
            // Calculate some expensive operations to add CPU load
            double result = 0;
            for (int i = 0; i < 100; i++) {
                result += Math.sqrt(event.amount * i) + Math.log(i + 1);
                result = Math.sin(result) * Math.cos(result);
            }

            // Just to use the result and prevent optimization
            event.totalAmount += result * 0.000001;

            return event;
        }
    }

    // Data classes
    public static class Event {
        public String userId;
        public String eventType;
        public double amount;
        public long timestamp;

        public Event() {}

        public Event(String userId, String eventType, double amount, long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("Event{user=%s, type=%s, amount=%.2f, ts=%d}",
                userId, eventType, amount, timestamp);
        }
    }

    public static class EnrichedEvent {
        public String userId;
        public String eventType;
        public double amount;
        public long timestamp;
        public long eventCount;
        public double totalAmount;

        public EnrichedEvent() {}

        @Override
        public String toString() {
            return String.format("EnrichedEvent{user=%s, type=%s, amount=%.2f, count=%d, total=%.2f}",
                userId, eventType, amount, eventCount, totalAmount);
        }
    }

    public static class AggregatedStats {
        public String userId;
        public long eventCount;
        public double totalAmount;
        public double avgAmount;
        public long windowStart;
        public long windowEnd;

        public AggregatedStats() {}

        @Override
        public String toString() {
            return String.format("AggregatedStats{user=%s, count=%d, total=%.2f, avg=%.2f, window=%d-%d}",
                userId, eventCount, totalAmount, avgAmount, windowStart, windowEnd);
        }
    }

    public static class UserProfile {
        public String userId;
        public long totalEvents;
        public double lifetimeValue;
        public Map<String, Long> eventTypeCounts;

        public UserProfile() {
            this.eventTypeCounts = new HashMap<>();
        }

        @Override
        public String toString() {
            return String.format("UserProfile{user=%s, events=%d, ltv=%.2f, types=%s}",
                userId, totalEvents, lifetimeValue, eventTypeCounts);
        }
    }

    // Operator 1: Stateful enrichment - maintains per-user counters
    public static class StatefulEnrichmentFunction extends RichMapFunction<Event, EnrichedEvent> {
        private transient ValueState<Long> eventCountState;
        private transient ValueState<Double> totalAmountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            eventCountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("eventCount", Long.class)
            );
            totalAmountState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalAmount", Double.class)
            );
        }

        @Override
        public EnrichedEvent map(Event event) throws Exception {
            Long currentCount = eventCountState.value();
            Double currentTotal = totalAmountState.value();

            long newCount = (currentCount == null ? 0 : currentCount) + 1;
            double newTotal = (currentTotal == null ? 0 : currentTotal) + event.amount;

            eventCountState.update(newCount);
            totalAmountState.update(newTotal);

            EnrichedEvent enriched = new EnrichedEvent();
            enriched.userId = event.userId;
            enriched.eventType = event.eventType;
            enriched.amount = event.amount;
            enriched.timestamp = event.timestamp;
            enriched.eventCount = newCount;
            enriched.totalAmount = newTotal;

            return enriched;
        }
    }

    // Operator 2: Windowed aggregation - memory-intensive windowing
    public static class WindowedAggregationFunction
            extends ProcessWindowFunction<EnrichedEvent, AggregatedStats, String, TimeWindow> {

        @Override
        public void process(String userId,
                          Context context,
                          Iterable<EnrichedEvent> elements,
                          Collector<AggregatedStats> out) {
            long count = 0;
            double total = 0;

            for (EnrichedEvent event : elements) {
                count++;
                total += event.amount;
            }

            AggregatedStats stats = new AggregatedStats();
            stats.userId = userId;
            stats.eventCount = count;
            stats.totalAmount = total;
            stats.avgAmount = count > 0 ? total / count : 0;
            stats.windowStart = context.window().getStart();
            stats.windowEnd = context.window().getEnd();

            out.collect(stats);
        }
    }

    // Operator 3: State accumulation - grows state over time (memory-intensive)
    public static class StateAccumulationFunction
            extends KeyedProcessFunction<String, AggregatedStats, UserProfile> {

        private transient ValueState<Long> totalEventsState;
        private transient ValueState<Double> lifetimeValueState;
        private transient MapState<String, Long> eventTypeCountsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            totalEventsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalEvents", Long.class)
            );
            lifetimeValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lifetimeValue", Double.class)
            );
            eventTypeCountsState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("eventTypeCounts", String.class, Long.class)
            );
        }

        @Override
        public void processElement(AggregatedStats stats,
                                  Context context,
                                  Collector<UserProfile> out) throws Exception {
            // Accumulate totals
            Long currentEvents = totalEventsState.value();
            Double currentLTV = lifetimeValueState.value();

            long newTotalEvents = (currentEvents == null ? 0 : currentEvents) + stats.eventCount;
            double newLTV = (currentLTV == null ? 0 : currentLTV) + stats.totalAmount;

            totalEventsState.update(newTotalEvents);
            lifetimeValueState.update(newLTV);

            // Build profile
            UserProfile profile = new UserProfile();
            profile.userId = stats.userId;
            profile.totalEvents = newTotalEvents;
            profile.lifetimeValue = newLTV;

            // Copy event type counts from state
            for (Map.Entry<String, Long> entry : eventTypeCountsState.entries()) {
                profile.eventTypeCounts.put(entry.getKey(), entry.getValue());
            }

            out.collect(profile);
        }
    }

    // Operator 4: Final processing
    public static class FinalProcessingFunction extends RichMapFunction<UserProfile, String> {
        private transient Random random;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
        }

        @Override
        public String map(UserProfile profile) {
            // Simulate some processing
            String segment = profile.lifetimeValue > 5000 ? "VIP" :
                           profile.lifetimeValue > 1000 ? "GOLD" : "STANDARD";

            return String.format("[%s] %s - Events: %d, LTV: $%.2f",
                segment, profile.userId, profile.totalEvents, profile.lifetimeValue);
        }
    }
}
