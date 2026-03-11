# AI-Suggested Fix — Flink AI Debugger

**Generated:** 2026-03-11T18:34:11.338457+00:00
**Repository:** purushah/flink-memory-autotunning-example

## Issue
General code quality review and optimization for Apache Flink job.

## Files Analyzed
- `src/main/java/com/yahoo/flink/memory/MemoryAutotuningJob.java`

## Fix
I'll analyze the source code for issues and provide targeted fixes.

Key issues I can identify:

1. **Truncated file** - The source is cut off, but I can fix what's visible
2. **`RichMapFunction` used for stateful enrichment** - Should be `KeyedProcessFunction` for proper keyed state access after `keyBy()`
3. **Excessive parallelism** hardcoded (80, 60, 50) - defeats autoscaler purpose
4. **`Random` used in potentially shared context** - thread-safety concern
5. **`VariableLoadGenerator` rate limiting** - Rate is set at source construction time with `HIGH_LOAD_RATE` only; variable load via `VariableLoadGenerator` won't actually change the `RateLimiterStrategy` at runtime
6. **Missing `@Override` and proper state initialization patterns**
7. **`StatefulEnrichmentFunction` extends `RichMapFunction`** but is used after `keyBy()` — state should be declared properly
8. **`LOAD_CYCLE_DURATION_MS` logging** uses integer division correctly, but `HIGH_LOAD_RATE` cast issue

===DIFF: src/main/java/com/yahoo/flink/memory/MemoryAutotuningJob.java===
--- a/src/main/java/com/yahoo/flink/memory/MemoryAutotuningJob.java
+++ b/src/main/java/com/yahoo/flink/memory/MemoryAutotuningJob.java
@@ -19,7 +19,6 @@
 package com.yahoo.flink.memory;
 
 import org.apache.flink.api.common.eventtime.WatermarkStrategy;
-import org.apache.flink.api.common.functions.RichMapFunction;
 import org.apache.flink.configuration.Configuration;
 import org.apache.flink.api.common.state.MapState;
 import org.apache.flink.api.common.state.MapStateDescriptor;
@@ -27,7 +26,7 @@ import org.apache.flink.api.common.state.ValueState;
 import org.apache.flink.api.common.state.ValueStateDescriptor;
 import org.apache.flink.api.common.typeinfo.TypeHint;
 import org.apache.flink.api.common.typeinfo.TypeInformation;
-import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
+import org.apache.flink.connector.datagen.source.GeneratorFunction;
 import org.apache.flink.connector.datagen.source.DataGeneratorSource;
 import org.apache.flink.streaming.api.datastream.DataStream;
 import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
@@ -40,7 +39,8 @@ import org.apache.flink.util.Collector;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;
 
+import java.io.Serializable;
 import java.time.Duration;
-import java.util.HashMap;
 import java.util.Map;
 import java.util.Random;
+import java.util.concurrent.ThreadLocalRandom;
@@ -57,8 +57,8 @@ import java.util.Random;
  *   job.autoscaler.memory.tuning.enabled: true
  *   job.autoscaler.scaling.enabled: true
  */
-public class MemoryAutotuningJob {
+public final class MemoryAutotuningJob {
 
     private static final Logger LOG = LoggerFactory.getLogger(MemoryAutotuningJob.class);
 
@@ -66,6 +66,10 @@ public class MemoryAutotuningJob {
     private static final long HIGH_LOAD_RATE = 100_000; // events/sec - triggers scale up
     private static final long LOW_LOAD_RATE = 500;      // events/sec - triggers scale down
     private static final long LOAD_CYCLE_DURATION_MS = 10 * 60 * 1000; // 10 minutes per phase
+    // Default parallelism — intentionally low so the autoscaler can scale up from a
+    // reasonable baseline rather than immediately over-provisioning.
+    private static final int DEFAULT_PARALLELISM = 4;
+    // Source parallelism is fixed; rate-limiting is handled inside the generator.
+    private static final int SOURCE_PARALLELISM = 4;
     private static final int USER_COUNT = 10_000; // More users = more state
 
     public static void main(String[] args) throws Exception {
@@ -75,7 +79,12 @@ public class MemoryAutotuningJob {
         env.enableCheckpointing(60000); // Checkpoint every 60 seconds
 
         // High default parallelism to trigger more TaskManagers under load
-        // The autoscaler will adjust this based on actual load
-        env.setParallelism(50);
+        // Keep this low: the autoscaler will scale UP based on observed lag/throughput.
+        // Setting it to 50 up-front defeats the purpose of autoscaling and wastes resources
+        // before the job has had any time to observe actual load.
+        env.setParallelism(DEFAULT_PARALLELISM);
 
         LOG.info("=================================================================");
         LOG.info("Starting Variable Load Job for Autoscaling Demonstration");
@@ -85,27 +94,42 @@ public class MemoryAutotuningJob {
         LOG.info("=================================================================");
 
         // Generate random events with VARIABLE rates that cycle over time
+        // NOTE: DataGeneratorSource's RateLimiterStrategy is static after construction.
+        // Variable throughput is modelled inside VariableLoadGenerator itself by
+        // occasionally sleeping, which lets the autoscaler observe real backpressure.
         DataGeneratorSource<Event> source = new DataGeneratorSource<>(
             new VariableLoadGenerator(),
             Long.MAX_VALUE, // Generate unlimited events
-            RateLimiterStrategy.perSecond(HIGH_LOAD_RATE), // Start with high load
+            // Cap at HIGH_LOAD_RATE per parallel instance; actual rate is varied
+            // by the generator sleeping to simulate low-load periods.
+            org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
+                .perSecond(HIGH_LOAD_RATE),
             TypeInformation.of(Event.class)
         );
 
         DataStream<Event> events = env.fromSource(
             source,
             WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                 .withTimestampAssigner((event, timestamp) -> event.timestamp),
             "VariableLoadEventSource"
-        ).setParallelism(20); // High parallelism for source
+        ).setParallelism(SOURCE_PARALLELISM);
 
-        // Vertex 1: Stateful enrichment - maintains state for each user
-        // HIGH PARALLELISM to utilize many TaskManagers
+        // Vertex 1: Stateful enrichment — maintains per-user state.
+        // Parallelism is left at the job default so the autoscaler can tune it.
+        // StatefulEnrichmentFunction is a KeyedProcessFunction so that it has
+        // legitimate access to Flink-managed keyed state after keyBy().
         DataStream<EnrichedEvent> enriched = events
             .keyBy(event -> event.userId)
-            .map(new StatefulEnrichmentFunction())
+            .process(new StatefulEnrichmentFunction())
             .name("StatefulEnrichment")
-            .setParallelism(80); // Very high parallelism
+            .uid("stateful-enrichment");   // stable UID for savepoint compatibility
 
         // Vertex 2: CPU-intensive processing to create backpressure
         DataStream<EnrichedEvent> processed = enriched
             .map(new CpuIntensiveFunction())
             .name("CpuIntensiveProcessing")
-            .setParallelism(60);
+            .uid("

---
*Generated by [Flink AI Debugger](https://github.com/storm/flink-ai-debugger)*
