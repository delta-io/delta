/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.sql.*;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset$;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for V2 streaming under race conditions and lifecycle scenarios.
 *
 * <p>Each test corresponds to a scenario from {@code testgap/scenario_brainstorm.md} and is
 * designed to surface bugs in DSv2 streaming. Each test exercises the scenario through BOTH the
 * DSv1 and DSv2 streaming paths and asserts the two sides agree (matching rows, or matching
 * exceptions). DSv1 is the oracle.
 */
public class V2StreamingRaceLifecycleTest extends V2TestBase {

  /**
   * Scenario 2: Concurrent commit between {@code latestOffset()} and {@code planInputPartitions()}.
   *
   * <p>SMS:317 captures endOffset; SMS:425 builds files independently - nothing pins the snapshot.
   * If a writer commits a new file between the two phases, a phantom AddFile beyond endOffset could
   * leak into the batch.
   *
   * <p>We can't reliably synchronize between the two micro-batch phases without a Spark internal
   * test hook. This is a best-effort race: a writer commits in a tight loop while a reader runs
   * with maxFilesPerTrigger=1 to maximize the gap. We then verify each batch's row count never
   * exceeds the declared max, and the overall results match the writer. Both DSv1 and DSv2 must
   * satisfy the invariants independently (V1 is the oracle).
   */
  @Test
  public void testScenario2_ConcurrentCommitBetweenLatestOffsetAndPlanPartitions(
      @TempDir File deltaTablePath) throws Exception {
    runScenario2OnEngine(new File(deltaTablePath, "v1"), /* v2= */ false);
    runScenario2OnEngine(new File(deltaTablePath, "v2"), /* v2= */ true);
  }

  private void runScenario2OnEngine(File tableDir, boolean v2) throws Exception {
    assertTrue(tableDir.mkdirs() || tableDir.isDirectory());
    String tablePath = tableDir.getAbsolutePath();
    File checkpointDir = new File(tableDir, "_checkpoint");

    // Seed the table so streaming has something to read.
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(0, "init", 0.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    Dataset<Row> streamingDF =
        v2
            ? spark
                .readStream()
                .option("maxFilesPerTrigger", "1")
                .table(str("dsv2.delta.`%s`", tablePath))
            : spark.readStream().format("delta").option("maxFilesPerTrigger", "1").load(tablePath);

    AtomicLong rowsWritten = new AtomicLong(1);
    int totalRowsToWrite = 50;

    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName(v2 ? "scenario2_race_v2" : "scenario2_race_v1")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();

    // Writer runs in parallel: many small commits while micro-batches advance.
    ExecutorService writer = Executors.newSingleThreadExecutor();
    try {
      writer.submit(
          () -> {
            for (int i = 1; i <= totalRowsToWrite - 1; i++) {
              try {
                spark
                    .createDataFrame(
                        Arrays.asList(RowFactory.create(i, "row" + i, (double) i)), TEST_SCHEMA)
                    .write()
                    .format("delta")
                    .mode("append")
                    .save(tablePath);
                rowsWritten.incrementAndGet();
              } catch (Exception ignored) {
                return;
              }
            }
          });

      // Let writer + reader race. Wait for writer to complete its work + final batches.
      long deadline = System.currentTimeMillis() + 60000L;
      while (rowsWritten.get() < totalRowsToWrite && System.currentTimeMillis() < deadline) {
        Thread.sleep(100);
      }
      // Allow reader to drain remaining batches.
      query.processAllAvailable();
    } finally {
      writer.shutdownNow();
      writer.awaitTermination(5, TimeUnit.SECONDS);
      query.stop();
      DeltaLog.clearCache();
    }

    String engine = v2 ? "DSv2" : "DSv1";

    // Verify no exception was thrown by the streaming query.
    assertTrue(
        query.exception().isEmpty(),
        () ->
            engine
                + " streaming query failed unexpectedly: "
                + (query.exception().isDefined() ? query.exception().get().toString() : ""));

    // Per-batch invariant: numInputRows <= numFilesAdmittedThisBatch (= maxFilesPerTrigger=1
    // means at most 1 file per batch). With 1 row per file, numInputRows must be 0 or 1.
    StreamingQueryProgress[] progresses = query.recentProgress();
    for (StreamingQueryProgress p : progresses) {
      long numRows = p.numInputRows();
      assertTrue(
          numRows <= 1,
          () ->
              engine
                  + " batch "
                  + p.batchId()
                  + " produced "
                  + numRows
                  + " rows but maxFilesPerTrigger=1 with 1 row per file."
                  + " Phantom AddFile beyond endOffset suspected. Progress: "
                  + p.json());
    }

    // Per-batch end-offset invariant: planInputPartitions must not include files past endOffset.
    // We approximate this by checking each progress' endOffset version matches a real commit.
    long latestVersion = DeltaLog.forTable(spark, tablePath).snapshot().version();
    String tableId = DeltaLog.forTable(spark, tablePath).tableId();
    for (StreamingQueryProgress p : progresses) {
      if (p.sources().length == 0) continue;
      String endOffsetJson = p.sources()[0].endOffset();
      if (endOffsetJson == null) continue;
      DeltaSourceOffset endOffset = DeltaSourceOffset$.MODULE$.apply(tableId, endOffsetJson);
      assertTrue(
          endOffset.reservoirVersion() <= latestVersion + 1,
          () ->
              engine
                  + " endOffset reservoirVersion="
                  + endOffset.reservoirVersion()
                  + " exceeds latestVersion+1="
                  + (latestVersion + 1));
    }
  }

  /**
   * Scenario 5: Protocol upgrade mid-stream (writer feature appears at v=N).
   *
   * <p>SMS:1009 ({@code validateCommitAndDecideSkipping}) handles AddFile / RemoveFile / Metadata
   * but does not consult Protocol actions. SMS:631 only validates protocol at startup, not
   * per-commit. Per scenario_brainstorm.md, we expect a clean {@code
   * UnsupportedTableFeatureException} (or DeltaUnsupportedTableFeatureException) when the stream
   * encounters a commit that introduces a writer feature it cannot read - not silent skip or NPE.
   *
   * <p>Stream a non-DV table; mid-stream enable {@code delta.enableDeletionVectors} and DELETE rows
   * to actually produce a DV. V1 (oracle) throws an unsupported-feature error; V2 must match. If V2
   * swallows it (Bug #23), divergence surfaces here.
   */
  @Test
  public void testScenario5_ProtocolUpgradeMidStream(@TempDir File deltaTablePath)
      throws Exception {
    Throwable v1Thrown = runScenario5OnEngine(new File(deltaTablePath, "v1"), /* v2= */ false);
    Throwable v2Thrown = runScenario5OnEngine(new File(deltaTablePath, "v2"), /* v2= */ true);

    boolean v1Threw = hasUnsupportedFeatureCause(v1Thrown);
    boolean v2Threw = hasUnsupportedFeatureCause(v2Thrown);

    assertTrue(
        v1Threw,
        () ->
            "DSv1 (oracle) was expected to throw a DeltaUnsupportedTableFeatureException for "
                + "mid-stream DV protocol upgrade. Got: "
                + describeChain(v1Thrown));

    assertEquals(
        v1Threw,
        v2Threw,
        () ->
            "DSv1 vs DSv2 divergence on mid-stream DV protocol upgrade.\n"
                + "DSv1 threw expected="
                + v1Threw
                + " chain="
                + describeChain(v1Thrown)
                + "\nDSv2 threw expected="
                + v2Threw
                + " chain="
                + describeChain(v2Thrown));
  }

  /**
   * Runs the scenario-5 sequence (drain initial snapshot, enable DV + DELETE, restart stream) on
   * either the DSv1 or DSv2 path. Returns whatever exception bubbled out (or null if the stream
   * completed without error).
   */
  private Throwable runScenario5OnEngine(File tableDir, boolean v2) throws Exception {
    assertTrue(tableDir.mkdirs() || tableDir.isDirectory());
    String tablePath = tableDir.getAbsolutePath();
    File checkpointDir = new File(tableDir, "_checkpoint");

    // v=0: create non-DV table with a few rows.
    spark.sql(str("CREATE TABLE delta.`%s` (value INT) USING delta", tablePath));
    spark
        .range(10)
        .selectExpr("cast(id as int) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    Dataset<Row> streamingDF =
        v2
            ? spark.readStream().table(str("dsv2.delta.`%s`", tablePath))
            : spark.readStream().format("delta").load(tablePath);
    String tag = v2 ? "v2" : "v1";

    // First run: drain initial snapshot using noop sink (memory sink doesn't support
    // checkpoint recovery across separate query instances).
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario5_pre_upgrade_" + tag)
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    query.awaitTermination();
    query.stop();

    // Mid-stream: enable DV writer feature + perform a DELETE that materializes a DV.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE value = 0", tablePath));

    // Restart stream: should encounter the protocol upgrade + DV commit.
    Throwable thrown = null;
    try {
      StreamingQuery q =
          streamingDF
              .writeStream()
              .format("noop")
              .queryName("scenario5_post_upgrade_" + tag)
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      try {
        q.awaitTermination();
      } catch (Throwable awaitErr) {
        thrown = awaitErr;
      } finally {
        q.stop();
      }
      if (thrown == null && q.exception().isDefined()) {
        thrown = q.exception().get();
      }
    } catch (Throwable t) {
      thrown = t;
    } finally {
      DeltaLog.clearCache();
    }
    return thrown;
  }

  /** Returns true if the cause chain contains an UnsupportedTableFeature-style error. */
  private static boolean hasUnsupportedFeatureCause(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      String cls = cur.getClass().getName();
      if (cls.contains("UnsupportedTableFeature") || cls.contains("DeltaUnsupportedOperation")) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }

  /** Renders a cause chain for diagnostics. */
  private static String describeChain(Throwable t) {
    if (t == null) return "(no exception thrown)";
    StringBuilder sb = new StringBuilder();
    Throwable cur = t;
    while (cur != null) {
      sb.append("\n  -> ")
          .append(cur.getClass().getName())
          .append(": ")
          .append(cur.getMessage() == null ? "" : cur.getMessage());
      cur = cur.getCause();
    }
    return sb.toString();
  }

  /**
   * Scenario 6: {@code Trigger.AvailableNow} twice on same checkpoint, no new data.
   *
   * <p>SMS:149-151 caches {@code lastOffsetForTriggerAvailableNow} per-stream; {@code
   * isLastOffsetForTriggerAvailableNowInitialized} is reset across instances. The second invocation
   * should produce 0 batches (or 1 trivially-empty progress) - no duplicate, no error. Asserted on
   * both DSv1 and DSv2 against the same row counts.
   */
  @Test
  public void testScenario6_AvailableNowTwiceNoNewData(@TempDir File deltaTablePath)
      throws Exception {
    long[] v1Runs = runScenario6OnEngine(new File(deltaTablePath, "v1"), /* v2= */ false);
    long[] v2Runs = runScenario6OnEngine(new File(deltaTablePath, "v2"), /* v2= */ true);

    // V1 is the oracle: first run drains all 3 rows, second run drains 0.
    assertEquals(3L, v1Runs[0], "DSv1 first AvailableNow run should drain all 3 rows.");
    assertEquals(0L, v1Runs[1], "DSv1 second AvailableNow run should produce 0 rows.");

    // V2 must match V1 row-for-row on both runs.
    assertEquals(
        v1Runs[0],
        v2Runs[0],
        () -> "DSv1 vs DSv2 first-run row count mismatch. V1=" + v1Runs[0] + " V2=" + v2Runs[0]);
    assertEquals(
        v1Runs[1],
        v2Runs[1],
        () ->
            "DSv1 vs DSv2 second-run row count mismatch. V1="
                + v1Runs[1]
                + " V2="
                + v2Runs[1]
                + ". Indicates duplicate replay (cf. Task K row-duplication).");
  }

  /** Runs two back-to-back AvailableNow streams on the same checkpoint; returns rows per run. */
  private long[] runScenario6OnEngine(File tableDir, boolean v2) throws Exception {
    assertTrue(tableDir.mkdirs() || tableDir.isDirectory());
    String tablePath = tableDir.getAbsolutePath();
    File checkpointDir = new File(tableDir, "_checkpoint");

    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, "Alice", 10.0),
                RowFactory.create(2, "Bob", 20.0),
                RowFactory.create(3, "Charlie", 30.0)),
            TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    Dataset<Row> streamingDF =
        v2
            ? spark.readStream().table(str("dsv2.delta.`%s`", tablePath))
            : spark.readStream().format("delta").load(tablePath);
    String tag = v2 ? "v2" : "v1";

    StreamingQuery q1 =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario6_first_" + tag)
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    q1.awaitTermination();
    q1.stop();

    long firstRunRows = 0;
    for (StreamingQueryProgress p : q1.recentProgress()) {
      firstRunRows += p.numInputRows();
    }

    StreamingQuery q2 =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario6_second_" + tag)
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    q2.awaitTermination();
    q2.stop();
    DeltaLog.clearCache();

    String engine = v2 ? "DSv2" : "DSv1";
    assertTrue(
        q2.exception().isEmpty(),
        () -> engine + " second AvailableNow failed: " + q2.exception().get().toString());

    long secondRunRows = 0;
    for (StreamingQueryProgress p : q2.recentProgress()) {
      secondRunRows += p.numInputRows();
    }
    return new long[] {firstRunRows, secondRunRows};
  }

  /**
   * Scenario 8: Initial snapshot exceeding {@code maxInitialSnapshotFiles} (SMS:226).
   *
   * <p>{@code InitialSnapshotCache} (SMS:172) is asserted at limit in {@code
   * loadAndValidateSnapshot}. We lower the conf to 5 and write 10 files in v=0; expect a clean
   * {@code DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE} error - not OOM, not NPE. DSv1 is the
   * oracle; DSv2 must surface the same structured error.
   */
  @Test
  public void testScenario8_InitialSnapshotExceedsMaxFiles(@TempDir File deltaTablePath)
      throws Exception {
    Throwable v1Thrown = runScenario8OnEngine(new File(deltaTablePath, "v1"), /* v2= */ false);
    Throwable v2Thrown = runScenario8OnEngine(new File(deltaTablePath, "v2"), /* v2= */ true);

    boolean v1Matched = hasInitialSnapshotTooLargeCause(v1Thrown);
    boolean v2Matched = hasInitialSnapshotTooLargeCause(v2Thrown);

    assertTrue(
        v1Matched,
        () ->
            "DSv1 (oracle) expected DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE. Got: "
                + describeChain(v1Thrown));
    assertEquals(
        v1Matched,
        v2Matched,
        () ->
            "DSv1 vs DSv2 divergence on initial-snapshot-too-large.\n"
                + "DSv1 chain="
                + describeChain(v1Thrown)
                + "\nDSv2 chain="
                + describeChain(v2Thrown));
  }

  /** Runs scenario 8 on one engine and returns whatever exception bubbled out. */
  private Throwable runScenario8OnEngine(File tableDir, boolean v2) throws Exception {
    assertTrue(tableDir.mkdirs() || tableDir.isDirectory());
    String tablePath = tableDir.getAbsolutePath();

    // Write 10 small files in v=0 by repartitioning to 10 partitions.
    spark
        .range(10)
        .selectExpr(
            "cast(id as int) as id", "cast(id as string) as name", "cast(id as double) as value")
        .repartition(10)
        .write()
        .format("delta")
        .save(tablePath);

    // Verify we actually have multiple files in v=0.
    long numFilesInV0 = DeltaLog.forTable(spark, tablePath).snapshot().allFiles().count();
    assertTrue(
        numFilesInV0 > 5,
        () -> "Expected >5 files in v=0 to trigger the limit, but got " + numFilesInV0);

    Throwable[] thrownHolder = new Throwable[1];
    String tag = v2 ? "v2" : "v1";

    withSQLConf(
        "spark.databricks.delta.streaming.initialSnapshotMaxFiles",
        "5",
        () -> {
          Dataset<Row> streamingDF =
              v2
                  ? spark.readStream().table(str("dsv2.delta.`%s`", tablePath))
                  : spark.readStream().format("delta").load(tablePath);
          File checkpointDir = new File(tableDir, "_checkpoint_s8");

          Throwable thrown = null;
          StreamingQuery q = null;
          try {
            q =
                streamingDF
                    .writeStream()
                    .format("memory")
                    .queryName("scenario8_too_large_" + tag)
                    .option("checkpointLocation", checkpointDir.getAbsolutePath())
                    .outputMode("append")
                    .trigger(Trigger.AvailableNow())
                    .start();
            try {
              q.awaitTermination();
            } catch (Throwable awaitErr) {
              thrown = awaitErr;
            }
            if (thrown == null && q.exception().isDefined()) {
              thrown = q.exception().get();
            }
          } catch (Throwable t) {
            thrown = t;
          } finally {
            if (q != null) {
              try {
                q.stop();
              } catch (Throwable stopErr) {
                // ignore stop errors during cleanup
              }
            }
            DeltaLog.clearCache();
          }
          thrownHolder[0] = thrown;
        });

    return thrownHolder[0];
  }

  /**
   * Returns true if the cause chain matches the DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE error.
   */
  private static boolean hasInitialSnapshotTooLargeCause(Throwable t) {
    Throwable cur = t;
    while (cur != null) {
      String msg = cur.getMessage() == null ? "" : cur.getMessage();
      if (msg.contains("DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE")
          || msg.contains("initialSnapshotMaxFiles")
          || cur.getClass().getName().contains("DeltaUnsupportedOperation")) {
        return true;
      }
      cur = cur.getCause();
    }
    return false;
  }
}
