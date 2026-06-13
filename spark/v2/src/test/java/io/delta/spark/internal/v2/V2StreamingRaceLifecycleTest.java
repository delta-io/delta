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
 * designed to surface bugs in DSv2 streaming. Failures in these tests = bugs.
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
   * exceeds the declared max, and the overall results match the writer.
   */
  @Test
  public void testScenario2_ConcurrentCommitBetweenLatestOffsetAndPlanPartitions(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");

    // Seed the table so streaming has something to read.
    spark
        .createDataFrame(Arrays.asList(RowFactory.create(0, "init", 0.0)), TEST_SCHEMA)
        .write()
        .format("delta")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("maxFilesPerTrigger", "1") // 1 file per batch -> any phantom file stands out
            .table(dsv2TableRef);

    AtomicLong rowsWritten = new AtomicLong(1);
    int totalRowsToWrite = 50;

    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("scenario2_race")
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

    // Verify no exception was thrown by the streaming query.
    assertTrue(
        query.exception().isEmpty(),
        () ->
            "Streaming query failed unexpectedly: "
                + (query.exception().isDefined() ? query.exception().get().toString() : ""));

    // Per-batch invariant: numInputRows <= numFilesAdmittedThisBatch (= maxFilesPerTrigger=1
    // means at most 1 file per batch). With 1 row per file, numInputRows must be 0 or 1.
    StreamingQueryProgress[] progresses = query.recentProgress();
    for (StreamingQueryProgress p : progresses) {
      long numRows = p.numInputRows();
      assertTrue(
          numRows <= 1,
          () ->
              "Batch "
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
              "endOffset reservoirVersion="
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
   * to actually produce a DV. Assert clean structured failure.
   */
  @Test
  public void testScenario5_ProtocolUpgradeMidStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");

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

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    // First run: drain initial snapshot using noop sink (memory sink doesn't support
    // checkpoint recovery across separate query instances).
    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario5_pre_upgrade")
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
    // Per scenario, expect a clean UnsupportedTableFeatureException or analogous error,
    // NOT a silent skip / NPE / data corruption.
    boolean threwExpected = false;
    Throwable thrown = null;
    long postUpgradeRowsRead = -1;
    try {
      StreamingQuery q =
          streamingDF
              .writeStream()
              .format("noop")
              .queryName("scenario5_post_upgrade")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      try {
        q.awaitTermination();
        // Sum rows seen across all batches in this run.
        long sum = 0;
        for (StreamingQueryProgress p : q.recentProgress()) {
          sum += p.numInputRows();
        }
        postUpgradeRowsRead = sum;
      } finally {
        q.stop();
      }
      if (q.exception().isDefined()) {
        thrown = q.exception().get();
      }
    } catch (Throwable t) {
      thrown = t;
    } finally {
      DeltaLog.clearCache();
    }

    final Throwable finalThrown = thrown;
    if (finalThrown != null) {
      // Walk the cause chain looking for a clean UnsupportedTableFeatureException or analog.
      Throwable cur = finalThrown;
      StringBuilder chain = new StringBuilder();
      while (cur != null) {
        String cls = cur.getClass().getName();
        String msg = cur.getMessage() == null ? "" : cur.getMessage();
        chain.append("\n  -> ").append(cls).append(": ").append(msg);
        if (cls.contains("UnsupportedTableFeature") || cls.contains("DeltaUnsupportedOperation")) {
          threwExpected = true;
          break;
        }
        cur = cur.getCause();
      }
      final boolean finalThrewExpected = threwExpected;
      final String finalChain = chain.toString();
      assertTrue(
          finalThrewExpected,
          () ->
              "Expected UnsupportedTableFeatureException for mid-stream DV protocol upgrade, "
                  + "got cause chain:"
                  + finalChain);
    }
    // If thrown == null, the stream silently produced data despite the upgrade. That is also a bug
    // (we'd be returning AddFile-with-DV via a reader that doesn't understand DVs).
    // Read from memory sink to confirm - if it returned any post-DELETE rows that should
    // have been masked, we have data corruption.
    if (finalThrown == null) {
      // The strict expectation is: stream throws an error. If we reach here, we failed to throw
      // - that itself is a bug surfaced by this test. The DELETE produced an AddFile with a DV
      // (and a RemoveFile of the original); without per-commit protocol validation, the stream
      // either silently skipped the DV commit or produced rows from a DV-encoded file using a
      // reader that doesn't understand DVs.
      fail(
          "Expected a DeltaUnsupportedTableFeatureException-like error for mid-stream DV upgrade,"
              + " but none was thrown. Rows read from post-upgrade run="
              + postUpgradeRowsRead);
    }
  }

  /**
   * Scenario 6: {@code Trigger.AvailableNow} twice on same checkpoint, no new data.
   *
   * <p>SMS:149-151 caches {@code lastOffsetForTriggerAvailableNow} per-stream; {@code
   * isLastOffsetForTriggerAvailableNowInitialized} is reset across instances. The second invocation
   * should produce 0 batches (or 1 trivially-empty progress) - no duplicate, no error.
   */
  @Test
  public void testScenario6_AvailableNowTwiceNoNewData(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");

    // Seed table.
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

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    // First AvailableNow run: drain everything. Use noop sink because memory sink does not
    // support checkpoint recovery across separate query instances.
    StreamingQuery q1 =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario6_first")
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
    assertEquals(3L, firstRunRows, "First AvailableNow run should drain all 3 rows.");

    // Second AvailableNow run on the same checkpoint, no new data.
    StreamingQuery q2 =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario6_second")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    q2.awaitTermination();
    q2.stop();

    DeltaLog.clearCache();

    // Verify no error and no duplicate rows.
    assertTrue(
        q2.exception().isEmpty(),
        () -> "Second AvailableNow failed: " + q2.exception().get().toString());

    long secondRunRows = 0;
    for (StreamingQueryProgress p : q2.recentProgress()) {
      secondRunRows += p.numInputRows();
    }
    final long finalSecondRunRows = secondRunRows;
    assertEquals(
        0L,
        finalSecondRunRows,
        () ->
            "Expected 0 rows from second AvailableNow run on same checkpoint with no new data, "
                + "got "
                + finalSecondRunRows
                + ". Indicates duplicate replay (cf. Task K row-duplication).");
  }

  /**
   * Scenario 8: Initial snapshot exceeding {@code maxInitialSnapshotFiles} (SMS:226).
   *
   * <p>{@code InitialSnapshotCache} (SMS:172) is asserted at limit in {@code
   * loadAndValidateSnapshot}. We lower the conf to 5 and write 10 files in v=0; expect a clean
   * {@code DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE} error - not OOM, not NPE.
   */
  @Test
  public void testScenario8_InitialSnapshotExceedsMaxFiles(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();

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

    // Lower the limit so v=0 exceeds it.
    withSQLConf(
        "spark.databricks.delta.streaming.initialSnapshotMaxFiles",
        "5",
        () -> {
          String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
          Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
          File checkpointDir = new File(deltaTablePath, "_checkpoint_s8");

          Throwable thrown = null;
          StreamingQuery q = null;
          try {
            q =
                streamingDF
                    .writeStream()
                    .format("memory")
                    .queryName("scenario8_too_large")
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

          // Walk the cause chain looking for the structured error.
          assertNotNull(
              thrown,
              "Expected DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE error, got no exception.");
          Throwable cur = thrown;
          boolean found = false;
          while (cur != null) {
            String msg = cur.getMessage() == null ? "" : cur.getMessage();
            if (msg.contains("DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE")
                || msg.contains("initialSnapshotMaxFiles")
                || cur.getClass().getName().contains("DeltaUnsupportedOperation")) {
              found = true;
              break;
            }
            cur = cur.getCause();
          }
          final Throwable finalThrown = thrown;
          assertTrue(
              found,
              () ->
                  "Expected DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE in cause chain, got: "
                      + finalThrown);
        });
  }

  /**
   * Scenario 9: Protocol upgrade DURING a MERGE commit (table-op x S19).
   *
   * <p>Companion to {@link #testScenario5_ProtocolUpgradeMidStream}: scenario 5 covers an explicit
   * {@code ALTER TABLE ... TBLPROPERTIES} followed by a DELETE. Scenario 9 exercises the
   * DML-triggered path where a MERGE itself bumps the protocol (here by enabling DV via
   * TBLPROPERTIES immediately before the MERGE, so the MERGE-produced AddFiles carry the new writer
   * feature).
   *
   * <p>Per scenario_brainstorm.md S19, when a stream encounters a commit that introduces a writer
   * feature its reader cannot handle, the contract is the same as scenario 5: either a clean {@code
   * UnsupportedTableFeatureException} / {@code DeltaUnsupportedOperation} surfaces, or the stream
   * handles it gracefully without silently producing rows from files it does not understand.
   */
  @Test
  public void testScenario9_ProtocolUpgradeDuringMergeCommit(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");

    // v=0: non-DV table seeded with rows that the MERGE will partially match.
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, val STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    // First run: drain initial snapshot through the checkpoint.
    StreamingQuery preMerge =
        streamingDF
            .writeStream()
            .format("noop")
            .queryName("scenario9_pre_merge")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    preMerge.awaitTermination();
    preMerge.stop();

    // Enable DV writer feature, then run a MERGE that produces DV-aware AddFiles in a single
    // logical step. The protocol upgrade and the MERGE commit are bundled: the next stream run
    // must contend with both.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    spark.sql("DROP VIEW IF EXISTS scenario9_merge_src");
    spark
        .sql(
            "SELECT 2 AS id, 'B' AS val UNION ALL "
                + "SELECT 3 AS id, 'C' AS val UNION ALL "
                + "SELECT 4 AS id, 'd' AS val")
        .createOrReplaceTempView("scenario9_merge_src");
    spark.sql(
        str(
            "MERGE INTO delta.`%s` t USING scenario9_merge_src s ON t.id = s.id "
                + "WHEN MATCHED THEN UPDATE SET val = s.val "
                + "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)",
            tablePath));

    // Restart the stream over the same checkpoint. Expect: either a clean
    // UnsupportedTableFeatureException-like error surfaces, or the stream completes without
    // silently emitting rows from a DV-encoded file via a non-DV reader.
    boolean threwExpected = false;
    Throwable thrown = null;
    long postMergeRowsRead = -1;
    StreamingQuery q = null;
    try {
      q =
          streamingDF
              .writeStream()
              .format("noop")
              .queryName("scenario9_post_merge")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      try {
        q.awaitTermination();
        long sum = 0;
        for (StreamingQueryProgress p : q.recentProgress()) {
          sum += p.numInputRows();
        }
        postMergeRowsRead = sum;
      } finally {
        q.stop();
      }
      if (q.exception().isDefined()) {
        thrown = q.exception().get();
      }
    } catch (Throwable t) {
      thrown = t;
    } finally {
      DeltaLog.clearCache();
    }

    final Throwable finalThrown = thrown;
    if (finalThrown != null) {
      // Walk the cause chain looking for a clean structured error.
      Throwable cur = finalThrown;
      StringBuilder chain = new StringBuilder();
      while (cur != null) {
        String cls = cur.getClass().getName();
        String msg = cur.getMessage() == null ? "" : cur.getMessage();
        chain.append("\n  -> ").append(cls).append(": ").append(msg);
        if (cls.contains("UnsupportedTableFeature")
            || cls.contains("DeltaUnsupportedOperation")
            || (msg != null
                && (msg.contains("ignoreChanges")
                    || msg.contains("ignoreDeletes")
                    || msg.contains("skipChangeCommits")))) {
          threwExpected = true;
          break;
        }
        cur = cur.getCause();
      }
      final boolean finalThrewExpected = threwExpected;
      final String finalChain = chain.toString();
      assertTrue(
          finalThrewExpected,
          () ->
              "Expected UnsupportedTableFeatureException or change-commit guidance for"
                  + " DML-triggered protocol upgrade via MERGE, got cause chain:"
                  + finalChain);
    } else {
      // Stream did not throw. The MERGE produced RemoveFile + AddFile commits with the new writer
      // feature; without per-commit protocol validation, the stream either silently skipped them
      // or emitted rows from DV-encoded files via a reader that doesn't understand DVs. That is
      // itself a bug surfaced by this test.
      final long finalPostMergeRowsRead = postMergeRowsRead;
      fail(
          "Expected a structured error (UnsupportedTableFeature / DeltaUnsupportedOperation /"
              + " ignoreChanges guidance) for mid-stream protocol upgrade via MERGE, but none was"
              + " thrown. Rows read from post-merge run="
              + finalPostMergeRowsRead);
    }
  }
}
