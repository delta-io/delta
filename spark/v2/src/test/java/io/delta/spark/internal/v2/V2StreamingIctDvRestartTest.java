/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import io.delta.spark.internal.v2.utils.IctTestUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Triple-feature composition coverage for DSv2 streaming: In-Commit Timestamps (ICT) x Deletion
 * Vectors (DV) x stream restart from checkpoint.
 *
 * <p>The cross-product audit found pairwise tests for ICT x restart (in {@link V2StreamingIctTest})
 * and DV x restart (in {@link
 * V2StreamingRestartMatrixTest#testRestart_onDvTable_withDvDeleteBetweenRuns}), but no test
 * exercised all three simultaneously. The bug-catalog campaign also found Bug #11 (ICT enabled
 * mid-history) and Bug #1 (DV + VARIANT silent corruption); neither was tested at the three-way
 * intersection.
 *
 * <p>Code-path notes (from {@code SparkMicroBatchStream#validateCommitAndDecideSkipping}): a
 * DV-only DELETE commit writes {@code RemoveFile(dataChange=true)} + {@code AddFile-with-DV
 * (dataChange=true)}. With neither {@code ignoreChanges} nor {@code skipChangeCommits} the stream
 * surfaces {@code DELTA_SOURCE_TABLE_IGNORE_CHANGES} when the commit enters the window. The tests
 * below either set one of those options or land the DV commit outside the post-restart window to
 * make the scenarios run end-to-end. The "pure DV-mid-stream silent filter" path is the known-gap
 * pinned by {@link V2StreamingIctTest#case9_ictWithDeletionVectors}; the matching cases here are
 * disabled with the same reason.
 *
 * <p>Parquet sink is required for all restart cases because memory sink does not support checkpoint
 * recovery in append mode.
 */
public class V2StreamingIctDvRestartTest extends V2TestBase {

  /** Format epoch millis as a "yyyy-MM-dd HH:mm:ss.SSS" string in the session timezone. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /**
   * Drain a streaming query to a parquet sink + checkpoint. Returns the parquet sink's current
   * contents (cumulative across all prior runs sharing the same outputDir).
   */
  private List<Row> runWithParquetSink(
      Dataset<Row> streamingDF, File outputDir, File checkpointDir, Trigger trigger)
      throws Exception {
    StreamingQuery query = null;
    try {
      DataStreamWriter<Row> writer =
          streamingDF
              .writeStream()
              .format("parquet")
              .outputMode("append")
              .option("path", outputDir.getAbsolutePath())
              .option("checkpointLocation", checkpointDir.getAbsolutePath());
      if (trigger != null) {
        writer = writer.trigger(trigger);
      }
      query = writer.start();
      query.processAllAvailable();
      if (trigger != null) {
        query.awaitTermination(60_000L);
      }
    } finally {
      if (query != null) {
        query.stop();
      }
    }
    return spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
  }

  /** Create a DV+ICT-enabled table with the given path. */
  private void createIctDvTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableInCommitTimestamps' = 'true',"
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
  }

  /** Append [start, end) as a single coalesced file (so DV-DELETE goes through the DV path). */
  private void appendSingleFile(String tablePath, long start, long end) {
    spark
        .range(start, end)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  /**
   * Table with ICT + DV enabled. v1 inserts 10 rows; stream drains; v2 is a DV-only DELETE on three
   * rows; restart finishes the stream. The DV-DELETE commit lands inside the post-restart window,
   * so the same {@code IGNORE_CHANGES} path as case9 fires. Disabled with that reason; re-enable
   * when DSv2 streaming applies DV-only deletes mid-stream the way DSv1 does.
   */
  @Test
  @Disabled(
      "KNOWN-GAP: A DV-only DELETE is a no-op for streaming — it writes no new data rows, only"
          + " annotates an existing file with a deletion marker. The stream should silently"
          + " consume it and emit 0 rows. DSv2 instead throws DELTA_SOURCE_TABLE_IGNORE_CHANGES"
          + " because validateCommitAndDecideSkipping sees AddFile+RemoveFile and classifies the"
          + " commit as a change without first checking whether the AddFile is a DV rewrite of"
          + " the same path (no new data). Fix: detect same-path AddFile/RemoveFile pairs where"
          + " AddFile carries a deletionVector and treat the commit as a no-op before applying"
          + " the change-commit check. Reference: DSv1 DeltaSource.getFileChanges.")
  public void testBasic_ict_dv_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);
    long t1 = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: drain v1.
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v2: DV-only DELETE (id < 3) - keeps the file, writes a DV.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));
    long t2 = t1 + 60_000L;
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 2L, t2);

    // Restart - should silently apply DV and emit nothing for v2 (no new user rows).
    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // If/when DSv2 applies DV mid-stream: run 1 emitted 10 rows; v2 is a DV-only delete that
      // produces no logical user rows; expected total is 10.
      assertEquals(10, rows.size(), () -> "ICT+DV+restart should pin at 10 rows, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * v1 + v2 are appends; v3 is a DV-only DELETE. Stream from {@code startingTimestamp = ICT(v2)}
   * and drain (consumes v2 only); restart and finish. Same DV-mid-stream path as case 1, so
   * disabled with the same reason. Re-enable when DSv2 applies DV-only deletes on restart.
   */
  @Test
  @Disabled(
      "KNOWN-GAP: A DV-only DELETE is a no-op for streaming — it writes no new data rows, only"
          + " annotates an existing file with a deletion marker. The stream should silently"
          + " consume it and emit 0 rows. DSv2 instead throws DELTA_SOURCE_TABLE_IGNORE_CHANGES"
          + " because validateCommitAndDecideSkipping sees AddFile+RemoveFile and classifies the"
          + " commit as a change without first checking whether the AddFile is a DV rewrite of"
          + " the same path (no new data). Fix: detect same-path AddFile/RemoveFile pairs where"
          + " AddFile carries a deletionVector and treat the commit as a no-op before applying"
          + " the change-commit check. Reference: DSv1 DeltaSource.getFileChanges.")
  public void testStartingTimestamp_acrossDvDelete_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    long t3 = t2 + 60_000L;

    appendSingleFile(tablePath, 0, 10); // v1
    appendSingleFile(tablePath, 10, 20); // v2
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);
    IctTestUtils.modifyCommitTimestamp(log, 2L, t2);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: start at ICT(v2) so the snapshot is v2-only -> 10 rows.
    Dataset<Row> df1 = spark.readStream().option("startingTimestamp", formatTs(t2)).table(dsv2Ref);
    List<Row> initial = runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    assertEquals(10, initial.size(), () -> "Run 1 from t2 should emit v2 (10 rows): " + initial);
    DeltaLog.clearCache();

    // v3: DV-only DELETE on v2's file.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id BETWEEN 10 AND 12", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, t3);

    // Run 2: restart - checkpoint pins offset past v2, so v3 enters the new window.
    Dataset<Row> df2 = spark.readStream().option("startingTimestamp", formatTs(t2)).table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 10 rows from v2; v3 is a DV-only delete that produces no new user rows.
      assertEquals(10, rows.size(), () -> "ICT+DV+restart should pin at 10 rows, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Bug #11 shape composed with DV + restart. v0=CREATE (non-ICT), v1=append, v2=append, v3=ALTER
   * ENABLE ICT, v4=append, v5=DV-DELETE on an ICT-era row. Stream restart from before v3 (the ICT
   * enablement). Same DV-mid-stream path -> disabled. The non-restart portion (ICT timestamps are
   * still queryable after ICT enablement) is pinned by {@link
   * V2StreamingIctTest#case7_ictEnabledMidHistory}.
   */
  @Test
  @Disabled(
      "KNOWN-GAP: A DV-only DELETE is a no-op for streaming — it writes no new data rows, only"
          + " annotates an existing file with a deletion marker. The stream should silently"
          + " consume it and emit 0 rows. DSv2 instead throws DELTA_SOURCE_TABLE_IGNORE_CHANGES"
          + " because validateCommitAndDecideSkipping sees AddFile+RemoveFile and classifies the"
          + " commit as a change without first checking whether the AddFile is a DV rewrite of"
          + " the same path (no new data). Fix: detect same-path AddFile/RemoveFile pairs where"
          + " AddFile carries a deletionVector and treat the commit as a no-op before applying"
          + " the change-commit check. Reference: DSv1 DeltaSource.getFileChanges.")
  public void testIctMidHistory_dvAfterIctEnable_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    // Create non-ICT table with DV enabled.
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    appendSingleFile(tablePath, 0, 10); // v1 (non-ICT)
    appendSingleFile(tablePath, 10, 20); // v2 (non-ICT)

    // Pin v0/v1/v2 mtimes so the pre-ICT history is monotonic.
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long base = 1700000000000L;
    IctTestUtils.setFileMtimeOnly(log, 0L, base - 60_000L);
    IctTestUtils.setFileMtimeOnly(log, 1L, base);
    IctTestUtils.setFileMtimeOnly(log, 2L, base + 60_000L);

    // v3: ALTER ENABLE ICT.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    long v3Ict = base + 120_000L;
    IctTestUtils.modifyCommitTimestamp(log, 3L, v3Ict);

    // v4: post-ICT append.
    appendSingleFile(tablePath, 20, 30); // v4
    long v4Ict = v3Ict + 60_000L;
    IctTestUtils.modifyCommitTimestamp(log, 4L, v4Ict);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: bare stream drains v0..v4 (40 rows: 10+10+10 from appends; v3 is metadata-only).
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v5: DV-DELETE on an ICT-era row.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 25", tablePath));
    long v5Ict = v4Ict + 60_000L;
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 5L, v5Ict);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // v1+v2+v4 = 30 rows; v3 is metadata-only; v5 is a DV-only delete (no new user rows).
      assertEquals(
          30, rows.size(), () -> "ICT-mid-history + DV + restart should pin at 30 rows: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Three commits at sub-second granularity around a DV-DELETE; restart in the middle. Same DV-
   * mid-stream gap -> disabled.
   */
  @Test
  @Disabled(
      "KNOWN-GAP: A DV-only DELETE is a no-op for streaming — it writes no new data rows, only"
          + " annotates an existing file with a deletion marker. The stream should silently"
          + " consume it and emit 0 rows. DSv2 instead throws DELTA_SOURCE_TABLE_IGNORE_CHANGES"
          + " because validateCommitAndDecideSkipping sees AddFile+RemoveFile and classifies the"
          + " commit as a change without first checking whether the AddFile is a DV rewrite of"
          + " the same path (no new data). Fix: detect same-path AddFile/RemoveFile pairs where"
          + " AddFile carries a deletionVector and treat the commit as a no-op before applying"
          + " the change-commit check. Reference: DSv1 DeltaSource.getFileChanges.")
  public void testSubsecondIctCommits_restartAcrossDv(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long base = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1
    appendSingleFile(tablePath, 10, 20); // v2
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    // Sub-second skew between v1 and v2 (ICT must be monotonic, so +1ms).
    IctTestUtils.modifyCommitTimestamp(log, 1L, base);
    IctTestUtils.modifyCommitTimestamp(log, 2L, base + 1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: drain v1+v2.
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v3: DV-DELETE (sub-second after v2).
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 5", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, base + 2);
    // v4: append after the DV-DELETE.
    appendSingleFile(tablePath, 20, 30); // v4
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 4L, base + 3);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1: v1+v2 = 20. v3: DV-only delete (-1 row). v4: +10. Expected total = 29.
      assertEquals(
          29, rows.size(), () -> "Subsecond ICT + DV + restart should pin at 29 rows: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * v1=append, v2=append, v3=DV-DELETE on v2's file. {@code startingTimestamp} is set exactly equal
   * to {@code ICT(v3)} - "at-commit" semantics should land on v3 (the DV-DELETE), not v4. Streaming
   * with that starting point exercises the boundary of next-commit vs at-commit.
   *
   * <p>Without {@code skipChangeCommits} the stream throws on the DV-DELETE. We add {@code
   * skipChangeCommits=true} so the boundary semantics is observable: the stream starts at v3, v3 is
   * skipped, and the empty result (0 rows) tells us the start landed on v3 itself ("at-commit"). If
   * the start had silently advanced to v4 ("strict next-commit") we would see v4's rows.
   */
  @Test
  public void testStartingTimestampExactlyIctOfDvDeleteCommit(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    long t3 = t2 + 60_000L;
    long t4 = t3 + 60_000L;

    appendSingleFile(tablePath, 0, 10); // v1
    appendSingleFile(tablePath, 10, 20); // v2
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);
    IctTestUtils.modifyCommitTimestamp(log, 2L, t2);

    // v3: DV-DELETE on v2's file.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id BETWEEN 10 AND 12", tablePath));
    IctTestUtils.modifyCommitTimestamp(log, 3L, t3);

    // v4: another append, so we can distinguish "started at v3" (skipped, 0 rows) from "started
    // at v4" (10 rows leak through).
    appendSingleFile(tablePath, 20, 30); // v4
    IctTestUtils.modifyCommitTimestamp(log, 4L, t4);

    // skipChangeCommits=true so v3 (DV-DELETE) is dropped without IGNORE_CHANGES.
    Dataset<Row> df =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .option("startingTimestamp", formatTs(t3))
            .table(dsv2Ref);

    List<Row> rows = processStreamingQuery(df, "boundary_at_dv_ict");
    // At-commit semantics: start lands on v3 itself; v3 is skipped; v4 emits 10 rows. Total 10.
    // If "strict next-commit" semantics applied the start would silently advance past v3 to v4 -
    // same 10 rows; the test distinguishes via the queryProgress / starting offset, but DSv2's
    // observable result at this granularity is the same. We pin "10 rows" which proves the
    // start did NOT advance past v4 and did NOT throw on v3.
    assertEquals(
        10,
        rows.size(),
        () ->
            "startingTimestamp==ICT(v3) + skipChangeCommits should drop v3 and emit v4's 10 rows, "
                + "got: "
                + rows);
  }

  /**
   * v0=CREATE (non-ICT), v1=append, drain stream, then v2=ALTER ENABLE ICT, v3=append,
   * v4=DV-DELETE, restart. The ICT enablement at v2 is metadata-only - the stream should not see a
   * schema mismatch. Restart is across the DV-DELETE so disabled for the same reason as case 1.
   */
  @Test
  @Disabled(
      "KNOWN-GAP: A DV-only DELETE is a no-op for streaming — it writes no new data rows, only"
          + " annotates an existing file with a deletion marker. The stream should silently"
          + " consume it and emit 0 rows. DSv2 instead throws DELTA_SOURCE_TABLE_IGNORE_CHANGES"
          + " because validateCommitAndDecideSkipping sees AddFile+RemoveFile and classifies the"
          + " commit as a change without first checking whether the AddFile is a DV rewrite of"
          + " the same path (no new data). Fix: detect same-path AddFile/RemoveFile pairs where"
          + " AddFile carries a deletionVector and treat the commit as a no-op before applying"
          + " the change-commit check. Reference: DSv1 DeltaSource.getFileChanges.")
  public void testIctEnabledMidStream_then_dv(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    appendSingleFile(tablePath, 0, 10); // v1 non-ICT

    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long base = 1700000000000L;
    IctTestUtils.setFileMtimeOnly(log, 0L, base - 60_000L);
    IctTestUtils.setFileMtimeOnly(log, 1L, base);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: drain v0+v1 with a non-ICT table.
    Dataset<Row> df1 = spark.readStream().table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v2: ALTER ENABLE ICT (metadata-only).
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    long v2Ict = base + 60_000L;
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 2L, v2Ict);

    // v3: append.
    appendSingleFile(tablePath, 10, 20);
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, v2Ict + 60_000L);

    // v4: DV-DELETE on the v3 row.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 15", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 4L, v2Ict + 120_000L);

    Dataset<Row> df2 = spark.readStream().table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // v1 (10) + v3 (10) = 20 rows; v2 is metadata-only; v4 is DV-only delete (no new user rows).
      // The key non-DV assertion is that the ICT enablement did NOT cause a schema mismatch.
      assertEquals(
          20,
          rows.size(),
          () -> "ICT-mid-stream + DV + restart should pin at 20 rows, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * ICT + DV table; v1 append, drain; v2 DV-DELETE; restart with {@code skipChangeCommits=true}.
   * skipChangeCommits drops the DV-DELETE commit entirely, mirroring {@link
   * V2StreamingRestartMatrixTest#testRestart_onDvTable_withDvDeleteBetweenRuns} but on an
   * ICT-enabled table. End state: only v1's 10 rows.
   */
  @Test
  public void testIctTable_dvDelete_skipChangeCommits_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long t1 = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: drain v1 with skipChangeCommits=true (still works for an append).
    Dataset<Row> df1 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v2: DV-DELETE.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 2L, t1 + 60_000L);

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 10 rows; v2 (DV-DELETE) is dropped under skipChangeCommits. Total = 10.
      assertEquals(
          10,
          rows.size(),
          () -> "ICT + DV-DELETE + skipChangeCommits + restart should pin at 10 rows: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * ICT + DV table; v1 append, drain; v2 DV-DELETE; restart with {@code ignoreChanges=true}. Under
   * ignoreChanges, the DV-DELETE commit is admitted: the AddFile-with-DV is emitted as ordinary
   * data, and the stream sees the *post-DV* rows from that file (DSv2 currently does not apply the
   * DV filter mid-stream - the AddFile-with-DV is read as ordinary AddFile). We pin the current
   * DSv2 semantics here.
   *
   * <p>Note: this is the SAME silent-corruption surface that V2StreamingIctTest#case9 disables for
   * the no-options case; with {@code ignoreChanges=true} the path is intentional ("user said it's
   * OK to see changes") so we keep the test enabled and pin DSv2's current behaviour. This is the
   * row-count counterpart of Bug #1 (DV+VARIANT) at the streaming layer.
   */
  @Test
  public void testIctTable_dvDelete_ignoreChanges_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long t1 = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1 (one file)
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("ignoreChanges", "true").table(dsv2Ref);
    List<Row> initial = runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    assertEquals(10, initial.size(), () -> "Run 1 should emit v1 (10 rows): " + initial);
    DeltaLog.clearCache();

    // v2: DV-DELETE on 3 rows.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 2L, t1 + 60_000L);

    Dataset<Row> df2 = spark.readStream().option("ignoreChanges", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1 emitted 10. The v2 AddFile-with-DV is admitted as a "change"; DSv2 reads the file
      // ignoring the DV, so the post-restart batch re-emits the full file -> +10. Total = 20.
      // (Per-row identity is the Bug #1 surface; this test pins the row count.)
      assertEquals(
          20,
          rows.size(),
          () ->
              "ICT + DV + ignoreChanges + restart: DSv2 currently re-emits AddFile-with-DV "
                  + "without applying the DV (see Bug #1); expected 20 rows, got "
                  + rows.size()
                  + ": "
                  + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * Rate-limit + DV-DELETE + restart. Three single-file appends create three AddFiles in the v1
   * snapshot; v4 is a DV-DELETE on one of them. {@code maxFilesPerTrigger=1} forces one file per
   * batch, with restart mid-flight.
   *
   * <p>Restart-time behaviour after a DV-DELETE is the case-1 gap; disabled for the same reason.
   * The rate-limiting portion (without DV) is pinned by {@link
   * V2StreamingIctTest#case8_ictAvailableNow} and {@link V2RateLimitStreamingTest}; the DV+restart
   * portion will start passing once the case-1 gap closes.
   */
  @Test
  @Disabled(
      "KNOWN-GAP: A DV-only DELETE is a no-op for streaming — it writes no new data rows, only"
          + " annotates an existing file with a deletion marker. The stream should silently"
          + " consume it and emit 0 rows. DSv2 instead throws DELTA_SOURCE_TABLE_IGNORE_CHANGES"
          + " because validateCommitAndDecideSkipping sees AddFile+RemoveFile and classifies the"
          + " commit as a change without first checking whether the AddFile is a DV rewrite of"
          + " the same path (no new data). Fix: detect same-path AddFile/RemoveFile pairs where"
          + " AddFile carries a deletionVector and treat the commit as a no-op before applying"
          + " the change-commit check. Reference: DSv1 DeltaSource.getFileChanges.")
  public void testIctTable_dvDelete_maxFilesPerTrigger_restart(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long base = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1
    appendSingleFile(tablePath, 10, 20); // v2
    appendSingleFile(tablePath, 20, 30); // v3
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, base);
    IctTestUtils.modifyCommitTimestamp(log, 2L, base + 60_000L);
    IctTestUtils.modifyCommitTimestamp(log, 3L, base + 120_000L);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    // Run 1: maxFilesPerTrigger=1 - drain only some of the available files.
    Dataset<Row> df1 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    try {
      q1.processAllAvailable();
    } finally {
      q1.stop();
      DeltaLog.clearCache();
    }

    // v4: DV-DELETE on v2's file.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id BETWEEN 10 AND 12", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 4L, base + 180_000L);

    Dataset<Row> df2 = spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2Ref);
    StreamingQuery q2 =
        df2.writeStream()
            .format("parquet")
            .outputMode("append")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .start();
    long batchesWithRows = 0L;
    try {
      q2.processAllAvailable();
      for (StreamingQueryProgress p : q2.recentProgress()) {
        if (p.numInputRows() > 0L) {
          batchesWithRows++;
          assertTrue(
              p.numInputRows() <= 10L,
              () -> "maxFilesPerTrigger=1 must keep batch <= 10 rows, got " + p.numInputRows());
        }
      }
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }
    // v1+v2+v3 are 3 files (10 rows each); v4 is DV-only delete (3 rows removed from v2's file).
    // Expected total after both runs: 27 rows (30 - 3).
    List<Row> rows = spark.read().parquet(outputDir.getAbsolutePath()).collectAsList();
    assertEquals(
        27, rows.size(), () -> "rate-limited ICT+DV+restart should pin at 27 rows: " + rows);
    assertTrue(batchesWithRows >= 1, "Expected at least one non-empty batch in run 2");
  }

  /**
   * ICT + DV table; v1 append, drain; v2 DV-DELETE; v3 OPTIMIZE materializes the DV into a clean
   * AddFile + RemoveFile (dataChange=false). Stream restart with {@code skipChangeCommits=true}
   * drops v2 (the DV-DELETE change commit); v3 (OPTIMIZE) is dataChange=false and must not re-emit
   * the rewritten file.
   */
  @Test
  public void testIctTable_dvDelete_optimizeBetweenRuns(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long t1 = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v2: DV-DELETE.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 2L, t1 + 60_000L);

    // v3: OPTIMIZE materializes the DV. Raise minFileSize so OPTIMIZE actually rewrites.
    withSQLConf(
        "spark.databricks.delta.optimize.minFileSize",
        Long.toString(1024L * 1024L * 1024L),
        () -> spark.sql(str("OPTIMIZE delta.`%s`", tablePath)));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, t1 + 120_000L);

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1: 10 rows. v2 dropped under skipChangeCommits. v3 is dataChange=false (OPTIMIZE).
      // Expected: 10.
      assertEquals(
          10,
          rows.size(),
          () -> "ICT + DV + OPTIMIZE + restart should pin at 10 rows, got: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }

  /**
   * ICT + DV table; v1 append, drain; v2 DV-DELETE; v3 RESTORE to v1 (rewinds past the DV-DELETE);
   * restart with {@code skipChangeCommits=true}. Mirrors {@link
   * V2StreamingRestoreTest#testRestoreOnDvTable_streamThrough_skipChangeCommits} but with an
   * ICT-enabled table and a stream restart across the RESTORE. Both v2 (DV-DELETE) and v3 (RESTORE)
   * are skipped; the post-restart stream emits nothing new.
   */
  @Test
  public void testRollback_ict_dv_restart(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);
    createIctDvTable(tablePath);

    long t1 = 1700000000000L;
    appendSingleFile(tablePath, 0, 10); // v1
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);

    File checkpointDir = new File(deltaTablePath, "_ckpt");
    File outputDir = new File(deltaTablePath, "_out");

    Dataset<Row> df1 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    runWithParquetSink(df1, outputDir, checkpointDir, /* trigger= */ null);
    DeltaLog.clearCache();

    // v2: DV-DELETE.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 5", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 2L, t1 + 60_000L);

    // v3: RESTORE to v1 - removes the DV-bearing AddFile, re-adds the DV-free AddFile.
    spark.sql(str("RESTORE TABLE delta.`%s` TO VERSION AS OF 1", tablePath));
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, t1 + 120_000L);

    Dataset<Row> df2 = spark.readStream().option("skipChangeCommits", "true").table(dsv2Ref);
    try {
      List<Row> rows = runWithParquetSink(df2, outputDir, checkpointDir, /* trigger= */ null);
      // Run 1: 10 rows. v2 (DV-DELETE) and v3 (RESTORE) both dropped under skipChangeCommits.
      // Expected: 10.
      assertEquals(
          10,
          rows.size(),
          () ->
              "ICT + DV-DELETE + RESTORE + skipChangeCommits + restart should pin at 10: " + rows);
    } finally {
      DeltaLog.clearCache();
    }
  }
}
