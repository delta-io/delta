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

import io.delta.spark.internal.v2.utils.IctTestUtils;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end DSv2 streaming tests for tables with {@code delta.enableInCommitTimestamps}.
 *
 * <p>The cross-product test matrix only covers ICT helper-level behavior; this suite exercises full
 * streaming queries against ICT-enabled tables to find bugs in {@code startingTimestamp}/{@code
 * startingVersion} resolution, restart semantics, sub-second skew, mtime drift, mid-table ICT
 * enablement, AvailableNow trigger, deletion vectors, and column mapping.
 *
 * <p>DSv1 reference: {@code DeltaSourceSuite.testQuietly("startingTimestamp")} in {@code
 * spark/src/test/scala/org/apache/spark/sql/delta/DeltaSourceSuite.scala}.
 */
public class V2StreamingIctTest extends V2TestBase {

  /** Format epoch millis as a "yyyy-MM-dd HH:mm:ss.SSS" string in the session timezone. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Create an ICT-enabled table and append {@code numCommits} commits with timestamps. */
  private void createIctTableWithCommits(String tablePath, long[] timestamps) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta "
                + "TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    for (int i = 0; i < timestamps.length; i++) {
      long rangeStart = (long) i * 10;
      long rangeEnd = rangeStart + 10;
      spark.range(rangeStart, rangeEnd).write().format("delta").mode("append").save(tablePath);
      // Versions: 0=CREATE TABLE, 1..N=appends.
      IctTestUtils.modifyCommitTimestamp(log, /* version= */ i + 1, timestamps[i]);
    }
  }

  // ===================================================================================
  // Case 1: ICT + startingTimestamp == known commit's ICT -> stream starts from that commit
  // ===================================================================================
  @Test
  public void case1_startingTimestampEqualsCommitIct(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(t2))
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "ict_case1");
    // Expect rows from version 2 only (ids 10..19).
    assertEquals(10, rows.size(), () -> "Rows: " + rows);
  }

  // ===================================================================================
  // Case 2: ICT + startingTimestamp between two commits -> next-commit semantics
  // ===================================================================================
  @Test
  public void case2_startingTimestampBetweenCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = 1700000000000L;
    long t2 = t1 + 120_000L;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    long between = t1 + 30_000L; // strictly between t1 and t2
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(between))
            .table(str("dsv2.delta.`%s`", tablePath));

    List<Row> rows = processStreamingQuery(df, "ict_case2");
    // Next-commit semantics: between two commits should resolve to t2 (version 2): ids 10..19.
    assertEquals(
        10,
        rows.size(),
        () -> "Expected next-commit (10 rows from v2) but got " + rows.size() + ": " + rows);
  }

  // ===================================================================================
  // Case 3: ICT sub-second skew: two commits within the same wall-clock millisecond
  // ===================================================================================
  @Test
  public void case3_subsecondSameMillisDisambiguation(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long sameMs = 1700000000000L;
    // Two commits with the SAME ICT millisecond. ICT requires monotonicity, so the second commit
    // is forced to ICT(v1)+1. We model the "same ms" intent by setting both ICT and mtime to
    // sameMs initially, then set v2's ICT to sameMs+1 to satisfy monotonicity.
    createIctTableWithCommits(tablePath, new long[] {sameMs, sameMs + 1});

    // Asking for the exact ms should land on v1 (first commit at-or-before).
    Dataset<Row> dfA =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(sameMs))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rowsA = processStreamingQuery(dfA, "ict_case3_a");
    assertEquals(20, rowsA.size(), () -> "starting@sameMs should include both commits: " + rowsA);

    // Asking for sameMs+1 should land on v2 (next-commit).
    Dataset<Row> dfB =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(sameMs + 1))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rowsB = processStreamingQuery(dfB, "ict_case3_b");
    assertEquals(10, rowsB.size(), () -> "starting@sameMs+1 should include only v2: " + rowsB);
  }

  // ===================================================================================
  // Case 4: ICT vs filesystem mtime drift -> DSv2 must use ICT, not mtime
  // ===================================================================================
  @Test
  public void case4_mtimeDriftUsesIct(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    // Tamper: set v2's filesystem mtime to BEFORE t1 (e.g., a restore overwrote it).
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long bogusMtime = t1 - 600_000L;
    IctTestUtils.setFileMtimeOnly(log, /* version= */ 2, bogusMtime);

    // Ask for t2: with ICT, should still resolve to v2 (10 rows). With mtime, would resolve to
    // v0 because v2's mtime is now before t1 -> would return more rows (entire table).
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(t2))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "ict_case4");
    assertEquals(
        10,
        rows.size(),
        () -> "DSv2 should consult ICT (10 rows from v2); mtime-based would return more: " + rows);
  }

  // ===================================================================================
  // Case 5: ICT + startingTimestamp = future -> error consistent with DSv1
  // ===================================================================================
  @Test
  public void case5_startingTimestampFutureErrors(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    long future = t2 + 10 * 365L * 24 * 3600 * 1000L; // far future
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(future))
            .table(str("dsv2.delta.`%s`", tablePath));

    StreamingQueryException ex =
        assertThrows(StreamingQueryException.class, () -> processStreamingQuery(df, "ict_case5"));
    String msg = ex.getMessage() == null ? "" : ex.getMessage();
    String causeMsg = ex.getCause() == null ? "" : String.valueOf(ex.getCause());
    assertTrue(
        msg.contains("after the latest")
            || msg.contains("is after")
            || causeMsg.contains("after the latest")
            || causeMsg.contains("is after"),
        () -> "Expected DSv1-style 'after the latest' error, got: " + msg + " / " + causeMsg);
  }

  // ===================================================================================
  // Case 6: Restart with startingTimestamp set -> should ignore and resume from checkpoint
  // ===================================================================================
  @Test
  public void case6_restartIgnoresStartingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    File outputDir = new File(deltaTablePath, "_out");
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    // Parquet sink (instead of memory sink) so checkpoint recovery is supported across restart.
    // Both queries share the same output dir; we measure what each run added via cumulative counts.
    Dataset<Row> df1 = spark.readStream().option("startingTimestamp", formatTs(t1)).table(dsv2Ref);
    StreamingQuery q1 =
        df1.writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();
    q1.processAllAvailable();
    long initialRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
    q1.stop();
    DeltaLog.clearCache();
    assertEquals(20, initialRows, "Initial run should process 20 rows");

    // Append a new commit (v3).
    long t3 = t2 + 60_000L;
    spark.range(20, 30).write().format("delta").mode("append").save(tablePath);
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, t3);

    // Restart with startingTimestamp = t1 again. Per DSv1 semantics, restart with checkpoint
    // ignores startingTimestamp and resumes from where we left off (i.e., should only read v3).
    Dataset<Row> df2 = spark.readStream().option("startingTimestamp", formatTs(t1)).table(dsv2Ref);
    StreamingQuery q2 =
        df2.writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();
    try {
      q2.processAllAvailable();
      long totalRows = spark.read().parquet(outputDir.getAbsolutePath()).count();
      long restartRows = totalRows - initialRows;
      assertEquals(
          10,
          restartRows,
          () ->
              "Restart should ignore startingTimestamp and only process v3 (10 rows), got "
                  + restartRows);
    } finally {
      q2.stop();
      DeltaLog.clearCache();
    }
  }

  // ===================================================================================
  // Case 7: ICT enabled mid-history (commits 0-2 non-ICT, commits 3+ ICT)
  // ===================================================================================
  @Test
  public void case7_ictEnabledMidHistory(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    // Create non-ICT table first.
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT) USING delta", tablePath));
    // v1, v2 = non-ICT appends.
    spark.range(0, 10).write().format("delta").mode("append").save(tablePath);
    spark.range(10, 20).write().format("delta").mode("append").save(tablePath);
    // Set deterministic mtimes for v0, v1, v2 so the non-ICT search has a
    // monotonically-increasing wall-clock history. (v0's natural mtime is "now()" — far in the
    // future relative to the synthetic v1/v2 mtimes, which would corrupt monotonization.)
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long base = 1700000000000L;
    long v0Mtime = base - 60_000L;
    long v1Mtime = base;
    long v2Mtime = base + 60_000L;
    IctTestUtils.setFileMtimeOnly(log, 0L, v0Mtime);
    IctTestUtils.setFileMtimeOnly(log, 1L, v1Mtime);
    IctTestUtils.setFileMtimeOnly(log, 2L, v2Mtime);
    // v3: ALTER TABLE turns on ICT (this commit itself enables ICT mid-history).
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    long v3Ict = v2Mtime + 120_000L;
    IctTestUtils.modifyCommitTimestamp(log, 3L, v3Ict);
    // v4: post-ICT data append.
    spark.range(20, 30).write().format("delta").mode("append").save(tablePath);
    long v4Ict = v3Ict + 60_000L;
    IctTestUtils.modifyCommitTimestamp(log, 4L, v4Ict);

    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    // 7a: startingTimestamp = v2's mtime should land on v2 (read v2..v4 = 20 rows).
    Dataset<Row> dfA =
        spark.readStream().option("startingTimestamp", formatTs(v2Mtime)).table(dsv2Ref);
    List<Row> rowsA = processStreamingQuery(dfA, "ict_case7_a");
    assertEquals(
        20,
        rowsA.size(),
        () -> "starting@v2.mtime should read v2..v4 (20 rows), got: " + rowsA.size());

    // 7b: startingTimestamp = v3's ICT should land on v3 (read v3..v4 = 10 rows; v3 is metadata-
    // only so 10 rows from v4).
    Dataset<Row> dfB =
        spark.readStream().option("startingTimestamp", formatTs(v3Ict)).table(dsv2Ref);
    List<Row> rowsB = processStreamingQuery(dfB, "ict_case7_b");
    assertEquals(
        10,
        rowsB.size(),
        () -> "starting@v3.ICT should read v3..v4 (10 rows from v4), got: " + rowsB.size());
  }

  // ===================================================================================
  // Case 8: ICT x Trigger.AvailableNow
  // ===================================================================================
  @Test
  public void case8_ictAvailableNow(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    long t3 = t2 + 60_000L;
    createIctTableWithCommits(tablePath, new long[] {t1, t2, t3});

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(t2))
            .table(str("dsv2.delta.`%s`", tablePath));
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName("ict_case8")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      q.processAllAvailable();
      // Block until query terminates (AvailableNow self-terminates after consuming snapshot).
      assertTrue(q.awaitTermination(60_000L), "AvailableNow query should terminate");
      long rows = spark.sql("SELECT COUNT(*) FROM ict_case8").collectAsList().get(0).getLong(0);
      // starting@t2 -> read v2 + v3 = 20 rows.
      assertEquals(
          20, rows, () -> "AvailableNow + ICT starting@t2 should read 20 rows, got " + rows);
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  // ===================================================================================
  // Case 9: ICT x Deletion Vectors
  // ===================================================================================
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
  public void case9_ictWithDeletionVectors(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta TBLPROPERTIES ("
                + "'delta.enableInCommitTimestamps' = 'true',"
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    long t1 = 1700000000000L;
    spark.range(0, 10).coalesce(1).write().format("delta").mode("append").save(tablePath);
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);

    // Apply DV: delete id=0,1,2.
    long t2 = t1 + 60_000L;
    spark.sql(str("DELETE FROM delta.`%s` WHERE id < 3", tablePath));
    IctTestUtils.modifyCommitTimestamp(log, 2L, t2);

    // Stream from t1 with ICT-enabled DV table.
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(t1))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "ict_case9");
    // v1 produces 10 rows in initial snapshot; the DV at v2 filters 0,1,2 leaving 7 rows.
    assertEquals(7, rows.size(), () -> "ICT + DV: expected 7 rows after DV applied, got: " + rows);
  }

  // ===================================================================================
  // Case 10: ICT x Column mapping
  // ===================================================================================
  @Test
  public void case10_ictWithColumnMapping(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta TBLPROPERTIES ("
                + "'delta.enableInCommitTimestamps' = 'true',"
                + "'delta.columnMapping.mode' = 'name',"
                + "'delta.minReaderVersion' = '2',"
                + "'delta.minWriterVersion' = '5')",
            tablePath));
    long t1 = 1700000000000L;
    long t2 = t1 + 60_000L;
    spark.range(0, 10).write().format("delta").mode("append").save(tablePath);
    spark.range(10, 20).write().format("delta").mode("append").save(tablePath);
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);
    IctTestUtils.modifyCommitTimestamp(log, 2L, t2);

    // Stream from t2 (next-commit semantics: lands on v2 -> 10 rows).
    Dataset<Row> df =
        spark
            .readStream()
            .option("startingTimestamp", formatTs(t2))
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> rows = processStreamingQuery(df, "ict_case10");
    assertEquals(
        10, rows.size(), () -> "ICT + column-mapping: expected 10 rows from v2, got: " + rows);
  }
}
