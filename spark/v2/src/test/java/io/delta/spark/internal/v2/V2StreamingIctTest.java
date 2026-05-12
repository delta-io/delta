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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end streaming tests for tables with {@code delta.enableInCommitTimestamps}, run through
 * BOTH the DSv1 and DSv2 streaming readers and asserted for parity.
 *
 * <p>The cross-product test matrix only covers ICT helper-level behavior; this suite exercises full
 * streaming queries against ICT-enabled tables to find bugs in {@code startingTimestamp}/{@code
 * startingVersion} resolution, restart semantics, sub-second skew, mtime drift, mid-table ICT
 * enablement, AvailableNow trigger, deletion vectors, and column mapping.
 *
 * <p>DSv1 reference: {@code DeltaSourceSuite.testQuietly("startingTimestamp")} in {@code
 * spark/src/test/scala/org/apache/spark/sql/delta/DeltaSourceSuite.scala}. DSv1 is treated as the
 * oracle: every test runs both readers with identical options and compares the resulting rows.
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

  /** Build a single-entry options map for a starting-timestamp stream. */
  private static Map<String, String> startingTimestamp(String ts) {
    Map<String, String> opts = new HashMap<>();
    opts.put("startingTimestamp", ts);
    return opts;
  }

  /**
   * Run the same streaming read against the DSv1 ("delta") and DSv2 ("dsv2.delta.`...`") sources
   * with identical options and assert their row sets are equal. DSv1 is the oracle.
   *
   * @return the DSv1 row set (also equal to the DSv2 row set on success), sorted by id.
   */
  private List<Row> assertV1V2StreamingParity(
      String tablePath, String tag, Map<String, String> options) throws Exception {
    DataStreamReader v1Reader = spark.readStream().format("delta");
    for (Map.Entry<String, String> e : options.entrySet()) {
      v1Reader = v1Reader.option(e.getKey(), e.getValue());
    }
    List<Row> v1Rows = sortedById(processStreamingQuery(v1Reader.load(tablePath), tag + "_v1"));

    DataStreamReader v2Reader = spark.readStream();
    for (Map.Entry<String, String> e : options.entrySet()) {
      v2Reader = v2Reader.option(e.getKey(), e.getValue());
    }
    List<Row> v2Rows =
        sortedById(
            processStreamingQuery(v2Reader.table(str("dsv2.delta.`%s`", tablePath)), tag + "_v2"));

    assertEquals(v1Rows.toString(), v2Rows.toString(), tag + ": V1 vs V2 row mismatch");
    return v1Rows;
  }

  /** Sort rows by their first column ("id") so memory-sink ordering doesn't perturb compares. */
  private static List<Row> sortedById(List<Row> rows) {
    List<Row> copy = new ArrayList<>(rows);
    copy.sort(
        (a, b) -> {
          long av = a.getLong(0);
          long bv = b.getLong(0);
          return Long.compare(av, bv);
        });
    return copy;
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

    List<Row> rows =
        assertV1V2StreamingParity(tablePath, "ict_case1", startingTimestamp(formatTs(t2)));
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
    List<Row> rows =
        assertV1V2StreamingParity(tablePath, "ict_case2", startingTimestamp(formatTs(between)));
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
    List<Row> rowsA =
        assertV1V2StreamingParity(tablePath, "ict_case3_a", startingTimestamp(formatTs(sameMs)));
    assertEquals(20, rowsA.size(), () -> "starting@sameMs should include both commits: " + rowsA);

    // Asking for sameMs+1 should land on v2 (next-commit).
    List<Row> rowsB =
        assertV1V2StreamingParity(
            tablePath, "ict_case3_b", startingTimestamp(formatTs(sameMs + 1)));
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
    List<Row> rows =
        assertV1V2StreamingParity(tablePath, "ict_case4", startingTimestamp(formatTs(t2)));
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
    String futureTs = formatTs(future);

    // V1: should error with a DSv1-style "after the latest" message. V1 is the oracle for the
    // error shape.
    Dataset<Row> v1Df =
        spark.readStream().format("delta").option("startingTimestamp", futureTs).load(tablePath);
    StreamingQueryException v1Ex =
        assertThrows(
            StreamingQueryException.class, () -> processStreamingQuery(v1Df, "ict_case5_v1"));
    assertAfterLatestError("V1", v1Ex);

    // V2: should error with an equivalent "after the latest" message.
    Dataset<Row> v2Df =
        spark
            .readStream()
            .option("startingTimestamp", futureTs)
            .table(str("dsv2.delta.`%s`", tablePath));
    StreamingQueryException v2Ex =
        assertThrows(
            StreamingQueryException.class, () -> processStreamingQuery(v2Df, "ict_case5_v2"));
    assertAfterLatestError("V2", v2Ex);
  }

  /** Assert a streaming query failure carries a DSv1-style "after the latest" message. */
  private static void assertAfterLatestError(String label, StreamingQueryException ex) {
    String msg = ex.getMessage() == null ? "" : ex.getMessage();
    String causeMsg = ex.getCause() == null ? "" : String.valueOf(ex.getCause());
    assertTrue(
        msg.contains("after the latest")
            || msg.contains("is after")
            || causeMsg.contains("after the latest")
            || causeMsg.contains("is after"),
        () ->
            label
                + ": expected DSv1-style 'after the latest' error, got: "
                + msg
                + " / "
                + causeMsg);
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

    // Each reader gets its own checkpoint + output dir so the V1 and V2 streams don't fight over
    // state. We compare the per-reader "second-run" delta to assert parity.
    String startTs = formatTs(t1);
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    long v1Initial =
        runRestartFirstPass(
            spark.readStream().format("delta").option("startingTimestamp", startTs).load(tablePath),
            new File(deltaTablePath, "_chk_v1"),
            new File(deltaTablePath, "_out_v1"));
    long v2Initial =
        runRestartFirstPass(
            spark.readStream().option("startingTimestamp", startTs).table(dsv2Ref),
            new File(deltaTablePath, "_chk_v2"),
            new File(deltaTablePath, "_out_v2"));
    assertEquals(20, v1Initial, "V1 initial run should process 20 rows");
    assertEquals(v1Initial, v2Initial, "V1 vs V2 initial run row count mismatch");

    // Append a new commit (v3).
    long t3 = t2 + 60_000L;
    spark.range(20, 30).write().format("delta").mode("append").save(tablePath);
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, t3);

    // Restart each query against its own checkpoint. Per DSv1 semantics, restart with checkpoint
    // ignores startingTimestamp and resumes from where we left off (i.e., should only read v3).
    long v1Restart =
        runRestartSecondPass(
            spark.readStream().format("delta").option("startingTimestamp", startTs).load(tablePath),
            new File(deltaTablePath, "_chk_v1"),
            new File(deltaTablePath, "_out_v1"),
            v1Initial);
    long v2Restart =
        runRestartSecondPass(
            spark.readStream().option("startingTimestamp", startTs).table(dsv2Ref),
            new File(deltaTablePath, "_chk_v2"),
            new File(deltaTablePath, "_out_v2"),
            v2Initial);

    assertEquals(
        v1Restart,
        v2Restart,
        () -> "V1 restart added " + v1Restart + " rows, V2 added " + v2Restart);
    assertEquals(
        10,
        v1Restart,
        () ->
            "Restart should ignore startingTimestamp and only process v3 (10 rows), got "
                + v1Restart);
  }

  /** Run a streaming query to completion against a parquet sink and return the row count. */
  private long runRestartFirstPass(Dataset<Row> df, File checkpointDir, File outputDir)
      throws Exception {
    StreamingQuery q =
        df.writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      return spark.read().parquet(outputDir.getAbsolutePath()).count();
    } finally {
      q.stop();
      DeltaLog.clearCache();
    }
  }

  /** Restart the query and return how many rows the second pass added on top of {@code prior}. */
  private long runRestartSecondPass(Dataset<Row> df, File checkpointDir, File outputDir, long prior)
      throws Exception {
    StreamingQuery q =
        df.writeStream()
            .format("parquet")
            .option("path", outputDir.getAbsolutePath())
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start();
    try {
      q.processAllAvailable();
      long total = spark.read().parquet(outputDir.getAbsolutePath()).count();
      return total - prior;
    } finally {
      q.stop();
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

    // 7a: startingTimestamp = v2's mtime should land on v2 (read v2..v4 = 20 rows).
    List<Row> rowsA =
        assertV1V2StreamingParity(tablePath, "ict_case7_a", startingTimestamp(formatTs(v2Mtime)));
    assertEquals(
        20,
        rowsA.size(),
        () -> "starting@v2.mtime should read v2..v4 (20 rows), got: " + rowsA.size());

    // 7b: startingTimestamp = v3's ICT should land on v3 (read v3..v4 = 10 rows; v3 is metadata-
    // only so 10 rows from v4).
    List<Row> rowsB =
        assertV1V2StreamingParity(tablePath, "ict_case7_b", startingTimestamp(formatTs(v3Ict)));
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

    String startTs = formatTs(t2);
    String dsv2Ref = str("dsv2.delta.`%s`", tablePath);

    long v1Rows =
        runAvailableNow(
            spark.readStream().format("delta").option("startingTimestamp", startTs).load(tablePath),
            "ict_case8_v1",
            new File(deltaTablePath, "_chk_v1"));
    long v2Rows =
        runAvailableNow(
            spark.readStream().option("startingTimestamp", startTs).table(dsv2Ref),
            "ict_case8_v2",
            new File(deltaTablePath, "_chk_v2"));

    assertEquals(v1Rows, v2Rows, () -> "V1=" + v1Rows + " V2=" + v2Rows);
    // starting@t2 -> read v2 + v3 = 20 rows.
    assertEquals(
        20, v1Rows, () -> "AvailableNow + ICT starting@t2 should read 20 rows, got " + v1Rows);
  }

  /** Run an AvailableNow streaming query and return the COUNT(*) from its memory sink. */
  private long runAvailableNow(Dataset<Row> df, String queryName, File checkpointDir)
      throws Exception {
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName(queryName)
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      q.processAllAvailable();
      assertTrue(q.awaitTermination(60_000L), queryName + ": AvailableNow query should terminate");
      return spark.sql("SELECT COUNT(*) FROM " + queryName).collectAsList().get(0).getLong(0);
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
      "KNOWN-GAP: DSv2 streaming does not yet honor DV-only deletes when the source has a DELETE "
          + "commit between the starting timestamp and the latest version; the stream surfaces "
          + "DELTA_SOURCE_TABLE_IGNORE_CHANGES instead of applying the DV against the snapshot. "
          + "Re-enable when DSv2 streaming treats DV-only deletes the way DSv1 does.")
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
    List<Row> rows =
        assertV1V2StreamingParity(tablePath, "ict_case9", startingTimestamp(formatTs(t1)));
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
    List<Row> rows =
        assertV1V2StreamingParity(tablePath, "ict_case10", startingTimestamp(formatTs(t2)));
    assertEquals(
        10, rows.size(), () -> "ICT + column-mapping: expected 10 rows from v2, got: " + rows);
  }
}
