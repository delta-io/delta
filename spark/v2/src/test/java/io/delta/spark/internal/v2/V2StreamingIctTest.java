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

  private static final long BASE_TS = 1700000000000L;
  private static final long ONE_MINUTE = 60_000L;
  private static final int ROWS_PER_COMMIT = 10;

  /** Format epoch millis as a "yyyy-MM-dd HH:mm:ss.SSS" string in the session timezone. */
  private String formatTs(long millis) {
    String tz = spark.sessionState().conf().sessionLocalTimeZone();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone(tz));
    return sdf.format(new Date(millis));
  }

  /** Create an ICT-enabled table and append one data commit per timestamp. */
  private void createIctTableWithCommits(String tablePath, long[] timestamps) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta "
                + "TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    for (int i = 0; i < timestamps.length; i++) {
      appendRows(tablePath, (long) i * ROWS_PER_COMMIT);
      // Versions: 0=CREATE TABLE, 1..N=appends.
      IctTestUtils.modifyCommitTimestamp(log, /* version= */ i + 1, timestamps[i]);
    }
  }

  private void appendRows(String tablePath, long rangeStart) {
    spark
        .range(rangeStart, rangeStart + ROWS_PER_COMMIT)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
  }

  private void appendIctRows(
      String tablePath, DeltaLog log, long version, long rangeStart, long ict, long mtime) {
    appendRows(tablePath, rangeStart);
    setIctAndMtime(log, version, ict, mtime);
  }

  private void setIctAndMtime(DeltaLog log, long version, long ict, long mtime) {
    IctTestUtils.modifyCommitTimestamp(log, version, ict);
    IctTestUtils.setFileMtimeOnly(log, version, mtime);
  }

  /** Build a single-entry options map for a starting-timestamp stream. */
  private static Map<String, String> startingTimestamp(String ts) {
    Map<String, String> opts = new HashMap<>();
    opts.put("startingTimestamp", ts);
    return opts;
  }

  private List<Row> assertStartingTimestampRows(
      String tablePath, String tag, long timestampMillis, int expectedRows, String expectation)
      throws Exception {
    List<Row> rows =
        assertV1V2StreamingParity(tablePath, tag, startingTimestamp(formatTs(timestampMillis)));
    assertEquals(expectedRows, rows.size(), () -> expectation + ", got: " + rows);
    return rows;
  }

  private Dataset<Row> streamingRead(String tablePath, Map<String, String> options, boolean useV2) {
    DataStreamReader reader = useV2 ? spark.readStream() : spark.readStream().format("delta");
    for (Map.Entry<String, String> e : options.entrySet()) {
      reader = reader.option(e.getKey(), e.getValue());
    }
    return useV2 ? reader.table(str("dsv2.delta.`%s`", tablePath)) : reader.load(tablePath);
  }

  /**
   * Run the same streaming read against the DSv1 ("delta") and DSv2 ("dsv2.delta.`...`") sources
   * with identical options and assert their row sets are equal. DSv1 is the oracle.
   *
   * @return the DSv1 row set (also equal to the DSv2 row set on success), sorted by id.
   */
  private List<Row> assertV1V2StreamingParity(
      String tablePath, String tag, Map<String, String> options) throws Exception {
    List<Row> v1Rows =
        sortedById(processStreamingQuery(streamingRead(tablePath, options, false), tag + "_v1"));
    List<Row> v2Rows =
        sortedById(processStreamingQuery(streamingRead(tablePath, options, true), tag + "_v2"));

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

  @Test
  public void startingTimestampAtCommitIct(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = BASE_TS;
    long t2 = t1 + ONE_MINUTE;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    assertStartingTimestampRows(
        tablePath, "ict_exact_match", t2, ROWS_PER_COMMIT, "starting at v2's ICT should read v2");
  }

  @Test
  public void startingTimestampBetweenCommits(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = BASE_TS;
    long t2 = t1 + 2 * ONE_MINUTE;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    assertStartingTimestampRows(
        tablePath,
        "ict_between_commits",
        t1 + 30_000L,
        ROWS_PER_COMMIT,
        "timestamp between v1 and v2 should read v2");
  }

  @Test
  public void adjacentMillisecondIcts(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long sameMs = BASE_TS;
    // Adjacent millisecond timestamps verify the exact-match boundary at millisecond precision.
    createIctTableWithCommits(tablePath, new long[] {sameMs, sameMs + 1});

    assertStartingTimestampRows(
        tablePath,
        "ict_adjacent_ms_v1",
        sameMs,
        2 * ROWS_PER_COMMIT,
        "starting at v1's timestamp should read v1+v2");
    assertStartingTimestampRows(
        tablePath,
        "ict_adjacent_ms_v2",
        sameMs + 1,
        ROWS_PER_COMMIT,
        "starting at v2's timestamp should read v2");
  }

  // ICT wins over filesystem mtime drift
  @Test
  public void mtimeDriftUsesIct(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = BASE_TS;
    long t2 = t1 + ONE_MINUTE;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    // Tamper: set v2's filesystem mtime to BEFORE t1 (e.g., a restore overwrote it).
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long bogusMtime = t1 - 10 * ONE_MINUTE;
    IctTestUtils.setFileMtimeOnly(log, /* version= */ 2, bogusMtime);

    // Ask for t2: with ICT, this should still resolve to v2 even though v2's mtime is stale.
    assertStartingTimestampRows(
        tablePath, "ict_mtime_drift", t2, ROWS_PER_COMMIT, "ICT lookup should read v2");
  }

  @Test
  public void startingTimestampAfterLatestErrors(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = BASE_TS;
    long t2 = t1 + ONE_MINUTE;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    long future = t2 + 10 * 365L * 24 * 3600 * 1000L; // far future
    String futureTs = formatTs(future);

    assertStartingTimestampAfterLatestError(
        "V1", tablePath, "ict_after_latest_v1", futureTs, false);
    assertStartingTimestampAfterLatestError("V2", tablePath, "ict_after_latest_v2", futureTs, true);
  }

  private void assertStartingTimestampAfterLatestError(
      String label, String tablePath, String queryName, String timestamp, boolean useV2) {
    Dataset<Row> df = streamingRead(tablePath, startingTimestamp(timestamp), useV2);
    StreamingQueryException ex =
        assertThrows(StreamingQueryException.class, () -> processStreamingQuery(df, queryName));
    assertAfterLatestError(label, ex);
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

  @Test
  public void restartIgnoresStartingTimestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = BASE_TS;
    long t2 = t1 + ONE_MINUTE;
    createIctTableWithCommits(tablePath, new long[] {t1, t2});

    // Each reader gets its own checkpoint + output dir so the V1 and V2 streams don't fight over
    // state. We compare the per-reader "second-run" delta to assert parity.
    String startTs = formatTs(t1);
    Map<String, String> options = startingTimestamp(startTs);

    long v1Initial =
        runRestartFirstPass(
            streamingRead(tablePath, options, false),
            new File(deltaTablePath, "_chk_v1"),
            new File(deltaTablePath, "_out_v1"));
    long v2Initial =
        runRestartFirstPass(
            streamingRead(tablePath, options, true),
            new File(deltaTablePath, "_chk_v2"),
            new File(deltaTablePath, "_out_v2"));
    assertEquals(2 * ROWS_PER_COMMIT, v1Initial, "V1 initial run should process v1+v2");
    assertEquals(v1Initial, v2Initial, "V1 vs V2 initial run row count mismatch");

    // Append a new commit (v3).
    long t3 = t2 + ONE_MINUTE;
    appendRows(tablePath, 2 * ROWS_PER_COMMIT);
    IctTestUtils.modifyCommitTimestamp(DeltaLog.forTable(spark, tablePath), 3L, t3);

    // Restart each query against its own checkpoint. Per DSv1 semantics, restart with checkpoint
    // ignores startingTimestamp and resumes from where we left off (i.e., should only read v3).
    long v1Restart =
        runRestartSecondPass(
            streamingRead(tablePath, options, false),
            new File(deltaTablePath, "_chk_v1"),
            new File(deltaTablePath, "_out_v1"),
            v1Initial);
    long v2Restart =
        runRestartSecondPass(
            streamingRead(tablePath, options, true),
            new File(deltaTablePath, "_chk_v2"),
            new File(deltaTablePath, "_out_v2"),
            v2Initial);

    assertEquals(
        v1Restart,
        v2Restart,
        () -> "V1 restart added " + v1Restart + " rows, V2 added " + v2Restart);
    assertEquals(
        ROWS_PER_COMMIT,
        v1Restart,
        () -> "Restart should ignore startingTimestamp and only process v3, got " + v1Restart);
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

  @Test
  public void midHistoryIctIgnoresPostIctMtimes(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id BIGINT) USING delta", tablePath));
    appendRows(tablePath, 0);
    appendRows(tablePath, ROWS_PER_COMMIT);
    // Keep the pre-ICT history ordered so v2 is the correct non-ICT resolver result.
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    long base = BASE_TS;
    long v0Mtime = base - ONE_MINUTE;
    long v1Mtime = base;
    long v2Mtime = base + 30_000L;
    IctTestUtils.setFileMtimeOnly(log, 0L, v0Mtime);
    IctTestUtils.setFileMtimeOnly(log, 1L, v1Mtime);
    IctTestUtils.setFileMtimeOnly(log, 2L, v2Mtime);
    // v3: ALTER TABLE turns on ICT (this commit itself enables ICT mid-history).
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    long v3Mtime = base + 60_000L;
    long v3Ict = base + 150_000L;
    setIctAndMtime(log, 3L, v3Ict, v3Mtime);

    long v4Mtime = base + 2 * ONE_MINUTE;
    long v4Ict = v3Ict + ONE_MINUTE;
    appendIctRows(tablePath, log, 4L, 2 * ROWS_PER_COMMIT, v4Ict, v4Mtime);

    long v5Mtime = v4Mtime + 20_000L;
    long v5Ict = v4Ict + ONE_MINUTE;
    appendIctRows(tablePath, log, 5L, 3 * ROWS_PER_COMMIT, v5Ict, v5Mtime);

    // This is logically before ICT starts but after v4's file mtime. The bounded search starts at
    // v3 and emits v4+v5
    long target = v4Mtime + 10_000L;
    assertStartingTimestampRows(
        tablePath,
        "ict_mid_history",
        target,
        2 * ROWS_PER_COMMIT,
        "starting in the pre-ICT trap zone should read v4+v5");
  }

  @Test
  public void availableNowWithIct(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    long t1 = BASE_TS;
    long t2 = t1 + ONE_MINUTE;
    long t3 = t2 + ONE_MINUTE;
    createIctTableWithCommits(tablePath, new long[] {t1, t2, t3});

    String startTs = formatTs(t2);
    Map<String, String> options = startingTimestamp(startTs);

    long v1Rows =
        runAvailableNow(
            streamingRead(tablePath, options, false),
            "ict_available_now_v1",
            new File(deltaTablePath, "_chk_v1"));
    long v2Rows =
        runAvailableNow(
            streamingRead(tablePath, options, true),
            "ict_available_now_v2",
            new File(deltaTablePath, "_chk_v2"));

    assertEquals(v1Rows, v2Rows, () -> "V1=" + v1Rows + " V2=" + v2Rows);
    assertEquals(
        2 * ROWS_PER_COMMIT,
        v1Rows,
        () -> "AvailableNow + ICT starting at v2 should read v2+v3, got " + v1Rows);
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

  @Test
  public void columnMappingWithIct(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id BIGINT) USING delta TBLPROPERTIES ("
                + "'delta.enableInCommitTimestamps' = 'true',"
                + "'delta.columnMapping.mode' = 'name',"
                + "'delta.minReaderVersion' = '2',"
                + "'delta.minWriterVersion' = '5')",
            tablePath));
    long t1 = BASE_TS;
    long t2 = t1 + ONE_MINUTE;
    appendRows(tablePath, 0);
    appendRows(tablePath, ROWS_PER_COMMIT);
    DeltaLog log = DeltaLog.forTable(spark, tablePath);
    IctTestUtils.modifyCommitTimestamp(log, 1L, t1);
    IctTestUtils.modifyCommitTimestamp(log, 2L, t2);

    assertStartingTimestampRows(
        tablePath,
        "ict_column_mapping",
        t2,
        ROWS_PER_COMMIT,
        "ICT + column mapping should read v2");
  }
}
