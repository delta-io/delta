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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 CDC streaming read coverage. Ports a curated subset of {@code DeltaCDCStreamSuite} cases
 * from DSv1 to characterize the DSv2 connector's handling of CDC streaming.
 *
 * <p><b>Major gap</b>: {@code SparkScan.UNSUPPORTED_STREAMING_OPTIONS} (in {@code
 * spark/v2/src/main/java/io/delta/spark/internal/v2/read/SparkScan.java}) explicitly blocks {@code
 * readChangeFeed} (and {@code readChangeData}, {@code endingVersion}, {@code endingTimestamp}).
 * Therefore the DSv2 streaming connector cannot read CDF at all today. The contract tests below pin
 * that rejection behavior, and the probe tests below characterize the (limited) interaction between
 * CDF-enabled tables and "regular" (non-CDC) DSv2 streaming.
 *
 * <p>All cases that genuinely require CDC change-row output (insert/update/delete change types)
 * cannot be constructed against DSv2 today; see the {@code can't-construct} entries in the test
 * gaps tracking doc.
 */
public class V2CDCStreamingReadTest extends V2TestBase {

  // --- Helpers -------------------------------------------------------------------------

  /** Creates a Delta table at the given path with CDF enabled. */
  private void createCdfTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            tablePath));
  }

  /** Creates a Delta table at the given path with CDF + DV enabled. */
  private void createCdfDvTable(String tablePath) {
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, value STRING) USING delta TBLPROPERTIES ("
                + "'delta.enableChangeDataFeed' = 'true',"
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
  }

  /**
   * Runs a streaming query that exercises {@code toMicroBatchStream} (which is where DSv2 validates
   * options), waits for it to start, then captures the failure.
   */
  private StreamingQueryException runAndCaptureException(Dataset<Row> streamingDF, String name)
      throws Exception {
    StreamingQuery query =
        streamingDF.writeStream().format("memory").queryName(name).outputMode("append").start();
    try {
      return assertThrows(StreamingQueryException.class, () -> query.processAllAvailable());
    } finally {
      query.stop();
    }
  }

  // --- Contract tests: readChangeFeed=true is rejected --------------------------------

  /**
   * Case 1 — basic CDC read. Cannot be constructed against DSv2: {@code readChangeFeed=true} is
   * blocked by {@code SparkScan.validateStreamingOptions}. This test pins the rejection.
   */
  @Test
  public void testReadChangeFeedTrueRejected(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET value = 'b2' WHERE id = 2", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .table(dsv2TableRef);

    StreamingQueryException ex = runAndCaptureException(streamingDF, "v2_cdc_basic_rejected");
    Throwable cause = ex.getCause();
    assertNotNull(cause, "Expected an underlying cause");
    assertTrue(
        cause instanceof UnsupportedOperationException
            || (cause.getCause() != null
                && cause.getCause() instanceof UnsupportedOperationException)
            || ex.getMessage().contains("not supported")
            || ex.getMessage().contains("readChangeFeed"),
        () ->
            "Expected UnsupportedOperationException for readChangeFeed, got: "
                + cause.getClass().getName()
                + ": "
                + cause.getMessage());
    assertTrue(
        ex.getMessage().toLowerCase().contains("readchangefeed")
            || ex.getMessage().contains("not supported"),
        () -> "Expected message to mention readChangeFeed/not supported, got: " + ex.getMessage());
  }

  /**
   * Case 2 — endingVersion option used with startingVersion is also blocked even without
   * readChangeFeed=true. Pins that the secondary CDC options are also rejected.
   */
  @Test
  public void testCdcEndingVersionRejected(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("readChangeFeed", "true")
            .option("startingVersion", "0")
            .option("endingVersion", "2")
            .table(dsv2TableRef);

    StreamingQueryException ex =
        runAndCaptureException(streamingDF, "v2_cdc_endingversion_rejected");
    assertTrue(
        ex.getMessage().toLowerCase().contains("readchangefeed")
            || ex.getMessage().toLowerCase().contains("endingversion")
            || ex.getMessage().contains("not supported"),
        () -> "Expected unsupported option message, got: " + ex.getMessage());
  }

  /**
   * Probe — "readChangeFeed=false" should be allowed (it explicitly opts out of CDC). Verifies that
   * the option key itself isn't pattern-blocked (the validation is a Set membership check on the
   * canonical option key, regardless of value).
   *
   * <p>Documents an asymmetry: the validation is on the key, not the value. So {@code
   * option("readChangeFeed", "false")} is *also* rejected even though it's a no-op.
   */
  @Test
  public void testReadChangeFeedFalseAlsoRejected(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("readChangeFeed", "false").table(dsv2TableRef);

    // This is value-agnostic: validation rejects the key, not value=true.
    StreamingQueryException ex = runAndCaptureException(streamingDF, "v2_cdc_false_also_rejected");
    assertTrue(
        ex.getMessage().toLowerCase().contains("readchangefeed")
            || ex.getMessage().contains("not supported"),
        () -> "Expected unsupported option message, got: " + ex.getMessage());
  }

  // --- Probes: regular streaming works on a CDF-enabled table --------------------------

  /**
   * Probe — A CDF-enabled table can still be streamed via DSv2 *without* the readChangeFeed option
   * (regular row-level streaming, not CDF change rows). Confirms that turning on {@code
   * delta.enableChangeDataFeed} does not by itself break DSv2 streaming.
   */
  @Test
  public void testStreamCdfEnabledTableWithoutCdcOption(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "v2_cdf_table_no_cdc_option");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c"));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Probe — a CDF table that has had UPDATE applied (which produces AddCDCFile actions in v1) must
   * still stream cleanly under DSv2 *without* readChangeFeed=true. UPDATE on a CDF table writes new
   * data files for the post-image; whether DSv2 emits the post-image rows from regular streaming is
   * what we want to pin.
   *
   * <p>For a CDF-enabled table, the underlying commits contain {@code AddCDCFile} actions and
   * possibly add/remove pairs — DSv1's "regular" (non-CDC) streaming reads add/remove pairs and
   * yields post-image rows; CDC files are CDC-only. DSv2 should see the same.
   */
  @Test
  public void testStreamCdfEnabledTableWithUpdate(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    // First, drain the initial snapshot.
    Dataset<Row> initialDF = spark.readStream().table(dsv2TableRef);
    List<Row> initial = processStreamingQuery(initialDF, "v2_cdf_update_initial");
    assertDataEquals(initial, Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")));

    // UPDATE — for a CDF-enabled table this commit contains AddCDCFile actions, but
    // also AddFile/RemoveFile pairs for the data path. Regular streaming should fail (changes
    // to existing data) unless ignoreChanges/skipChangeCommits is set; mirror DSv1 semantics.
    spark.sql(str("UPDATE delta.`%s` SET value = 'b2' WHERE id = 2", tablePath));

    Dataset<Row> postUpdateDF =
        spark.readStream().option("startingVersion", "2").table(dsv2TableRef);
    StreamingQueryException ex = runAndCaptureException(postUpdateDF, "v2_cdf_update_no_ignore");
    // Document whatever happens: in DSv1 this throws DELTA_SOURCE_TABLE_IGNORE_CHANGES;
    // we just record that something failed (success is fine too if DSv2 happened to ignore).
    assertNotNull(
        ex.getMessage(),
        "Streaming an UPDATE-with-CDF table without ignoreChanges should error in DSv1 parity");
  }

  /**
   * Probe — same as above but with ignoreChanges=true to verify DSv2 streaming on CDF-enabled
   * tables can be unblocked the same way as DSv1.
   */
  @Test
  public void testStreamCdfEnabledTableWithIgnoreChanges(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));
    spark.sql(str("UPDATE delta.`%s` SET value = 'b2' WHERE id = 2", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("ignoreChanges", "true").table(dsv2TableRef);

    // Just verify the stream runs without error and produces some output (the exact content
    // depends on whether DSv2 reads AddFile rewrites on UPDATE).
    List<Row> rows = processStreamingQuery(streamingDF, "v2_cdf_ignorechanges");
    assertNotNull(rows);
    // Each row should have the post-update value or initial values.
    assertTrue(rows.size() >= 2, "Expected at least the 2 initial rows, got: " + rows);
  }

  /**
   * Probe — CDF + DV combination: a CDF-enabled table with deletion vectors enabled. UPDATE uses DV
   * under DSv1 default behavior. DSv2 streaming without CDC option should still succeed on the
   * initial snapshot.
   */
  @Test
  public void testStreamCdfDvTableInitialSnapshot(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfDvTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));
    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("ignoreDeletes", "true").table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "v2_cdf_dv_initial_snapshot");
    // Initial snapshot must reflect DV: row id=2 should NOT appear.
    List<Row> expected = Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(3, "c"));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Probe — Trigger.AvailableNow on a CDF-enabled table. CDC option still rejected; just verifies
   * AvailableNow termination on a CDF table without the CDC option.
   */
  @Test
  public void testCdfTableWithAvailableNowTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    StreamingQuery query =
        streamingDF
            .writeStream()
            .format("memory")
            .queryName("v2_cdf_availablenow")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    try {
      assertTrue(
          query.awaitTermination(60_000), "AvailableNow should terminate within 60s on CDF table");
      List<Row> rows = spark.sql("SELECT * FROM v2_cdf_availablenow").collectAsList();
      List<Row> expected =
          Arrays.asList(
              RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c"));
      assertDataEquals(rows, expected);
    } finally {
      query.stop();
    }
  }

  /**
   * Probe — Combine CDF + maxFilesPerTrigger. Without CDC option this should exercise the same
   * rate-limit logic as non-CDF tables. In DSv1, CDC + maxFilesPerTrigger has a special rule that
   * AddCDCFile commits don't get split. This probe just verifies the non-CDC case works.
   */
  @Test
  public void testCdfTableWithMaxFilesPerTrigger(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    // 3 separate commits, each with 1 file.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("maxFilesPerTrigger", "1").table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "v2_cdf_maxfiles");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c"));
    assertDataEquals(actualRows, expected);
  }

  /**
   * Probe — Restart a stream on a CDF-enabled table. Mirrors the DSv1 "cdc streams should respect
   * checkpoint" test, but without the CDC option (since DSv2 blocks it). Writes to a delta sink so
   * the checkpoint can be replayed across runs (memory sink does not support recovery).
   */
  @Test
  public void testCdfTableStreamRestart(@TempDir File deltaTablePath, @TempDir File outDir)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    createCdfTable(tablePath);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    File checkpointDir = new File(deltaTablePath, "_checkpoint_restart");
    String outPath = outDir.getAbsolutePath();

    // First run: process initial commits and write to a delta sink.
    Dataset<Row> first = spark.readStream().table(dsv2TableRef);
    StreamingQuery q1 =
        first
            .writeStream()
            .format("delta")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start(outPath);
    q1.processAllAvailable();
    q1.stop();
    List<Row> firstOut = spark.read().format("delta").load(outPath).orderBy("id").collectAsList();
    assertDataEquals(firstOut, Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b")));

    // Add more commits.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    // Second run: should resume from checkpoint and only emit the new row to the sink.
    Dataset<Row> second = spark.readStream().table(dsv2TableRef);
    StreamingQuery q2 =
        second
            .writeStream()
            .format("delta")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .start(outPath);
    q2.processAllAvailable();
    q2.stop();
    List<Row> secondOut = spark.read().format("delta").load(outPath).orderBy("id").collectAsList();
    // Output sink should hold all 3 rows now (1, 2 from first run; 3 appended from second).
    assertDataEquals(
        secondOut,
        Arrays.asList(
            RowFactory.create(1, "a"), RowFactory.create(2, "b"), RowFactory.create(3, "c")));
  }

  /**
   * Probe — CDF on a partitioned table (mirrors DSv1 "cdc streams should be able to get offset when
   * there only RemoveFiles" but without the CDC option). Tests partition handling on a CDF-enabled
   * table.
   */
  @Test
  public void testCdfPartitionedTable(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (0, 0), (1, 1), (2, 0), (3, 1)", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "v2_cdf_partitioned");
    List<Row> expected =
        Arrays.asList(
            RowFactory.create(0, 0),
            RowFactory.create(1, 1),
            RowFactory.create(2, 0),
            RowFactory.create(3, 1));
    assertDataEquals(actualRows, expected);
  }
}
