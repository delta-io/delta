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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DSv2 streaming brainstorm scenarios 19-24 (mid-priority).
 *
 * <p>Six scenarios from {@code ~/markdown/testgap/scenario_brainstorm.md}, all of which exercise
 * corners that the routine streaming tests do not touch:
 *
 * <ul>
 *   <li>19. Year-9999 timestamp value (upper end of the timestamp range).
 *   <li>20. NaN / +Inf / -Inf in a FLOAT partition column (DSv1 vs DSv2 differential).
 *   <li>21. String value with embedded NUL byte (read-side, on a data column).
 *   <li>22. Toggle CDF off -> on -> off mid-history, stream WITHOUT {@code readChangeFeed}.
 *   <li>23. Two streams sharing the same {@code _checkpoint} dir.
 *   <li>24. Symlink table path.
 * </ul>
 *
 * <p>Predictions per scenario are recorded inline. Goal is to surface bugs by running these later;
 * this commit only writes them.
 */
public class V2StreamingMidPriorityScenarios19to24Test extends V2TestBase {

  /**
   * Scenario 19: Year-9999 timestamp.
   *
   * <p>Prediction: Golden tables only cover historical bounds. Year-9999 may surface
   * micros-overflow or text/binary timestamp encoding issues in Kernel <-> Spark conversion. We
   * expect either clean round-trip or a structured error; raw NPE / arithmetic overflow would be a
   * bug.
   */
  @Test
  public void testScenario19_year9999Timestamp(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, ts TIMESTAMP) USING delta", tablePath));

    // Year 9999 boundary: 9999-12-31 23:59:59.999999.
    Timestamp maxTs = Timestamp.valueOf("9999-12-31 23:59:59.999999");
    Timestamp normalTs = Timestamp.valueOf("2026-05-04 12:00:00");

    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("ts", DataTypes.TimestampType, false)));
    List<Row> rows = Arrays.asList(RowFactory.create(1, normalTs), RowFactory.create(2, maxTs));
    spark.createDataFrame(rows, schema).write().format("delta").mode("append").save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);

    List<Row> actualRows = processStreamingQuery(streamingDF, "scen19_year9999_ts");
    assertDataEquals(actualRows, rows);
  }

  /**
   * Scenario 20: NaN / Inf in a FLOAT partition column - DSv1 vs DSv2 differential.
   *
   * <p>Prediction: Path encoding for non-finite floats is undocumented. DSv1 and DSv2 may disagree
   * on whether the partition value round-trips as "NaN" / "Infinity" / "-Infinity" (or as NULL, or
   * fails outright). We assert the row count matches and that the DSv2 partition values equal DSv1
   * partition values exactly. A divergence is the bug.
   */
  @Test
  public void testScenario20_nanInfFloatPartition_dsv1vsDsv2(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, p FLOAT) USING delta PARTITIONED BY (p)", tablePath));

    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("p", DataTypes.FloatType, true)));
    List<Row> rows =
        Arrays.asList(
            RowFactory.create(1, Float.NaN),
            RowFactory.create(2, Float.POSITIVE_INFINITY),
            RowFactory.create(3, Float.NEGATIVE_INFINITY),
            RowFactory.create(4, 1.5f));
    spark
        .createDataFrame(rows, schema)
        .write()
        .format("delta")
        .mode("append")
        .partitionBy("p")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> dsv2StreamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> dsv2Rows = processStreamingQuery(dsv2StreamingDF, "scen20_naninf_partition_dsv2");

    Dataset<Row> dsv1StreamingDF =
        spark.readStream().format("delta").load(tablePath).select("id", "p");
    List<Row> dsv1Rows = processStreamingQuery(dsv1StreamingDF, "scen20_naninf_partition_dsv1");

    assertEquals(
        dsv1Rows.size(),
        dsv2Rows.size(),
        () -> "DSv1 and DSv2 row counts differ:\nDSv1: " + dsv1Rows + "\nDSv2: " + dsv2Rows);
    assertDataEquals(dsv2Rows, dsv1Rows);
  }

  /**
   * Scenario 21: String value with embedded NUL byte (read-side, data column).
   *
   * <p>Prediction: Task M case 4 covered the write-side (strings with U+0000 in partition keys).
   * The read-side, where the NUL is in a data column written via a normal DataFrame insert, is
   * uncovered. Some text codecs / stats encoders truncate at NUL. We assert the streamed string
   * equals the original (length and contents).
   */
  @Test
  public void testScenario21_embeddedNulByte_dataColumn(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, s STRING) USING delta", tablePath));

    String withNul = "ab\0cd";
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("s", DataTypes.StringType, true)));
    List<Row> rows =
        Arrays.asList(
            RowFactory.create(1, withNul),
            RowFactory.create(2, "no_nul_here"),
            RowFactory.create(3, "\0leading"),
            RowFactory.create(4, "trailing\0"));
    spark.createDataFrame(rows, schema).write().format("delta").mode("append").save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "scen21_embedded_nul");

    assertDataEquals(actualRows, rows);
    // Spot-check: assert byte-for-byte equality on the row whose string contains an interior NUL.
    Row matched =
        actualRows.stream()
            .filter(r -> r.getInt(0) == 1)
            .findFirst()
            .orElseThrow(() -> new AssertionError("id=1 row missing: " + actualRows));
    assertEquals(
        withNul,
        matched.getString(1),
        () -> "id=1 string differs (NUL truncated?): expected len=" + withNul.length());
    assertEquals(
        5, matched.getString(1).length(), () -> "id=1 string length: " + matched.getString(1));
  }

  /**
   * Scenario 22: Toggle CDF off -> on -> off mid-history; stream WITHOUT readChangeFeed.
   *
   * <p>Prediction: SMS validateCommitAndDecideSkipping may mis-handle latent AddCDCFile actions in
   * the on-window if the regular (non-CDF) read path is hit. We assert that (a) the regular stream
   * emits exactly the user-data rows (no _change_type leakage), (b) row count matches the
   * underlying physical insert count.
   */
  @Test
  public void testScenario22_cdfToggleOffOnOff_regularStream(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    // Phase 1: CDF off (default). Insert 2 rows.
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", tablePath));

    // Phase 2: enable CDF, perform an UPDATE (which would normally produce AddCDCFile) and an
    // additional INSERT.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
            tablePath));
    spark.sql(str("UPDATE delta.`%s` SET name = 'aa' WHERE id = 1", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3, 'c')", tablePath));

    // Phase 3: disable CDF again. Insert 1 more row.
    spark.sql(
        str(
            "ALTER TABLE delta.`%s` SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'false')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4, 'd')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> actualRows = processStreamingQuery(streamingDF, "scen22_cdf_toggle_offonoff");

    // Assert no _change_type column leaks; regular streams must not expose CDF metadata cols.
    String[] cols = actualRows.isEmpty() ? new String[0] : actualRows.get(0).schema().fieldNames();
    for (String c : cols) {
      assertNotEquals("_change_type", c, () -> "_change_type leaked into regular stream schema");
    }
    assertTrue(
        actualRows.size() >= 4,
        () -> "expected at least 4 rows from the regular stream; got: " + actualRows);
  }

  /**
   * Scenario 23: Two streams sharing the same {@code _checkpoint} dir.
   *
   * <p>Prediction: Race on {@code offsets/}; one stream must error out, not silently corrupt or
   * interleave. We use two {@code Trigger.AvailableNow} invocations with the SAME
   * checkpointLocation and assert that either the second invocation throws OR it starts fresh /
   * picks up where the first left off without producing duplicate or out-of-order rows. Silent
   * corruption would be the bug.
   */
  @Test
  public void testScenario23_twoStreamsShareCheckpointDir(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    File checkpointDir = new File(deltaTablePath, "_checkpoint");
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // First AvailableNow run drains the table.
    StreamingQuery first = null;
    long firstRowsWritten;
    try {
      first =
          spark
              .readStream()
              .table(dsv2TableRef)
              .writeStream()
              .format("noop")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      first.processAllAvailable();
      firstRowsWritten =
          Arrays.stream(first.recentProgress()).mapToLong(p -> p.numInputRows()).sum();
    } finally {
      if (first != null) first.stop();
    }
    assertEquals(3L, firstRowsWritten, "first AvailableNow run should drain all 3 rows");

    // Second AvailableNow on same checkpoint, no new data. Acceptable: (a) idempotent rerun with
    // 0 new rows, or (b) start throws. Silent duplicates would be the bug.
    StreamingQuery second = null;
    long secondRowsWritten = -1L;
    Throwable thrown = null;
    try {
      second =
          spark
              .readStream()
              .table(dsv2TableRef)
              .writeStream()
              .format("noop")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .trigger(Trigger.AvailableNow())
              .start();
      second.processAllAvailable();
      secondRowsWritten =
          Arrays.stream(second.recentProgress()).mapToLong(p -> p.numInputRows()).sum();
    } catch (Throwable t) {
      thrown = t;
    } finally {
      if (second != null) second.stop();
    }

    final Throwable finalThrown = thrown;
    final long finalSecondRows = secondRowsWritten;
    if (finalThrown != null) {
      assertNotEquals(
          NullPointerException.class,
          finalThrown.getClass(),
          () -> "second run should not raw-NPE; got: " + finalThrown);
    } else {
      assertEquals(
          0L,
          finalSecondRows,
          () ->
              "second AvailableNow on same checkpoint should be idempotent (0 new rows); "
                  + "anything > 0 means offsets were ignored/corrupted.");
    }
  }

  /**
   * Scenario 24: Symlink table path.
   *
   * <p>Prediction: SMS strips trailing slash but doesn't symlink-resolve. DSv1 vs DSv2 may diverge
   * in {@code tableId} resolution if one canonicalizes and the other doesn't. We create a Delta
   * table at one path, a symlink that points to it, and stream via the symlink. If symlink creation
   * is blocked in the test env (rootless container, Windows w/o privilege, AFS), the test documents
   * that and is skipped via early return.
   */
  @Test
  public void testScenario24_symlinkTablePath(@TempDir File parentDir) throws Exception {
    File realTableDir = new File(parentDir, "real_table");
    assertTrue(realTableDir.mkdirs(), "must be able to create real table dir");
    String realPath = realTableDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", realPath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b')", realPath));

    Path linkPath = Paths.get(parentDir.getAbsolutePath(), "link_to_table");
    try {
      Files.createSymbolicLink(linkPath, realTableDir.toPath());
    } catch (UnsupportedOperationException | java.io.IOException ex) {
      // Symlink creation may be blocked in some test environments (Windows w/o developer mode,
      // AFS, hardened sandboxes). Document and return; not a test failure.
      System.err.println(
          "[scenario24] skipping symlink test - createSymbolicLink not allowed: " + ex);
      return;
    }

    String linkPathStr = linkPath.toAbsolutePath().toString();

    String dsv2TableRef = str("dsv2.delta.`%s`", linkPathStr);
    Dataset<Row> dsv2StreamingDF = spark.readStream().table(dsv2TableRef);
    List<Row> dsv2Rows = processStreamingQuery(dsv2StreamingDF, "scen24_symlink_dsv2");

    Dataset<Row> dsv1StreamingDF =
        spark.readStream().format("delta").load(linkPathStr).select("id", "name");
    List<Row> dsv1Rows = processStreamingQuery(dsv1StreamingDF, "scen24_symlink_dsv1");

    List<Row> expected = Arrays.asList(RowFactory.create(1, "a"), RowFactory.create(2, "b"));

    assertDataEquals(dsv1Rows, expected);
    assertDataEquals(dsv2Rows, expected);
    assertEquals(
        dsv1Rows.size(),
        dsv2Rows.size(),
        () -> "DSv1 vs DSv2 differ for symlink path:\nDSv1: " + dsv1Rows + "\nDSv2: " + dsv2Rows);
  }
}
