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
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * DSv2 streaming tests for medium-priority scenarios 13-18 from the brainstorm doc {@code
 * ~/markdown/testgap/scenario_brainstorm.md}. Each test targets one suspected bug; the goal is to
 * surface failures that DSv1 either prevents or surfaces differently. Patterns mirror {@code
 * V2StreamingEdgeDataReadTest}.
 */
public class V2StreamingMidPriorityScenarios13to18Test extends V2TestBase {

  /**
   * Scenario 13: excludeRegex against a %XX-encoded partition path.
   *
   * <p>SMS:443-447 applies excludeRegex to AddFile.getPath(), which for partitioned tables holds
   * the URL-encoded directory segment (e.g. "p=a%3Db/part-0000.parquet"). DSv1 (DeltaSource.scala)
   * matches against the same encoded path, so this is a parity check. A bug surfaces if DSv2
   * decodes the path before regex match, diverging from DSv1.
   */
  @Test
  public void testScenario13_excludeRegex_encodedPartitionPath(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, p STRING) USING delta PARTITIONED BY (p)",
            tablePath));

    // 'a=b' partition value -> directory "p=a%3Db". Use DataFrame to avoid SQL escaping.
    List<Row> rows =
        Arrays.asList(
            RowFactory.create(1, "a=b"), RowFactory.create(2, "ok"), RowFactory.create(3, "a=b"));
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("p", DataTypes.StringType, true)));
    spark
        .createDataFrame(rows, schema)
        .write()
        .format("delta")
        .mode("append")
        .partitionBy("p")
        .save(tablePath);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    // Regex matches the URL-ENCODED form. DSv1 parity expected.
    Dataset<Row> dsv2Stream =
        spark.readStream().option("excludeRegex", "p=a%3Db").table(dsv2TableRef);
    List<Row> dsv2Rows = processStreamingQuery(dsv2Stream, "scn13_dsv2_encoded");

    Dataset<Row> dsv1Stream =
        spark.readStream().format("delta").option("excludeRegex", "p=a%3Db").load(tablePath);
    List<Row> dsv1Rows = processStreamingQuery(dsv1Stream, "scn13_dsv1_encoded");

    assertEquals(
        dsv1Rows.size(),
        dsv2Rows.size(),
        () ->
            "DSv1 vs DSv2 row count diverge for excludeRegex against %XX-encoded partition path."
                + " DSv1="
                + dsv1Rows
                + " DSv2="
                + dsv2Rows);
    assertDataEquals(dsv2Rows, dsv1Rows);
  }

  /**
   * Scenario 14: Stream restart after table dropped + recreated with CM and DV.
   *
   * <p>Extends DSv1 DeltaSourceSuite ":845". SMS:214 captures tableId only at init from the initial
   * snapshot. After restart, DeltaSourceOffset.apply(tableId, json) compares the new tableId
   * against the offset's reservoirId; mismatch should raise DELTA_RESERVOIR_ID_MISMATCH (or DSv2
   * equivalent), NOT silently resume reading the new table from the old offset. We accept either a
   * clean error or a re-bootstrap, but reject silent data loss.
   */
  @Test
  public void testScenario14_streamRestart_afterDropAndRecreate_withCMAndDV(
      @TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    File checkpointDir = new File(deltaTablePath, "_checkpoint");

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, v STRING) USING delta TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a')", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2, 'b')", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);

    StreamingQuery q1 =
        spark
            .readStream()
            .table(dsv2TableRef)
            .writeStream()
            .format("noop")
            .queryName("scn14_first")
            .option("checkpointLocation", checkpointDir.getAbsolutePath())
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    q1.awaitTermination();
    q1.stop();
    org.apache.spark.sql.delta.DeltaLog.clearCache();

    // Drop and recreate at the same path.
    deleteRecursively(deltaTablePath);
    assertTrue(deltaTablePath.mkdirs(), "Failed to recreate table directory");
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, v STRING) USING delta TBLPROPERTIES ("
                + "'delta.columnMapping.mode' = 'name', "
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (99, 'NEW')", tablePath));

    StreamingQuery q2 = null;
    Throwable thrown = null;
    try {
      q2 =
          spark
              .readStream()
              .table(dsv2TableRef)
              .writeStream()
              .format("noop")
              .queryName("scn14_second")
              .option("checkpointLocation", checkpointDir.getAbsolutePath())
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .start();
      q2.awaitTermination();
    } catch (Throwable t) {
      thrown = t;
    } finally {
      if (q2 != null) q2.stop();
      org.apache.spark.sql.delta.DeltaLog.clearCache();
    }

    if (thrown != null || (q2 != null && q2.exception().isDefined())) {
      String msg = (thrown != null ? thrown.toString() : q2.exception().get().toString());
      assertFalse(
          msg.contains("NullPointerException"),
          () -> "Drop+recreate restart raised NPE instead of structured error: " + msg);
    } else {
      long rows = 0;
      for (org.apache.spark.sql.streaming.StreamingQueryProgress p : q2.recentProgress()) {
        rows += p.numInputRows();
      }
      assertTrue(
          rows >= 1L,
          () ->
              "DSv2 silently resumed from old checkpoint after table drop+recreate; new row was"
                  + " never read. This indicates tableId mismatch is not enforced.");
    }
  }

  /**
   * Scenario 15: maxBytesPerTrigger when limit equals the larger of two files.
   *
   * <p>Exercises the per-file admission boundary in {@code DeltaSourceAdmissionBase.admit}: after
   * the first file is admitted via the deadlock guard, the second must actually fit in the
   * remaining capacity. With limit == max(f1,f2), neither file can ever fit alongside the other, so
   * each must land in its own batch regardless of which file streaming sees first.
   */
  @Test
  public void testScenario15_maxBytesPerTrigger_fileSizeEqualsLimit(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));

    spark
        .range(50)
        .selectExpr("cast(id as int) as id", "concat('row', cast(id as string)) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    spark
        .range(50, 100)
        .selectExpr("cast(id as int) as id", "concat('row', cast(id as string)) as name")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    org.apache.spark.sql.delta.DeltaLog deltaLog =
        org.apache.spark.sql.delta.DeltaLog.forTable(
            spark, new org.apache.hadoop.fs.Path(tablePath));
    org.apache.spark.sql.delta.actions.AddFile[] addsArr =
        (org.apache.spark.sql.delta.actions.AddFile[])
            deltaLog.update(false, scala.Option.empty(), scala.Option.empty()).allFiles().collect();
    assertEquals(2, addsArr.length, "Expected exactly 2 AddFiles for this scenario.");
    long file1Size = addsArr[0].size();
    long file2Size = addsArr[1].size();
    // Use max() so the limit is deterministic regardless of allFiles() listing order: with
    // limit == max(f1,f2), the two files must split across batches no matter which one is read
    // first, so the bug repros 5/5 instead of being masked by listing order.
    long limit = Math.max(file1Size, file2Size);

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    String maxBytes = limit + "b";

    StreamingQuery q =
        spark
            .readStream()
            .option("maxBytesPerTrigger", maxBytes)
            .table(dsv2TableRef)
            .writeStream()
            .format("noop")
            .queryName("scn15_eq_limit")
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .start();
    q.awaitTermination();
    q.stop();

    int nonEmptyBatches = 0;
    long totalRows = 0;
    for (org.apache.spark.sql.streaming.StreamingQueryProgress p : q.recentProgress()) {
      if (p.numInputRows() > 0) {
        nonEmptyBatches++;
      }
      totalRows += p.numInputRows();
    }
    final int finalNonEmpty = nonEmptyBatches;
    assertEquals(100L, totalRows, "Total rows should equal 100 across batches.");
    assertTrue(
        finalNonEmpty >= 2,
        () ->
            "Expected at least 2 non-empty batches when maxBytesPerTrigger equals max(file1,file2) "
                + "(file1="
                + file1Size
                + " file2="
                + file2Size
                + " limit="
                + limit
                + "); got "
                + finalNonEmpty
                + ". Indicates per-file admit re-fires the deadlock guard for every file with any"
                + " positive remaining capacity, instead of only for the first file in the batch.");
  }

  /**
   * Scenario 17: MAP&lt;STRUCT, INT&gt; with a fully-NULL composite key.
   *
   * <p>ColumnarMap key/value DV-wrapped paths are uncovered. The fix in dea78c848 wraps non-Struct
   * child vectors so getChild(0)/getChild(1) on a MAP-typed column applies the DV row-id mapping to
   * the keyArray and valueArray. The composite STRUCT key adds another layer: keyArray's children
   * are the struct fields. Without the fix, getChild on the MAP returns a raw delegate child whose
   * null bitmap is keyed on input row ids - a NULL key in the input may surface at the wrong row in
   * output.
   *
   * <p>Spark does not allow NULL as a map key directly; we use a struct key whose fields are all
   * NULL.
   */
  @Test
  public void testScenario17_mapStructIntKey_nullCompositeKey(@TempDir File deltaTablePath)
      throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` ("
                + "id INT, "
                + "m MAP<STRUCT<a: INT, b: STRING>, INT>) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));

    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES "
                + "(1, map(named_struct('a', 1, 'b', 'k1'), 100)), "
                + "(2, map(named_struct('a', 2, 'b', 'k2'), 200)), "
                + "(3, map(named_struct('a', cast(NULL as int), 'b', cast(NULL as string)), 300)), "
                + "(4, map(named_struct('a', 4, 'b', 'k4'), 400))",
            tablePath));

    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> dsv2Stream = spark.readStream().table(dsv2TableRef);
    List<Row> dsv2Rows = processStreamingQuery(dsv2Stream, "scn17_dsv2_map_struct_null");

    Dataset<Row> dsv1Stream = spark.readStream().format("delta").load(tablePath);
    List<Row> dsv1Rows = processStreamingQuery(dsv1Stream, "scn17_dsv1_map_struct_null");

    assertEquals(
        3,
        dsv2Rows.size(),
        () -> "Expected 3 rows after DELETE (id=2 removed via DV); got " + dsv2Rows);
    assertDataEquals(dsv2Rows, dsv1Rows);
  }

  /**
   * Scenario 18: Decimal(38, 38) byte[]-storage column under DV.
   *
   * <p>Type-fanout coverage only included Decimal(30, 6). Spark stores any decimal with precision
   * &gt; Decimal.MAX_LONG_DIGITS (=18) as BigDecimal-backed byte[] (BinaryType in Parquet), which
   * routes through the byte[] accessor path of ColumnVectorWithFilter. With DV applied, the
   * remapping must apply to that byte[] read - dea78c848 ensures non-Struct children are wrapped.
   *
   * <p>Decimal(38, 38) means scale == precision: legal values are in (-1, 1) with up to 38 digits
   * after the decimal point. Bug indicator: wrong-row decimal returned for an undeleted row.
   */
  @Test
  public void testScenario18_decimal38_38_underDV(@TempDir File deltaTablePath) throws Exception {
    String tablePath = deltaTablePath.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, d DECIMAL(38, 38)) USING delta TBLPROPERTIES ("
                + "'delta.enableDeletionVectors' = 'true')",
            tablePath));

    BigDecimal d1 = new BigDecimal("0." + repeat("1", 38));
    BigDecimal d2 = new BigDecimal("0." + repeat("2", 38));
    BigDecimal d3 = new BigDecimal("0." + repeat("3", 38));
    BigDecimal d4 = new BigDecimal("0." + repeat("4", 38));

    List<Row> seedRows =
        Arrays.asList(
            RowFactory.create(1, d1),
            RowFactory.create(2, d2),
            RowFactory.create(3, d3),
            RowFactory.create(4, d4));
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("d", DataTypes.createDecimalType(38, 38), true)));
    spark.createDataFrame(seedRows, schema).write().format("delta").mode("append").save(tablePath);

    spark.sql(str("DELETE FROM delta.`%s` WHERE id = 2", tablePath));

    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> stream = spark.readStream().table(dsv2TableRef);
    List<Row> rows = processStreamingQuery(stream, "scn18_decimal38_38_dv");

    assertEquals(3, rows.size(), () -> "Expected 3 surviving rows; got: " + rows);

    Map<Integer, BigDecimal> expected = new HashMap<>();
    expected.put(1, d1);
    expected.put(3, d3);
    expected.put(4, d4);

    Map<Integer, BigDecimal> actual = new HashMap<>();
    for (Row r : rows) {
      actual.put(r.getInt(0), r.getDecimal(1));
    }
    assertEquals(
        expected,
        actual,
        () ->
            "Decimal(38,38) values misaligned with ids after DV remap. Indicates row-id mapping "
                + "missing on byte[] decimal storage path. Expected="
                + expected
                + " Actual="
                + actual);
  }

  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder(s.length() * n);
    for (int i = 0; i < n; i++) sb.append(s);
    return sb.toString();
  }

  private static void deleteRecursively(File f) {
    if (f == null) return;
    if (f.isDirectory()) {
      File[] children = f.listFiles();
      if (children != null) {
        for (File c : children) {
          deleteRecursively(c);
        }
      }
    }
    f.delete();
  }
}
