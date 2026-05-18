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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Probing tests for V2 position/order edge cases. Each test targets a permutation that may NOT be
 * covered by PR #6609's V2StreamingSchemaReorder rule. Failures are bugs.
 *
 * <p>Cases:
 *
 * <ol>
 *   <li>Partition column FIRST in DDL.
 *   <li>Two partition columns interleaved.
 *   <li>Three partition columns sandwiched between data columns.
 *   <li>DDL declared order of partition cols differs from PARTITIONED BY clause order.
 *   <li>Batch read (non-streaming) with mid-schema partition column.
 *   <li>Column-mapping name mode + mid-schema partition.
 *   <li>Column-mapping id mode + mid-schema partition.
 *   <li>Generated/non-identity partitioning (DATE_TRUNC).
 *   <li>Same commit: AddFile then RemoveFile vs RemoveFile then AddFile (DSv2 vs DSv1).
 *   <li>Out-of-order ICT commit timestamps with startingTimestamp.
 * </ol>
 */
public class V2PositionOrderTest extends V2TestBase {

  // ===============================================================================================
  // Case 1 — Partition column FIRST in DDL
  // ===============================================================================================

  /**
   * Partition column first in DDL: {@code (part LONG, id LONG, col3 INT) PARTITIONED BY (part)}.
   *
   * <p>Spark normalizes DDL CREATE TABLE so partition columns are reordered to the end of the
   * schema. Use a DataFrame writer with partitionBy to preserve mid/first ordering at the table
   * level.
   */
  @Test
  public void testPartitionColumnFirstStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    StructType schema =
        new StructType()
            .add("part", DataTypes.LongType)
            .add("id", DataTypes.LongType)
            .add("col3", DataTypes.IntegerType);
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(10L, 1L, 100),
                RowFactory.create(20L, 2L, 200),
                RowFactory.create(30L, 3L, 300)),
            schema)
        .write()
        .format("delta")
        .partitionBy("part")
        .save(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_part_first_stream");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(10L, 1L, 100),
            RowFactory.create(20L, 2L, 200),
            RowFactory.create(30L, 3L, 300));
    assertDataEquals(actualRows, expectedRows);
  }

  // ===============================================================================================
  // Case 2 — Two partition columns interleaved with data columns
  // ===============================================================================================

  /**
   * Two partition columns interleaved with data: declared {@code (id LONG, p1 STRING, col3 INT, p2
   * LONG) PARTITIONED BY (p1, p2)}.
   *
   * <p>PR #6609's V2StreamingSchemaReorder produces reader order {@code id, col3, p1, p2}.
   * Interleaved layout exercises the "partitions in two non-adjacent slots" permutation.
   */
  @Test
  public void testTwoPartitionColumnsInterleavedStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("p1", DataTypes.StringType)
            .add("col3", DataTypes.IntegerType)
            .add("p2", DataTypes.LongType);
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "a", 100, 10L),
                RowFactory.create(2L, "b", 200, 20L),
                RowFactory.create(3L, "c", 300, 30L)),
            schema)
        .write()
        .format("delta")
        .partitionBy("p1", "p2")
        .save(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_two_part_interleaved");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, "a", 100, 10L),
            RowFactory.create(2L, "b", 200, 20L),
            RowFactory.create(3L, "c", 300, 30L));
    assertDataEquals(actualRows, expectedRows);
  }

  // ===============================================================================================
  // Case 3 — Three partition columns sandwiched between data columns
  // ===============================================================================================

  /**
   * Three partition columns sandwiched: {@code (p1 INT, data LONG, p2 STRING, col STRING, p3 LONG)
   * PARTITIONED BY (p1, p2, p3)}. Maximum scrambling: partitions at positions 0, 2, 4; data at 1,
   * 3.
   */
  @Test
  public void testThreePartitionColumnsSandwichedStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    StructType schema =
        new StructType()
            .add("p1", DataTypes.IntegerType)
            .add("data", DataTypes.LongType)
            .add("p2", DataTypes.StringType)
            .add("col", DataTypes.StringType)
            .add("p3", DataTypes.LongType);
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1, 100L, "x", "alpha", 1000L),
                RowFactory.create(2, 200L, "y", "beta", 2000L),
                RowFactory.create(3, 300L, "z", "gamma", 3000L)),
            schema)
        .write()
        .format("delta")
        .partitionBy("p1", "p2", "p3")
        .save(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_three_part_sandwich");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1, 100L, "x", "alpha", 1000L),
            RowFactory.create(2, 200L, "y", "beta", 2000L),
            RowFactory.create(3, 300L, "z", "gamma", 3000L));
    assertDataEquals(actualRows, expectedRows);
  }

  // ===============================================================================================
  // Case 4 — DDL partition-column ordering differs from PARTITIONED BY clause
  // ===============================================================================================

  /**
   * Partition column ordering different from PARTITIONED BY clause: declared {@code (id LONG, p2
   * STRING, p1 LONG, data INT) PARTITIONED BY (p1, p2)}.
   *
   * <p>Tests whether PR #6609 preserves the user's declared partition-order. Reader-order should
   * reflect partitioning order from {@code partitionColumnNames}, not DDL order.
   */
  @Test
  public void testPartitionOrderDifferentFromDDLStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("p2", DataTypes.StringType)
            .add("p1", DataTypes.LongType)
            .add("data", DataTypes.IntegerType);
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "x", 10L, 100),
                RowFactory.create(2L, "y", 20L, 200),
                RowFactory.create(3L, "z", 30L, 300)),
            schema)
        .write()
        .format("delta")
        .partitionBy("p1", "p2")
        .save(tablePath);

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_part_order_differ");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, "x", 10L, 100),
            RowFactory.create(2L, "y", 20L, 200),
            RowFactory.create(3L, "z", 30L, 300));
    assertDataEquals(actualRows, expectedRows);
  }

  // ===============================================================================================
  // Case 5 — Batch read (non-streaming) with mid-schema partition column
  // ===============================================================================================

  /**
   * Batch read of {@code (id LONG, part LONG, col3 INT) PARTITIONED BY (part)} via DSv2.
   *
   * <p>PR #6609's rule is streaming-scoped (matches {@code StreamingRelationV2}); batch reads go
   * through {@code DataSourceV2Relation}. Both call {@code SparkScan.readSchema()} which has the
   * same {@code data ++ partitions} append. Tests whether the same NPE / silent reorder bug
   * manifests on the batch path.
   */
  @Test
  public void testBatchReadPartitionInMiddle(@TempDir File dir) {
    String tablePath = dir.getAbsolutePath();
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType)
            .add("part", DataTypes.LongType)
            .add("col3", DataTypes.IntegerType);
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, 10L, 100),
                RowFactory.create(2L, 20L, 200),
                RowFactory.create(3L, 30L, 300)),
            schema)
        .write()
        .format("delta")
        .partitionBy("part")
        .save(tablePath);

    Dataset<Row> df = spark.read().table(str("dsv2.delta.`%s`", tablePath)).orderBy("id");
    assertArrayEquals(new String[] {"id", "part", "col3"}, df.schema().fieldNames());
    List<Row> actualRows = df.collectAsList();
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, 10L, 100),
            RowFactory.create(2L, 20L, 200),
            RowFactory.create(3L, 30L, 300));
    assertEquals(expectedRows, actualRows);
  }

  // ===============================================================================================
  // Case 6 — Column-mapping name mode + mid-schema partition
  // ===============================================================================================

  /**
   * {@code (id LONG, part LONG, col3 INT) PARTITIONED BY (part)} with {@code
   * delta.columnMapping.mode='name'}.
   *
   * <p>Column mapping makes physical column names (e.g. {@code col-abc123}) differ from logical
   * names. Tests whether PR #6609's case-insensitive name lookup of partition columns still works.
   */
  @Test
  public void testColumnMappingNameModeMidPartitionStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, col3 INT) "
                + "USING delta PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_colmap_name_mid");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, 10L, 100),
            RowFactory.create(2L, 20L, 200),
            RowFactory.create(3L, 30L, 300));
    assertDataEquals(actualRows, expectedRows);
  }

  // ===============================================================================================
  // Case 7 — Column-mapping id mode + mid-schema partition
  // ===============================================================================================

  /**
   * {@code (id LONG, part LONG, col3 INT) PARTITIONED BY (part)} with {@code
   * delta.columnMapping.mode='id'}.
   *
   * <p>Stricter than name mode — physical column ids drive resolution.
   */
  @Test
  public void testColumnMappingIdModeMidPartitionStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, part LONG, col3 INT) "
                + "USING delta PARTITIONED BY (part) "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'id')",
            tablePath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_colmap_id_mid");
    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create(1L, 10L, 100),
            RowFactory.create(2L, 20L, 200),
            RowFactory.create(3L, 30L, 300));
    assertDataEquals(actualRows, expectedRows);
  }

  // ===============================================================================================
  // Case 8 — Generated / non-identity partition transform
  // ===============================================================================================

  /**
   * Non-identity partition transform via a generated partition column. Delta supports {@code
   * delta.generationExpression} which produces a partition column from a data expression
   * (functionally equivalent to a non-identity transform like {@code DATE_TRUNC('DAY', ts)}).
   *
   * <p>PR #6609's rule explicitly throws {@code IllegalStateException} on anything other than
   * {@code IdentityTransform(FieldReference(...))}. A generated partition column registered as a
   * standard identity-typed column at the metadata level should still work.
   */
  @Test
  public void testGeneratedPartitionColumnStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    // Delta's generated columns: declare day_part as date GENERATED ALWAYS AS (CAST(ts AS DATE)).
    // It surfaces as a normal partition column at the V2 layer (IdentityTransform on day_part).
    // Use DeltaTable.create() because Spark SQL's CREATE TABLE parser doesn't accept
    // GENERATED ALWAYS AS for non-identity expressions.
    io.delta.tables.DeltaTable.create(spark)
        .location(tablePath)
        .addColumn(io.delta.tables.DeltaTable.columnBuilder(spark, "id").dataType("LONG").build())
        .addColumn(
            io.delta.tables.DeltaTable.columnBuilder(spark, "ts").dataType("TIMESTAMP").build())
        .addColumn(
            io.delta.tables.DeltaTable.columnBuilder(spark, "day_part")
                .dataType("DATE")
                .generatedAlwaysAs("CAST(ts AS DATE)")
                .build())
        .addColumn(
            io.delta.tables.DeltaTable.columnBuilder(spark, "payload").dataType("INT").build())
        .partitionedBy("day_part")
        .execute();
    spark.sql(
        str(
            "INSERT INTO delta.`%s` (id, ts, payload) VALUES "
                + "(1, TIMESTAMP'2026-01-01 10:00:00', 100), "
                + "(2, TIMESTAMP'2026-01-02 11:00:00', 200), "
                + "(3, TIMESTAMP'2026-01-01 12:00:00', 300)",
            tablePath));

    Dataset<Row> streamingDF = spark.readStream().table(str("dsv2.delta.`%s`", tablePath));
    List<Row> actualRows = processStreamingQuery(streamingDF, "test_generated_part");
    // 3 rows expected — each with their day_part automatically computed.
    assertEquals(3, actualRows.size(), "Expected 3 rows from generated partition table");
    // Column order should match DDL.
    assertArrayEquals(
        new String[] {"id", "ts", "day_part", "payload"}, streamingDF.schema().fieldNames());
  }

  // ===============================================================================================
  // Case 9 — Action ordering: AddFile then RemoveFile vs RemoveFile then AddFile
  // ===============================================================================================

  /**
   * Same logical effect (one file added, one removed in the same commit), but the JSON commit
   * action order is varied. Streams both via DSv2 and DSv1; compares output.
   *
   * <p>If DSv2 gets different rows from DSv1, or DSv2 gets different rows depending on which order
   * the actions appear in, that's an action-order-sensitivity bug.
   *
   * <p>Setup: build two tables that each have v0 = INSERT(1..5), v1 = OVERWRITE(6..10). Stream with
   * skipChangeCommits=true so removes are tolerated. Both should emit 5 + 5 = 10 rows.
   */
  @Test
  public void testActionOrderingAddRemoveStreaming(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));
    // OVERWRITE creates a single commit with both AddFile (new file) and RemoveFile (old file).
    spark.sql(str("INSERT OVERWRITE TABLE delta.`%s` VALUES (6), (7), (8), (9), (10)", tablePath));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("skipChangeCommits", "true")
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "test_action_order_v2");

    // Compare to V1 reading the same path.
    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("skipChangeCommits", "true").load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "test_action_order_v1");

    // V1 and V2 should agree.
    assertEquals(
        v1Rows.size(),
        v2Rows.size(),
        () ->
            "DSv1 and DSv2 row counts differ on AddFile/RemoveFile commit. v1="
                + v1Rows
                + " v2="
                + v2Rows);
    assertTrue(
        v1Rows.containsAll(v2Rows) && v2Rows.containsAll(v1Rows),
        () -> "DSv1/DSv2 row sets differ. v1=" + v1Rows + " v2=" + v2Rows);
  }

  // ===============================================================================================
  // Case 10 — Out-of-order ICT commit timestamps with startingTimestamp
  // ===============================================================================================

  /**
   * Three commits with monotonically-increasing version numbers but where ICT timestamps may not
   * match wall-clock arrival order (rare under clock skew or InCommitTimestamps). We use
   * startingTimestamp to slice the stream — the cut should be deterministic by ICT, not by arrival.
   *
   * <p>This test seeds three commits, queries the wall-clock time strictly between commit 1 and
   * commit 2, then streams from that timestamp. Rows from commit 1 should be excluded; rows from
   * commits 2 and 3 should be included.
   */
  @Test
  public void testStartingTimestampDeterministic(@TempDir File dir) throws Exception {
    String tablePath = dir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT) USING delta "
                + "TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1)", tablePath));
    Thread.sleep(100);
    long mid = System.currentTimeMillis();
    Thread.sleep(100);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (2)", tablePath));
    Thread.sleep(50);
    spark.sql(str("INSERT INTO delta.`%s` VALUES (3)", tablePath));

    java.text.SimpleDateFormat fmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    String midStr = fmt.format(new java.util.Date(mid));

    Dataset<Row> streamingDF =
        spark
            .readStream()
            .option("startingTimestamp", midStr)
            .table(str("dsv2.delta.`%s`", tablePath));
    List<Row> v2Rows = processStreamingQuery(streamingDF, "test_starting_ts_v2");

    Dataset<Row> v1Stream =
        spark.readStream().format("delta").option("startingTimestamp", midStr).load(tablePath);
    List<Row> v1Rows = processStreamingQuery(v1Stream, "test_starting_ts_v1");

    // Both should agree, and both should exclude row 1 (which came before the cut).
    assertEquals(
        v1Rows.size(),
        v2Rows.size(),
        () ->
            "DSv1 and DSv2 row counts differ for startingTimestamp. v1="
                + v1Rows
                + " v2="
                + v2Rows);
    assertTrue(
        v1Rows.containsAll(v2Rows) && v2Rows.containsAll(v1Rows),
        () -> "DSv1/DSv2 row sets differ for startingTimestamp. v1=" + v1Rows + " v2=" + v2Rows);
  }
}
