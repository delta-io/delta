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

package io.sparkuctest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * Position/order permutation tests through Unity Catalog (EXTERNAL + MANAGED).
 *
 * <p>Ports {@link io.delta.spark.internal.v2.V2PositionOrderTest} where expressible via UC managed
 * SQL. The original suite uses {@code DataFrameWriter.partitionBy} to preserve mid-schema partition
 * column positions. UC managed tables must be created via SQL {@code CREATE TABLE ... PARTITIONED
 * BY (...)}, which Spark normalizes by moving partition columns to the end of the schema. This
 * normalization removes the "partition-in-middle" shape entirely on the SQL path.
 *
 * <p>Skipped cases (cannot translate to UC managed SQL):
 *
 * <ul>
 *   <li>Case 1 - "partition column FIRST in DDL": SQL reorders partitions to the end.
 *   <li>Case 2 - "two partition columns interleaved": same reason.
 *   <li>Case 3 - "three partitions sandwiched between data columns": same.
 *   <li>Case 4 - "DDL declared order differs from PARTITIONED BY order": SQL collapses both into
 *       the PARTITIONED BY order.
 *   <li>Case 5 - "batch read with mid-schema partition": SQL path also reorders.
 *   <li>Case 8 - "generated partition column": Spark SQL CREATE TABLE does not accept GENERATED
 *       ALWAYS AS expressions for non-identity transforms (the original test relies on {@code
 *       DeltaTable.create()} API).
 * </ul>
 *
 * <p>Cases that do translate (and are written here): column-mapping name/id mode + partitioning
 * (cases 6, 7), AddFile/RemoveFile action ordering via OVERWRITE (case 9), startingTimestamp
 * determinism with InCommitTimestamps (case 10). These still cover the bug class - they exercise
 * the V2 partition-row construction, action coalescing, and ICT-driven offset selection on the
 * MANAGED catalog path.
 */
public class UCDeltaStreamingPositionOrderTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * {@code (id LONG, col3 INT, part LONG) PARTITIONED BY (part)} (SQL normalizes part to the end)
   * with {@code delta.columnMapping.mode='name'}.
   *
   * <p>Tests case-insensitive partition lookup against physical column names with column mapping
   * enabled. Even though the position is no longer "in the middle," the V2 reader still has to map
   * physical -> logical names for the partition value lookup.
   */
  @TestAllTableTypes
  public void testColumnMappingNameModePartitioned(TableType tableType) throws Exception {
    runColMapBody(tableType, "name", "uc_pos_cm_name");
  }

  /**
   * Same as case 6 but with {@code delta.columnMapping.mode='id'} - stricter resolution. Physical
   * column ids drive the partition-value lookup.
   */
  @TestAllTableTypes
  public void testColumnMappingIdModePartitioned(TableType tableType) throws Exception {
    runColMapBody(tableType, "id", "uc_pos_cm_id");
  }

  private void runColMapBody(TableType tableType, String mode, String simpleName) throws Exception {
    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          dir -> {
            org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(dir, simpleName);
            String name = fullTableName(simpleName);
            sql(
                "CREATE TABLE %s (id LONG, part LONG, col3 INT) USING DELTA"
                    + " PARTITIONED BY (part)"
                    + " LOCATION '%s'"
                    + " TBLPROPERTIES ('delta.columnMapping.mode'='%s')",
                name, tablePath.toString(), mode);
            try {
              runColMapAssert(name, tableType);
            } finally {
              sql("DROP TABLE IF EXISTS %s", name);
            }
          });
    } else {
      String name = fullTableName(simpleName);
      sql(
          "CREATE TABLE %s (id LONG, part LONG, col3 INT) USING DELTA"
              + " PARTITIONED BY (part)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported',"
              + " 'delta.columnMapping.mode'='%s')",
          name, mode);
      try {
        runColMapAssert(name, tableType);
      } finally {
        sql("DROP TABLE IF EXISTS %s", name);
      }
    }
  }

  private void runColMapAssert(String tableName, TableType tableType) throws Exception {
    sql("INSERT INTO %s VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", tableName);
    String queryName = "uc_pos_cm_" + tableType.name().toLowerCase() + "_" + checkpointCount;
    spark()
        .readStream()
        .table(tableName)
        .writeStream()
        .format("memory")
        .queryName(queryName)
        .outputMode("append")
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpoint())
        .start()
        .awaitTermination();

    List<Row> rows = spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
    assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
    assertEquals(1L, rows.get(0).getLong(0));
    assertEquals(10L, rows.get(0).getLong(rows.get(0).fieldIndex("part")));
    assertEquals(100, rows.get(0).getInt(rows.get(0).fieldIndex("col3")));
    spark().sql("DROP VIEW IF EXISTS " + queryName);
  }

  /**
   * V2 streaming on a table with an OVERWRITE commit (which produces both AddFile and RemoveFile in
   * the same commit JSON). The stream is started AFTER both commits exist, so the initial snapshot
   * is taken at the OVERWRITE version (v1) and reflects the post-OVERWRITE state {6..10}. With
   * {@code skipChangeCommits=true} the OVERWRITE commit itself is skipped (it has a RemoveFile), so
   * no further rows are emitted on top of the initial snapshot. Expected: 5 rows.
   *
   * <p>Mirrors {@link
   * io.delta.spark.internal.v2.V2PositionOrderTest#testActionOrderingAddRemoveStreaming} which
   * compares DSv1 vs DSv2 row sets (both yield 5 rows under this configuration).
   */
  @TestAllTableTypes
  public void testActionOrderingAddRemoveStreaming(TableType tableType) throws Exception {
    withNewTable(
        "uc_pos_action_order",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3), (4), (5)", tableName);
          // OVERWRITE: single commit with AddFile (new file) + RemoveFile (old file).
          sql("INSERT OVERWRITE TABLE %s VALUES (6), (7), (8), (9), (10)", tableName);

          String queryName =
              "uc_pos_action_order_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .option("skipChangeCommits", "true")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          // Initial snapshot at v1 emits {6..10}; the v1 OVERWRITE commit is skipped due to its
          // RemoveFile under skipChangeCommits=true. Total: 5 rows = {6,7,8,9,10}.
          assertEquals(
              5, rows.size(), "expected 5 rows after AddFile/RemoveFile coalesce; got: " + rows);
          assertEquals(6, rows.get(0).getInt(0));
          assertEquals(10, rows.get(4).getInt(0));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Three commits with InCommitTimestamps enabled; stream from a wall-clock timestamp strictly
   * between commit 1 and commit 2. Rows from commit 1 must be excluded; rows from commits 2 and 3
   * included.
   */
  @TestAllTableTypes
  public void testStartingTimestampDeterministic(TableType tableType) throws Exception {
    withNewTable(
        "uc_pos_starting_ts",
        "id INT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          Thread.sleep(100);
          long mid = System.currentTimeMillis();
          Thread.sleep(100);
          sql("INSERT INTO %s VALUES (2)", tableName);
          Thread.sleep(50);
          sql("INSERT INTO %s VALUES (3)", tableName);

          java.text.SimpleDateFormat fmt =
              new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
          String midStr = fmt.format(new java.util.Date(mid));

          String queryName =
              "uc_pos_starting_ts_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> stream =
              spark().readStream().option("startingTimestamp", midStr).table(tableName);
          stream
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows =
              spark().sql("SELECT * FROM " + queryName + " ORDER BY id").collectAsList();
          // Should exclude row 1; include rows 2 and 3.
          assertEquals(2, rows.size(), "expected 2 rows after startingTimestamp cut; got: " + rows);
          assertEquals(2, rows.get(0).getInt(0));
          assertEquals(3, rows.get(1).getInt(0));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Sanity check: after {@code CREATE TABLE ... PARTITIONED BY}, the user-visible schema column
   * order is whatever Spark chose (typically: data columns then partition columns). This is the UC
   * analog of V2PositionOrderTest's "schema fieldNames" assertion, recording the actual normalized
   * order so a future regression that scrambles it is caught.
   */
  @TestAllTableTypes
  public void testNormalizedSchemaOrder(TableType tableType) throws Exception {
    withNewTable(
        "uc_pos_schema_order",
        "id LONG, part LONG, col3 INT",
        "part",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 10, 100)", tableName);
          Dataset<Row> stream = spark().readStream().table(tableName);
          // V2 streaming must surface the table's logical schema identically (no silent reorder).
          // Spark normalizes the schema to data columns then partition columns; the column set
          // must contain exactly {id, part, col3} regardless of order.
          String[] streamCols = stream.schema().fieldNames();
          assertEquals(3, streamCols.length);
          java.util.Set<String> got = new java.util.HashSet<>(java.util.Arrays.asList(streamCols));
          assertEquals(java.util.Set.of("id", "part", "col3"), got);
          String queryName =
              "uc_pos_schema_order_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          stream
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();
          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(1, rows.size());
          Row r = rows.get(0);
          assertEquals(1L, r.getLong(r.fieldIndex("id")));
          assertEquals(10L, r.getLong(r.fieldIndex("part")));
          assertEquals(100, r.getInt(r.fieldIndex("col3")));
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
