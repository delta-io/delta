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

package io.sparkuctest;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Dynamic Partition Overwrite (DPO) with Unity Catalog tables.
 *
 * <p>These tests specifically target the bug reported in ES-1617946 where using
 * partitionOverwriteMode=dynamic with saveAsTable on a CCv2 managed table could cause the table to
 * be dropped due to a catalog switching issue.
 *
 * <p>The original bug occurred when:
 * 1. A table is created in Unity Catalog (CCv2 managed)
 * 2. User attempts to write to the table using DataFrame.write.saveAsTable() with
 *    option("partitionOverwriteMode", "dynamic")
 * 3. Spark incorrectly looks for the table in spark_catalog instead of the Unity Catalog
 * 4. This causes a NoSuchNamespaceException and potentially drops the table
 *
 * <p>NOTE: These tests are currently disabled because they reproduce the ES-1617946 bug.
 * They will be enabled once the bug is fixed. The tests document the expected behavior
 * where dynamic partition overwrite should only affect partitions present in the new data,
 * preserving all other partitions.
 */
public class UCDeltaTableDynamicPartitionOverwriteTest extends UCDeltaTableIntegrationBaseTest {

  /**
   * Test that insertInto with dynamic partition overwrite mode works correctly.
   * This uses insertInto which properly supports dynamic partition overwrite.
   *
   * <p>Expected behavior: When writing data for partition hour_level=12 with dynamic partition
   * overwrite enabled, only that partition should be affected. Partitions hour_level=10 and
   * hour_level=11 should be preserved.
   *
   * <p>Bug behavior (ES-1617946): All partitions are overwritten, not just the affected ones.
   */
  @Test
  @Disabled("ES-1617946: Dynamic partition overwrite does not work correctly with Unity Catalog")
  public void testInsertIntoWithDynamicPartitionOverwrite() throws Exception {
    withNewTable(
        "dpo_insertinto_test",
        "value INT, date_level STRING, hour_level INT",
        "date_level, hour_level",
        TableType.EXTERNAL,
        tableName -> {
          // Step 1: Insert initial data into multiple partitions
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, '2024-01-01', 10), "
                  + "(2, '2024-01-01', 11), "
                  + "(3, '2024-01-01', 12), "
                  + "(4, '2024-01-02', 10)",
              tableName);

          // Verify initial data
          check(
              tableName,
              List.of(
                  List.of("1", "2024-01-01", "10"),
                  List.of("2", "2024-01-01", "11"),
                  List.of("3", "2024-01-01", "12"),
                  List.of("4", "2024-01-02", "10")));

          // Enable dynamic partition overwrite
          sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = true");

          try {
            // Step 2: Read data from a specific partition
            Dataset<Row> dfToWrite =
                spark()
                    .sql(String.format("SELECT * FROM %s WHERE hour_level = 12", tableName))
                    .limit(2);

            // Step 3: Perform dynamic partition overwrite using insertInto
            dfToWrite.write().mode(SaveMode.Overwrite).insertInto(tableName);

            // Step 4: Verify the table still exists and data integrity is maintained
            // With dynamic partition overwrite, only partition hour_level=12 should be affected
            // Other partitions should remain intact
            List<List<String>> result = sql("SELECT * FROM %s ORDER BY value", tableName);

            // Verify unaffected partitions are preserved
            long hour10Count = result.stream().filter(row -> row.get(2).equals("10")).count();
            long hour11Count = result.stream().filter(row -> row.get(2).equals("11")).count();

            if (hour10Count != 2) {
              throw new AssertionError(
                  String.format(
                      "Partition hour_level=10 should have 2 rows, got %d. "
                          + "Table may have been incorrectly dropped/modified.",
                      hour10Count));
            }

            if (hour11Count != 1) {
              throw new AssertionError(
                  String.format(
                      "Partition hour_level=11 should have 1 row, got %d. "
                          + "Table may have been incorrectly dropped/modified.",
                      hour11Count));
            }
          } finally {
            sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = false");
          }
        });
  }

  /**
   * Test that INSERT OVERWRITE TABLE with a subquery and dynamic partition overwrite preserves
   * unaffected partitions. This uses the SQL API.
   *
   * <p>Expected behavior: When INSERT OVERWRITE from source table that only contains partition p1,
   * partition p2 in the target table should be preserved.
   *
   * <p>Bug behavior (ES-1617946): All partitions including p2 are overwritten.
   */
  @Test
  @Disabled("ES-1617946: Dynamic partition overwrite does not work correctly with Unity Catalog")
  public void testInsertOverwriteSelectWithDynamicPartitionOverwrite() throws Exception {
    withNewTable(
        "dpo_insert_overwrite_source",
        "id INT, value STRING, partition_col STRING",
        "partition_col",
        TableType.EXTERNAL,
        sourceTable -> {
          withNewTable(
              "dpo_insert_overwrite_target",
              "id INT, value STRING, partition_col STRING",
              "partition_col",
              TableType.EXTERNAL,
              targetTable -> {
                // Setup initial data in target table with multiple partitions
                sql(
                    "INSERT INTO %s VALUES "
                        + "(1, 'old1', 'p1'), "
                        + "(2, 'old2', 'p1'), "
                        + "(3, 'old3', 'p2'), "
                        + "(4, 'old4', 'p2')",
                    targetTable);

                // Setup source data with only p1 partition
                sql("INSERT INTO %s VALUES (5, 'new5', 'p1'), (6, 'new6', 'p1')", sourceTable);

                // Verify initial state
                check(
                    targetTable,
                    List.of(
                        List.of("1", "old1", "p1"),
                        List.of("2", "old2", "p1"),
                        List.of("3", "old3", "p2"),
                        List.of("4", "old4", "p2")));

                // Enable dynamic partition overwrite
                sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = true");

                try {
                  // Insert overwrite from source table (only has p1 partition)
                  sql("INSERT OVERWRITE %s SELECT * FROM %s", targetTable, sourceTable);

                  // Verify partition p2 is preserved and p1 is overwritten
                  check(
                      targetTable,
                      List.of(
                          List.of("3", "old3", "p2"),
                          List.of("4", "old4", "p2"),
                          List.of("5", "new5", "p1"),
                          List.of("6", "new6", "p1")));
                } finally {
                  // Reset the config
                  sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = false");
                }
              });
        });
  }

  /**
   * Test that reading from one partition and writing back to the same table with dynamic partition
   * overwrite using insertInto preserves other partitions. This is a common ETL pattern.
   *
   * <p>Expected behavior: When reprocessing events from 2024-01-02, only that partition should be
   * affected. Partitions 2024-01-01 and 2024-01-03 should be preserved.
   *
   * <p>Bug behavior (ES-1617946): All partitions are overwritten, losing data from unaffected
   * partitions.
   */
  @Test
  @Disabled("ES-1617946: Dynamic partition overwrite does not work correctly with Unity Catalog")
  public void testReadWriteSameTableWithDynamicPartitionOverwrite() throws Exception {
    withNewTable(
        "dpo_read_write_same_test",
        "event_id INT, event_type STRING, event_date STRING",
        "event_date",
        TableType.EXTERNAL,
        tableName -> {
          // Setup: Insert events across multiple dates
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, 'click', '2024-01-01'), "
                  + "(2, 'view', '2024-01-01'), "
                  + "(3, 'click', '2024-01-02'), "
                  + "(4, 'purchase', '2024-01-02'), "
                  + "(5, 'view', '2024-01-03')",
              tableName);

          // Enable dynamic partition overwrite
          sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = true");

          try {
            // Read events from 2024-01-02, transform them, and write back
            // This simulates a reprocessing scenario
            Dataset<Row> eventsToReprocess =
                spark()
                    .sql(
                        String.format(
                            "SELECT event_id, "
                                + "CONCAT(event_type, '_reprocessed') as event_type, "
                                + "event_date FROM %s WHERE event_date = '2024-01-02'",
                            tableName));

            // Write back with dynamic partition overwrite using insertInto
            eventsToReprocess.write().mode(SaveMode.Overwrite).insertInto(tableName);

            // Verify results
            List<List<String>> result = sql("SELECT * FROM %s ORDER BY event_id", tableName);

            // Check that dates 2024-01-01 and 2024-01-03 are preserved
            long jan01Count =
                result.stream().filter(row -> row.get(2).equals("2024-01-01")).count();
            long jan03Count =
                result.stream().filter(row -> row.get(2).equals("2024-01-03")).count();

            if (jan01Count != 2) {
              throw new AssertionError(
                  String.format(
                      "Partition event_date='2024-01-01' should have 2 rows, got %d. "
                          + "This indicates the table was dropped/modified incorrectly.",
                      jan01Count));
            }

            if (jan03Count != 1) {
              throw new AssertionError(
                  String.format(
                      "Partition event_date='2024-01-03' should have 1 row, got %d. "
                          + "This indicates the table was dropped/modified incorrectly.",
                      jan03Count));
            }

            // Check that 2024-01-02 partition was reprocessed
            long jan02ReprocessedCount =
                result.stream()
                    .filter(
                        row ->
                            row.get(2).equals("2024-01-02") && row.get(1).contains("_reprocessed"))
                    .count();

            if (jan02ReprocessedCount != 2) {
              throw new AssertionError(
                  String.format(
                      "Partition event_date='2024-01-02' should have 2 reprocessed rows, got %d",
                      jan02ReprocessedCount));
            }
          } finally {
            sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = false");
          }
        });
  }

  /**
   * Test DataFrame write with dynamic partition overwrite using insertInto.
   * This tests the DataFrame API pattern that is commonly used in ETL pipelines.
   *
   * <p>Expected behavior: When writing data for partition category='A' with dynamic partition
   * overwrite enabled, only that partition should be affected. Partition category='B' should be
   * preserved.
   *
   * <p>Bug behavior (ES-1617946): All partitions are overwritten, losing data from partition 'B'.
   */
  @Test
  @Disabled("ES-1617946: Dynamic partition overwrite does not work correctly with Unity Catalog")
  public void testDataFrameWriteWithDynamicPartitionOverwrite() throws Exception {
    withNewTable(
        "dpo_dataframe_test",
        "id INT, name STRING, category STRING",
        "category",
        TableType.EXTERNAL,
        tableName -> {
          // Step 1: Insert initial data into multiple partitions
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, 'Alice', 'A'), "
                  + "(2, 'Bob', 'A'), "
                  + "(3, 'Charlie', 'B'), "
                  + "(4, 'Diana', 'B')",
              tableName);

          // Verify initial data
          check(
              tableName,
              List.of(
                  List.of("1", "Alice", "A"),
                  List.of("2", "Bob", "A"),
                  List.of("3", "Charlie", "B"),
                  List.of("4", "Diana", "B")));

          // Enable dynamic partition overwrite
          sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = true");

          try {
            // Step 2: Read subset of data from partition 'A'
            Dataset<Row> dataToOverwrite =
                spark()
                    .sql(String.format("SELECT * FROM %s WHERE category = 'A'", tableName))
                    .limit(1);

            // Step 3: Perform dynamic partition overwrite using insertInto
            dataToOverwrite.write().mode(SaveMode.Overwrite).insertInto(tableName);

            // Step 4: Verify data integrity
            List<List<String>> result = sql("SELECT * FROM %s ORDER BY id", tableName);

            // Verify partition 'B' is preserved (should have 2 rows)
            long categoryBCount = result.stream().filter(row -> row.get(2).equals("B")).count();

            if (categoryBCount != 2) {
              throw new AssertionError(
                  String.format(
                      "Partition category='B' should have 2 rows preserved, got %d. "
                          + "Table may have been incorrectly dropped/modified.",
                      categoryBCount));
            }

            // Verify partition 'A' was overwritten (should have 1 row due to limit(1))
            long categoryACount = result.stream().filter(row -> row.get(2).equals("A")).count();

            if (categoryACount != 1) {
              throw new AssertionError(
                  String.format(
                      "Partition category='A' should have 1 row after overwrite, got %d.",
                      categoryACount));
            }
          } finally {
            sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = false");
          }
        });
  }
}
