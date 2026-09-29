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

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.datasources.v2.DeleteFromTableExec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.util.QueryExecutionListener;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for the DSv2 metadata-only DELETE path.
 */
public class V2MetadataOnlyDeleteTest extends DeltaV2TestBase {

  private static final String V2_ENABLE_MODE = "spark.databricks.delta.v2.enableMode";

  @Test
  public void testPartitionPredicateDeleteRemovesMatchingPartition() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'a'), (3, 'b'), (4, 'c')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part = 'a'");

          assertTableRows(
              tablePath, "id, part", RowFactory.create(3, "b"), RowFactory.create(4, "c"));
        });
  }

  @Test
  public void testMetadataOnlyDeletePreservesNumRecordsInRemoveFile() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'a'), (3, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part = 'a'");

          Dataset<Row> transactionLog = spark.read().json(tablePath + "/_delta_log/*.json");
          Dataset<Row> addedFileNumRecords =
              transactionLog
                  .where("add IS NOT NULL")
                  .selectExpr(
                      "add.path AS path",
                      "CAST(get_json_object(add.stats, '$.numRecords') AS BIGINT) AS numRecords");
          Dataset<Row> removedFileNumRecords =
              transactionLog
                  .where("remove IS NOT NULL")
                  .selectExpr(
                      "remove.path AS path",
                      "CAST(get_json_object(remove.stats, '$.numRecords') AS BIGINT) "
                          + "AS numRecords");

          assertFalse(removedFileNumRecords.isEmpty());
          assertEquals(0, removedFileNumRecords.where("numRecords IS NULL").count());
          assertTrue(removedFileNumRecords.exceptAll(addedFileNumRecords).isEmpty());
        });
  }

  @Test
  public void testMetadataOnlyDeleteInAutoMode() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b')");

          withSQLConf(
              V2_ENABLE_MODE,
              "AUTO",
              () -> {
                String sql =
                    String.format(
                        "DELETE FROM %s WHERE part = 'a'", sparkCatalogTable(tablePath));
                boolean ranAsDsv2Delete =
                    executeAndCapturePlans(sql).stream()
                        .anyMatch(plan -> plan.exists(p -> p instanceof DeleteFromTableExec));
                assertFalse(ranAsDsv2Delete, "AUTO mode should fall back to the V1 DELETE path");
              });

          assertTableRows(tablePath, "id, part", RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testNonMetadataPredicateDoesNotUseMetadataOnlyDelete() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b')");

          assertThrows(
              UnsupportedOperationException.class,
              () ->
                  spark.sql(
                      String.format(
                          "DELETE FROM %s WHERE part = 'a' OR id = 2", dsv2Table(tablePath))));

          assertTableRows(
              tablePath, "id, part", RowFactory.create(1, "a"), RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testColumnMappingTableDeleteUsesPhysicalSchema() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE delta.`%s` (id INT, part STRING) USING delta "
                      + "PARTITIONED BY (part) "
                      + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
                  tablePath));
          insertRows(tablePath, "(1, 'a'), (2, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part = 'a'");

          assertTableRows(tablePath, "id, part", RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testWholeTableDeleteRemovesAllRows() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, null);

          assertTableRows(tablePath, "id, part");
        });
  }

  @Test
  public void testPartitionPredicateDeleteWithNoMatchIsNoOp() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part = 'nonExistent'");

          assertTableRows(
              tablePath, "id, part", RowFactory.create(1, "a"), RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testMetadataOnlyDeleteLeavesUntouchedPartitionFilesStable() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          // Each partition is written as its own file.
          insertRows(tablePath, "(1, 'a')");
          insertRows(tablePath, "(2, 'b')");

          List<Row> initialFiles = filePathsById(tablePath);
          assertEquals(2, initialFiles.size());
          String deletedFilePath = initialFiles.get(0).getString(1);
          String untouchedFilePath = initialFiles.get(1).getString(1);
          assertNotEquals(deletedFilePath, untouchedFilePath);

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part = 'a'");

          List<Row> remainingFiles = filePathsById(tablePath);
          assertEquals(1, remainingFiles.size());
          assertEquals(2, remainingFiles.get(0).getInt(0));
          // The surviving partition's file is untouched (not rewritten): metadata-only delete only
          // removes the matching file and never rewrites the others.
          assertEquals(untouchedFilePath, remainingFiles.get(0).getString(1));
          assertTableRows(tablePath, "id, part", RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testInListDeleteRemovesMatchingPartitions() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b'), (3, 'c')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part IN ('a', 'c')");

          assertTableRows(tablePath, "id, part", RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testOrOnSamePartitionColumnRemovesBothPartitions() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b'), (3, 'c')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(
              tablePath, "part = 'a' OR part = 'c'");

          assertTableRows(tablePath, "id, part", RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testRangeComparisonDeleteRemovesMatchingPartitions() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b'), (3, 'c')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part >= 'b'");

          assertTableRows(tablePath, "id, part", RowFactory.create(1, "a"));
        });
  }

  @Test
  public void testInequalityDeleteRemovesMatchingPartitions() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b'), (3, 'c')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part <> 'a'");

          assertTableRows(tablePath, "id, part", RowFactory.create(1, "a"));
        });
  }

  @Test
  public void testLikePrefixDeleteRemovesMatchingPartitions() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'apple'), (2, 'apricot'), (3, 'banana')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part LIKE 'a%'");

          assertTableRows(tablePath, "id, part", RowFactory.create(3, "banana"));
        });
  }

  @Test
  public void testSubstringOnPartitionColumnDelete() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'apple'), (2, 'avocado'), (3, 'banana')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(
              tablePath, "substring(part, 1, 1) = 'a'");

          assertTableRows(tablePath, "id, part", RowFactory.create(3, "banana"));
        });
  }

  @Test
  public void testUdfOnPartitionColumnDelete() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          spark
              .udf()
              .register(
                  "starts_with_a",
                  (UDF1<String, Boolean>) s -> s != null && s.startsWith("a"),
                  DataTypes.BooleanType);
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'apple'), (2, 'avocado'), (3, 'banana')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "starts_with_a(part)");

          assertTableRows(tablePath, "id, part", RowFactory.create(3, "banana"));
        });
  }

  @Test
  public void testArithmeticOnIntPartitionColumnDelete() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE delta.`%s` (id INT, pid INT) USING delta PARTITIONED BY (pid)",
                  tablePath));
          spark.sql(
              String.format(
                  "INSERT INTO delta.`%s` VALUES (1, 1), (2, 2), (3, 3)", tablePath));

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "pid + 1 = 3");

          assertTableRows(
              tablePath, "id, pid", RowFactory.create(1, 1), RowFactory.create(3, 3));
        });
  }

  @Test
  public void testMixedOrWithUntranslatableAndSimpleLeg() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b'), (3, 'c')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(
              tablePath, "part = 'a' OR substring(part, 1, 1) = 'c'");

          assertTableRows(tablePath, "id, part", RowFactory.create(2, "b"));
        });
  }

  @Test
  public void testAndAcrossTwoPartitionColumns() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          spark.sql(
              String.format(
                  "CREATE TABLE delta.`%s` (id INT, p0 STRING, p1 STRING) USING delta "
                      + "PARTITIONED BY (p0, p1)",
                  tablePath));
          spark.sql(
              String.format(
                  "INSERT INTO delta.`%s` VALUES "
                      + "(1, 'a', 'x'), (2, 'a', 'y'), (3, 'b', 'x')",
                  tablePath));

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(
              tablePath, "p0 = 'a' AND p1 = 'x'");

          assertTableRows(
              tablePath,
              "id, p0, p1",
              RowFactory.create(2, "a", "y"),
              RowFactory.create(3, "b", "x"));
        });
  }

  @Test
  public void testAlwaysTrueConditionRemovesAllRows() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "1 = 1");

          assertTableRows(tablePath, "id, part");
        });
  }

  @Test
  public void testIsNullOnPartitionColumnDelete() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, null), (3, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part IS NULL");

          assertTableRows(
              tablePath, "id, part", RowFactory.create(1, "a"), RowFactory.create(3, "b"));
        });
  }

  @Test
  public void testIsNotNullOnPartitionColumnDelete() throws Exception {
    String tablePath = newTablePath("delta-v2-mod");
    withTable(
        tablePath,
        () -> {
          createIdPartTable(tablePath);
          insertRows(tablePath, "(1, 'a'), (2, null), (3, 'b')");

          executeAndAssertDsv2MetadataOnlyDeleteAtPath(tablePath, "part IS NOT NULL");

          assertTableRows(tablePath, "id, part", RowFactory.create(2, null));
        });
  }

  private void createIdPartTable(String tablePath) {
    spark.sql(
        String.format(
            "CREATE TABLE delta.`%s` (id INT, part STRING) USING delta PARTITIONED BY (part)",
            tablePath));
  }

  private String sparkCatalogTable(String tablePath) {
    return String.format("delta.`%s`", tablePath);
  }

  private void insertRows(String tablePath, String valuesSql) {
    spark.sql(String.format("INSERT INTO delta.`%s` VALUES %s", tablePath, valuesSql));
  }

  private List<Row> filePathsById(String tablePath) {
    return spark
        .sql(
            String.format(
                "SELECT id, _metadata.file_path AS file_path FROM delta.`%s` ORDER BY id",
                tablePath))
        .collectAsList();
  }

  private void assertTableRows(String tablePath, String columns, Row... expectedRows) {
    List<Row> actualRows =
        spark
            .sql(String.format("SELECT %s FROM delta.`%s` ORDER BY id", columns, tablePath))
            .collectAsList();
    List<Row> expected = Arrays.asList(expectedRows);
    assertEquals(
        expected,
        actualRows,
        () -> "Unexpected table contents for " + tablePath + ": " + actualRows);
    assertEquals(
        spark
            .sql(String.format("SELECT %s FROM %s ORDER BY id", columns, dsv2Table(tablePath)))
            .collectAsList(),
        actualRows,
        "DSv2 and V1 reads should observe the same committed rows");
  }

  /**
   * Runs a DELETE against the {@code dsv2} catalog and asserts that it executed as a DSv2
   * metadata-only delete: the physical plan is the DSv2 {@link DeleteFromTableExec} node.
   *
   * @param tablePath the table's filesystem path
   * @param whereClause the DELETE predicate without the {@code WHERE} keyword, or {@code null} for
   *     a whole-table delete
   */
  private void executeAndAssertDsv2MetadataOnlyDeleteAtPath(String tablePath, String whereClause) {
    executeAndAssertDsv2MetadataOnlyDelete(dsv2Table(tablePath), whereClause);
  }

  private void executeAndAssertDsv2MetadataOnlyDelete(String table, String whereClause) {
    String sql =
        whereClause == null
            ? String.format("DELETE FROM %s", table)
            : String.format("DELETE FROM %s WHERE %s", table, whereClause);

    boolean ranAsMetadataOnlyDelete =
        executeAndCapturePlans(sql).stream()
            .anyMatch(plan -> plan.exists(p -> p instanceof DeleteFromTableExec));
    assertTrue(
        ranAsMetadataOnlyDelete,
        "Expected the DELETE to execute as a DSv2 metadata-only DeleteFromTableExec");
  }

  /**
   * Runs {@code sql} and returns every executed physical plan captured via a listener. A DSv2
   * metadata-only delete selects files via {@code filesForScan}, which runs its own nested Spark
   * queries (materializing {@code allFiles}). Hence, more than one plan is executed. Callers inspect
   * the collected plans for the node they care about rather than assuming a single one.
   */
  private List<SparkPlan> executeAndCapturePlans(String sql) {
    List<SparkPlan> captured = java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    QueryExecutionListener listener = new QueryExecutionListener() {
      @Override
      public void onSuccess(String funcName, QueryExecution qe, long durationNs) {
        captured.add(qe.executedPlan());
      }

      @Override
      public void onFailure(String funcName, QueryExecution qe, Exception exception) {}
    };

    spark.listenerManager().register(listener);
    try {
      spark.sql(sql);
      spark.sparkContext().listenerBus().waitUntilEmpty(60_000L);
    } catch (java.util.concurrent.TimeoutException e) {
      throw new RuntimeException("Timed out waiting for the query-execution listener", e);
    } finally {
      spark.listenerManager().unregister(listener);
    }
    assertFalse(
        captured.isEmpty(), "QueryExecutionListener did not capture an executed plan for: " + sql);
    return captured;
  }
}
