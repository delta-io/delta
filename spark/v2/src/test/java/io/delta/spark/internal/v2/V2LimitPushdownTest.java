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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * End-to-end tests for LIMIT pushdown ({@code SupportsPushDownLimit}) in the Delta V2 connector.
 *
 * <p>The pushdown tests verify that {@code SELECT ... LIMIT N} returns the correct number of rows
 * and that the limit is reflected in (or correctly absent from) the physical plan. File-level
 * pruning behavior is covered separately by {@code DeltaV2ScanTest}.
 *
 * <p>Some of the tests also compare ordered DSv1 and DSv2 results as connector sanity checks. Those
 * queries intentionally use {@code ORDER BY} for deterministic row comparison and therefore do not
 * exercise plain LIMIT pushdown (Delta does not implement {@code SupportsPushDownTopN}).
 */
public class V2LimitPushdownTest extends V2TestBase {

  /**
   * Asserts that the physical plan for a DSv2 {@code LIMIT} query records a pushed limit of the
   * expected value. This confirms the limit reached the scan, not just that the row count was
   * correct (which Spark's own LocalLimit would also guarantee).
   */
  private void assertLimitPushed(String tablePath, int expectedLimit) {
    String query = str("SELECT * FROM dsv2.delta.`%s` LIMIT %d", tablePath, expectedLimit);
    assertLimitPushedForQuery(query, expectedLimit);
  }

  private void assertLimitPushedForQuery(String query, int expectedLimit) {
    String plan = spark.sql(query).queryExecution().executedPlan().toString();
    assertTrue(
        plan.contains("PushedLimit: " + expectedLimit),
        str("Expected 'PushedLimit: %d' in physical plan, but got:\n%s", expectedLimit, plan));
  }

  /**
   * Asserts that the physical plan for a filtered DSv2 {@code LIMIT} query does NOT record a pushed
   * limit. Used where a data filter produces a post-scan residual, which makes limit pushdown
   * unsafe (the connector must not prune files that could hold the only matching rows).
   */
  private void assertLimitNotPushed(String tablePath, String filter, int limit) {
    String query = str("SELECT * FROM dsv2.delta.`%s` WHERE %s LIMIT %d", tablePath, filter, limit);
    assertLimitNotPushedForQuery(query);
  }

  private void assertLimitNotPushedForQuery(String query) {
    String plan = spark.sql(query).queryExecution().executedPlan().toString();
    assertFalse(
        plan.contains("PushedLimit:"),
        "Expected no PushedLimit in the physical plan for query:\n" + query + "\nPlan:\n" + plan);
  }

  // Asserts ordered DSv1 and DSv2 results match as a connector-level sanity check.
  private void assertOrderedV1V2Sanity(String tablePath, String orderByCol, int limit) {
    assertOrderedV1V2SanityWithFilter(tablePath, null, orderByCol, limit);
  }

  /**
   * Asserts ordered DSv1 and DSv2 results match for an optionally filtered connector sanity query.
   * This does not exercise plain LIMIT pushdown because the query includes ORDER BY clause.
   */
  private void assertOrderedV1V2SanityWithFilter(
      String tablePath, String filter, String orderByCol, int limit) {
    String where = (filter != null) ? " WHERE " + filter : "";
    String v1Query =
        str("SELECT * FROM delta.`%s`%s ORDER BY %s LIMIT %d", tablePath, where, orderByCol, limit);
    String v2Query =
        str(
            "SELECT * FROM dsv2.delta.`%s`%s ORDER BY %s LIMIT %d",
            tablePath, where, orderByCol, limit);

    List<Row> v1Rows = spark.sql(v1Query).collectAsList();
    List<Row> v2Rows = spark.sql(v2Query).collectAsList();

    assertEquals(
        v1Rows.size(),
        v2Rows.size(),
        str("Row count mismatch: V1=%d, V2=%d", v1Rows.size(), v2Rows.size()));
    for (int i = 0; i < v1Rows.size(); i++) {
      assertEquals(
          v1Rows.get(i).toString(),
          v2Rows.get(i).toString(),
          str("Row %d differs between V1 and V2", i));
    }
  }

  // Correctness Tests
  @Test
  public void testLimitBasic(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 3", tablePath)).count();
    assertEquals(3, count, "LIMIT 3 should return exactly 3 rows");
    assertLimitPushed(tablePath, 3);
  }

  @Test
  public void testLimitLargerThanTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 100", tablePath)).count();
    assertEquals(3, count, "LIMIT larger than the table should return all rows");
    assertLimitPushed(tablePath, 100);
  }

  @Test
  public void testLimit0(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 0", tablePath)).count();
    assertEquals(0, count, "LIMIT 0 should return 0 rows");
  }

  @Test
  public void testLimit1(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 1", tablePath)).count();
    assertEquals(1, count, "LIMIT 1 should return exactly 1 row");
    assertLimitPushed(tablePath, 1);
  }

  @Test
  public void testLimitEmptyTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 10", tablePath)).count();
    assertEquals(0, count, "LIMIT on an empty table should return 0 rows");
    assertLimitPushed(tablePath, 10);
  }

  @Test
  public void testLimitWithDeletionVectors(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));
    spark
        .range(1000)
        .selectExpr("id", "cast(id as string) as value")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete half the rows, producing deletion vectors.
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created");

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 50", tablePath)).count();
    assertEquals(50, count, "LIMIT 50 with DVs should return exactly 50 rows");
    assertLimitPushed(tablePath, 50);
  }

  @Test
  public void testLimitWithHeavyDVs(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // Two single-file batches so file-level DV accounting matters for the limit.
    spark
        .range(0, 100)
        .selectExpr("id", "cast(id as string) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);
    spark
        .range(100, 200)
        .selectExpr("id", "cast(id as string) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete 60 rows from each file via DVs, leaving 40 logical rows in each. LIMIT 50 must
    // therefore span both files regardless of their enumeration order.
    String deletePredicate = "(id >= 40 AND id < 100) OR (id >= 140 AND id < 200)";
    spark.sql(str("DELETE FROM delta.`%s` WHERE %s", tablePath, deletePredicate));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 50", tablePath)).count();
    assertEquals(50, count, "LIMIT 50 with heavy DVs should return exactly 50 rows");
    assertLimitPushed(tablePath, 50);
  }

  @Test
  public void testLimitWithPartitionFilter(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part STRING) USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'), (5, 'b')",
            tablePath));

    // A partition filter is fully pushed (no post-scan residual), so limit pushdown stays active.
    long count =
        spark.sql(str("SELECT * FROM dsv2.delta.`%s` WHERE part = 'a' LIMIT 2", tablePath)).count();
    assertEquals(2, count, "Partition filter + LIMIT should return 2 rows");
    assertLimitPushedForQuery(
        str("SELECT * FROM dsv2.delta.`%s` WHERE part = 'a' LIMIT 2", tablePath), 2);
    assertOrderedV1V2SanityWithFilter(tablePath, "part = 'a'", "id", 2);
  }

  @Test
  public void testLimitWithDataFilter(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            tablePath));

    // A data filter becomes a post-scan residual, so the limit must NOT be pushed — but the query
    // must still return correct results.
    long count =
        spark.sql(str("SELECT * FROM dsv2.delta.`%s` WHERE id > 2 LIMIT 2", tablePath)).count();
    assertEquals(2, count, "Data filter + LIMIT should return 2 rows");
    assertLimitNotPushed(tablePath, "id > 2", 2);
  }

  @Test
  public void testLimitWithDynamicPartitionPruningIsNotPushed(@TempDir File tempDir)
      throws Exception {
    String factPath = new File(tempDir, "fact").getAbsolutePath();
    String dimensionPath = new File(tempDir, "dimension").getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part INT) USING delta PARTITIONED BY (part)",
            factPath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 1), (2, 2), (3, 3)", factPath));
    spark.sql(str("CREATE TABLE delta.`%s` (part INT, tag STRING) USING delta", dimensionPath));
    spark.sql(
        str("INSERT INTO delta.`%s` VALUES (1, 'drop'), (2, 'keep'), (3, 'drop')", dimensionPath));

    String query =
        str(
            "SELECT fact.id FROM dsv2.delta.`%s` fact "
                + "JOIN dsv2.delta.`%s` dimension ON fact.part = dimension.part "
                + "WHERE dimension.tag = 'keep' LIMIT 1",
            factPath, dimensionPath);

    withSQLConf(
        "spark.sql.adaptive.enabled",
        "false",
        () ->
            withSQLConf(
                "spark.sql.optimizer.dynamicPartitionPruning.enabled",
                "true",
                () -> {
                  Dataset<Row> result = spark.sql(query);
                  String plan = result.queryExecution().executedPlan().toString();

                  assertTrue(
                      plan.contains("RuntimeFilters: [dynamicpruningexpression("),
                      "Expected a DSv2 dynamic partition pruning runtime filter, but got:\n"
                          + plan);
                  assertFalse(
                      plan.contains("PushedLimit:"),
                      "LIMIT must not be pushed into a scan receiving a runtime filter:\n" + plan);
                  assertEquals(
                      1,
                      result.collectAsList().size(),
                      "Runtime pruning with LIMIT should return one row");
                }));
  }

  @Test
  public void testLimitWithColumnProjection(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str("CREATE TABLE delta.`%s` (id INT, name STRING, value DOUBLE) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)",
            tablePath));

    Dataset<Row> result = spark.sql(str("SELECT name FROM dsv2.delta.`%s` LIMIT 2", tablePath));
    assertEquals(2, result.count(), "Column projection + LIMIT should return 2 rows");
    assertEquals(1, result.columns().length, "Projected result should have exactly 1 column");
    assertEquals("name", result.columns()[0]);
    assertLimitPushedForQuery(str("SELECT name FROM dsv2.delta.`%s` LIMIT 2", tablePath), 2);
  }

  @Test
  public void testLimitWithColumnMapping(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 2", tablePath)).count();
    assertEquals(2, count, "LIMIT with column mapping should return 2 rows");
    assertLimitPushed(tablePath, 2);
  }

  // Robustness Tests
  @Test
  public void testLimitWithMultipleFiles(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));

    // One row per file (20 files) so the limit must terminate part-way through the file list.
    for (int i = 0; i < 20; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 3", tablePath)).count();
    assertEquals(3, count, "LIMIT 3 over 20 single-row files should return 3 rows");
    assertLimitPushed(tablePath, 3);
  }

  @Test
  public void testLimitWithNonDeterministicFilterIsNotPushed(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark
        .range(100)
        .selectExpr("cast(id as int) as id")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // A seeded rand gives reproducible results but is still a non-deterministic Catalyst expression
    // that must remain a post-scan filter. Pruning files before evaluating it would be unsafe.
    String query = str("SELECT * FROM dsv2.delta.`%s` WHERE rand(0) > 0.5 LIMIT 5", tablePath);
    assertEquals(5, spark.sql(query).count());
    assertLimitNotPushedForQuery(query);
  }

  @Test
  public void testLimitWithOffsetPushesCombinedRowRequirement(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3), (4), (5)", tablePath));

    String query = str("SELECT * FROM dsv2.delta.`%s` LIMIT 2 OFFSET 1", tablePath);
    assertEquals(2, spark.sql(query).count());
    // Spark pushes LIMIT + OFFSET so the retained OFFSET still has three input rows available.
    assertLimitPushedForQuery(query, 3);
  }

  @Test
  public void testLimitStreamingUnaffected(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    // Write via V1 (V2 does not support writes yet).
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4), (5)", tablePath));

    // A streaming read must read all data regardless of any batch limit-pushdown state.
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "1").table(dsv2TableRef);

    String queryName = "limit_streaming_test_" + System.nanoTime();
    List<Row> rows = processStreamingQuery(streamingDF, queryName);
    assertEquals(5, rows.size(), "Streaming should read all 5 rows regardless of limit pushdown");
  }

  @Test
  public void testLimitOrderBy(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd')",
            tablePath));

    // ORDER BY + LIMIT is not a pushed TopN (no SupportsPushDownTopN); Spark handles it via
    // Sort + LocalLimit. Results must still be correct and match V1.
    List<Row> rows =
        spark
            .sql(str("SELECT * FROM dsv2.delta.`%s` ORDER BY id LIMIT 3", tablePath))
            .collectAsList();
    assertEquals(3, rows.size(), "ORDER BY + LIMIT should return 3 rows");
    assertEquals(1, rows.get(0).getInt(0), "First row should have id=1");
    assertEquals(2, rows.get(1).getInt(0), "Second row should have id=2");
    assertEquals(3, rows.get(2).getInt(0), "Third row should have id=3");
    assertLimitNotPushedForQuery(
        str("SELECT * FROM dsv2.delta.`%s` ORDER BY id LIMIT 3", tablePath));
    assertOrderedV1V2Sanity(tablePath, "id", 3);
  }
}
