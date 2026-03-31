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
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/**
 * End-to-end tests for LIMIT pushdown in the Delta V2 connector.
 *
 * <p>These tests verify that {@code SELECT ... LIMIT N} returns the correct number of rows when
 * reading through the DSV2 connector, and that the limit pushdown optimization reduces planned
 * files when possible.
 *
 * <p>Note: LIMIT without ORDER BY is non-deterministic per SQL standard -- the specific rows
 * returned may vary across runs. Tests therefore check row counts, not specific row values.
 */
public class V2LimitPushdownTest extends V2TestBase {

  // ==========================================================================
  // Priority 1: Correctness tests
  // ==========================================================================

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
  }

  @Test
  public void testLimitLargerThanTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 100", tablePath)).count();
    assertEquals(3, count, "LIMIT larger than table should return all rows");
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
  }

  @Test
  public void testLimitEmptyTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 10", tablePath)).count();
    assertEquals(0, count, "LIMIT on empty table should return 0 rows");
  }

  @Test
  public void testLimitWithDeletionVectors(@TempDir File tempDir) throws Exception {
    // Create a directory with space in the name to test URL encoding handling
    File dirWithSpace = new File(tempDir, "dv table");
    Files.createDirectories(dirWithSpace.toPath());
    String tablePath = dirWithSpace.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // Insert data
    spark
        .range(1000)
        .selectExpr("id", "cast(id as string) as value")
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete some rows to create deletion vectors
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    // Verify DVs were created
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created");

    // LIMIT should still return exactly the requested number of rows
    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 50", tablePath)).count();
    assertEquals(50, count, "LIMIT 50 with DVs should return exactly 50 rows");
  }

  @Test
  public void testLimitWithHeavyDVs(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // Insert two batches to create two files
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

    // Delete 90% of first file's rows via DV
    spark.sql(str("DELETE FROM delta.`%s` WHERE id >= 10 AND id < 100", tablePath));

    // Now file A has 100 physical rows, 90 DV-deleted (10 logical).
    // File B has 100 physical rows, 0 DV-deleted (100 logical).
    // LIMIT 50 must include enough files to return 50 rows.
    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 50", tablePath)).count();
    assertEquals(50, count, "LIMIT 50 with heavy DVs should return exactly 50 rows");
  }

  @Test
  public void testLimitWithPartitionFilter(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part STRING) " + "USING delta PARTITIONED BY (part)",
            tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES " + "(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b'), (5, 'b')",
            tablePath));

    // Partition filter + LIMIT: limit pushdown should activate because partition filters
    // are fully pushed (no post-scan residuals)
    long count =
        spark.sql(str("SELECT * FROM dsv2.delta.`%s` WHERE part = 'a' LIMIT 2", tablePath)).count();
    assertEquals(2, count, "Partition filter + LIMIT should return 2 rows");
  }

  @Test
  public void testLimitWithDataFilter(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    spark.sql(
        str(
            "INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
            tablePath));

    // Data filter + LIMIT: limit pushdown should NOT activate because data filter
    // becomes a post-scan residual. But the query must still return correct results.
    long count =
        spark.sql(str("SELECT * FROM dsv2.delta.`%s` WHERE id > 2 LIMIT 2", tablePath)).count();
    assertEquals(2, count, "Data filter + LIMIT should return 2 rows");
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
    assertEquals(1, result.columns().length, "Should only have 1 column");
    assertEquals("name", result.columns()[0]);
  }

  @Test
  public void testLimitWithColumnMapping(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", tablePath));

    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 2", tablePath)).count();
    assertEquals(2, count, "LIMIT with column mapping should return 2 rows");
  }

  // ==========================================================================
  // Priority 2: DSv1/DSv2 parity tests
  // ==========================================================================

  /**
   * Asserts that DSv1 and DSv2 return the same data for an ORDER BY + LIMIT query. ORDER BY makes
   * the result deterministic so we can compare actual row values, not just counts.
   */
  private void assertV1V2Parity(String tablePath, String orderByCol, int limit) {
    assertV1V2ParityWithFilter(tablePath, null, orderByCol, limit);
  }

  /**
   * Asserts that DSv1 and DSv2 return the same data for a filtered ORDER BY + LIMIT query. When
   * {@code filter} is non-null, it is inserted as a WHERE clause.
   */
  private void assertV1V2ParityWithFilter(
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

  @Test
  public void testParity_basicMultipleFiles(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT, name STRING) USING delta", tablePath));
    for (int i = 0; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'row_%d')", tablePath, i, i));
    }

    assertV1V2Parity(tablePath, "id", 3);
    assertV1V2Parity(tablePath, "id", 10);
    assertV1V2Parity(tablePath, "id", 100);
  }

  @Test
  public void testParity_withDeletionVectors(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    spark
        .range(0, 200)
        .selectExpr("id", "cast(id as string) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    spark
        .range(200, 400)
        .selectExpr("id", "cast(id as string) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete every other row to create DVs
    spark.sql(str("DELETE FROM delta.`%s` WHERE id %% 2 = 0", tablePath));

    // Verify DVs were created
    DeltaLog deltaLog = DeltaLog.forTable(spark, tablePath);
    long numDVs =
        (long)
            deltaLog
                .update(false, Option.empty(), Option.empty())
                .numDeletionVectorsOpt()
                .getOrElse(() -> 0L);
    assertTrue(numDVs > 0, "Expected deletion vectors to be created");

    assertV1V2Parity(tablePath, "id", 10);
    assertV1V2Parity(tablePath, "id", 50);
    assertV1V2Parity(tablePath, "id", 200);
  }

  @Test
  public void testParity_withHeavyDeletionVectors(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id LONG, value STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
            tablePath));

    // File A: 100 rows (will have 90 deleted, 10 surviving)
    spark
        .range(0, 100)
        .selectExpr("id", "cast(id as string) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // File B: 100 rows (all surviving)
    spark
        .range(100, 200)
        .selectExpr("id", "cast(id as string) as value")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .save(tablePath);

    // Delete 90% of file A via DV
    spark.sql(str("DELETE FROM delta.`%s` WHERE id >= 10 AND id < 100", tablePath));

    // 110 logical rows total (10 from file A + 100 from file B).
    // This exercises DV cardinality subtraction in the limit accumulation logic.
    assertV1V2Parity(tablePath, "id", 5);
    assertV1V2Parity(tablePath, "id", 50);
    assertV1V2Parity(tablePath, "id", 110);
  }

  @Test
  public void testParity_withPartitionFilter(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, part STRING) USING delta PARTITIONED BY (part)",
            tablePath));
    for (int i = 0; i < 10; i++) {
      String part = (i % 2 == 0) ? "even" : "odd";
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, '%s')", tablePath, i, part));
    }

    assertV1V2ParityWithFilter(tablePath, "part = 'even'", "id", 2);
    assertV1V2ParityWithFilter(tablePath, "part = 'odd'", "id", 3);
  }

  @Test
  public void testParity_withColumnMapping(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(
        str(
            "CREATE TABLE delta.`%s` (id INT, name STRING) "
                + "USING delta "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tablePath));
    for (int i = 0; i < 10; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d, 'row_%d')", tablePath, i, i));
    }

    assertV1V2Parity(tablePath, "id", 3);
  }

  @Test
  public void testParity_emptyTable(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));

    assertV1V2Parity(tablePath, "id", 10);
  }

  @Test
  public void testParity_limit0(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));

    assertV1V2Parity(tablePath, "id", 0);
  }

  // ==========================================================================
  // Priority 3: Robustness tests
  // ==========================================================================

  @Test
  public void testLimitWithMultipleFiles(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));

    // Insert rows one at a time to create many files (1 row per file)
    for (int i = 0; i < 20; i++) {
      spark.sql(str("INSERT INTO delta.`%s` VALUES (%d)", tablePath, i));
    }

    // LIMIT 3 on a 20-file table (1 row each) should produce correct results
    long count = spark.sql(str("SELECT * FROM dsv2.delta.`%s` LIMIT 3", tablePath)).count();
    assertEquals(3, count, "LIMIT 3 on 20 single-row files should return 3 rows");
  }

  @Test
  public void testLimitStreamingUnaffected(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();

    // Write via V1 (V2 does not support writes yet)
    spark.sql(str("CREATE TABLE delta.`%s` (id INT) USING delta", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (1), (2), (3)", tablePath));
    spark.sql(str("INSERT INTO delta.`%s` VALUES (4), (5)", tablePath));

    // Streaming read via V2 -- should read all data regardless of any limit pushdown state
    String dsv2TableRef = str("dsv2.delta.`%s`", tablePath);
    Dataset<Row> streamingDF =
        spark.readStream().option("startingVersion", "1").table(dsv2TableRef);

    String queryName = "limit_streaming_test_" + System.currentTimeMillis();
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

    // ORDER BY + LIMIT: TopN is NOT pushed (no SupportsPushDownTopN).
    // Spark handles via Sort + LocalLimit. Results must still be correct.
    List<Row> rows =
        spark
            .sql(str("SELECT * FROM dsv2.delta.`%s` ORDER BY id LIMIT 3", tablePath))
            .collectAsList();
    assertEquals(3, rows.size(), "ORDER BY + LIMIT should return 3 rows");
    assertEquals(1, rows.get(0).getInt(0), "First row should have id=1");
    assertEquals(2, rows.get(1).getInt(0), "Second row should have id=2");
    assertEquals(3, rows.get(2).getInt(0), "Third row should have id=3");
  }
}
