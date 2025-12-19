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
package io.delta.kernel.spark;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base class for V2 tests providing common setup and helper methods.
 *
 * <p>This test setup configures a SparkSession that uses V2 for reading (via the "dsv2" catalog)
 * and V1 for writing (via spark_catalog with DeltaCatalogV1). This hybrid configuration is
 * necessary until V2 supports write operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class V2TestBase {

  protected SparkSession spark;
  protected String nameSpace;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    // Spark doesn't allow '-'
    nameSpace = "ns_" + UUID.randomUUID().toString().replace('-', '_');
    SparkConf conf =
        new SparkConf()
            // V2 catalog for reading
            .set("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            // V1 extensions and catalog for writing (until V2 supports write operations)
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .setMaster("local[*]")
            .setAppName(getClass().getSimpleName());
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  /** Shorthand for String.format(). */
  protected static String str(String format, Object... args) {
    return String.format(format, args);
  }

  /**
   * Executes a SQL query and verifies the result matches the expected rows.
   *
   * @param sql the SQL query to execute
   * @param expectedRows list of expected rows, where each row is a list of column values
   */
  protected void check(String sql, List<List<Object>> expectedRows) {
    Dataset<Row> result = spark.sql(sql);
    List<Row> expected =
        expectedRows.stream()
            .map(row -> RowFactory.create(row.toArray()))
            .collect(Collectors.toList());
    assertDatasetEquals(result, expected);
  }

  protected void assertDatasetEquals(Dataset<Row> actual, List<Row> expectedRows) {
    List<Row> actualRows = actual.collectAsList();
    assertEquals(
        expectedRows,
        actualRows,
        () -> "Datasets differ: expected=" + expectedRows + "\nactual=" + actualRows);
  }

  /** Helper to create a row as a list. */
  protected static List<Object> row(Object... values) {
    return Arrays.asList(values);
  }
}

