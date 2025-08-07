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
package io.delta.spark.dsv2;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Dsv2BasicTest {

  private SparkSession spark;
  private String nameSpace;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    // Spark doesn't allow '-'
    nameSpace = "ns_" + UUID.randomUUID().toString().replace('-', '_');
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.spark.dsv2.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            // Enable Delta for path-based writes like delta.`path`
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .setMaster("local[*]")
            .setAppName("Dsv2BasicTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateTable() {
    spark.sql(
        String.format(
            "CREATE TABLE dsv2.%s.create_table_test (id INT, name STRING, value DOUBLE)",
            nameSpace));

    Dataset<Row> actual =
        spark.sql(String.format("DESCRIBE TABLE dsv2.%s.create_table_test", nameSpace));

    List<Row> expectedRows =
        Arrays.asList(
            RowFactory.create("id", "int", null),
            RowFactory.create("name", "string", null),
            RowFactory.create("value", "double", null));
    assertDatasetEquals(actual, expectedRows);
  }

  @Test
  public void testQueryTable() {
    spark.sql(
        String.format(
            "CREATE TABLE dsv2.%s.query_test (id INT, name STRING, value DOUBLE)", nameSpace));

    // Lookup the physical path stored by TestCatalog as a table property
    Dataset<Row> props =
        spark.sql(String.format("SHOW TBLPROPERTIES dsv2.%s.query_test", nameSpace));
    List<Row> propRows = props.collectAsList();
    String tablePath = null;
    for (Row r : propRows) {
      if ("_test_table_location".equals(r.getString(0))) {
        tablePath = r.getString(1);
        break;
      }
    }
    assertNotNull(tablePath, "_test_table_location should exist in table properties");

    // Insert some test data via delta.`path`
    spark.sql(
        String.format(
            "INSERT INTO delta.`%s` VALUES (1, 'test1', 1.1), (2, 'test2', 2.2)", tablePath));

    // Test that we can query the table via catalog
    Dataset<Row> result =
        spark.sql(String.format("SELECT * FROM dsv2.%s.query_test ORDER BY id", nameSpace));

    // Verify the result
    List<Row> rows = result.collectAsList();
    assertEquals(2, rows.size(), "Should have 2 rows");

    // Check first row
    Row firstRow = rows.get(0);
    assertEquals(1, firstRow.getInt(0), "First row id should be 1");
    assertEquals("test1", firstRow.getString(1), "First row name should be 'test1'");
    assertEquals(1.1, firstRow.getDouble(2), 0.001, "First row value should be 1.1");

    // Check second row
    Row secondRow = rows.get(1);
    assertEquals(2, secondRow.getInt(0), "Second row id should be 2");
    assertEquals("test2", secondRow.getString(1), "Second row name should be 'test2'");
    assertEquals(2.2, secondRow.getDouble(2), 0.001, "Second row value should be 2.2");
  }

  @Test
  public void testQueryTableNotExist() {
    AnalysisException e =
        assertThrows(
            AnalysisException.class,
            () -> spark.sql(String.format("SELECT * FROM dsv2.%s.not_found_test", nameSpace)));
    assertEquals(
        "TABLE_OR_VIEW_NOT_FOUND",
        e.getErrorClass(),
        "Missing table should raise TABLE_OR_VIEW_NOT_FOUND");
  }

  //////////////////////
  // Private helpers //
  /////////////////////
  private void assertDatasetEquals(Dataset<Row> actual, List<Row> expectedRows) {
    List<Row> actualRows = actual.collectAsList();
    assertEquals(
        expectedRows,
        actualRows,
        () -> "Datasets differ: expected=" + expectedRows + "\nactual=" + actualRows);
  }
}
