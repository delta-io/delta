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

package com.sparkuctest;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test suite to verify that the UnityCatalogSupport class correctly integrates
 * Delta Lake with Unity Catalog server.
 *
 * These tests validate that:
 * 1. The UC server is started and accessible
 * 2. Tables created via Spark are registered in UC
 * 3. Basic operations work correctly through UC
 *
 * The UC server is started automatically via UnityCatalogSupport class.
 * We use the UC SDK to directly query the UC server to confirm operations.
 */
public class UnityCatalogSupportSuite extends UnityCatalogSupport {

  private SparkSession spark;

  @Before
  public void setUp() throws Exception {
    super.setupUnityCatalog(); // Start UC server first

    SparkConf conf = new SparkConf()
        .setAppName("UnityCatalog Support Tests")
        .setMaster("local[2]")
        .set("spark.ui.enabled", "false")
        .set("spark.sql.shuffle.partitions", "5")
        // Delta Lake required configurations
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");

    // Configure with Unity Catalog
    conf = configureSparkWithUnityCatalog(conf);

    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @After
  public void tearDown() throws Exception {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
    super.tearDownUnityCatalog(); // Stop UC server last
  }

  /**
   * Helper to create temp directory.
   */
  private void withTempDir(TempDirCode code) throws Exception {
    File tempDir = Files.createTempDirectory("spark-test-").toFile();
    try {
      code.run(tempDir);
    } finally {
      deleteRecursively(tempDir);
    }
  }

  /**
   * Recursively delete a directory.
   */
  private void deleteRecursively(File file) {
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File child : files) {
          deleteRecursively(child);
        }
      }
    }
    file.delete();
  }

  @FunctionalInterface
  private interface TempDirCode {
    void run(File dir) throws Exception;
  }

  /**
   * Helper to list tables in UC server directly via SDK.
   */
  private List<String> listTables(String catalogName, String schemaName) throws Exception {
    ApiClient client = createUnityCatalogClient();
    TablesApi tablesApi = new TablesApi(client);
    ListTablesResponse response = tablesApi.listTables(catalogName, schemaName, null, null);

    if (response.getTables() != null) {
      return response.getTables().stream()
          .map(TableInfo::getName)
          .collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * Helper to check answer - simplified version for Java tests.
   */
  private void checkAnswer(Dataset<Row> df, List<Row> expected) {
    Row[] actual = (Row[]) df.collect();
    Assert.assertEquals("Row count mismatch", expected.size(), actual.length);
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals("Row " + i + " mismatch", expected.get(i), actual[i]);
    }
  }

  @Test
  public void testUnityCatalogSupportStartsServerAndConfiguresSpark() throws Exception {
    // 1. Verify UC server is accessible via URI
    Assert.assertTrue(
        "Unity Catalog URI should be localhost, got: " + getUnityCatalogUri(),
        getUnityCatalogUri().startsWith("http://localhost:")
    );

    // 2. Verify we can access schemas in the UC catalog via Spark
    Dataset<Row> schemasDs = spark.sql("SHOW SCHEMAS IN " + getUnityCatalogName());
    Row[] schemasArr = (Row[]) schemasDs.collect();
    List<String> schemas = Arrays.stream(schemasArr)
        .map(row -> row.getString(0))
        .collect(Collectors.toList());
    Assert.assertTrue(
        "Unity Catalog should have 'default' schema. Found: " + String.join(", ", schemas),
        schemas.contains("default")
    );

    // 3. Verify we can query UC server directly via SDK
    List<String> ucTables = listTables(getUnityCatalogName(), "default");
    Assert.assertNotNull("Should be able to query UC server via SDK", ucTables);

    // 4. Verify we can create a table in the UC catalog
    withTempDir((File dir) -> {
      File tableDir = new File(dir, "test_verify");
      String tablePath = tableDir.getAbsolutePath();
      String testTable = getUnityCatalogName() + ".default.test_verify_catalog";

      spark.sql(
          "CREATE TABLE " + testTable + " (id INT) USING PARQUET LOCATION '" + tablePath + "'"
      );

      // Insert data
      spark.sql("INSERT INTO " + testTable + " VALUES (1), (2), (3)");

      // Verify we can select the data
      Dataset<Row> result = spark.sql("SELECT * FROM " + testTable + " ORDER BY id");
      Row[] actualRows = (Row[]) result.collect();
      Assert.assertEquals("Should have 3 rows", 3, actualRows.length);
      Assert.assertEquals(1, actualRows[0].getInt(0));
      Assert.assertEquals(2, actualRows[1].getInt(0));
      Assert.assertEquals(3, actualRows[2].getInt(0));

      // Cleanup
      spark.sql("DROP TABLE " + testTable);
    });
  }

  @Test
  public void testCreateTableViaSparkRegistersTableInUCServer() throws Exception {
    withTempDir((File dir) -> {
      File tableDir = new File(dir, "test_table");
      String tablePath = tableDir.getAbsolutePath();
      String tableName = "test_table_create";
      String fullTableName = getUnityCatalogName() + ".default." + tableName;

      // Create an external Delta table via Spark
      spark.sql(
          "CREATE TABLE " + fullTableName + " (" +
          "  id INT, " +
          "  name STRING" +
          ") USING DELTA " +
          "LOCATION '" + tablePath + "'"
      );

      try {
        // Verify table is visible via Spark
        Dataset<Row> sparkTablesDs = spark.sql("SHOW TABLES IN " + getUnityCatalogName() + ".default");
        Row[] sparkTablesArr = (Row[]) sparkTablesDs.collect();
        List<String> sparkTables = Arrays.stream(sparkTablesArr)
            .map(row -> row.getString(1))
            .collect(Collectors.toList());
        Assert.assertTrue(
            "Table should be visible via Spark: " + String.join(", ", sparkTables),
            sparkTables.contains(tableName)
        );

        // Verify table is registered in UC server by querying directly via SDK
        List<String> ucTables = listTables(getUnityCatalogName(), "default");
        Assert.assertTrue(
            "Table '" + tableName + "' should be registered in UC server. Found: " +
            String.join(", ", ucTables),
            ucTables.contains(tableName)
        );
      } finally {
        spark.sql("DROP TABLE IF EXISTS " + fullTableName);
      }
    });
  }

  @Test
  public void testInsertAndSelectOperationsWorkThroughUCServer() throws Exception {
    withTempDir((File dir) -> {
      File tableDir = new File(dir, "test_insert");
      String tablePath = tableDir.getAbsolutePath();
      String tableName = "test_table_insert";
      String fullTableName = getUnityCatalogName() + ".default." + tableName;

      // Create table
      spark.sql(
          "CREATE TABLE " + fullTableName + " (" +
          "  id INT, " +
          "  value STRING" +
          ") USING DELTA " +
          "LOCATION '" + tablePath + "'"
      );

      try {
        // Verify table exists in UC server via SDK before insert
        List<String> tablesBeforeInsert = listTables(getUnityCatalogName(), "default");
        Assert.assertTrue(
            "Table should exist in UC before INSERT",
            tablesBeforeInsert.contains(tableName)
        );

        // Insert data via Spark
        spark.sql(
            "INSERT INTO " + fullTableName + " VALUES " +
            "(1, 'row1'), " +
            "(2, 'row2'), " +
            "(3, 'row3')"
        );

        // Verify data via Spark
        Dataset<Row> result = spark.sql("SELECT * FROM " + fullTableName + " ORDER BY id");
        Row[] actualRows = (Row[]) result.collect();
        Assert.assertEquals(3, actualRows.length);
        Assert.assertEquals(1, actualRows[0].getInt(0));
        Assert.assertEquals("row1", actualRows[0].getString(1));
        Assert.assertEquals(2, actualRows[1].getInt(0));
        Assert.assertEquals("row2", actualRows[1].getString(1));
        Assert.assertEquals(3, actualRows[2].getInt(0));
        Assert.assertEquals("row3", actualRows[2].getString(1));

        // Verify table still exists in UC server via SDK after insert
        List<String> tablesAfterInsert = listTables(getUnityCatalogName(), "default");
        Assert.assertTrue(
            "Table should still exist in UC after INSERT",
            tablesAfterInsert.contains(tableName)
        );
      } finally {
        spark.sql("DROP TABLE IF EXISTS " + fullTableName);
      }
    });
  }

  @Test
  public void testUpdateAndDeleteOperationsWorkThroughUCServer() throws Exception {
    withTempDir((File dir) -> {
      File tableDir = new File(dir, "test_update_delete");
      String tablePath = tableDir.getAbsolutePath();
      String tableName = "test_table_update_delete";
      String fullTableName = getUnityCatalogName() + ".default." + tableName;

      // Create and populate table
      spark.sql(
          "CREATE TABLE " + fullTableName + " (" +
          "  id INT, " +
          "  status STRING" +
          ") USING DELTA " +
          "LOCATION '" + tablePath + "'"
      );

      spark.sql(
          "INSERT INTO " + fullTableName + " VALUES " +
          "(1, 'pending'), " +
          "(2, 'pending'), " +
          "(3, 'completed'), " +
          "(4, 'pending')"
      );

      try {
        // Verify table exists in UC server via SDK before UPDATE
        List<String> tablesBeforeUpdate = listTables(getUnityCatalogName(), "default");
        Assert.assertTrue(
            "Table should exist in UC before UPDATE",
            tablesBeforeUpdate.contains(tableName)
        );

        // Perform UPDATE
        spark.sql(
            "UPDATE " + fullTableName + " " +
            "SET status = 'completed' " +
            "WHERE id <= 2"
        );

        // Verify UPDATE results
        Dataset<Row> afterUpdate = spark.sql(
            "SELECT * FROM " + fullTableName + " WHERE status = 'completed' ORDER BY id"
        );
        Row[] updateRows = (Row[]) afterUpdate.collect();
        Assert.assertEquals(3, updateRows.length);
        Assert.assertEquals(1, updateRows[0].getInt(0));
        Assert.assertEquals("completed", updateRows[0].getString(1));
        Assert.assertEquals(2, updateRows[1].getInt(0));
        Assert.assertEquals("completed", updateRows[1].getString(1));
        Assert.assertEquals(3, updateRows[2].getInt(0));
        Assert.assertEquals("completed", updateRows[2].getString(1));

        // Perform DELETE
        spark.sql(
            "DELETE FROM " + fullTableName + " " +
            "WHERE status = 'pending'"
        );

        // Verify DELETE results
        Dataset<Row> afterDelete = spark.sql(
            "SELECT * FROM " + fullTableName + " ORDER BY id"
        );
        Row[] deleteRows = (Row[]) afterDelete.collect();
        Assert.assertEquals(3, deleteRows.length);
        Assert.assertEquals(1, deleteRows[0].getInt(0));
        Assert.assertEquals("completed", deleteRows[0].getString(1));
        Assert.assertEquals(2, deleteRows[1].getInt(0));
        Assert.assertEquals("completed", deleteRows[1].getString(1));
        Assert.assertEquals(3, deleteRows[2].getInt(0));
        Assert.assertEquals("completed", deleteRows[2].getString(1));

        // Verify table still exists in UC server via SDK after operations
        List<String> tablesAfterOperations = listTables(getUnityCatalogName(), "default");
        Assert.assertTrue(
            "Table should still exist in UC after UPDATE and DELETE",
            tablesAfterOperations.contains(tableName)
        );
      } finally {
        spark.sql("DROP TABLE IF EXISTS " + fullTableName);
      }
    });
  }
}

