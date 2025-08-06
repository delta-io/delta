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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class Dsv2BasicTest {

  private static SparkSession spark;

  @BeforeAll
  public static void setUp(@TempDir File tempDir) {
    SparkConf conf =
        new SparkConf()
            .set("spark.sql.catalog.dsv2", "io.delta.spark.dsv2.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            .setMaster("local[*]")
            .setAppName("Dsv2BasicTest");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testCreateTable() {
    spark.sql(
        "CREATE TABLE dsv2.test_namespace.create_table_test (id INT, name STRING, value DOUBLE)");

    Dataset<Row> actualTableInfo =
        spark.sql("DESCRIBE TABLE dsv2.test_namespace.create_table_test");

    java.util.List<Row> expectedRows =
        java.util.Arrays.asList(
            org.apache.spark.sql.RowFactory.create("id", "int", null),
            org.apache.spark.sql.RowFactory.create("name", "string", null),
            org.apache.spark.sql.RowFactory.create("value", "double", null));
    assertDatasetEquals(
        actualTableInfo, spark.createDataFrame(expectedRows, actualTableInfo.schema()));
  }

  @Test
  public void testQueryTable() {
    spark.sql("CREATE TABLE dsv2.test_namespace.query_test (id INT, name STRING, value DOUBLE)");
    AnalysisException exception =
        assertThrows(
            AnalysisException.class,
            () -> spark.sql("select * from dsv2.test_namespace.query_test"));
    assertTrue(exception.getMessage().contains("does not support batch scan"));
  }

  @Test
  public void testQueryTableNotExist() {
    AnalysisException exception =
        assertThrows(
            AnalysisException.class,
            () -> spark.sql("select * from dsv2.test_namespace.not_found_test"));
    assertTrue(exception.getMessage().contains("TABLE_OR_VIEW_NOT_FOUND"));
  }

  private void assertDatasetEquals(Dataset<Row> actual, Dataset<Row> expected) {
    assertEquals(expected.count(), actual.count());
    assertTrue(expected.except(actual).collectAsList().isEmpty());
  }
}
