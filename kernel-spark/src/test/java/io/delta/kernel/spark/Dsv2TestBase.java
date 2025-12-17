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
import java.util.List;
import java.util.UUID;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base class for DSv2 tests in v2 component providing common setup and helper methods.
 *
 * <p>This test setup configures a SparkSession that uses DSv2 for reading (via the "dsv2" catalog)
 * and DSv1 for writing (via spark_catalog with DeltaCatalogV1). This hybrid configuration is
 * necessary until DSv2 supports write operations.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class Dsv2TestBase {

  protected SparkSession spark;
  protected String nameSpace;

  @BeforeAll
  public void setUp(@TempDir File tempDir) {
    // Spark doesn't allow '-'
    nameSpace = "ns_" + UUID.randomUUID().toString().replace('-', '_');
    SparkConf conf =
        new SparkConf()
            // DSv2 catalog for reading
            .set("spark.sql.catalog.dsv2", "io.delta.kernel.spark.catalog.TestCatalog")
            .set("spark.sql.catalog.dsv2.base_path", tempDir.getAbsolutePath())
            // DSv1 extensions and catalog for writing (until DSv2 supports write operations)
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

  protected void assertDatasetEquals(Dataset<Row> actual, List<Row> expectedRows) {
    List<Row> actualRows = actual.collectAsList();
    assertEquals(
        expectedRows,
        actualRows,
        () -> "Datasets differ: expected=" + expectedRows + "\nactual=" + actualRows);
  }
}
