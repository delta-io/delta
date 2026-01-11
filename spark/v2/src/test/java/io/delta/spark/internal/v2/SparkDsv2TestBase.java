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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class SparkDsv2TestBase {

  protected static SparkSession spark;
  protected static Engine defaultEngine;

  @BeforeAll
  public static void setUpSparkAndEngine() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("SparkKernelDsv2Tests")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtensionV1")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalogV1")
            .getOrCreate();
    defaultEngine = DefaultEngine.create(spark.sessionState().newHadoopConf());
  }

  @AfterAll
  public static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  protected void createTestTableWithData(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, value DOUBLE) USING delta LOCATION '%s'",
            tableName, path));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.5), (3, 'Charlie', 30.5)",
            tableName));
  }

  protected void createEmptyTestTable(String path, String tableName) {
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'", tableName, path));
  }

  protected static void createPartitionedTable(String tableName, String path) {
    spark.sql(
        String.format(
            "CREATE TABLE `%s` (part INT, date STRING, city STRING, name STRING, cnt INT) USING delta LOCATION '%s' PARTITIONED BY (date, city, part)",
            tableName, path));
    spark.sql(
        String.format(
            "INSERT INTO %s VALUES "
                + "('1', '20180520', 'hz', 'Alice', '10'),"
                + "('1', '20180718', 'hz', 'Bob', '20'),"
                + "('1', '20180512', 'sh', 'Charlie', '30'),"
                + "('2', '20180520', 'bj', 'David', '40'),"
                + "('2', '20181212', 'sz', 'Eve', '50')",
            tableName));
  }
}
