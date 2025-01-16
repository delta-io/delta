/*
 * Copyright (2021) The Delta Lake Project Authors.
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
package example

import org.apache.spark.sql.SparkSession

object Clustering {

  def main(args: Array[String]): Unit = {
    val tableName = "deltatable"

    val deltaSpark = SparkSession
      .builder()
      .appName("Clustering-Delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    // Clear up old session
    deltaSpark.sql(s"DROP TABLE IF EXISTS $tableName")

    // Enable preview config for clustering
    deltaSpark.conf.set(
      "spark.databricks.delta.clusteredTable.enableClusteringTablePreview", "true")

    try {
      // Create a table
      println("Creating a table")
      deltaSpark.sql(
        s"""CREATE TABLE $tableName (col1 INT, col2 STRING) using DELTA
           |CLUSTER BY (col1, col2)""".stripMargin)

      // Insert new data
      println("Insert new data")
      deltaSpark.sql(s"INSERT INTO $tableName VALUES (123, '123')")

      // Optimize the table
      println("Optimize the table")
      deltaSpark.sql(s"OPTIMIZE $tableName")
    } finally {
      // Cleanup
      deltaSpark.sql(s"DROP TABLE IF EXISTS $tableName")
      deltaSpark.stop()
    }
  }
}

