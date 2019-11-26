/*
 * Copyright 2019 Databricks, Inc.
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

import java.io.File

import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils

import org.apache.spark.sql.SparkSession

object Utilities {
  def main(args: Array[String]): Unit = {
    // Create a Spark Session with SQL enabled
    val spark = SparkSession
      .builder()
      .appName("Utilities")
      .master("local[*]")
      // config io.delta.sql.DeltaSparkSessionExtension -
      // to enable custom Delta-specific SQL commands
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      // config parallelPartitionDiscovery.parallelism -
      // control the parallelism for vacuum
      .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "4")
      .getOrCreate()

    // Create a table
    println("Create a parquet table")
    val data = spark.range(0, 5)
    val file = new File("/tmp/parquet-table")
    val path = file.getAbsolutePath
    data.write.format("parquet").save(path)

    // Convert to delta
    println("Convert to Delta")
    DeltaTable.convertToDelta(spark, s"parquet.`$path`")

    // Read table as delta
    var df = spark.read.format("delta").load(path)

    // Read old version of data using time travel
    df = spark.read.format("delta").option("versionAsOf", 0).load(path)
    df.show()

    val deltaTable = DeltaTable.forPath(path)

    // Utility commands
    println("Vacuum the table")
    deltaTable.vacuum()

    println("Describe History for the table")
    deltaTable.history().show()

    // SQL utility commands
    println("SQL Vacuum")
    spark.sql(s"VACUUM '$path' RETAIN 169 HOURS")

    println("SQL Describe History")
    println(spark.sql(s"DESCRIBE HISTORY '$path'").collect())

    // Cleanup
    FileUtils.deleteDirectory(new File(path))
    spark.stop()
  }
}
