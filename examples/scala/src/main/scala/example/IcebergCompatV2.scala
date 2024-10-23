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

import java.io.{File, IOException}
import java.net.ServerSocket

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.SparkSession
/**
 * This example relies on an external Hive metastore (HMS) instance to run.
 *
 * A standalone HMS can be created using the following docker command.
 *  ************************************************************
 *  docker run -d -p 9083:9083 --env SERVICE_NAME=metastore \
 *  --name metastore-standalone apache/hive:4.0.0
 *  ************************************************************
 *  The URL of this standalone HMS is thrift://localhost:9083
 */
object IcebergCompatV2 {

  def main(args: Array[String]): Unit = {
    // Update this according to the metastore config
    val port = 9083
    val warehousePath = FileUtils.getTempDirectoryPath()

    if (!UniForm.hmsReady(port)) {
      print("HMS not available. Exit.")
      return
    }

    val testTableName = "uniform_table3"
    FileUtils.deleteDirectory(new File(s"${warehousePath}${testTableName}"))

    val deltaSpark = SparkSession
      .builder()
      .appName("UniForm-Delta")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("hive.metastore.uris", s"thrift://localhost:$port")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    deltaSpark.sql(s"DROP TABLE IF EXISTS ${testTableName}")
    deltaSpark.sql(
      s"""CREATE TABLE `${testTableName}`
         | (id INT, ts TIMESTAMP, array_data array<int>, map_data map<int, int>)
         | using DELTA""".stripMargin)
    deltaSpark.sql(
      s"""
         |INSERT INTO `$testTableName` (id, ts, array_data, map_data)
         | VALUES (123, '2024-01-01 00:00:00', array(2, 3, 4, 5), map(3, 6, 8, 7))""".stripMargin)
    deltaSpark.sql(
      s"""REORG TABLE `$testTableName` APPLY (UPGRADE UNIFORM
         | (ICEBERG_COMPAT_VERSION = 2))""".stripMargin)

    val icebergSpark = SparkSession.builder()
      .master("local[*]")
      .appName("UniForm-Iceberg")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("hive.metastore.uris", s"thrift://localhost:$port")
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()

    icebergSpark.sql(s"SELECT * FROM ${testTableName}").show()
  }
}
