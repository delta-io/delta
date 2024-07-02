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
object UniForm {

  def main(args: Array[String]): Unit = {
    // Update this according to the metastore config
    val port = 9083
    val warehousePath = FileUtils.getTempDirectoryPath()

    if (!hmsReady(port)) {
      print("HMS not available. Exit.")
      return
    }

    val testTableName = "deltatable"
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
      s"""CREATE TABLE `${testTableName}` (col1 INT) using DELTA
         |TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'name',
         |  'delta.enableIcebergCompatV1' = 'true',
         |  'delta.universalFormat.enabledFormats' = 'iceberg'
         |)""".stripMargin)
    deltaSpark.sql(s"INSERT INTO `$testTableName` VALUES (123)")

    // Wait for the conversion to be done
    Thread.sleep(10000)

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

  def hmsReady(port: Int): Boolean = {
    var ss: ServerSocket = null
    try {
      ss = new ServerSocket(port)
      ss.setReuseAddress(true)
      return false
    } catch {
      case e: IOException =>
    } finally {
      if (ss != null) {
        try ss.close()
        catch {
          case e: IOException =>
        }
      }
    }
    true
  }
}
