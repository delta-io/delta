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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery}
import io.delta.tables._

import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

object ChangeDataFeed {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("ChangeDataFeed")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .getOrCreate()

    val path = "/tmp/delta-change-data-feed"
    val file = new File(path)
    if (file.exists()) FileUtils.deleteDirectory(file)

    def readCDCByPath(startingVersion: Int): DataFrame = {
      spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", startingVersion.toString)
        .load(path)
        .orderBy("_change_type", "id")
    }

    def readCDCByTableName(startingVersion: Int): DataFrame = {
      spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", startingVersion.toString)
        .table("student")
        .orderBy("_change_type", "id")
    }

    def streamCDCByPath(startingVersion: Int): StreamingQuery = {
      spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", startingVersion.toString)
        .load(path)
        .writeStream
        .format("console")
        .option("numRows", 1000)
        .start()
    }

    def streamCDCByTableName(startingVersion: Int): StreamingQuery = {
      spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", startingVersion.toString)
        .table("student")
        .writeStream
        .format("console")
        .option("numRows", 1000)
        .start()
    }

    spark.sql(
      s"""
         |CREATE TABLE student (id INT, name STRING, age INT)
         |USING DELTA
         |PARTITIONED BY (age)
         |TBLPROPERTIES (delta.enableChangeDataFeed = true)
         |LOCATION '$path'""".stripMargin) // v0

    spark.range(0, 10)
      .selectExpr(
        "CAST(id as INT) as id",
        "CAST(id as STRING) as name",
        "CAST(id % 4 + 18 as INT) as age")
      .write.format("delta").mode("append").save(path)  // v1

    println("(v1) Initial Table")
    spark.read.format("delta").load(path).orderBy("id").show()

    println("(v1) CDC changes")
    readCDCByPath(1).show()

    val table = io.delta.tables.DeltaTable.forPath(path)

    println("(v2) Updated id -> id + 1")
    table.update(Map("id" -> expr("id + 1"))) // v2
    readCDCByPath(2).show()

    println("(v3) Deleted where id >= 7")
    table.delete(expr("id >= 7")) // v3
    readCDCByTableName(3).show()

    println("(v4) Deleted where age = 18")
    table.delete(expr("age = 18")) // v4, partition delete
    readCDCByTableName(4).show()

    // TODO merge

    println("Streaming by path")
    val cdfStream1 = streamCDCByPath(0)
    cdfStream1.awaitTermination(5000)
    cdfStream1.stop()

    println("Streaming by table name")
    val cdfStream2 = streamCDCByTableName(0)
    cdfStream2.awaitTermination(5000)
    cdfStream2.stop()
  }
}
