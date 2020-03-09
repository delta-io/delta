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

package test

import shadedelta.io.delta.tables._

import java.nio.file.Files

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// scalastyle:off println
object Test {

  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .getOrCreate()


    // Create a table
    val dir = Files.createTempDirectory("delta-table")
    println(s"Creating a table at $dir")
    val path = dir.toFile.getCanonicalPath
    var data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(path)

    // Read table
    println("Reading the table")
    val df = spark.read.format("delta").load(path)
    df.show()

    spark.stop()

    // Cleanup
    println("Finished the test. Cleaning up the table")
    Files.walk(dir).iterator.asScala.toSeq.reverse.foreach { f =>
      println(s"Deleting $f")
      Files.delete(f)
    }
  }
}
