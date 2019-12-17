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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// import shadedelta.org.apache.commons.io.FileUtils

import java.io.File

object Test {

  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession
      .builder()
      .appName("Quickstart")
      .master("local[*]")
      .getOrCreate()


    // Create a table
    // scalastyle:off println
    println("Creating a table")
    // scalastyle:on println
    val file = new File("/tmp/delta-table")
    val path = file.getCanonicalPath
    var data = spark.range(0, 5)
    data.write.format("delta").mode("overwrite").save(path)

    // Read table
    // scalastyle:off println
    println("Reading the table")
    // scalastyle:on println
    val df = spark.read.format("delta").load(path)
    df.show()

    // Cleanup
    // FileUtils.deleteDirectory(file)
    spark.stop()
  }
}
