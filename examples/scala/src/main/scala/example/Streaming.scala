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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object Streaming {

  def main(args: Array[String]): Unit = {
    // Create a Spark Session
    val spark = SparkSession
      .builder()
      .appName("Streaming")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Create a table
    val data = spark.range(0, 5)
    val path = new File("/tmp/delta-table").getAbsolutePath
    data.write.format("delta").save(path)

    // Read table
    val df = spark.read.format("delta").load(path)
    df.show()

    println("Streaming write")
    val streamingDf = spark.readStream.format("rate").load()
    val tablePath2 = new File("/tmp/delta-table2").getCanonicalPath
    val stream = streamingDf
      .select($"value" as "id")
      .writeStream
      .format("delta")
      .option("checkpointLocation", new File("/tmp/checkpoint").getCanonicalPath)
      .start(tablePath2)

    stream.awaitTermination(10000)
    stream.stop()

    println("Reading from stream")
    val stream2 = spark
      .readStream
      .format("delta")
      .load(tablePath2)
      .writeStream
      .format("console")
      .start()

    stream2.awaitTermination(10000)
    stream2.stop()

    // Function to upsert microBatchOutputDF into Delta Lake table using merge
    println("Streaming upgrades in update mode")
    def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
      val deltaTable = DeltaTable.forPath(path)
      deltaTable.as("t")
        .merge(
          microBatchOutputDF.select($"value" as "id").as("s"),
          "s.id = t.id")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }

    val streamingAggregatesDf = spark
      .readStream
      .format("rate")
      .load()
      .withColumn("key", col("value") % 10)
      .drop("timestamp")

    // Write the output of a streaming aggregation query into Delta Lake table
    println("Original Delta Table")
    val deltaTable = DeltaTable.forPath(path)
    deltaTable.toDF.show()

    val stream3 = streamingAggregatesDf.writeStream
      .format("delta")
      .foreachBatch(upsertToDelta _)
      .outputMode("update")
      .start()

    stream3.awaitTermination(10000)
    stream3.stop()

    println("Delta Table after streaming upsert")
    deltaTable.toDF.show()

    // Cleanup
    FileUtils.deleteDirectory(new File(path))
    FileUtils.deleteDirectory(new File(tablePath2))
    spark.stop()
  }
}
