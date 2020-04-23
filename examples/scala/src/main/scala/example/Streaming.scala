/*
 * Copyright (2020) The Delta Lake Project Authors.
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

    println("=== Section 1: write and read delta table using batch queries, and initialize table for later sections")
    // Create a table
    val data = spark.range(0, 5)
    val path = new File("/tmp/delta-table").getAbsolutePath
    data.write.format("delta").save(path)

    // Read table
    val df = spark.read.format("delta").load(path)
    df.show()


    println("=== Section 2: write and read delta using structured streaming")
    val streamingDf = spark.readStream.format("rate").load()
    val tablePath2 = new File("/tmp/delta-table2").getCanonicalPath
    val checkpointPath = new File("/tmp/checkpoint").getCanonicalPath
    val stream = streamingDf
      .select($"value" as "id")
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .start(tablePath2)

    stream.awaitTermination(10000)
    stream.stop()

    val stream2 = spark
      .readStream
      .format("delta")
      .load(tablePath2)
      .writeStream
      .format("console")
      .start()

    stream2.awaitTermination(10000)
    stream2.stop()


    println("=== Section 3: Streaming upserts using MERGE")
    // Function to upsert microBatchOutputDF into Delta Lake table using merge
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

    stream3.awaitTermination(20000)
    stream3.stop()

    println("Delta Table after streaming upsert")
    deltaTable.toDF.show()

    // Streaming append and concurrent repartition using  data change = false
    // tbl1 is the sink and tbl2 is the source
    println("############ Streaming appends with concurrent table repartition  ##########")
    val tbl1 = "/tmp/delta-table4"
    val tbl2 = "/tmp/delta-table5"
    val numRows = 10
    spark.range(numRows).write.mode("overwrite").format("delta").save(tbl1)
    spark.read.format("delta").load(tbl1).show()
    spark.range(numRows, numRows * 10).write.mode("overwrite").format("delta").save(tbl2)

    // Start reading tbl2 as a stream and do a streaming write to tbl1
    // Prior to Delta 0.5.0 this would throw StreamingQueryException: Detected a data update in the source table. This is currently not supported.
    val stream4 = spark.readStream.format("delta").load(tbl2).writeStream.format("delta")
      .option("checkpointLocation", new File("/tmp/checkpoint/tbl1").getCanonicalPath)
      .outputMode("append")
      .start(tbl1)

    // repartition table while streaming job is running
    spark.read.format("delta").load(tbl2).repartition(10).write.format("delta").mode("overwrite").option("dataChange", "false").save(tbl2)

    stream4.awaitTermination(10)
    stream4.stop()
    println("######### After streaming write #########")
    spark.read.format("delta").load(tbl1).show()

    println("=== In the end, clean all paths")
    // Cleanup
    Seq(path, tbl1, tbl2, "/tmp/checkpoint/tbl1", tablePath2).foreach { path =>
    	FileUtils.deleteDirectory(new File(path))
    }
    spark.stop()
  }
}
