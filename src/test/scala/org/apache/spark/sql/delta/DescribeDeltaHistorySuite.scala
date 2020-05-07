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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaOperations.{Delete, Merge, Operation, Update}
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.scalatest.Tag

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait DescribeDeltaHistorySuiteBase
  extends QueryTest
  with SharedSparkSession {

  import testImplicits._

  protected val evolvabilityResource = "src/test/resources/delta/history/delta-0.2.0"

  protected val evolvabilityLastOp = Seq("STREAMING UPDATE", null, null)

  protected def testWithFlag(name: String, tags: Tag*)(f: => Unit): Unit = {
    test(name, tags: _*) {
      withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "true") {
        f
      }
    }
  }

  protected def checkLastOperation(
      basePath: String,
      expected: Seq[String],
      columns: Seq[Column] = Seq($"operation", $"operationParameters.mode")): Unit = {
    val df = getHistory(basePath, Some(1))
    checkAnswer(
      df.select(columns: _*),
      Seq(Row(expected: _*))
    )
  }

  protected def checkOperationMetrics(
      expectedMetrics: Map[String, String],
      operationMetrics: Map[String, String],
      metricsSchema: Set[String]): Unit = {
    if (metricsSchema != operationMetrics.keySet) {
      fail(
        s"""The collected metrics does not match the defined schema for the metrics.
           | Expected : $metricsSchema
           | Actual : ${operationMetrics.keySet}
           """.stripMargin)
    }
    expectedMetrics.keys.foreach { key =>
      if (!operationMetrics.contains(key)) {
        fail(s"The recorded operation metrics does not contain key: $key")
      }
      if (expectedMetrics(key) != operationMetrics(key)) {
        fail(
          s"""The recorded metric for $key does not equal the expected value.
             | expected = ${expectedMetrics(key)} ,
             | But actual = ${operationMetrics(key)}
           """.stripMargin
        )
      }
    }
  }

  protected def getOperationMetrics(history: DataFrame): Map[String, String] = {
    history.select("operationMetrics")
      .take(1)
      .head
      .getMap(0)
      .asInstanceOf[Map[String, String]]
  }

  def getHistory(path: String, limit: Option[Int] = None): DataFrame = {
    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, path)
    if (limit.isDefined) {
      deltaTable.history(limit.get)
    } else {
      deltaTable.history()
    }
  }

  testWithFlag("logging and limit") {
    val tempDir = Utils.createTempDir().toString
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir)
    Seq(4, 5, 6).toDF().write.format("delta").mode("overwrite").save(tempDir)
    assert(getHistory(tempDir).count === 2)
    checkLastOperation(tempDir, Seq("WRITE", "Overwrite"))
  }

  testWithFlag("operations - insert append with partition columns") {
    val tempDir = Utils.createTempDir().toString
    Seq((1, "a"), (2, "3")).toDF("id", "data")
      .write
      .format("delta")
      .mode("append")
      .partitionBy("id")
      .save(tempDir)

    checkLastOperation(
      tempDir,
      Seq("WRITE", "Append", """["id"]"""),
      Seq($"operation", $"operationParameters.mode", $"operationParameters.partitionBy"))
  }

  testWithFlag("operations - insert append without partition columns") {
    val tempDir = Utils.createTempDir().toString
    Seq((1, "a"), (2, "3")).toDF("id", "data").write.format("delta").save(tempDir)
    checkLastOperation(
      tempDir,
      Seq("WRITE", "ErrorIfExists", """[]"""),
      Seq($"operation", $"operationParameters.mode", $"operationParameters.partitionBy"))
  }

  testWithFlag("operations - insert error if exists with partitions") {
    val tempDir = Utils.createTempDir().toString
    Seq((1, "a"), (2, "3")).toDF("id", "data")
        .write
        .format("delta")
        .partitionBy("id")
        .mode("errorIfExists")
        .save(tempDir)
    checkLastOperation(
      tempDir,
      Seq("WRITE", "ErrorIfExists", """["id"]"""),
      Seq($"operation", $"operationParameters.mode", $"operationParameters.partitionBy"))
  }

  testWithFlag("operations - insert error if exists without partitions") {
    val tempDir = Utils.createTempDir().toString
    Seq((1, "a"), (2, "3")).toDF("id", "data")
        .write
        .format("delta")
        .mode("errorIfExists")
        .save(tempDir)
    checkLastOperation(
      tempDir,
      Seq("WRITE", "ErrorIfExists", """[]"""),
      Seq($"operation", $"operationParameters.mode", $"operationParameters.partitionBy"))
  }

  test("operations - streaming append with transaction ids") {
    val tempDir = Utils.createTempDir().toString
    val checkpoint = Utils.createTempDir().toString

    val data = MemoryStream[Int]
    data.addData(1, 2, 3)
    val stream = data.toDF()
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpoint)
      .start(tempDir)
    stream.processAllAvailable()
    stream.stop()

    checkLastOperation(
      tempDir,
      Seq("STREAMING UPDATE", "Append", "0"),
      Seq($"operation", $"operationParameters.outputMode", $"operationParameters.epochId"))
  }

  testWithFlag("operations - insert overwrite with predicate") {
    val tempDir = Utils.createTempDir().toString
    Seq((1, "a"), (2, "3")).toDF("id", "data").write.format("delta").partitionBy("id").save(tempDir)

    Seq((1, "b")).toDF("id", "data").write
      .format("delta")
      .mode("overwrite")
      .option(DeltaOptions.REPLACE_WHERE_OPTION, "id = 1")
      .save(tempDir)

    checkLastOperation(
      tempDir,
      Seq("WRITE", "Overwrite", """id = 1"""),
      Seq($"operation", $"operationParameters.mode", $"operationParameters.predicate"))
  }

  testWithFlag("operations - delete with predicate") {
    val tempDir = Utils.createTempDir().toString
    Seq((1, "a"), (2, "3")).toDF("id", "data").write.format("delta").partitionBy("id").save(tempDir)
    val deltaLog = DeltaLog.forTable(spark, tempDir)
    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    deltaTable.delete("id = 1")

    checkLastOperation(
      tempDir,
      Seq("DELETE", """["(`id` = 1)"]"""),
      Seq($"operation", $"operationParameters.predicate"))
  }

  testWithFlag("old and new writers") {
    val tempDir = Utils.createTempDir().toString
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "false") {
      Seq(1, 2, 3).toDF().write.format("delta").save(tempDir.toString)
    }

    checkLastOperation(tempDir, Seq(null, null))
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "true") {
      Seq(1, 2, 3).toDF().write.format("delta").mode("append").save(tempDir.toString)
    }

    assert(getHistory(tempDir).count() === 2)
    checkLastOperation(tempDir, Seq("WRITE", "Append"))
  }

  testWithFlag("order history by version") {
    val tempDir = Utils.createTempDir().toString

    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "false") {
      Seq(0).toDF().write.format("delta").save(tempDir)
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tempDir)
    }
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "true") {
      Seq(2).toDF().write.format("delta").mode("append").save(tempDir)
      Seq(3).toDF().write.format("delta").mode("overwrite").save(tempDir)
    }
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "false") {
      Seq(4).toDF().write.format("delta").mode("overwrite").save(tempDir)
    }

    val ans = getHistory(tempDir).as[CommitInfo].collect()
    assert(ans.map(_.version) === Seq(Some(4), Some(3), Some(2), Some(1), Some(0)))
  }

  test("read version") {
    val tempDir = Utils.createTempDir().toString

    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "true") {
      Seq(0).toDF().write.format("delta").save(tempDir) // readVersion = None as first commit
      Seq(1).toDF().write.format("delta").mode("overwrite").save(tempDir) // readVersion = Some(0)
    }

    val log = DeltaLog.forTable(spark, tempDir)
    val txn = log.startTransaction()   // should read snapshot version 1

    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "true") {
      Seq(2).toDF().write.format("delta").mode("append").save(tempDir)  // readVersion = Some(0)
      Seq(3).toDF().write.format("delta").mode("append").save(tempDir)  // readVersion = Some(2)
    }

    txn.commit(Seq.empty, DeltaOperations.Truncate())  // readVersion = Some(1)

    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "false") {
      Seq(5).toDF().write.format("delta").mode("append").save(tempDir)   // readVersion = None
    }
    val ans = getHistory(tempDir).as[CommitInfo].collect()
    assert(ans.map(x => x.version.get -> x.readVersion) ===
      Seq(5 -> None, 4 -> Some(1), 3 -> Some(2), 2 -> Some(1), 1 -> Some(0), 0 -> None))
  }

  testWithFlag("evolvability test") {
    checkLastOperation(
      evolvabilityResource,
      evolvabilityLastOp,
      Seq($"operation", $"operationParameters.mode", $"operationParameters.partitionBy"))
  }

  testWithFlag("describe history with delta table identifier") {
    val tempDir = Utils.createTempDir().toString
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir)
    Seq(4, 5, 6).toDF().write.format("delta").mode("overwrite").save(tempDir)
    val df = sql(s"DESCRIBE HISTORY delta.`$tempDir` LIMIT 1")
    checkAnswer(df.select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite")))
  }

  test("using on non delta") {
    withTempDir { basePath =>
      val e = intercept[AnalysisException] {
        sql(s"describe history '$basePath'").collect()
      }
      assert(Seq("supported", "Delta").forall(e.getMessage.contains))
    }
  }

  test("describe history a non-existent path and a non Delta table") {
    def assertNotADeltaTableException(path: String): Unit = {
      for (table <- Seq(s"'$path'", s"delta.`$path`")) {
        val e = intercept[AnalysisException] {
          sql(s"describe history $table").show()
        }
        Seq("DESCRIBE HISTORY", "only supported for Delta tables").foreach { msg =>
          assert(e.getMessage.contains(msg))
        }
      }
    }
    withTempPath { tempDir =>
      assert(!tempDir.exists())
      assertNotADeltaTableException(tempDir.getCanonicalPath)
    }
    withTempPath { tempDir =>
      spark.range(1, 10).write.parquet(tempDir.getCanonicalPath)
      assertNotADeltaTableException(tempDir.getCanonicalPath)
    }
  }

  test("operation metrics - write metrics") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        // create table
        spark.range(100).repartition(5).write.format("delta").save(tempDir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.getAbsolutePath)

        // get last command history
        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map(
          "numFiles" -> "5",
          "numOutputRows" -> "100"
        )

        // Check if operation metrics from history are accurate
        checkOperationMetrics(expectedMetrics, operationMetrics, DeltaOperationMetrics.WRITE)
        assert(operationMetrics("numOutputBytes").toLong > 0)
      }
    }
  }

  test("operation metrics - merge") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        // create target
        spark.range(100).write.format("delta").save(tempDir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.getAbsolutePath)

        // run merge
        deltaTable.as("t")
          .merge(spark.range(50, 150).toDF().as("s"), "s.id = t.id")
          .whenMatched()
          .updateAll()
          .whenNotMatched()
          .insertAll()
          .execute()

        // Get operation metrics
        val operationMetrics: Map[String, String] = getOperationMetrics(deltaTable.history(1))

        val expectedMetrics = Map(
          "numTargetRowsInserted" -> "50",
          "numTargetRowsUpdated" -> "50",
          "numOutputRows" -> "100",
          "numSourceRows" -> "100"
        )
        checkOperationMetrics(expectedMetrics, operationMetrics, DeltaOperationMetrics.MERGE)
      }
    }
  }

  test("operation metrics - streaming update") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        val memoryStream = MemoryStream[Long]
        val df = memoryStream.toDF()

        val tbl = tempDir.getAbsolutePath + "tbl1"

        spark.range(10).write.format("delta").save(tbl)
        // ensure that you are writing out a single file per batch
        val q = df.coalesce(1)
          .withColumnRenamed("value", "id")
          .writeStream
          .format("delta")
          .option("checkpointLocation", tempDir + "checkpoint")
          .start(tbl)
        memoryStream.addData(1)
        q.processAllAvailable()
        val deltaTable = io.delta.tables.DeltaTable.forPath(tbl)
        var operationMetrics: Map[String, String] = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map(
          "numAddedFiles" -> "1",
          "numRemovedFiles" -> "0",
          "numOutputRows" -> "1"
        )
        checkOperationMetrics(
          expectedMetrics, operationMetrics, DeltaOperationMetrics.STREAMING_UPDATE)

        // check if second batch also returns correct metrics.
        memoryStream.addData(1, 2, 3)
        q.processAllAvailable()
        operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics2 = Map(
          "numAddedFiles" -> "1",
          "numRemovedFiles" -> "0",
          "numOutputRows" -> "3"
        )
        checkOperationMetrics(
          expectedMetrics2, operationMetrics, DeltaOperationMetrics.STREAMING_UPDATE)
        assert(operationMetrics("numOutputBytes").toLong > 0)
        q.stop()
      }
    }
  }

  test("operation metrics - streaming update - complete mode") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        val memoryStream = MemoryStream[Long]
        val df = memoryStream.toDF()

        val tbl = tempDir.getAbsolutePath + "tbl1"

        Seq(1L -> 1L, 2L -> 2L).toDF("value", "count")
          .coalesce(1)
          .write
          .format("delta")
          .save(tbl)

        // ensure that you are writing out a single file per batch
        val q = df.groupBy("value").count().coalesce(1)
          .writeStream
          .format("delta")
          .outputMode("complete")
          .option("checkpointLocation", tempDir + "checkpoint")
          .start(tbl)
        memoryStream.addData(1)
        q.processAllAvailable()

        val deltaTable = io.delta.tables.DeltaTable.forPath(tbl)
        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map(
          "numAddedFiles" -> "1",
          "numRemovedFiles" -> "1",
          "numOutputRows" -> "1"
        )
        checkOperationMetrics(
          expectedMetrics, operationMetrics, DeltaOperationMetrics.STREAMING_UPDATE)
      }
    }
  }

  test("operation metrics - update") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        // Create the initial table as a single file
        Seq(1, 2, 5, 11, 21, 3, 4, 6, 9, 7, 8, 0).toDF("key")
          .withColumn("value", 'key % 2)
          .write
          .format("delta")
          .save(tempDir.getAbsolutePath)

        // append additional data with the same number range to the table.
        // This data is saved as a separate file as well
        Seq(15, 16, 17).toDF("key")
          .withColumn("value", 'key % 2)
          .repartition(1)
          .write
          .format("delta")
          .mode("append")
          .save(tempDir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        deltaLog.snapshot.numOfFiles

        // update the table
        deltaTable.update(col("key") === lit("16"), Map("value" -> lit("1")))
        // The file from the append gets updated but the file from the initial table gets scanned
        // as well. We want to make sure numCopied rows is calculated from written files and not
        // scanned files[SC-33980]

        // get operation metrics
        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map(
          "numAddedFiles" -> "1",
          "numRemovedFiles" -> "1",
          "numUpdatedRows" -> "1",
          "numCopiedRows" -> "2" // There should be only three rows in total(updated + copied)
        )
        checkOperationMetrics(expectedMetrics, operationMetrics, DeltaOperationMetrics.UPDATE)
      }
    }
  }

  test("operation metrics - update - partitioned column") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      val numRows = 100
      val numPartitions = 5
      withTempDir { tempDir =>
        spark.range(numRows)
          .withColumn("c1", 'id + 1)
          .withColumn("c2", 'id % numPartitions)
          .write
          .partitionBy("c2")
          .format("delta")
          .save(tempDir.getAbsolutePath)

        val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        val numFilesBeforeUpdate = deltaLog.snapshot.numOfFiles
        deltaTable.update(col("c2") < 1, Map("c2" -> lit("1")))
        val numFilesAfterUpdate = deltaLog.snapshot.numOfFiles

        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val newFiles = numFilesAfterUpdate - numFilesBeforeUpdate
        val oldFiles = numFilesBeforeUpdate / numPartitions
        val addedFiles = newFiles + oldFiles
        val expectedMetrics = Map(
          "numUpdatedRows" -> (numRows / numPartitions).toString,
          "numCopiedRows" -> "0",
          "numAddedFiles" -> addedFiles.toString,
          "numRemovedFiles" -> (numFilesBeforeUpdate / numPartitions).toString
        )
        checkOperationMetrics(expectedMetrics, operationMetrics, DeltaOperationMetrics.UPDATE)
      }
    }
  }

  test("operation metrics - delete") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      withTempDir { tempDir =>
        // Create the initial table as a single file
        Seq(1, 2, 5, 11, 21, 3, 4, 6, 9, 7, 8, 0).toDF("key")
          .withColumn("value", 'key % 2)
          .repartition(1)
          .write
          .format("delta")
          .save(tempDir.getAbsolutePath)

        // Append to the initial table additional data in the same numerical range
        Seq(15, 16, 17).toDF("key")
          .withColumn("value", 'key % 2)
          .repartition(1)
          .write
          .format("delta")
          .mode("append")
          .save(tempDir.getAbsolutePath)
        val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        deltaLog.snapshot.numOfFiles

        // delete the table
        deltaTable.delete(col("key") === lit("16"))
        // The file from the append gets deleted but the file from the initial table gets scanned
        // as well. We want to make sure numCopied rows is calculated from the written files instead
        // of the scanned files.[SC-33980]

        // get operation metrics
        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map(
          "numAddedFiles" -> "1",
          "numRemovedFiles" -> "1",
          "numDeletedRows" -> "1",
          "numCopiedRows" -> "2" // There should be only three rows in total(deleted + copied)
        )
        checkOperationMetrics(expectedMetrics, operationMetrics, DeltaOperationMetrics.DELETE)
      }
    }
  }

  test("operation metrics - delete - partition column") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      val numRows = 100
      val numPartitions = 5
      withTempDir { tempDir =>
        spark.range(numRows)
          .withColumn("c1", 'id % numPartitions)
          .write
          .format("delta")
          .partitionBy("c1")
          .save(tempDir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        val numFilesBeforeDelete = deltaLog.snapshot.numOfFiles
        val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.getAbsolutePath)

        deltaTable.delete("c1 = 1")
        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map[String, String](
          "numRemovedFiles" -> (numFilesBeforeDelete / numPartitions).toString
        )
        // row level metrics are not collected for deletes with parition columns
        checkOperationMetrics(
          expectedMetrics, operationMetrics, DeltaOperationMetrics.DELETE_PARTITIONS)
      }
    }
  }

  test("operation metrics - delete - full") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      val numRows = 100
      val numPartitions = 5
      withTempDir { tempDir =>
        spark.range(numRows)
          .withColumn("c1", 'id % numPartitions)
          .write
          .format("delta")
          .partitionBy("c1")
          .save(tempDir.getAbsolutePath)
        val deltaLog = DeltaLog.forTable(spark, tempDir.getAbsolutePath)
        val numFilesBeforeDelete = deltaLog.snapshot.numOfFiles
        val deltaTable = io.delta.tables.DeltaTable.forPath(tempDir.getAbsolutePath)

        deltaTable.delete()

        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        val expectedMetrics = Map[String, String](
          "numRemovedFiles" -> numFilesBeforeDelete.toString
        )
        checkOperationMetrics(
          expectedMetrics, operationMetrics, DeltaOperationMetrics.DELETE_PARTITIONS)
      }
    }
  }

  test("operation metrics - convert to delta") {
    withSQLConf(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true") {
      val numPartitions = 5
      withTempDir { tempDir =>
        // Create a parquet table
        val dir = tempDir.getAbsolutePath()
        spark.range(10)
          .withColumn("col2", 'id % numPartitions)
          .write
          .format("parquet")
          .mode("overwrite")
          .partitionBy("col2")
          .save(dir)

        // convert to delta
        val deltaTable = io.delta.tables.DeltaTable.convertToDelta(spark, s"parquet.`$dir`",
          "col2 long")
        val deltaLog = DeltaLog.forTable(spark, dir)
        val expectedMetrics = Map(
          "numConvertedFiles" -> deltaLog.snapshot.numOfFiles.toString
        )
        val operationMetrics = getOperationMetrics(deltaTable.history(1))
        checkOperationMetrics(expectedMetrics, operationMetrics, DeltaOperationMetrics.CONVERT)
      }
    }
  }

}

class DescribeDeltaHistorySuite
  extends DescribeDeltaHistorySuiteBase with DeltaSQLCommandTest

