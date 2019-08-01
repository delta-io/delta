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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.scalatest.Tag

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

trait DescribeDeltaHistorySuiteBase
  extends QueryTest
  with SharedSQLContext {

  import testImplicits._

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

  def getHistory(path: String, limit: Option[Int] = None): DataFrame = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, deltaLog.dataPath.toString)
    if (limit.isDefined) {
      deltaTable.history(limit.get)
    } else {
      deltaTable.history()
    }
  }

  test("logging and limit") {
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

  test("old and new writers") {
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

  test("order history by version") {
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
}

class DescribeDeltaHistorySuite
  extends DescribeDeltaHistorySuiteBase
