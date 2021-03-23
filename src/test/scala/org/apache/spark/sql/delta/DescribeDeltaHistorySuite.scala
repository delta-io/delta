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
import java.io.File

 // Edge
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.scalactic.source.Position
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
  with SharedSparkSession  with DeltaSQLCommandTest {

  import testImplicits._

  protected val evolvabilityResource = {
    new File("src/test/resources/delta/history/delta-0.2.0").getAbsolutePath()
  }

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
    val df = io.delta.tables.DeltaTable.forPath(spark, basePath).history(1)
    checkAnswer(df.select(columns: _*), Seq(Row(expected: _*)))
    val df2 = spark.sql(s"DESCRIBE HISTORY delta.`$basePath` LIMIT 1")
    checkAnswer(df2.select(columns: _*), Seq(Row(expected: _*)))
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

  /**
   * Check all expected metrics exist and executime time (if expected to exist) is the largest time
   * metric.
   */
  protected def checkOperationTimeMetricsInvariant(
      expectedMetrics: Set[String],
      operationMetrics: Map[String, String]): Unit = {
    expectedMetrics.foreach {
      m => assert(operationMetrics.contains(m))
    }
    if (expectedMetrics.contains("executionTimeMs")) {
      val executionTimeMs = operationMetrics("executionTimeMs").toLong
      val maxTimeMs = operationMetrics.filterKeys(expectedMetrics.contains(_))
        .mapValues(v => v.toLong).valuesIterator.max
      assert(executionTimeMs == maxTimeMs)
    }
  }

  protected def getOperationMetrics(history: DataFrame): Map[String, String] = {
    history.select("operationMetrics")
      .take(1)
      .head
      .getMap(0)
      .asInstanceOf[Map[String, String]]
  }

  testWithFlag("basic case - Scala history with path-based table") {
    val tempDir = Utils.createTempDir().toString
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir)
    Seq(4, 5, 6).toDF().write.format("delta").mode("overwrite").save(tempDir)

    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir)
    // Full History
    checkAnswer(
      deltaTable.history().select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite"), Row("WRITE", "ErrorIfExists")))

    // History with limit
    checkAnswer(
      deltaTable.history(1).select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite")))
  }

  test("basic case - Scala history with name-based table") {
    withTable("delta_test") {
      Seq(1, 2, 3).toDF().write.format("delta").saveAsTable("delta_test")
      Seq(4, 5, 6).toDF().write.format("delta").mode("overwrite").saveAsTable("delta_test")

      val deltaTable = io.delta.tables.DeltaTable.forName(spark, "delta_test")
      // Full History
      checkAnswer(
        deltaTable.history().select("operation"),
        Seq(Row("CREATE OR REPLACE TABLE AS SELECT"), Row("CREATE TABLE AS SELECT")))

      // History with limit
      checkAnswer(
        deltaTable.history(1).select("operation"),
        Seq(Row("CREATE OR REPLACE TABLE AS SELECT")))
    }
  }

  testWithFlag("basic case - SQL describe history with path-based table") {
    val tempDir = Utils.createTempDir().toString
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir)
    Seq(4, 5, 6).toDF().write.format("delta").mode("overwrite").save(tempDir)

    // With delta.`path` format
    checkAnswer(
      sql(s"DESCRIBE HISTORY delta.`$tempDir`").select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite"), Row("WRITE", "ErrorIfExists")))

    checkAnswer(
      sql(s"DESCRIBE HISTORY delta.`$tempDir` LIMIT 1")
        .select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite")))

    // With direct path format
    checkAnswer(
      sql(s"DESCRIBE HISTORY '$tempDir'").select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite"), Row("WRITE", "ErrorIfExists")))

    checkAnswer(
      sql(s"DESCRIBE HISTORY '$tempDir' LIMIT 1")
        .select("operation", "operationParameters.mode"),
      Seq(Row("WRITE", "Overwrite")))
  }

  testWithFlag("basic case - SQL describe history with name-based table") {
    withTable("delta_test") {
      Seq(1, 2, 3).toDF().write.format("delta").saveAsTable("delta_test")
      Seq(4, 5, 6).toDF().write.format("delta").mode("overwrite").saveAsTable("delta_test")

      checkAnswer(
        sql(s"DESCRIBE HISTORY delta_test").select("operation"),
        Seq(Row("CREATE OR REPLACE TABLE AS SELECT"), Row("CREATE TABLE AS SELECT")))

      checkAnswer(
        sql(s"DESCRIBE HISTORY delta_test LIMIT 1").select("operation"),
        Seq(Row("CREATE OR REPLACE TABLE AS SELECT")))
    }
  }

  testWithFlag("describe history fails on views") {
    val tempDir = Utils.createTempDir().toString
    Seq(1, 2, 3).toDF().write.format("delta").save(tempDir)
    val viewName = "delta_view"
    withView(viewName) {
      sql(s"create view $viewName as select * from delta.`$tempDir`")

      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE HISTORY $viewName").collect()
      }
      assert(e.getMessage.contains("history of a view"))
    }
  }

  testWithFlag("operations - create table") {
    withTable("delta_test") {
      sql(
        s"""create table delta_test (
           |  a int,
           |  b string
           |)
           |using delta
           |partitioned by (b)
           |comment 'this is my table'
           |tblproperties (delta.appendOnly=true)
         """.stripMargin)
      checkLastOperation(
        spark.sessionState.catalog.defaultTablePath(TableIdentifier("delta_test")).toString,
        Seq(
          "CREATE TABLE",
          "true",
          """["b"]""",
          """{"delta.appendOnly":"true"}""",
          "this is my table"),
        Seq(
          $"operation", $"operationParameters.isManaged", $"operationParameters.partitionBy",
          $"operationParameters.properties", $"operationParameters.description"))
    }
  }

  testWithFlag("operations - ctas (saveAsTable)") {
    val tempDir = Utils.createTempDir().toString
    withTable("delta_test") {
      Seq((1, "a"), (2, "3")).toDF("id", "data").write.format("delta")
        .option("path", tempDir).saveAsTable("delta_test")
      checkLastOperation(
        tempDir,
        Seq("CREATE TABLE AS SELECT", "false", """[]""", "{}", null),
        Seq($"operation", $"operationParameters.isManaged", $"operationParameters.partitionBy",
          $"operationParameters.properties", $"operationParameters.description"))
    }
  }

  testWithFlag("operations - ctas (sql)") {
    val tempDir = Utils.createTempDir().toString
    withTable("delta_test") {
      sql(
        s"""create table delta_test
           |using delta
           |location '$tempDir'
           |tblproperties (delta.appendOnly=true)
           |partitioned by (b)
           |as select 1 as a, 'x' as b
         """.stripMargin)
      checkLastOperation(
        tempDir,
        Seq("CREATE TABLE AS SELECT",
          "false",
          """["b"]""",
          """{"delta.appendOnly":"true"}""", null),
        Seq($"operation", $"operationParameters.isManaged", $"operationParameters.partitionBy",
          $"operationParameters.properties", $"operationParameters.description"))
    }
    val tempDir2 = Utils.createTempDir().toString
    withTable("delta_test") {
      sql(
        s"""create table delta_test
           |using delta
           |location '$tempDir2'
           |comment 'this is my table'
           |as select 1 as a, 'x' as b
         """.stripMargin)
      // TODO(burak): Fix comments for CTAS
      checkLastOperation(
        tempDir2,
        Seq("CREATE TABLE AS SELECT",
          "false", """[]""", """{}""", "this is my table"),
        Seq($"operation", $"operationParameters.isManaged", $"operationParameters.partitionBy",
          $"operationParameters.properties", $"operationParameters.description"))
    }
  }


  testWithFlag("operations - [un]set tbproperties") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      sql("""
            |ALTER TABLE delta_test
            |SET TBLPROPERTIES (
            |  'delta.checkpointInterval' = '20',
            |  'key' = 'value'
            |)""".stripMargin)
      checkLastOperation(
        spark.sessionState.catalog.defaultTablePath(TableIdentifier("delta_test")).toString,
        Seq("SET TBLPROPERTIES", """{"delta.checkpointInterval":"20","key":"value"}"""),
        Seq($"operation", $"operationParameters.properties"))

      sql("ALTER TABLE delta_test UNSET TBLPROPERTIES ('key')")
      checkLastOperation(
        spark.sessionState.catalog.defaultTablePath(TableIdentifier("delta_test")).toString,
        Seq("UNSET TBLPROPERTIES", """["key"]""", "true"),
        Seq($"operation", $"operationParameters.properties", $"operationParameters.ifExists"))
    }
  }

  testWithFlag("operations - add columns") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long, v4 int AFTER v1)")
      val column3 = """{"name":"v3","type":"long","nullable":true,"metadata":{}}"""
      val column4 = """{"name":"v4","type":"integer","nullable":true,"metadata":{}}"""
      checkLastOperation(
        spark.sessionState.catalog.defaultTablePath(TableIdentifier("delta_test")).toString,
        Seq("ADD COLUMNS",
          s"""[{"column":$column3},{"column":$column4,"position":"AFTER v1"}]"""),
        Seq($"operation", $"operationParameters.columns"))
    }
  }

  testWithFlag("operations - change column") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      sql("ALTER TABLE delta_test CHANGE COLUMN v1 v1 integer AFTER v2")
      checkLastOperation(
        spark.sessionState.catalog.defaultTablePath(TableIdentifier("delta_test")).toString,
        Seq("CHANGE COLUMN",
          s"""{"name":"v1","type":"integer","nullable":true,"metadata":{}}""",
          "AFTER v2"),
        Seq($"operation", $"operationParameters.column", $"operationParameters.position"))
    }
  }

  test("operations - upgrade protocol") {
    withTempDir { path =>
      val log = DeltaLog.forTable(spark, path)
      log.ensureLogDirectoryExist()
      log.store.write(
        FileNames.deltaFile(log.logPath, 0),
        Iterator(Metadata(schemaString = spark.range(1).schema.json).json, Protocol(1, 1).json))
      log.update()
      log.upgradeProtocol()
      checkLastOperation(
        path.toString,
        Seq("UPGRADE PROTOCOL",
          s"""{"minReaderVersion":${Action.readerVersion},""" +
            s""""minWriterVersion":${Action.writerVersion}}"""),
        Seq($"operation", $"operationParameters.newProtocol"))
    }
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


  testWithFlag("old and new writers") {
    val tempDir = Utils.createTempDir().toString
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "false") {
      Seq(1, 2, 3).toDF().write.format("delta").save(tempDir.toString)
    }

    checkLastOperation(tempDir, Seq(null, null))
    withSQLConf(DeltaSQLConf.DELTA_COMMIT_INFO_ENABLED.key -> "true") {
      Seq(1, 2, 3).toDF().write.format("delta").mode("append").save(tempDir.toString)
    }

    assert(spark.sql(s"DESCRIBE HISTORY delta.`$tempDir`").count() === 2)
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

    val ans = io.delta.tables.DeltaTable.forPath(spark, tempDir).history().as[CommitInfo].collect()
    assert(ans.map(_.version) === Seq(Some(4), Some(3), Some(2), Some(1), Some(0)))

    val ans2 = sql(s"DESCRIBE HISTORY delta.`$tempDir`").as[CommitInfo].collect()
    assert(ans2.map(_.version) === Seq(Some(4), Some(3), Some(2), Some(1), Some(0)))
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
    val ans = sql(s"DESCRIBE HISTORY delta.`$tempDir`").as[CommitInfo].collect()
    assert(ans.map(x => x.version.get -> x.readVersion) ===
      Seq(5 -> None, 4 -> Some(1), 3 -> Some(2), 2 -> Some(1), 1 -> Some(0), 0 -> None))
  }

  testWithFlag("evolvability test") {
    checkLastOperation(
      evolvabilityResource,
      evolvabilityLastOp,
      Seq($"operation", $"operationParameters.mode", $"operationParameters.partitionBy"))
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
        val expectedTimeMetrics = Set("executionTimeMs", "scanTimeMs", "rewriteTimeMs")
        checkOperationTimeMetricsInvariant(expectedTimeMetrics, operationMetrics)
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
        val expectedTimeMetrics = Set("executionTimeMs", "scanTimeMs", "rewriteTimeMs")
        checkOperationTimeMetricsInvariant(expectedTimeMetrics, operationMetrics)
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
        val expectedTimeMetrics = Set("executionTimeMs", "scanTimeMs", "rewriteTimeMs")
        checkOperationTimeMetricsInvariant(expectedTimeMetrics, operationMetrics)
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
        val expectedTimeMetrics = Set("executionTimeMs", "scanTimeMs", "rewriteTimeMs")
        checkOperationTimeMetricsInvariant(expectedTimeMetrics, operationMetrics)
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
        val expectedTimeMetrics = Set("executionTimeMs", "scanTimeMs", "rewriteTimeMs")
        checkOperationTimeMetricsInvariant(expectedTimeMetrics, operationMetrics)
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

  test("sort and collect the DESCRIBE HISTORY result") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      Seq(1, 2, 3).toDF().write.format("delta").save(path)
      val rows = sql(s"DESCRIBE HISTORY delta.`$path`")
        .orderBy("version")
        .collect()
      assert(rows.map(_.getAs[Long]("version")).toList == 0L :: Nil)
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val rows = sql(s"DESCRIBE HISTORY delta.`$path`")
          .filter("version >= 0")
          .orderBy("version")
          .collect()
        assert(rows.map(_.getAs[Long]("version")).toList == 0L :: Nil)
      }
    }
  }

}

class DescribeDeltaHistorySuite
  extends DescribeDeltaHistorySuiteBase with DeltaSQLCommandTest
