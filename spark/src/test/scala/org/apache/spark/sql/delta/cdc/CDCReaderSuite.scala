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

package org.apache.spark.sql.delta.cdc

// scalastyle:off import.ordering.noEmptyLine
import java.io.File

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.Delete
import org.apache.spark.sql.delta.DeltaTestUtils.{recordDataChangeInCommitInfo, BOOLEAN_DOMAIN}
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.files.DelayedCommitProtocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.execution.{LogicalRDD, SQLExecution}
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StringType, StructType}

class CDCReaderSuite
  extends QueryTest
  with CheckCDCAnswer
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CDFStartingCommitSchemaTests
  with DeltaColumnMappingTestUtils
  with CDCSchemaChangeTests {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  /**
   * Write a commit with just CDC data. Returns the committed version.
   */
  private def writeCdcData(
      log: DeltaLog,
      data: DataFrame,
      extraActions: Seq[Action] = Seq.empty): Long = {
    log.withNewTransaction { txn =>
      val qe = data.queryExecution
      val basePath = log.dataPath.toString

      // column mapped mode forces to use random file prefix
      val randomPrefixes = if (columnMappingEnabled) {
        Some(DeltaConfigs.RANDOM_PREFIX_LENGTH.fromMetaData(log.snapshot.metadata))
      } else {
        None
      }
      // we need to convert to physical name in column mapping mode
      val mappedOutput = if (columnMappingEnabled) {
        val metadata = log.snapshot.metadata
        DeltaColumnMapping.createPhysicalAttributes(
          qe.analyzed.output, metadata.schema, metadata.columnMappingMode
        )
      } else {
        qe.analyzed.output
      }

      SQLExecution.withNewExecutionId(qe) {
        var committer = new DelayedCommitProtocol("delta", basePath, randomPrefixes, None)
        FileFormatWriter.write(
          sparkSession = spark,
          plan = qe.executedPlan,
          fileFormat = log.fileFormat(log.snapshot.protocol, log.unsafeVolatileMetadata),
          committer = committer,
          outputSpec = FileFormatWriter.OutputSpec(basePath, Map.empty, mappedOutput),
          hadoopConf = log.newDeltaHadoopConf(),
          partitionColumns = Seq.empty,
          bucketSpec = None,
          statsTrackers = Seq.empty,
          options = Map.empty)

        val cdc = committer.addedStatuses.map { a =>
          AddCDCFile(a.path, Map.empty, a.size)
        }
        txn.commit(extraActions ++ cdc, DeltaOperations.ManualUpdate)
      }
    }
  }


  def createCDFDF(start: Long, end: Long, commitVersion: Long, changeType: String): DataFrame = {
    spark.range(start, end)
      .withColumn("v", lit(null))
      .withColumn(CDC_TYPE_COLUMN_NAME, lit(changeType))
      .withColumn(CDC_COMMIT_VERSION, lit(commitVersion))
  }

  test("a commit recording dataChange = false yields no CDC rows") {
    // The CommitInfo and the file actions are made to disagree, which no write path produces. It
    // is the only way to observe which of the two the reader consulted: from the commit's own
    // summary it infers no change, from the file actions an insert per added file.
    Seq(true, false).foreach { readFromCommitInfo =>
      withSQLConf(
          DeltaSQLConf.DELTA_COMMIT_INFO_DATA_CHANGE_READ_ENABLED.key ->
            readFromCommitInfo.toString) {
        withTempDir { dir =>
          val path = dir.getAbsolutePath
          val data = spark.range(10)
          data.write.format("delta").save(path)
          recordDataChangeInCommitInfo(
            DeltaLog.forTable(spark, path), version = 0, dataChange = Some(false))

          val log = DeltaLog.forTable(spark, path)
          val inserted = if (readFromCommitInfo) spark.range(0) else data
          checkCDCAnswer(
            log,
            CDCReader.changesToBatchDF(log, 0, 0, spark),
            inserted
              .withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
              .withColumn(CDC_COMMIT_VERSION, lit(0)))
        }
      }
    }
  }

  test("simple CDC scan") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)
      val cdcData = spark.range(20, 25).withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))

      data.write.format("delta").save(dir.getAbsolutePath)
      sql(s"DELETE FROM delta.`${dir.getAbsolutePath}`")
      writeCdcData(log, cdcData)

      // For this basic test, we check each of the versions individually in addition to the full
      // range to try and catch weird corner cases.
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 0, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 1, 1, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
          .withColumn(CDC_COMMIT_VERSION, lit(1))
      )
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 2, 2, spark),
        cdcData.withColumn(CDC_COMMIT_VERSION, lit(2))
      )
      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 2, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
          .unionAll(data
            .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
            .withColumn(CDC_COMMIT_VERSION, lit(1)))
          .unionAll(cdcData.withColumn(CDC_COMMIT_VERSION, lit(2)))
      )
    }
  }

  test("CDC has correct stats") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)
      val cdcData = spark.range(20, 25).withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))

      data.write.format("delta").save(dir.getAbsolutePath)
      sql(s"DELETE FROM delta.`${dir.getAbsolutePath}`")
      writeCdcData(log, cdcData)

      assert(
        CDCReader
          .changesToBatchDF(log, 0, 2, spark)
          .queryExecution
          .optimizedPlan
          .collectLeaves()
          .exists {
            case l: LogicalRDD => l.stats.sizeInBytes == 0 && !l.isStreaming
            case _ => false
          }
      )
    }
  }

  test("cdc update ops") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)

      data.write.format("delta").save(dir.getAbsolutePath)
      writeCdcData(
        log,
        spark.range(20, 25).toDF().withColumn(CDC_TYPE_COLUMN_NAME, lit("update_pre")))
      writeCdcData(
        log,
        spark.range(30, 35).toDF().withColumn(CDC_TYPE_COLUMN_NAME, lit("update_post")))

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 2, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
          .unionAll(spark.range(20, 25).withColumn(CDC_TYPE_COLUMN_NAME, lit("update_pre"))
              .withColumn(CDC_COMMIT_VERSION, lit(1))
          )
          .unionAll(spark.range(30, 35).withColumn(CDC_TYPE_COLUMN_NAME, lit("update_post"))
              .withColumn(CDC_COMMIT_VERSION, lit(2))
          )
      )
    }
  }

  test("dataChange = false operations ignored") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)

      data.write.format("delta").save(dir.getAbsolutePath)
      sql(s"OPTIMIZE delta.`${dir.getAbsolutePath}`")

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 1, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )
    }
  }

  test("range with start and end equal") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      val data = spark.range(10)
      val cdcData = spark.range(0, 5).withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
          .withColumn(CDC_COMMIT_VERSION, lit(1))

      data.write.format("delta").save(dir.getAbsolutePath)
      writeCdcData(log, cdcData)

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 0, spark),
        data.withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 1, 1, spark),
        cdcData)
    }
  }

  test("range past the end of the log") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)

      checkCDCAnswer(
        log,
        CDCReader.changesToBatchDF(log, 0, 1, spark),
        spark.range(10).withColumn(CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDC_COMMIT_VERSION, lit(0))
      )
    }
  }

  test("invalid range - end before start") {
    withTempDir { dir =>
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      spark.range(20).write.format("delta").mode("append").save(dir.getAbsolutePath)

      intercept[IllegalArgumentException] {
        CDCReader.changesToBatchDF(log, 1, 0, spark)
      }
    }
  }

  testQuietly("invalid range - start after last version of CDF") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      spark.range(20).write.format("delta").mode("append").save(dir.getAbsolutePath)

      val e = intercept[DeltaIllegalArgumentException] {
        spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", Long.MaxValue)
          .option("endingVersion", Long.MaxValue)
          .load(dir.toString)
          .count()
      }
      checkError(
        e,
        condition = "DELTA_CDC_START_VERSION_AFTER_LATEST",
        sqlState = "22003",
        parameters = Map("start" -> Long.MaxValue.toString, "latest" -> "1")
      )
    }
  }

  test("partition filtering of removes and cdc files") {
    withTempDir { dir =>
      withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
        val path = dir.getAbsolutePath
        val log = DeltaLog.forTable(spark, path)
        spark.range(6).selectExpr("id", "'old' as text", "id % 2 as part")
          .write.format("delta").partitionBy("part").save(path)

        // Generate some CDC files.
        withTempView("source") {
          spark.range(4).createOrReplaceTempView("source")
          sql(
            s"""MERGE INTO delta.`$path` t USING source s ON s.id = t.id
               |WHEN MATCHED AND s.id = 1 THEN UPDATE SET text = 'new'
               |WHEN MATCHED AND s.id = 3 THEN DELETE""".stripMargin)
        }

        // This will generate just remove files due to the partition delete optimization.
        sql(s"DELETE FROM delta.`$path` WHERE part = 0")

        checkCDCAnswer(
          log,
          CDCReader.changesToBatchDF(log, 0, 2, spark).filter("_change_type = 'insert'"),
          Range(0, 6).map { i => Row(i, "old", i % 2, "insert", 0) })
        checkCDCAnswer(
          log,
          CDCReader.changesToBatchDF(log, 0, 2, spark).filter("_change_type = 'delete'"),
          Seq(0, 2, 3, 4).map { i => Row(i, "old", i % 2, "delete", if (i % 2 == 0) 2 else 1) })
        checkCDCAnswer(
          log,
          CDCReader.changesToBatchDF(log, 0, 2, spark).filter("_change_type = 'update_preimage'"),
          Row(1, "old", 1, "update_preimage", 1) :: Nil)
        checkCDCAnswer(
          log,
          CDCReader.changesToBatchDF(log, 0, 2, spark).filter("_change_type = 'update_postimage'"),
          Row(1, "new", 1, "update_postimage", 1) :: Nil)
      }
    }
  }

  test("file layout - unpartitioned") {
    withTempDir { dir =>
      withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
        val path = dir.getAbsolutePath
        spark.range(10).repartition(1).write.format("delta").save(path)
        sql(s"DELETE FROM delta.`$path` WHERE id < 5")

        val log = DeltaLog.forTable(spark, path)
        // The data path should contain four files: the delta log, the CDC folder `__is_cdc=true`,
        // and two data files with randomized names from before and after the DELETE command. The
        // commit protocol should have stripped out __is_cdc=false.
        val baseDirFiles =
          log.logPath.getFileSystem(log.newDeltaHadoopConf()).listStatus(log.dataPath)
        assert(baseDirFiles.length == 4)
        assert(baseDirFiles.exists { f => f.isDirectory && f.getPath.getName == "_delta_log"})
        assert(baseDirFiles.exists { f => f.isDirectory && f.getPath.getName == CDC_LOCATION})
        assert(!baseDirFiles.exists { f => f.getPath.getName.contains(CDC_PARTITION_COL) })
      }
    }
  }

  test("file layout - partitioned") {
    withTempDir { dir =>
      withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
        val path = dir.getAbsolutePath
        spark.range(10).withColumn("part", col("id") % 2)
          .repartition(1).write.format("delta").partitionBy("part").save(path)
        sql(s"DELETE FROM delta.`$path` WHERE id < 5")

        val log = DeltaLog.forTable(spark, path)
        // The data path should contain four directories: the delta log, the CDC folder
        // `__is_cdc=true`, and the two partition folders. The commit protocol
        // should have stripped out __is_cdc=false.
        val fs = log.logPath.getFileSystem(log.newDeltaHadoopConf())
        val baseDirFiles = fs.listStatus(log.dataPath)
        baseDirFiles.foreach { f => assert(f.isDirectory) }
        assert(baseDirFiles.map(_.getPath.getName).toSet ==
          Set("_delta_log", CDC_LOCATION, "part=0", "part=1"))

        // Each partition folder should contain only two data files from before and after the read.
        // In particular, they should not contain any __is_cdc folder - that should always be the
        // top level partition.
        for (partitionFolder <- Seq("part=0", "part=1")) {
          val files = fs.listStatus(new Path(log.dataPath, partitionFolder))
          assert(files.length === 2)
          files.foreach { f =>
            assert(!f.isDirectory)
            assert(!f.getPath.getName.startsWith(CDC_LOCATION))
          }
        }

        // The CDC folder should also contain the two partitions.
        val cdcPartitions = fs.listStatus(new Path(log.dataPath, CDC_LOCATION))
        cdcPartitions.foreach { f => assert(f.isDirectory, s"$f was not a directory") }
        assert(cdcPartitions.map(_.getPath.getName).toSet == Set("part=0", "part=1"))
      }
    }
  }

  test("for CDC add backtick in column name with dot [.] ") {
    import testImplicits._

    withTempDir { dir =>
      withSQLConf((DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
        val path = dir.getAbsolutePath
        // 0th commit
        Seq(2, 4).toDF("id.num")
          .withColumn("id.num`s", lit(10))
          .withColumn("struct_col", struct(lit(1).as("field"), lit(2).as("field.one")))
          .write.format("delta").save(path)
        // 1st commit
        Seq(1, 3, 5).toDF("id.num")
          .withColumn("id.num`s", lit(10))
          .withColumn("struct_col", struct(lit(1).as("field"), lit(2).as("field.one")))
          .write.format("delta").mode(SaveMode.Append).save(path)
        // Reading from 0th version
        val actual = spark.read.format("delta")
          .option("readChangeFeed", "true").option("startingVersion", 0)
          .load(path).drop(CDCReader.CDC_COMMIT_TIMESTAMP)

        val expected = spark.range(1, 6).toDF("id.num").withColumn("id.num`s", lit(10))
          .withColumn("struct_col", struct(lit(1).as("field"), lit(2).as("field.one")))
          .withColumn(CDCReader.CDC_TYPE_COLUMN_NAME, lit("insert"))
          .withColumn(CDCReader.CDC_COMMIT_VERSION, col("`id.num`") % 2)
        checkAnswer(actual, expected)
      }
    }
  }

  for (cdfEnabled <- BOOLEAN_DOMAIN)
  test(s"Coarse-grained CDF, cdfEnabled=$cdfEnabled") {
    // The test table has a NullType column (v) that is read back via CDF.
    assume(DeltaTestUtilsBase.nullTypeColumnsSupported)
    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> cdfEnabled.toString) {
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir.getAbsolutePath)

        // commit 0: 2 inserts
        spark.range(start = 0, end = 2, step = 1, numPartitions = 1)
          .withColumn("v", lit(null))
          .write.format("delta").save(dir.getAbsolutePath)
        var df = CDCReader.changesToBatchDF(
          log, 0, 1, spark, catalogTableOpt = None, useCoarseGrainedCDC = true)
        checkAnswer(df.drop(CDC_COMMIT_TIMESTAMP),
          createCDFDF(start = 0, end = 2, commitVersion = 0, changeType = "insert"))

        // commit 1: 2 inserts
        spark.range(start = 2, end = 4)
          .write.mode("append").format("delta").save(dir.getAbsolutePath)
        df = CDCReader.changesToBatchDF(
          log, 1, 2, spark, catalogTableOpt = None, useCoarseGrainedCDC = true)
        checkAnswer(df.drop(CDC_COMMIT_TIMESTAMP),
          createCDFDF(start = 2, end = 4, commitVersion = 1, changeType = "insert"))

        // commit 2
        sql(s"DELETE FROM delta.`$dir` WHERE id = 0")
        df = CDCReader.changesToBatchDF(
          log, 2, 3, spark, catalogTableOpt = None, useCoarseGrainedCDC = true)
          .drop(CDC_COMMIT_TIMESTAMP)

        // Using only Add and RemoveFiles should generate 2 deletes and 1 insert. Even when CDF
        // is enabled, we want to use only Add and RemoveFiles.
        val dfWithDeletesFirst = df.sort(CDC_TYPE_COLUMN_NAME)
        val expectedAnswer =
          createCDFDF(start = 0, end = 2, commitVersion = 2, changeType = "delete")
            .union(
              createCDFDF(start = 1, end = 2, commitVersion = 2, changeType = "insert"))
        checkAnswer(dfWithDeletesFirst, expectedAnswer)
      }
    }
  }

  test("Logs are generated for changesToDF") {
    withTempDir { dir =>
      val events = Log4jUsageLogger.track {
        val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
        val data = spark.range(10)

        data.write.format("delta").save(dir.getAbsolutePath)
        sql(s"DELETE FROM delta.`${dir.getAbsolutePath}`")
        CDCReader.changesToBatchDF(log, 0, 1, spark)
      }

      assert(events.exists(event => event.metric == "tahoeEvent" &&
        event.tags.get("opType") == Option("delta.changeDataFeed.changesToDF")))
    }
  }

  /** Collect and parse the `delta.changeDataFeed.changesToDF` usage logs among the events. */
  private def findChangesToDFEvents(events: Seq[UsageRecord]): Seq[Map[String, Any]] = {
    events.collect {
      case event if event.metric == "tahoeEvent" &&
        event.tags.get("opType").contains("delta.changeDataFeed.changesToDF") =>
        JsonUtils.fromJson[Map[String, Any]](event.blob)
    }
  }

  /** Parse the single `delta.changeDataFeed.changesToDF` usage log emitted by the events. */
  private def getChangesToDFMetrics(events: Seq[UsageRecord]): Map[String, Any] = {
    findChangesToDFEvents(events) match {
      case Seq(metrics) => metrics
      case other => fail("Expected exactly one delta.changeDataFeed.changesToDF usage event, " +
        s"got ${other.size} with opTypes: " + events.flatMap(_.tags.get("opType")).mkString(", "))
    }
  }

  /** Extract a non-negative numeric metric emitted in a parsed usage log. */
  private def getMetric(metrics: Map[String, Any], key: String): Long = {
    metrics.get(key)
      .map(_.asInstanceOf[Number].longValue())
      .getOrElse(fail(s"Metric '$key' not found in usage log: $metrics"))
  }

  test("changesToDF metrics count only AddCDCFiles when a commit has CDC actions") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val log = DeltaLog.forTable(spark, path)

      spark.range(10).write.format("delta").save(path)
      sql(s"UPDATE delta.`$path` SET id = id + 100 WHERE id < 5")

      val events = Log4jUsageLogger.track {
        CDCReader.changesToBatchDF(log, 1, 1, spark)
      }
      val metrics = getChangesToDFMetrics(events)

      assert(getMetric(metrics, "numAddCRCFiles") >= 1L,
        s"Expected the AddCDCFile actions to be counted: $metrics")
      assert(getMetric(metrics, "numAddFiles") == 0L,
        s"AddFile actions ignored when serving CDC must not be counted: $metrics")
      assert(getMetric(metrics, "numRemoveFiles") == 0L,
        s"RemoveFile actions ignored when serving CDC must not be counted: $metrics")
      assert(getMetric(metrics, "totalBytes") >= 1L,
        s"Expected the AddCDCFile sizes to contribute to totalBytes: $metrics")
    }
  }

  test("changesToDF metrics exclude dataChange = false file actions") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val log = DeltaLog.forTable(spark, path)

      spark.range(0, 50).write.format("delta").save(path)
      spark.range(50, 100).write.format("delta").mode("append").save(path)
      sql(s"OPTIMIZE delta.`$path`")

      val optimizeVersion = log.update().version
      assert(optimizeVersion == 2L,
        s"Expected OPTIMIZE to commit version 2, but the latest version is $optimizeVersion")

      val events = Log4jUsageLogger.track {
        CDCReader.changesToBatchDF(log, optimizeVersion, optimizeVersion, spark)
      }
      val metrics = getChangesToDFMetrics(events)

      assert(getMetric(metrics, "numAddFiles") == 0L,
        s"dataChange = false AddFile actions must not be counted: $metrics")
      assert(getMetric(metrics, "numRemoveFiles") == 0L,
        s"dataChange = false RemoveFile actions must not be counted: $metrics")
      assert(getMetric(metrics, "numAddCRCFiles") == 0L,
        s"OPTIMIZE produces no CDC actions: $metrics")
      assert(getMetric(metrics, "totalBytes") == 0L,
        s"No files are read for this commit, so totalBytes must be 0: $metrics")
    }
  }

  test("CDF reads explicit MERGE change files after type widening") {
    // Change files contain both images in the new INT schema; removed files use SMALLINT.
    withColumnMappingConf("none") {
      withSQLConf(
          DeltaSQLConf.DELTA_ALLOW_AUTOMATIC_WIDENING.key -> "always",
          DeltaSQLConf.DELTA_CDF_START_VERSION_SCHEMA_CHECK_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_CDF_UNSAFE_BATCH_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES.key -> "false") {
        withTable("cdf_start_merge") {
          sql("""CREATE TABLE cdf_start_merge(id SMALLINT) USING DELTA
            |TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true',
            |  'delta.enableTypeWidening' = 'true', 'delta.enableDeletionVectors' = 'false')
            |""".stripMargin)
          sql("INSERT INTO cdf_start_merge VALUES (1)")
          sql("""MERGE WITH SCHEMA EVOLUTION INTO cdf_start_merge t
            |USING (SELECT 2 AS id) s ON t.id + 1 = s.id
            |WHEN MATCHED THEN UPDATE SET *
            |""".stripMargin)
          assert(spark.table("cdf_start_merge").schema("id").dataType == IntegerType)

          checkAnswer(sql("SELECT id, _change_type, _commit_version " +
            "FROM table_changes('cdf_start_merge', 2, 2)"),
            Seq(Row(1, "update_preimage", 2L), Row(2, "update_postimage", 2L)))
        }
      }
    }
  }

}

trait CDFStartingCommitSchemaTests extends QueryTest with DeltaSQLCommandTest {

  private def startingCommitProperties: Map[String, String] =
    Map("delta.enableDeletionVectors" -> "false")

  protected def startingCommitTableProperties(mode: String): String = {
    val properties = startingCommitProperties + ("delta.columnMapping.mode" -> mode)
    "TBLPROPERTIES (" + properties.toSeq.sorted.map { case (key, value) =>
      s"'$key' = '$value'"
    }.mkString(", ") + ")"
  }

  protected def withStartingCommitTables(
      schemaCheckEnabled: Boolean = true)(f: (String, String) => Unit): Unit = {
    withSQLConf(
        DeltaSQLConf.DELTA_CDF_UNSAFE_BATCH_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES.key -> "false",
        DeltaSQLConf.DELTA_CDF_START_VERSION_SCHEMA_CHECK_ENABLED.key ->
          schemaCheckEnabled.toString,
        DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key -> "endVersion") {
      withTable("cdf_start_source", "cdf_start_target") {
        f("cdf_start_source", "cdf_start_target")
      }
    }
  }

  protected def readStartingCommitCDF(tableName: String, start: Long, end: Long): DataFrame = {
    sql(s"SELECT acct_id, balance, region, _change_type, _commit_version " +
      s"FROM table_changes('$tableName', $start, $end)")
  }

  protected def startingCommitCDFReads(tableName: String, version: Long): Seq[DataFrame] = {
    startingCommitCDFReads(tableName, version, version)
  }

  protected def startingCommitCDFReads(
      tableName: String,
      start: Long,
      end: Long): Seq[DataFrame] = {
    Seq(readStartingCommitCDF(tableName, start, end))
  }

  protected def assertBlockedStartingCommitCDF(
      tableName: String,
      version: Long,
      operation: String): Unit = {
    for (schemaMode <- Seq("legacy", "latest", "endVersion")) {
      withSQLConf(
          DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key -> schemaMode) {
        startingCommitCDFReads(tableName, version).foreach { read =>
          checkError(
            intercept[DeltaUnsupportedOperationException] { read.collect() },
            "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_STARTING_COMMIT_SCHEMA.KNOWN_OPERATION",
            sqlState = "0AKDC",
            parameters = Map(
              "operation" -> operation,
              "start" -> version.toString,
              "priorVersion" -> (version - 1).toString))
        }
      }
    }
  }


  private def createMappedTable(
      tableName: String,
      mode: String,
      nested: Boolean,
      swapped: Boolean): Unit = {
    def mappingMetadata(id: Int, physicalName: String): Metadata = {
      new MetadataBuilder()
        .putLong(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY, id)
        .putString(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
        .build()
    }

    val firstId = if (swapped) 3 else 2
    val secondId = if (swapped) 2 else 3
    val schema = if (nested) {
      new StructType()
        .add("acct_id", IntegerType, true, mappingMetadata(1, "physical_acct"))
        .add("balance", new StructType()
          .add("amount", IntegerType, true, mappingMetadata(firstId, "physical_amount"))
          .add("other", IntegerType, true, mappingMetadata(secondId, "physical_other")),
          true, mappingMetadata(4, "physical_balance"))
    } else {
      new StructType()
        .add("acct_id", IntegerType, true, mappingMetadata(firstId, "physical_acct"))
        .add("balance", IntegerType, true, mappingMetadata(secondId, "physical_balance"))
    }
    val builder = DeltaTable.createOrReplace(spark)
      .tableName(tableName)
      .addColumns(schema.add("region", StringType, true, mappingMetadata(5, "physical_region")))
      .property("delta.columnMapping.mode", mode)
    startingCommitProperties.foreach { case (key, value) => builder.property(key, value) }
    builder.execute()
  }

  for {
    mode <- Seq("name", "id")
    nested <- Seq(false, true)
    schemaCheckEnabled <- Seq(true, false)
  } {
    test(s"CDF starting commit changed IDs with stable physical names " +
      s"- mapping=$mode, nested=$nested, schemaCheckEnabled=$schemaCheckEnabled") {
      withStartingCommitTables(schemaCheckEnabled) { (_, target) =>
        createMappedTable(target, mode, nested, swapped = false)
        val balance = if (nested) "named_struct('amount', 100, 'other', 200)" else "100"
        sql(s"INSERT INTO $target VALUES (1, $balance, 'east')")
        createMappedTable(target, mode, nested, swapped = true)

          if (mode == "id" && schemaCheckEnabled) {
            assertBlockedStartingCommitCDF(target, version = 2,
              operation = "CREATE OR REPLACE TABLE")
          } else {
            val expected = (mode, nested) match {
              case ("id", true) => Row(1, Row(200, 100), "east", "delete", 2L)
              case ("id", false) => Row(100, 1, "east", "delete", 2L)
              case (_, true) => Row(1, Row(100, 200), "east", "delete", 2L)
              case (_, false) => Row(1, 100, "east", "delete", 2L)
            }
            startingCommitCDFReads(target, version = 2).foreach { read =>
              checkAnswer(read, expected)
            }
          }
      }
    }
  }

  for {
    nested <- Seq(false, true)
    schemaCheckEnabled <- Seq(true, false)
  } {
    test(s"CDF starting commit accepts new files after ID-changing empty REPLACE - " +
      s"nested=$nested, schemaCheckEnabled=$schemaCheckEnabled") {
      withStartingCommitTables(schemaCheckEnabled) { (_, target) =>
        createMappedTable(target, "id", nested, swapped = false)
        createMappedTable(target, "id", nested, swapped = true)
        val balance = if (nested) "named_struct('amount', 100, 'other', 200)" else "100"
        sql(s"INSERT INTO $target VALUES (1, $balance, 'east')")

        val expectedBalance = if (nested) Row(100, 200) else 100
        startingCommitCDFReads(target, 0, 2).foreach { read =>
          checkAnswer(read, Row(1, expectedBalance, "east", "insert", 2L))
        }
      }
    }
  }

  private def testTypeChangingReplace(mode: String): Unit = {
    test(s"CDF starting commit rejects type-changing REPLACE - mapping=$mode") {
      withStartingCommitTables() { (_, target) =>
        sql(s"CREATE TABLE $target(acct_id INT, balance INT, region STRING) USING DELTA " +
          startingCommitTableProperties(mode))
        sql(s"INSERT INTO $target VALUES (1, 100, 'east')")
        sql(s"CREATE OR REPLACE TABLE $target(acct_id BIGINT, balance INT, region STRING) " +
          s"USING DELTA ${startingCommitTableProperties(mode)}")

        assertBlockedStartingCommitCDF(target, version = 2,
          operation = "CREATE OR REPLACE TABLE")
      }
    }
  }

  testTypeChangingReplace("none")

  for (mode <- Seq("none", "name", "id")) {
    test(s"CDF starting commit accepts INSERT OVERWRITE with schema evolution - mapping=$mode") {
      withStartingCommitTables() { (_, target) =>
        sql(s"CREATE TABLE $target(acct_id INT, balance INT) USING DELTA " +
          startingCommitTableProperties(mode))
        sql(s"INSERT INTO $target VALUES (1, 100)")
        sql(s"INSERT WITH SCHEMA EVOLUTION OVERWRITE $target " +
          "SELECT 2 AS acct_id, 200 AS balance, 'west' AS region")

        startingCommitCDFReads(target, version = 2).foreach { read =>
          checkAnswer(read, Seq(
            Row(1, 100, null, "delete", 2L),
            Row(2, 200, "west", "insert", 2L)))
        }
      }
    }
  }

  for {
    mode <- Seq("none", "name", "id")
    schemaCheckEnabled <- Seq(true, false)
  } {
    test(s"CDF starting commit accepts compatible deletes - " +
      s"mapping=$mode, schemaCheckEnabled=$schemaCheckEnabled") {
      withStartingCommitTables(schemaCheckEnabled) { (_, target) =>
        // Matching the default avoids treating initial creation as a mapping upgrade in OSS.
        withSQLConf(DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> mode) {
          sql(s"CREATE TABLE $target USING DELTA ${startingCommitTableProperties(mode)} " +
            "AS SELECT 1 AS acct_id, 100 AS balance, 'east' AS region")
        }
        checkAnswer(readStartingCommitCDF(target, 0, 0), Row(1, 100, "east", "insert", 0L))

        sql(s"ALTER TABLE $target ADD COLUMN extra INT")
        sql(s"INSERT INTO $target(acct_id, balance, region) VALUES (2, 200, 'west')")
        sql(s"RESTORE TABLE $target TO VERSION AS OF 1")
          startingCommitCDFReads(target, version = 3).foreach { read =>
            checkAnswer(read, Row(2, 200, "west", "delete", 3L))
          }
      }
    }
  }

  private def restoreAfterReaddingBalance(tableName: String, mode: String): Unit = {
    sql(s"CREATE TABLE $tableName(acct_id INT, balance INT, region STRING) USING DELTA " +
      startingCommitTableProperties(mode))
    sql(s"INSERT INTO $tableName VALUES (1, 100, 'east')")
    sql(s"ALTER TABLE $tableName DROP COLUMN balance")
    sql(s"ALTER TABLE $tableName ADD COLUMN balance INT")
    sql(s"INSERT INTO $tableName(acct_id, balance, region) VALUES (2, 200, 'west')")
    sql(s"RESTORE TABLE $tableName TO VERSION AS OF 1")
  }

  for {
    mode <- Seq("name", "id")
    schemaCheckEnabled <- Seq(true, false)
  } {
    test(s"CDF starting commit RESTORE across drop/re-add - " +
      s"mapping=$mode, schemaCheckEnabled=$schemaCheckEnabled") {
      withStartingCommitTables(schemaCheckEnabled) { (_, target) =>
        restoreAfterReaddingBalance(target, mode)
        checkAnswer(readStartingCommitCDF(target, 3, 3), Seq.empty)

          if (schemaCheckEnabled) {
            assertBlockedStartingCommitCDF(target, version = 5, operation = "RESTORE")
          } else {
            startingCommitCDFReads(target, version = 5).foreach { read =>
              checkAnswer(read, Row(2, null, "west", "delete", 5L))
            }
          }

        // Schema incompatibilities within the queried range remain blocked with either flag value.
        val error = intercept[DeltaUnsupportedOperationException] {
          readStartingCommitCDF(target, 1, 5).collect()
        }
        assert(error.getErrorClass.startsWith("DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_"))
      }
    }
  }

  for {
    mode <- Seq("name", "id")
    schemaCheckEnabled <- Seq(true, false)
  } {
    test(s"CDF starting commit with expired prior history - " +
      s"mapping=$mode, schemaCheckEnabled=$schemaCheckEnabled") {
      withStartingCommitTables(schemaCheckEnabled) { (_, target) =>
        restoreAfterReaddingBalance(target, mode)
        val tableLocation = sql(s"DESCRIBE DETAIL $target").select("location").head().getString(0)
        val deltaLog = DeltaLog.forTable(spark, tableLocation)
        deltaLog.checkpoint()
        // Retain the starting commit and its checkpoint, as ordinary log cleanup can do.
        (0L until 5L).foreach { version =>
          assert(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, version).toUri).delete())
        }
        DeltaLog.invalidateCache(spark, deltaLog.dataPath)

          startingCommitCDFReads(target, version = 5).foreach { read =>
            checkAnswer(read, Row(2, null, "west", "delete", 5L))
          }
      }
    }
  }

}

trait CDCSchemaChangeTests {
  self: QueryTest with DeltaSQLCommandTest =>

  protected def columnMappingMode: String

  test("CDF fails when a column is dropped after the start version") {
    val tableName = "cdf_drop_column_after_start"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a INT, b INT) USING DELTA
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1, 10)")
      sql(s"ALTER TABLE $tableName DROP COLUMN b")

      withSQLConf(
          DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
        checkError(
          exception = intercept[DeltaUnsupportedOperationException] {
            sql(s"SELECT * FROM table_changes('$tableName', 1, 2)").collect()
          },
          condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
          sqlState = Some("0AKDC"),
          matchPVals = true,
          parameters = Map(
            "start" -> "1",
            "end" -> "2",
            "readSchema" -> ".*",
            "readVersion" -> "2",
            "incompatibleVersion" -> "1",
            "config" -> ".*defaultSchemaModeForColumnMappingTable"))
      }
    }
  }

  test("CDF fails when a column is renamed after the start version") {
    val tableName = "cdf_rename_column_after_start"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a INT, b INT) USING DELTA
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1, 10)")
      sql(s"ALTER TABLE $tableName RENAME COLUMN b TO c")

      withSQLConf(
          DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
        checkError(
          exception = intercept[DeltaUnsupportedOperationException] {
            sql(s"SELECT * FROM table_changes('$tableName', 1, 2)").collect()
          },
          condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
          sqlState = Some("0AKDC"),
          matchPVals = true,
          parameters = Map(
            "start" -> "1",
            "end" -> "2",
            "readSchema" -> ".*",
            "readVersion" -> "2",
            "incompatibleVersion" -> "1",
            "config" -> ".*defaultSchemaModeForColumnMappingTable"))
      }
    }
  }

  test("CDF fails when a column type changes after the start version") {
    val tableName = "cdf_change_column_type_after_start"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a INT) USING DELTA
           |TBLPROPERTIES ('delta.enableTypeWidening' = 'true')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1)")
      sql(s"ALTER TABLE $tableName ALTER COLUMN a TYPE DOUBLE")

      withSQLConf(
          DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
        checkError(
          exception = intercept[DeltaUnsupportedOperationException] {
            sql(s"SELECT * FROM table_changes('$tableName', 1, 2)").collect()
          },
          condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
          sqlState = Some("0AKDC"),
          matchPVals = true,
          parameters = Map(
            "start" -> "1",
            "end" -> "2",
            "readSchema" -> ".*",
            "readVersion" -> "2",
            "incompatibleVersion" -> "1",
            "config" -> ".*defaultSchemaModeForColumnMappingTable"))
      }
    }
  }

  // The tests below only really make sense when column mapping is disabled, otherwise every
  // REPLACE TABLE creates a brand-new set of columns and reads are always rejected.
  if (columnMappingMode == "none") {
    test("partition schema change check can be rolled out") {
      val tableName = "cdf_partition_schema_evolution"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA PARTITIONED BY (a)
          |""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (1, 10)")
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)

        val events = withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "false") {
          Log4jUsageLogger.track {
            sql(s"SELECT * FROM table_changes('$tableName', 1, 2)").collect()
          }
        }
        val event = events.find(event => event.metric == "tahoeEvent" &&
          event.tags.get("opType").contains("delta.changeDataFeed.schemaIncompatibility"))
        assert(event.isDefined)
        val eventData = JsonUtils.fromJson[Map[String, Any]](event.get.blob)
        assert(eventData("isSchemaCompatible") === true)
        assert(eventData("hasPartitionSchemaChange") === true)
        assert(eventData("partitionSchemaCheckEnabled") === false)

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              sql(s"SELECT * FROM table_changes('$tableName', 1, 2)").collect()
            },
            condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            sqlState = Some("0AKDC"),
            matchPVals = true,
            parameters = Map(
              "start" -> "1",
              "end" -> "2",
              "readSchema" -> ".*",
              "readVersion" -> "2",
              "incompatibleVersion" -> "1"))
        }
      }
    }

    test("CDF allows column reordering when the partition schema is unchanged") {
      val tableName = "cdf_reordered_columns_same_partition_schema"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA PARTITIONED BY (a)
          |""".stripMargin)
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (b INT, a INT) USING DELTA
             |PARTITIONED BY (a)
             |""".stripMargin)

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkAnswer(
            sql(s"SELECT a, b FROM table_changes('$tableName', 0, 1)"),
            Nil)
        }
      }
    }

    test("CDF allows initial partition setup after an empty version 0") {
      val tableName = "cdf_initial_partition_setup"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA
          |""".stripMargin)
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (1, 10)")

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          val events = Log4jUsageLogger.track {
            checkAnswer(
              sql(s"SELECT a, b FROM table_changes('$tableName', 0, 2)"),
              Row(1, 10))
          }
          val event = events.find(event => event.metric == "tahoeEvent" &&
            event.tags.get("opType").contains("delta.changeDataFeed.schemaIncompatibility"))
          assert(event.isDefined)
          val eventData = JsonUtils.fromJson[Map[String, Any]](event.get.blob)
          assert(eventData("isSchemaCompatible") === true)
          assert(eventData("hasPartitionSchemaChange") === true)
          assert(eventData("partitionSchemaBlockEnabled") === false)
        }
      }
    }

    test("CDF still rejects a partition change after data in an initially empty table") {
      val tableName = "cdf_partition_change_after_initial_data"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA
          |""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (1, 10)")
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (2, 20)")

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              sql(s"SELECT * FROM table_changes('$tableName', 0, 3)").collect()
            },
            condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            sqlState = Some("0AKDC"),
            matchPVals = true,
            parameters = Map(
              "start" -> "0",
              "end" -> "3",
              "readSchema" -> ".*",
              "readVersion" -> "3",
              "incompatibleVersion" -> "0"))
        }
      }
    }

    test("CDF fails for an intermediate partition schema when endpoints agree") {
      val tableName = "cdf_multiple_partition_changes"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA PARTITIONED BY (a)
          |""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (1, 10)")
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (a)
             |""".stripMargin)

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              sql(s"SELECT * FROM table_changes('$tableName', 1, 3)").collect()
            },
            condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            sqlState = Some("0AKDC"),
            matchPVals = true,
            parameters = Map(
              "start" -> "1",
              "end" -> "3",
              "readSchema" -> ".*",
              "readVersion" -> "3",
              "incompatibleVersion" -> "2"))
        }
      }
    }

    test("CDF fails for multiple partition changes on an empty table") {
      val tableName = "cdf_multiple_empty_partition_changes"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA PARTITIONED BY (a)
          |""".stripMargin)
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (a)
             |""".stripMargin)

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              sql(s"SELECT * FROM table_changes('$tableName', 0, 2)").collect()
            },
            condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            sqlState = Some("0AKDC"),
            matchPVals = true,
            parameters = Map(
              "start" -> "0",
              "end" -> "2",
              "readSchema" -> ".*",
              "readVersion" -> "2",
              "incompatibleVersion" -> "1"))
        }
      }
    }
  }

  test("CDF fails for an intermediate dropped column when endpoints agree") {
    val tableName = "cdf_drop_add_column"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a INT, b INT) USING DELTA
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1, 10)")
      sql(s"ALTER TABLE $tableName DROP COLUMN b")
      sql(s"ALTER TABLE $tableName ADD COLUMN b INT")

      withSQLConf(
          DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
        checkError(
          exception = intercept[DeltaUnsupportedOperationException] {
            sql(s"SELECT * FROM table_changes('$tableName', 1, 3)").collect()
          },
          condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA",
          sqlState = Some("0AKDC"),
          matchPVals = true,
          parameters = Map(
            "start" -> "1",
            "end" -> "3",
            "readSchema" -> ".*",
            "readVersion" -> "3",
            "incompatibleVersion" -> "1",
            "config" -> ".*defaultSchemaModeForColumnMappingTable"))
      }
    }
  }

  test("CDF fails for an intermediate renamed column when endpoints agree") {
    val tableName = "cdf_rename_column_back"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (a INT, b INT) USING DELTA
           |TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (1, 10)")
      sql(s"ALTER TABLE $tableName RENAME COLUMN b TO c")
      sql(s"ALTER TABLE $tableName RENAME COLUMN c TO b")

      checkError(
        exception = intercept[DeltaUnsupportedOperationException] {
          sql(s"SELECT * FROM table_changes('$tableName', 1, 3)").collect()
        },
        condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
        sqlState = Some("0AKDC"),
        matchPVals = true,
        parameters = Map(
          "start" -> "1",
          "end" -> "3",
          "readSchema" -> ".*",
          "readVersion" -> "3",
          "incompatibleVersion" -> "2"))
    }
  }

  if (columnMappingMode == "none") {
    test("CDF fails for a partition schema change followed by RESTORE") {
      val tableName = "cdf_partition_change_restore"
      withTable(tableName) {
        sql(s"""
          |CREATE TABLE $tableName (a INT, b INT) USING DELTA PARTITIONED BY (a)
          |""".stripMargin)
        sql(s"INSERT INTO $tableName VALUES (1, 10)")
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)
        sql(s"RESTORE TABLE $tableName TO VERSION AS OF 1")

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              sql(s"SELECT * FROM table_changes('$tableName', 1, 3)").collect()
            },
            condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            sqlState = Some("0AKDC"),
            matchPVals = true,
            parameters = Map(
              "start" -> "1",
              "end" -> "3",
              "readSchema" -> ".*",
              "readVersion" -> "3",
              "incompatibleVersion" -> "2"))
        }
      }
    }
  }

  if (columnMappingMode != "none") {
    test(s"CDF fails for a partition schema change with " +
        s"$columnMappingMode column mapping") {
      val tableName = "cdf_partition_change_with_column_mapping"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (a INT, b INT) USING DELTA PARTITIONED BY (a)")
        sql(s"INSERT INTO $tableName VALUES (1, 10)")
        sql(
          s"""
             |CREATE OR REPLACE TABLE $tableName (a INT, b INT) USING DELTA
             |PARTITIONED BY (b)
             |""".stripMargin)

        withSQLConf(
            DeltaSQLConf.DELTA_CDF_BLOCK_READS_ACROSS_PARTITION_SCHEMA_CHANGES.key -> "true") {
          checkError(
            exception = intercept[DeltaUnsupportedOperationException] {
              sql(s"SELECT * FROM table_changes('$tableName', 1, 2)").collect()
            },
            condition = "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE",
            sqlState = Some("0AKDC"),
            matchPVals = true,
            parameters = Map(
              "start" -> "1",
              "end" -> "2",
              "readSchema" -> ".*",
              "readVersion" -> "2",
              "incompatibleVersion" -> "1"))
        }
      }
    }
  }
}

