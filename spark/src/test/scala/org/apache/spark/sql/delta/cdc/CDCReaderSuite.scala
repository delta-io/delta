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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.Delete
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.files.DelayedCommitProtocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.execution.{LogicalRDD, SQLExecution}
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class CDCReaderSuite
  extends QueryTest  with CheckCDCAnswer
  with SharedSparkSession
  with DeltaSQLCommandTest
  with DeltaColumnMappingTestUtils {

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
      .withColumn(CDC_TYPE_COLUMN_NAME, lit(changeType))
      .withColumn(CDC_COMMIT_VERSION, lit(commitVersion))
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
      val log = DeltaLog.forTable(spark, dir.getAbsolutePath)
      spark.range(10).write.format("delta").save(dir.getAbsolutePath)
      spark.range(20).write.format("delta").mode("append").save(dir.getAbsolutePath)

      val e = intercept[IllegalArgumentException] {
        spark.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", Long.MaxValue)
          .option("endingVersion", Long.MaxValue)
          .load(dir.toString)
          .count()
      }
      assert(e.getMessage ==
        DeltaErrors.startVersionAfterLatestVersion(Long.MaxValue, 1).getMessage)
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
    withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> cdfEnabled.toString) {
      withTempDir { dir =>
        val log = DeltaLog.forTable(spark, dir.getAbsolutePath)

        // commit 0: 2 inserts
        spark.range(start = 0, end = 2, step = 1, numPartitions = 1)
          .write.format("delta").save(dir.getAbsolutePath)
        var df = CDCReader.changesToBatchDF(log, 0, 1, spark, useCoarseGrainedCDC = true)
        checkAnswer(df.drop(CDC_COMMIT_TIMESTAMP),
          createCDFDF(start = 0, end = 2, commitVersion = 0, changeType = "insert"))

        // commit 1: 2 inserts
        spark.range(start = 2, end = 4)
          .write.mode("append").format("delta").save(dir.getAbsolutePath)
        df = CDCReader.changesToBatchDF(log, 1, 2, spark, useCoarseGrainedCDC = true)
        checkAnswer(df.drop(CDC_COMMIT_TIMESTAMP),
          createCDFDF(start = 2, end = 4, commitVersion = 1, changeType = "insert"))

        // commit 2
        sql(s"DELETE FROM delta.`$dir` WHERE id = 0")
        df = CDCReader.changesToBatchDF(log, 2, 3, spark, useCoarseGrainedCDC = true)
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
}

