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

package org.apache.spark.sql.delta

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import scala.language.implicitConversions

import org.apache.spark.sql.delta.actions.AddCDCFile
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.{DeltaSourceOffset, DeltaSQLConf}
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import io.delta.tables._
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.types.StructType

trait DeltaCDCStreamSuiteBase extends StreamTest with DeltaSQLCommandTest
  with DeltaSourceSuiteBase with DeltaColumnMappingTestUtils {

  import testImplicits._
  import io.delta.implicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  /** Modify timestamp for a delta commit, used to test timestamp querying */
  def modifyDeltaTimestamp(deltaLog: DeltaLog, version: Long, time: Long): Unit = {
    val file = new File(FileNames.deltaFile(deltaLog.logPath, version).toUri)
    file.setLastModified(time)
    val crc = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
    if (crc.exists()) {
      crc.setLastModified(time)
    }
  }

  /**
   * Create two tests for maxFilesPerTrigger and maxBytesPerTrigger
   */
  protected def testRateLimit(
      name: String,
      maxFilesPerTrigger: String,
      maxBytesPerTrigger: String)(f: (String, String) => Unit): Unit = {
    Seq(("maxFilesPerTrigger", maxFilesPerTrigger), ("maxBytesPerTrigger", maxBytesPerTrigger))
      .foreach { case (key: String, value: String) =>
        test(s"rateLimit - $key - $name") {
          f(key, value)
        }
      }
  }

  testQuietly("no startingVersion should result fetch the entire snapshot") {
    withTempDir { inputDir =>
      withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "false") {
        // version 0
        Seq(1, 9).toDF("value").write.format("delta").save(inputDir.getAbsolutePath)

        val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
        // version 1
        deltaTable.delete("value = 9")

        // version 2
        Seq(2).toDF("value").write.format("delta")
          .mode("append")
          .save(inputDir.getAbsolutePath)
      }
      // enable cdc - version 3
      sql(s"ALTER TABLE delta.`${inputDir.getAbsolutePath}` SET TBLPROPERTIES " +
        s"(${DeltaConfigs.CHANGE_DATA_FEED.key}=true)")

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer((1, "insert", 3), (2, "insert", 3)),
        Execute { _ =>
          deltaTable.delete("value = 1") // version 4
        },
        ProcessAllAvailable(),
        CheckAnswer((1, "insert", 3), (2, "insert", 3), (1, "delete", 4))
      )
    }
  }

  test("startingVersion = latest") {
    withTempDir { inputDir =>
      Seq(1, 2).toDF("value").write.format("delta").save(inputDir.getAbsolutePath)

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "latest")
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer(),
        AddToReservoir(inputDir, Seq(3).toDF("value")),
        ProcessAllAvailable(),
        CheckAnswer((3, "insert", 1))
      )
    }
  }

  test("user provided startingVersion") {
    withTempDir { inputDir =>
      // version 0
      Seq(1, 2, 3).toDF("id").write.delta(inputDir.toString)

      // version 1
      Seq(4, 5).toDF("id").write.mode("append").delta(inputDir.toString)

      // version 2
      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "1")
        .format("delta")
        .load(inputDir.toString)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer((4, "insert", 1), (5, "insert", 1)),
        Execute { _ =>
          deltaTable.delete("id = 3") // version 2
        },
        ProcessAllAvailable(),
        CheckAnswer((4, "insert", 1), (5, "insert", 1), (3, "delete", 2))
      )
    }
  }

  test("user provided startingTimestamp") {
    withTempDir { inputDir =>
      // version 0
      Seq(1, 2, 3).toDF("id").write.delta(inputDir.toString)
      val deltaLog = DeltaLog.forTable(spark, inputDir.getAbsolutePath)
      modifyDeltaTimestamp(deltaLog, 0, 1000)

      // version 1
      Seq(-1).toDF("id").write.mode("append").delta(inputDir.toString)
      modifyDeltaTimestamp(deltaLog, 1, 2000)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      val startTs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date(2000))
      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingTimestamp", startTs)
        .format("delta")
        .load(inputDir.toString)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer((-1, "insert", 1)),
        Execute { _ =>
          deltaTable.update(expr("id == -1"), Map("id" -> lit("4")))
        },
        ProcessAllAvailable(),
        CheckAnswer((-1, "insert", 1), (-1, "update_preimage", 2), (4, "update_postimage", 2))
      )
    }
  }

  testQuietly("starting[Version/Timestamp] > latest version") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // version 0
      Seq(1, 2, 3, 4, 5, 6).toDF("id").write.delta(inputDir.toString)
      val deltaLog = DeltaLog.forTable(spark, inputDir.getAbsolutePath)
      modifyDeltaTimestamp(deltaLog, 0, 1000)

      val df1 = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", 1)
        .format("delta")
        .load(inputDir.toString)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      val startTs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new Date(3000))
      val commitTs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      .format(new Date(1000))
      val df2 = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingTimestamp", startTs)
        .format("delta")
        .load(inputDir.toString)

      val e1 = VersionNotFoundException(1, 0, 0).getMessage
      val e2 = DeltaErrors.timestampGreaterThanLatestCommit(
        new Timestamp(3000), new Timestamp(1000), commitTs).getMessage

      Seq((df1, e1), (df2, e2)).foreach { pair =>
        val df = pair._1
        val stream = df.select("id").writeStream
          .option("checkpointLocation", checkpointDir.toString)
          .outputMode("append")
          .format("delta")
          .start(outputDir.getAbsolutePath)
        val e = intercept[StreamingQueryException] {
          stream.processAllAvailable()
        }
        stream.stop()
        assert(e.cause.getMessage === pair._2)
      }
    }
  }

  test("check starting[Version/Timestamp] > latest version without error") {
    Seq("version", "timestamp").foreach { target =>
      withTempDir { inputDir =>
        withSQLConf(DeltaSQLConf.DELTA_CDF_ALLOW_OUT_OF_RANGE_TIMESTAMP.key -> "true") {
          // version 0
          Seq(1, 2, 3).toDF("id").write.delta(inputDir.toString)
          val inputPath = inputDir.getAbsolutePath
          val deltaLog = DeltaLog.forTable(spark, inputPath)
          modifyDeltaTimestamp(deltaLog, 0, 1000)

          val deltaTable = io.delta.tables.DeltaTable.forPath(inputPath)

          // Pick both the timestamp and version beyond latest commmit's version.
          val df = if (target == "timestamp") {
            // build dataframe with starting timestamp option.
            val startTs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              .format(new Date(2000))
            spark.readStream
              .option(DeltaOptions.CDC_READ_OPTION, "true")
              .option("startingTimestamp", startTs)
              .format("delta")
              .load(inputDir.toString)
              .drop(CDCReader.CDC_COMMIT_TIMESTAMP)
          } else {
            assert(target == "version")
            // build dataframe with starting version option.
            spark.readStream
              .option(DeltaOptions.CDC_READ_OPTION, "true")
              .option("startingVersion", 1)
              .format("delta")
              .load(inputDir.toString)
              .drop(CDCReader.CDC_COMMIT_TIMESTAMP)
          }

          testStream(df)(
            ProcessAllAvailable(),
            // Expect empty update from the read stream.
            CheckAnswer(),
            // Verify new updates after the start timestamp/version can be read.
            Execute { _ =>
              deltaTable.update(expr("id == 1"), Map("id" -> lit("4")))
            },
            ProcessAllAvailable(),
            CheckAnswer((1, "update_preimage", 1), (4, "update_postimage", 1))
          )
        }
      }
    }
  }

  testQuietly("startingVersion and startingTimestamp are both set") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      spark.range(10).write.format("delta").save(tableDir.getAbsolutePath)
      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", 0L)
        .option("startingTimestamp", "2020-07-15")
        .load(tablePath)
        .writeStream
        .format("console")
        .start()
      assert(intercept[StreamingQueryException] {
        q.processAllAvailable()
      }.getMessage.contains("Please either provide 'startingVersion' or 'startingTimestamp'"))
      q.stop()
    }
  }

  test("cdc streams should respect checkpoint") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // write 3 versions
      Seq(1, 2, 3).toDF("id").write.format("delta").save(inputDir.getAbsolutePath)
      Seq(4, 5, 6).toDF("id").write.format("delta")
        .mode("append")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      deltaTable.delete("id = 5")

      val checkpointDir1 = new Path(checkpointDir.getAbsolutePath, "ck1")
      val checkpointDir2 = new Path(checkpointDir.getAbsolutePath, "ck2")

      def streamChanges(
          startingVersion: Long,
          checkpointLocation: String): Unit = {
        val q = spark.readStream
          .format("delta")
          .option(DeltaOptions.CDC_READ_OPTION, "true")
          .option("startingVersion", startingVersion)
          .load(inputDir.getCanonicalPath)
          .select("id")
          .writeStream
          .format("delta")
          .option("checkpointLocation", checkpointLocation)
          .start(outputDir.getCanonicalPath)
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      streamChanges(1, checkpointDir1.toString)
      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        Seq(4, 5, 5, 6).map(_.toLong).toDF("id"))

      // Second time streaming should not write the rows again
      streamChanges(1, checkpointDir1.toString)
      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        Seq(4, 5, 5, 6).map(_.toLong).toDF("id"))

      // new checkpoint location
      streamChanges(1, checkpointDir2.toString)
      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        Seq(4, 4, 5, 5, 5, 5, 6, 6).map(_.toLong).toDF("id"))
    }
  }

  test("cdc streams with noop merge") {
    withSQLConf(
      DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true"
    ) {
      withTempDirs { (srcDir, targetDir, checkpointDir) =>
        // write source table
        Seq((1, "a"), (2, "b"))
          .toDF("key1", "val1")
          .write
          .format("delta")
          .save(srcDir.getCanonicalPath)

        // write target table
        Seq((1, "t"), (2, "u"))
          .toDF("key2", "val2")
          .write
          .format("delta")
          .save(targetDir.getCanonicalPath)

        val srcDF = spark.read.format("delta").load(srcDir.getCanonicalPath)
        val tgtTable = io.delta.tables.DeltaTable.forPath(targetDir.getCanonicalPath)

        // Perform the merge where all matching and non-matching conditions fail for
        // target rows.
        tgtTable
          .merge(srcDF,
            "key1 = key2")
          .whenMatched("key1 = 10")
          .updateExpr(Map("key2" -> "key1", "val2" -> "val1"))
          .whenNotMatched("key1 = 11")
          .insertExpr(Map("key2" -> "key1", "val2" -> "val1"))
          .execute()

        // Read the target dir with cdc read option and ensure that
        // data frame is empty.
        val q = spark.readStream
          .format("delta")
          .option(DeltaOptions.CDC_READ_OPTION, "true")
          .option("startingVersion", "1")
          .load(targetDir.getCanonicalPath)
          .writeStream
          .format("memory")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .queryName("testQuery")
          .start()
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }

        assert(spark.table("testQuery").isEmpty)
      }
    }
  }

  Seq(true, false).foreach { readChangeFeed =>
    test(s"streams updating latest offset with " +
        s"readChangeFeed=$readChangeFeed") {
      withTempDirs { (inputDir, checkpointDir, outputDir) =>
        withSQLConf(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey -> "true") {

          sql(s"CREATE TABLE delta.`$inputDir` (id BIGINT, value STRING) USING DELTA")
          // save some rows to input table.
          spark.range(10).withColumn("value", lit("a"))
            .write.format("delta").mode("overwrite")
            .option("enableChangeDataFeed", "true").save(inputDir.getAbsolutePath)

          // process the input table in a CDC manner
          val df = spark.readStream
            .option(DeltaOptions.CDC_READ_OPTION, readChangeFeed)
            .format("delta")
            .load(inputDir.getAbsolutePath)

          val query = df
            .select("id")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.getAbsolutePath)

          query.processAllAvailable()
          query.stop()
          query.awaitTermination()

          // Create a temp view and write to input table as a no-op merge
          spark.range(20, 30).withColumn("value", lit("b"))
            .createOrReplaceTempView("source_table")

          for (i <- 0 to 10) {
            sql(s"MERGE INTO delta.`${inputDir.getAbsolutePath}` AS tgt " +
              s"USING source_table src ON tgt.id = src.id " +
              s"WHEN MATCHED THEN UPDATE SET * " +
              s"WHEN NOT MATCHED AND src.id < 10 THEN INSERT *")
          }

          // Read again from input table and no new data should be generated
          val df1 = spark.readStream
            .option("readChangeFeed", readChangeFeed)
            .format("delta")
            .load(inputDir.getAbsolutePath)

          val query1 = df1
            .select("id")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.getAbsolutePath)

          query1.processAllAvailable()
          query1.stop()
          query1.awaitTermination()

          // check that the last batch was committed and that the
          // reservoirVersion for the table was updated to latest
          // in both cdf and non-cdf cases.
          assert(query1.lastProgress.batchId === 1)
          val endOffset = JsonUtils.mapper.readValue[DeltaSourceOffset](
            query1.lastProgress.sources.head.endOffset
          )
          var expectedReservoirVersion = 1
          var expectedIndex = 1
          assert(endOffset.reservoirVersion === expectedReservoirVersion)
          assert(endOffset.index === expectedIndex)
        }
      }
    }
  }

  test("cdc streams should be able to get offset when there only RemoveFiles") {
    withTempDir { inputDir =>
      // version 0
      spark.range(2).withColumn("part", 'id % 2)
          .write
          .format("delta")
          .partitionBy("part")
          .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", 0)
        .format("delta")
        .load(inputDir.toString)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer((0, 0, "insert", 0), (1, 1, "insert", 0)),
        Execute { _ =>
          deltaTable.delete("part = 0") // version 2
        },
        ProcessAllAvailable(),
        CheckAnswer((0, 0, "insert", 0), (1, 1, "insert", 0), (0, 0, "delete", 1))
      )
    }
  }

  test("cdc streams should work starting from RemoveFile") {
    withTempDir { inputDir =>
      // version 0
      spark.range(2).withColumn("part", 'id % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)

      deltaTable.delete("part = 0")

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", 1)
        .format("delta")
        .load(inputDir.toString)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer((0, 0, "delete", 1))
      )
    }
  }

  test("cdc streams should work starting from AddCDCFile") {
    withTempDir { inputDir =>
      // version 0
      spark.range(2).withColumn("col2", 'id % 2)
        .repartition(1)
        .write
        .format("delta")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)

      deltaTable.delete("col2 = 0")

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", 1)
        .format("delta")
        .load(inputDir.toString)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df) (
        ProcessAllAvailable(),
        CheckAnswer((0, 0, "delete", 1)),
        AddToReservoir(inputDir, spark.range(2, 3).withColumn("col2", 'id % 2)),
        ProcessAllAvailable(),
        CheckAnswer((0, 0, "delete", 1), (2, 0, "insert", 2))
      )
    }
  }

  testRateLimit(s"overall", "1", "1b") { (key, value) =>
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))

      // write - version 0 - 2 AddFiles - Adds 4 rows
      spark.range(0, 4, 1, 1).toDF("id")
        .withColumn("part", col("id") % 2) // 2 partitions
        .write
        .format("delta")
        .partitionBy("part")
        .save(inputDir.getAbsolutePath)

      assert(deltaLog.snapshot.version == 0)
      assert(deltaLog.snapshot.numOfFiles == 2)

      // write - version 1 - 1 AddFile - Adds 1 row
      Seq(4L).toDF("id").withColumn("part", lit(-1L))
        .write
        .format("delta")
        .mode("append")
        .partitionBy("part")
        .save(deltaLog.dataPath.toString)
      assert(deltaLog.snapshot.version == 1)
      assert(deltaLog.snapshot.numOfFiles == 3)

      // delete - version 2 - 1 RemoveFile - Removes 1 row
      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      deltaTable.delete("part = -1")
      assert(deltaLog.snapshot.version == 2)
      assert(deltaLog.snapshot.numOfFiles == 2)

      // update the table - version 3 - 2 cdc files - Updates 2 rows
      deltaTable.update(expr("id < 2"), Map("id" -> lit(0L)))

      // update the table - version 4 - 2 cdc files - Updates 2 rows
      deltaTable.update(expr("id > 1"), Map("id" -> lit(0L)))

      val rowsPerBatch = Seq(
        2, // 2 rows from 1 AddFile
        2, // 2 rows from the 2nd AddFile
        1, // 1 row from the 3rd AddFile
        1, // 1 row from the RemoveFile
        4, // 4 rows(pre_image and post_image) from the 2 AddCDCFile
        4 // 4 rows(pre_image and post_image) from the 2 AddCDCFile
      )
      val q = spark.readStream
        .format("delta")
        .option(key, value)
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "0")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(q) (
        ProcessAllAvailable(),
        CheckProgress(rowsPerBatch),
        CheckAnswer(
          (0, 0, "insert", 0),
          (1, 1, "insert", 0),
          (2, 0, "insert", 0),
          (3, 1, "insert", 0),
          (4, -1, "insert", 1),
          (4, -1, "delete", 2),
          (0, 0, "update_preimage", 3),
          (0, 0, "update_postimage", 3),
          (1, 1, "update_preimage", 3),
          (0, 1, "update_postimage", 3),
          (2, 0, "update_preimage", 4),
          (0, 0, "update_postimage", 4),
          (3, 1, "update_preimage", 4),
          (0, 1, "update_postimage", 4)
        )
      )
    }
  }

  testRateLimit(s"starting from initial snapshot", "1", "1b") { (key, value) =>
    withTempDir { inputDir =>
      // 3 commits - 3 AddFiles each
      (0 until 3).foreach { i =>
        spark.range(i, i + 1, 1, 1)
          .write
          .mode("append")
          .format("delta")
          .save(inputDir.getAbsolutePath)
      }
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      assert(deltaLog.snapshot.numOfFiles === 3)

      // 1 commit - 2 AddFiles
      spark.range(3, 5, 1, 2)
        .write
        .mode("append")
        .format("delta")
        .save(inputDir.getAbsolutePath)

      assert(deltaLog.snapshot.numOfFiles === 5)

      val q = spark.readStream
        .format("delta")
        .option(key, value)
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      // 5 batches for the 5 commits split across commits and index number.
      val rowsPerBatch = Seq(1, 1, 1, 1, 1)

      testStream(q)(
        ProcessAllAvailable(),
        CheckProgress(rowsPerBatch),
        CheckAnswer(
          (0, "insert", 3),
          (1, "insert", 3),
          (2, "insert", 3),
          (3, "insert", 3),
          (4, "insert", 3)
        )
      )
    }
  }

  testRateLimit(s"should not deadlock", "1", "1b") { (key, value) =>
    withTempDir { inputDir =>
      // version 0 - 2 AddFiles
      spark.range(2)
        .withColumn("part", 'id % 2)
        .withColumn("col3", lit(0))
        .repartition(1)
        .write
        .format("delta")
        .partitionBy("part")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      // version 1 - 2 AddCDCFiles
      deltaTable.update(expr("col3 < 2"), Map("col3" -> lit("0")))

      // version 2 - 2 AddCDCFiles
      deltaTable.update(expr("col3 < 2"), Map("col3" -> lit("1")))

      val df = spark.readStream
        .format("delta")
        .option(key, value)
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "1")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df)(
        ProcessAllAvailable(),
        CheckProgress(Seq(4, 4)),// 4 rows(2 pre- and 2 post-images) for each version
        CheckAnswer(
          (0, 0, 0, "update_preimage", 1),
          (0, 0, 0, "update_postimage", 1),
          (0, 0, 0, "update_preimage", 2),
          (0, 0, 1, "update_postimage", 2),
          (1, 1, 0, "update_preimage", 1),
          (1, 1, 0, "update_postimage", 1),
          (1, 1, 0, "update_preimage", 2),
          (1, 1, 1, "update_postimage", 2)
        )
      )
    }
  }

  test("maxFilesPerTrigger - 2 successive AddCDCFile commits") {
    withTempDir { inputDir =>
      // version 0 - 2 AddFiles
      spark.range(2)
        .withColumn("part", 'id % 2)
        .withColumn("col3", lit(0))
        .repartition(1)
        .write
        .format("delta")
        .partitionBy("part")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      // version 1 - 2 AddCDCFiles
      deltaTable.update(expr("col3 < 2"), Map("col3" -> lit("0")))

      // version 2 - 2 AddCDCFiles
      deltaTable.update(expr("col3 < 2"), Map("col3" -> lit("1")))

      val df = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "3")
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "0")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      // test whether the AddCDCFile commits do not get split up.
      val rowsPerBatch = Seq(
        2, // 2 rows from the 2 AddFile
        4, // 4 rows(pre and post image) from the 2 AddCDCFiles
        4 // 4 rows(pre and post image) from 2 AddCDCFiles
      )

      testStream(df)(
        ProcessAllAvailable(),
        CheckProgress(rowsPerBatch),
        CheckAnswer(
          (0, 0, 0, "insert", 0),
          (1, 1, 0, "insert", 0),
          (0, 0, 0, "update_preimage", 1),
          (0, 0, 0, "update_postimage", 1),
          (1, 1, 0, "update_preimage", 1),
          (1, 1, 0, "update_postimage", 1),
          (0, 0, 0, "update_preimage", 2),
          (0, 0, 1, "update_postimage", 2),
          (1, 1, 0, "update_preimage", 2),
          (1, 1, 1, "update_postimage", 2)
        )
      )
    }
  }

  test("maxFilesPerTrigger with Trigger.AvailableNow respects read limits") {
    withTempDir { inputDir =>
      // version 0 - 2 AddFiles
      spark.range(2)
        .withColumn("part", 'id % 2)
        .withColumn("col3", lit(0))
        .repartition(1)
        .write
        .format("delta")
        .partitionBy("part")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      // version 1 - 2 AddCDCFiles
      deltaTable.update(expr("col3 < 2"), Map("col3" -> lit("0")))

      // version 2 - 2 AddCDCFiles
      deltaTable.update(expr("col3 < 2"), Map("col3" -> lit("1")))

      val df = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "3")
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "0")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      // test whether the AddCDCFile commits do not get split up.
      val rowsPerBatch = Seq(
        2, // 2 rows from the 2 AddFile
        4, // 4 rows(pre and post image) from the 2 AddCDCFiles
        4 // 4 rows(pre and post image) from 2 AddCDCFiles
      )

      testStream(df)(
        StartStream(Trigger.AvailableNow),
        Execute { query =>
          assert(query.awaitTermination(10000))
        },
        CheckProgress(rowsPerBatch),
        CheckAnswer(
          (0, 0, 0, "insert", 0),
          (1, 1, 0, "insert", 0),
          (0, 0, 0, "update_preimage", 1),
          (0, 0, 0, "update_postimage", 1),
          (1, 1, 0, "update_preimage", 1),
          (1, 1, 0, "update_postimage", 1),
          (0, 0, 0, "update_preimage", 2),
          (0, 0, 1, "update_postimage", 2),
          (1, 1, 0, "update_preimage", 2),
          (1, 1, 1, "update_postimage", 2)
        )
      )
    }
  }

  test("excludeRegex works with cdc") {
    withTempDir { inputDir =>
      spark.range(2)
        .withColumn("part", 'id % 2)
        .repartition(1)
        .write
        .format("delta")
        .partitionBy("part")
        .save(inputDir.getAbsolutePath)

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "0")
        .option(DeltaOptions.EXCLUDE_REGEX_OPTION, "part=0")
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((1, 1, "insert", 0)) // first file should get excluded
      )
    }
  }

  test("excludeRegex on cdcPath should not return Add/RemoveFiles") {
    withTempDir { inputDir =>
      // version 0 - 1 AddFile
      Seq(0).toDF("id")
        .withColumn("col2", lit("0"))
        .repartition(1)
        .write
        .format("delta")
        .save(inputDir.getAbsolutePath)

      val deltaTable = io.delta.tables.DeltaTable.forPath(inputDir.getAbsolutePath)
      // version 1 - 1 ChangeFile
      deltaTable.update(expr("col2 < 2"), Map("col2" -> lit("1")))

      val deltaLog = DeltaLog.forTable(spark, inputDir.getAbsolutePath)
      val excludePath = deltaLog.getChanges(1).next()._2
        .filter(_.isInstanceOf[AddCDCFile])
        .head
        .asInstanceOf[AddCDCFile]
        .path

      val df = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", "0")
        .option(DeltaOptions.EXCLUDE_REGEX_OPTION, excludePath)
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((0, "0", "insert", 0)) // first file should get excluded
      )
    }
  }

  test("schema check for cdc stream") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        Seq(i).toDF.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("delta")
        .option(DeltaOptions.CDC_READ_OPTION, "true")
        .option("startingVersion", 0)
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer(
          (0, "insert", 0),
          (1, "insert", 1),
          (2, "insert", 2),
          (3, "insert", 3),
          (4, "insert", 4)
        ),
        // no schema changed exception should be thrown.
        AssertOnQuery { _ =>
          withMetadata(deltaLog, StructType.fromDDL("value int"))
          true
        },
        AssertOnQuery { _ =>
          withMetadata(deltaLog, StructType.fromDDL("id int, value string"))
          true
        },
        ExpectFailure[DeltaIllegalStateException](t =>
          assert(t.getMessage.contains("Detected schema change")))
      )
    }
  }

  test("should not attempt to read a non exist version") {
    withTempDirs { (inputDir1, inputDir2, checkpointDir) =>
      spark.range(1, 2).write.format("delta").save(inputDir1.getCanonicalPath)
      spark.range(1, 2).write.format("delta").save(inputDir2.getCanonicalPath)

      def startQuery(): StreamingQuery = {
        val df1 = spark.readStream
          .format("delta")
          .option("readChangeFeed", "true")
          .load(inputDir1.getCanonicalPath)
        val df2 = spark.readStream
          .format("delta")
          .option("readChangeFeed", "true")
          .load(inputDir2.getCanonicalPath)
        df1.union(df2).writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start()
      }

      var q = startQuery()
      try {
        q.processAllAvailable()
        // current offsets:
        // source1: DeltaSourceOffset(reservoirVersion=1,index=0,isStartingVersion=true)
        // source2: DeltaSourceOffset(reservoirVersion=1,index=0,isStartingVersion=true)

        spark.range(1, 2).write.format("delta").mode("append").save(inputDir1.getCanonicalPath)
        spark.range(1, 2).write.format("delta").mode("append").save(inputDir2.getCanonicalPath)
        q.processAllAvailable()
        // current offsets:
        // source1: DeltaSourceOffset(reservoirVersion=2,index=-1,isStartingVersion=false)
        // source2: DeltaSourceOffset(reservoirVersion=2,index=-1,isStartingVersion=false)
        // Note: version 2 doesn't exist in source1

        spark.range(1, 2).write.format("delta").mode("append").save(inputDir2.getCanonicalPath)
        q.processAllAvailable()
        // current offsets:
        // source1: DeltaSourceOffset(reservoirVersion=2,index=-1,isStartingVersion=false)
        // source2: DeltaSourceOffset(reservoirVersion=3,index=-1,isStartingVersion=false)
        // Note: version 2 doesn't exist in source1

        q.stop()
        // Restart the query. It will call `getBatch` on the previous two offsets of `source1` which
        // are both DeltaSourceOffset(reservoirVersion=2,index=-1,isStartingVersion=false)
        // As version 2 doesn't exist, we should not try to load version 2 in this case.
        q = startQuery()
        q.processAllAvailable()
      } finally {
        q.stop()
      }
    }
  }

  // LC-1281: Ensure that when we would split batches into one file at a time, we still produce
  // correct CDF even in cases where the CDF may need to compare multiple file actions from the
  // same commit to be correct, such as with persistent deletion vectors.
  test("double delete-only on the same file") {
    withTempDir { tableDir =>
      val tablePath = tableDir.toString
      spark.range(start = 0L, end = 10L, step = 1L, numPartitions = 1).toDF("id")
        .write.format("delta").save(tablePath)

      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id IN (1, 3, 6)")
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id IN (2, 4, 7)")

      val stream = spark.readStream
        .format("delta")
        .option(DeltaOptions.CDC_READ_OPTION, true)
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, 1)
        .option(DeltaOptions.STARTING_VERSION_OPTION, 1)
        .load(tablePath)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)

      testStream(stream)(
        ProcessAllAvailable(),
        CheckAnswer(
          (1L, "delete", 1L),
          (3L, "delete", 1L),
          (6L, "delete", 1L),
          (2L, "delete", 2L),
          (4L, "delete", 2L),
          (7L, "delete", 2L)
        )
      )
    }
  }
}

class DeltaCDCStreamDeletionVectorSuite extends DeltaCDCStreamSuite
  with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsForDeletes(spark)
  }
}

class DeltaCDCStreamSuite extends DeltaCDCStreamSuiteBase
abstract class DeltaCDCStreamColumnMappingSuiteBase extends DeltaCDCStreamSuite
  with ColumnMappingStreamingBlockedWorkflowSuiteBase with DeltaColumnMappingSelectedTestMixin {

  override protected def isCdcTest: Boolean = true


  override def runOnlyTests: Seq[String] = Seq(
    "no startingVersion should result fetch the entire snapshot",
    "user provided startingVersion",
    "maxFilesPerTrigger - 2 successive AddCDCFile commits",

    // streaming blocking semantics test
    "deltaLog snapshot should not be updated outside of the stream",
    "column mapping + streaming - allowed workflows - column addition",
    "column mapping + streaming - allowed workflows - upgrade to name mode",
    "column mapping + streaming: blocking workflow - drop column",
    "column mapping + streaming: blocking workflow - rename column"
  )

}

class DeltaCDCStreamIdColumnMappingSuite extends DeltaCDCStreamColumnMappingSuiteBase
  with DeltaColumnMappingEnableIdMode {
}

class DeltaCDCStreamNameColumnMappingSuite extends DeltaCDCStreamColumnMappingSuiteBase
  with DeltaColumnMappingEnableNameMode {
}
