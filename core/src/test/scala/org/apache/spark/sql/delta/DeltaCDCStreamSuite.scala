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
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamTest}
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
        CheckProgress(rowsPerBatch)
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
        CheckProgress(Seq(4, 4)) // 4 rows(pre and post image) from the 2 AddCDCFiles
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

      // test whether the AddCDCFile commits do not get split up.
      val rowsPerBatch = Seq(
        2, // 2 rows from the 2 AddFile
        4, // 4 rows(pre and post image) from the 2 AddCDCFiles
        4 // 4 rows(pre and post image) from 2 AddCDCFiles
      )

      testStream(df)(
        ProcessAllAvailable(),
        CheckProgress(rowsPerBatch)
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
        ExpectFailure[IllegalStateException](t =>
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

  test("should block CDC reads when Column Mapping enabled - streaming") {
    def assertError(f: => Any): Unit = {
      val e = intercept[StreamingQueryException] {
        f
      }.getCause.getMessage
      assert(e == "Change data feed (CDF) reads are currently not supported on tables " +
        "with column mapping enabled.")
    }

    Seq(0, 1).foreach { startingVersion =>
      withClue(s"using CDC starting version $startingVersion") {
        withTable("t1") {
          withTempDir { dir =>
            val path = dir.getCanonicalPath
            sql(
              s"""
                 |CREATE TABLE t1 (id LONG) USING DELTA
                 |TBLPROPERTIES(
                 |  '${DeltaConfigs.CHANGE_DATA_FEED.key}'='true',
                 |  '${DeltaConfigs.MIN_READER_VERSION.key}'='2',
                 |  '${DeltaConfigs.MIN_WRITER_VERSION.key}'='5'
                 |)
                 |LOCATION '$path'
                 |""".stripMargin)

            spark.range(10).write.format("delta").mode("append").save(path)
            spark.range(10, 20).write.format("delta").mode("append").save(path)

            val df = spark.readStream
              .format("delta")
              .option(DeltaOptions.CDC_READ_OPTION, "true")
              .option("startingVersion", startingVersion)
              .load(path)

            // case 1: column-mapping is enabled mid-stream
            testStream(df)(
              ProcessAllAvailable(),
              Execute { _ =>
                sql(s"""
                       |ALTER TABLE t1
                       |SET TBLPROPERTIES ('${DeltaConfigs.COLUMN_MAPPING_MODE.key}'='name')
                       |""".stripMargin)
              },
              AddToReservoir(dir, spark.range(10, 20).toDF()),
              Execute { q =>
                assertError {
                  q.processAllAvailable()
                }
              }
            )

            // case 2: perform CDC stream read on table with column mapping already enabled
            assertError {
              val stream = df.writeStream.format("console").start()
              stream.awaitTermination(2000)
              stream.stop()
            }
          }
        }
      }
    }
  }
}

class DeltaCDCStreamSuite extends DeltaCDCStreamSuiteBase
