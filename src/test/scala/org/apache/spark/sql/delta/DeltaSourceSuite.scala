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

import java.io.{File, FileNotFoundException}
import java.net.URI
import java.util.UUID



import org.apache.spark.sql.delta.actions.{AddFile, Format}
import org.apache.spark.sql.delta.sources.{DeltaSourceOffset, DeltaSQLConf}
import org.apache.spark.sql.delta.util.FileNames
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}

import org.apache.spark.sql.{AnalysisException, DataFrame, ForeachWriter, Row}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.{ManualClock, Utils}

class DeltaSourceSuite extends StreamTest with DeltaOSSTestUtils {

  import testImplicits._

  protected override def sparkConf = {
    super.sparkConf.set(
      DatabricksSQLConf.SPARK_SERVICE_CHECK_SERIALIZATION_PASSTHROUGH_UDFS.key, "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    switchToPrivilegedAclUser()
  }

  override def afterAll(): Unit = {
    switchToSuper()
    super.afterAll()
  }

  object AddToReservoir {
    def apply(path: File, data: DataFrame): AssertOnQuery =
      AssertOnQuery { _ =>
        data.write.format("tahoe").mode("append").save(path.getAbsolutePath)
        true
      }
  }

  private def withTempDirs(f: (File, File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        withTempDir { file3 =>
          f(file1, file2, file3)
        }
      }
    }
  }

  test("no schema should throw an exception") {
    withTempDir { inputDir =>
      new File(inputDir, "_delta_log").mkdir()
      val e = intercept[AnalysisException] {
        spark.readStream
          .format("tahoe")
          .load(inputDir.getCanonicalPath)
      }
      for (msg <- Seq("Table schema is not set", "CREATE TABLE")) {
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("disallow user specified schema") {
    withTempDir { inputDir =>
      new File(inputDir, "_delta_log").mkdir()
      val e = intercept[AnalysisException] {
        spark.readStream
          .schema(StructType.fromDDL("a INT, b STRING"))
          .format("tahoe")
          .load(inputDir.getCanonicalPath)
      }
      for (msg <- Seq("Delta does not support specifying the schema at read time")) {
        assert(e.getMessage.contains(msg))
      }
    }
  }

  private def withMetadata(
      deltaLog: DeltaLog,
      schema: StructType,
      format: String = "parquet"): Unit = {
    val txn = deltaLog.startTransaction()
    txn.commit(txn.metadata.copy(
      schemaString = schema.json,
      format = Format(format)
    ) :: Nil, DeltaOperations.ManualUpdate)
  }

  test("basic") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        ProcessAllAvailable(),
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AddToReservoir(inputDir, Seq("drop4", "keep5", "keep6").toDF),
        StartStream(),
        ProcessAllAvailable(),
        CheckAnswer("keep1", "keep2", "keep5", "keep6"),
        AddToReservoir(inputDir, Seq("keep7", "drop8", "keep9").toDF),
        ProcessAllAvailable(),
        CheckAnswer("keep1", "keep2", "keep5", "keep6", "keep7", "keep9")
      )
    }
  }

  test("allow to delete files before staring a streaming query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }
      sql(s"DELETE FROM tahoe.`$inputDir`")
      (5 until 10).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }
      deltaLog.checkpoint()
      assert(deltaLog.lastCheckpoint.nonEmpty, "this test requires a checkpoint")

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((5 until 10).map(_.toString): _*)
      )
    }
  }

  test("allow to delete files before staring a streaming query without checkpoint") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }
      sql(s"DELETE FROM tahoe.`$inputDir`")
      (5 until 7).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }
      assert(deltaLog.lastCheckpoint.isEmpty, "this test requires no checkpoint")

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q =>
          q.processAllAvailable()
          true
        },
        CheckAnswer((5 until 7).map(_.toString): _*)
      )
    }
  }

  test("allow to change schema before staring a streaming query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF("id")
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      withMetadata(deltaLog, StructType.fromDDL("id STRING, value STRING"))

      (5 until 10).foreach { i =>
        val v = Seq(i.toString -> i.toString).toDF("id", "value")
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)

      val expected = (
          (0 until 5).map(_.toString -> null) ++ (5 until 10).map(_.toString).map(x => x -> x)
        ).toDF("id", "value").collect()
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer(expected: _*)
      )
    }
  }

  testQuietly("disallow to change schema after staring a streaming query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer((0 until 5).map(_.toString): _*),
        AssertOnQuery { q =>
          withMetadata(deltaLog, StructType.fromDDL("id LONG, value STRING"))
          true
        },
        ExpectFailure[IllegalStateException](t =>
          assert(t.getMessage.contains("Detected schema change")))
      )
    }
  }

  test("maxFilesPerTrigger") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: metadata checkpoint") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 20).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 20)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 20).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: data checkpoint before starting query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }
      sql(s"OPTIMIZE '$inputDir'")

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        // The query starts after data checkpoint. It reads the compacted file.
        assert(progress.length === 1)
        progress.foreach { p =>
          assert(p.numInputRows === 5)
        }
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: data checkpoint after starting query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        sql(s"OPTIMIZE '$inputDir'")
        q.processAllAvailable()

        // The query starts before data checkpoint, so it just ignores it.
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: metadata checkpoint + data checkpoint") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }
      sql(s"OPTIMIZE '$inputDir'")
      (5 until 20).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        // We don't check the number of batches because it depends on how data checkpoint is
        // implemented.
        assert(q.recentProgress.map(_.numInputRows).sum === 20)
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 20).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: change and restart") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 10).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("tahoe")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 10)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(
          spark.read.format("tahoe").load(outputDir.getAbsolutePath),
          (0 until 10).map(_.toString).toDF())
      } finally {
        q.stop()
      }

      (10 until 20).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q2 = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("tahoe")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q2.processAllAvailable()
        val progress = q2.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 2)
        }

        checkAnswer(
          spark.read.format("tahoe").load(outputDir.getAbsolutePath),
          (0 until 20).map(_.toString).toDF())
      } finally {
        q2.stop()
      }
    }
  }

  testQuietly("maxFilesPerTrigger: invalid parameter") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      Seq(0, -1, "string").foreach { invalidMaxFilesPerTrigger =>
        val e = intercept[StreamingQueryException] {
          spark.readStream
            .format("tahoe")
            .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, invalidMaxFilesPerTrigger.toString)
            .load(inputDir.getCanonicalPath)
            .writeStream
            .format("console")
            .start()
            .processAllAvailable()
        }
        assert(e.getCause.isInstanceOf[IllegalArgumentException])
        for (msg <- Seq("Invalid", DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "positive")) {
          assert(e.getCause.getMessage.contains(msg))
        }
      }
    }
  }

  test("maxFilesPerTrigger: ignored when using Trigger.Once") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .trigger(Trigger.Once)
        .queryName("triggerOnceTest")
        .start()
      try {
        assert(q.awaitTermination(streamingTimeout.toMillis))
        assert(q.recentProgress.count(_.numInputRows != 0) == 1) // only one trigger was run
        checkAnswer(sql("SELECT * from triggerOnceTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("unknown sourceVersion value") {
    val json =
      s"""
         |{
         |  "sourceVersion": ${Long.MaxValue},
         |  "reservoirVersion": 1,
         |  "index": 1,
         |  "isStartingVersion": true
         |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    assert(e.getMessage.contains("Please upgrade your Spark"))
  }

  test("invalid sourceVersion value") {
    val json =
      """
        |{
        |  "sourceVersion": "foo",
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("foo", "invalid")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("missing sourceVersion") {
    val json =
      """
        |{
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("Cannot find", "sourceVersion")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("unmatched reservoir id") {
    val json =
      s"""
        |{
        |  "reservoirId": "${UUID.randomUUID().toString}",
        |  "sourceVersion": 1,
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("delete", "checkpoint", "restart")) {
      assert(e.getMessage.contains(msg))
    }
  }

  testQuietly("recreate the reservoir should fail the query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        ProcessAllAvailable(),
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AssertOnQuery { q =>
          Utils.deleteRecursively(inputDir)
          val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
          withMetadata(deltaLog, StructType.fromDDL("value STRING"))
          true
        },
        StartStream(),
        ExpectFailure[IllegalStateException] { e =>
          for (msg <- Seq("delete", "checkpoint", "restart")) {
            assert(e.getMessage.contains(msg))
          }
        }
      )
    }
  }

  test("parse DBR 3.1 ReservoirSourceOffset json") {
    val reservoirId = UUID.randomUUID().toString
    val json =
      s"""
         |{
         |  "reservoirId": "$reservoirId",
         |  "sourceVersion": 1,
         |  "reservoirVersion": 1,
         |  "index": 1,
         |  "isStartingVersion": true
         |}
      """.stripMargin
    val offset = DeltaSourceOffset(reservoirId, SerializedOffset(json))
    assert(offset.reservoirId === reservoirId)
    assert(offset.sourceVersion === 1L)
    assert(offset.reservoirVersion === 1L)
    assert(offset.index === 1L)
    assert(offset.isStartingVersion === true)
  }

  test("excludeRegex works and doesn't mess up offsets across restarts") {
    withTempDir { inputDir =>
      val chk = new File(inputDir, "_checkpoint").toString

      def excludeReTest(s: Option[String], expected: String*): Unit = {
        val dfr = spark.readStream
          .format("tahoe")
        s.foreach(regex => dfr.option(DeltaOptions.EXCLUDE_REGEX_OPTION, regex))
        val df = dfr.load(inputDir.getCanonicalPath).groupBy('value).count
        testStream(df, OutputMode.Complete())(
          StartStream(checkpointLocation = chk),
          AssertOnQuery { sq => sq.processAllAvailable(); true },
          CheckLastBatch(expected.map((_, 1)): _*),
          StopStream
        )
      }

      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, new StructType().add("value", StringType), "text")

      def writeFile(name: String, content: String): AddFile = {
        FileUtils.write(new File(inputDir, name), content)
        AddFile(name, Map.empty, content.length, System.currentTimeMillis(), dataChange = true)
      }

      def commitFiles(files: AddFile*): Unit = {
        deltaLog.startTransaction().commit(files, DeltaOperations.ManualUpdate)
      }

      commitFiles(
        writeFile("batch1-file1", "abc"),
        writeFile("batch1-ignore-file2", "def")
      )
      excludeReTest(None, "abc", "def")

      commitFiles(
        writeFile("batch2-file1", "ghi"),
        writeFile("batch2-ignore-file2", "jkl")
      )
      excludeReTest(Some("ignore"), "abc", "def", "ghi") // ignores jkl

      commitFiles(
        writeFile("batch3-file1", "mno"),
        writeFile("batch3-ignore-file2", "pqr")
      )
      excludeReTest(None, "abc", "def", "ghi", "mno", "pqr")
    }
  }

  testQuietly("excludeRegex throws good error on bad regex pattern") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val e = intercept[StreamingQueryException] {
        spark.readStream
          .format("tahoe")
          .option(DeltaOptions.EXCLUDE_REGEX_OPTION, "[abc")
          .load(inputDir.getCanonicalPath)
          .writeStream
          .format("console")
          .start()
          .awaitTermination()
      }.cause
      assert(e.isInstanceOf[IllegalArgumentException])
      assert(e.getMessage.contains(DeltaOptions.EXCLUDE_REGEX_OPTION))
    }
  }

  test("filterPushdown - partition filters") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir)

      (1 to 5).foreach { i =>
        spark.range(10).withColumn("part", lit(1)).write
          .format("tahoe").partitionByHack("part").mode("append").save(deltaLog.dataPath.toString)
      }
      // Create a snapshot
      deltaLog.checkpoint()

      val numFiles = deltaLog.snapshot.allFiles.count()
      // Choose a number that's less than existing numFiles so that we do actually see filtering
      // based on partition information
      val maxFilesPerTrigger = numFiles - 1

      // Write to a separate partition
      spark.range(2).withColumn("part", lit(2)).write
        .format("tahoe").partitionByHack("part").mode("append").save(deltaLog.dataPath.toString)

      val df = spark.readStream
        .format("tahoe")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, maxFilesPerTrigger)
        .load(inputDir.getCanonicalPath)
        .where('part >= 2)

      testStream(df)(
        StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock),
        AdvanceManualClock(10 * 1000L),
        CheckLastBatch((0, 2), (1, 2))
      )
    }
  }

  test("filterPushdown - partition and data filters") {
    import MonotonicallyIncreasingTimestampFS._

    withSQLConf(
      s"fs.${scheme}.impl" -> classOf[MonotonicallyIncreasingTimestampFS].getName) {
      withTempDir { inputDir =>
        val deltaLog = DeltaLog.forTable(
          spark,
          new Path(new URI(s"$scheme://${inputDir.toURI.getRawPath}")))

        val fs = deltaLog.dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        assert(fs.getClass.getName === classOf[MonotonicallyIncreasingTimestampFS].getName)

        (1 to 5).foreach { i =>
          spark.range(10).withColumn("part", lit(1)).write
            .format("tahoe").partitionByHack("part").mode("append").save(deltaLog.dataPath.toString)
        }

        // Write to a separate partition
        spark.range(20, 22).withColumn("part", lit(2)).coalesce(1).write
          .format("tahoe").partitionByHack("part").mode("append").save(deltaLog.dataPath.toString)

        spark.range(22, 25).withColumn("part", lit(2)).coalesce(1).write
          .format("tahoe").partitionByHack("part").mode("append").save(deltaLog.dataPath.toString)

        // Create a snapshot
        deltaLog.checkpoint()

        val df = spark.readStream
          .format("tahoe")
          .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, 1)
          .load(inputDir.getCanonicalPath)
          .where('part >= 2 && 'id >= 22L)

        testStream(df)(
          StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock),
          AdvanceManualClock(10 * 1000L),
          CheckLastBatch(), // first file is read
          AdvanceManualClock(10 * 1000L),
          CheckLastBatch((22, 2), (23, 2), (24, 2))
        )
      }
    }
  }

  private def ignoreOperationsTest(
      inputDir: String,
      sourceOptions: Seq[(String, String)],
      sqlCommand: String)(expectations: StreamAction*): Unit = {
    (0 until 5).foreach { i =>
      val v = Seq(i.toString).toDF().write.format("tahoe").mode("append").save(inputDir)
    }

    val df = spark.readStream.format("tahoe").options(sourceOptions.toMap).load(inputDir)

    val base = Seq(
      AssertOnQuery { q =>
        q.processAllAvailable()
        true
      },
      CheckAnswer((0 until 5).map(_.toString): _*),
      AssertOnQuery { q =>
        sql(sqlCommand)
        true
      })

    testStream(df)((base ++ expectations): _*)
  }


  testQuietly("deleting files fails query if ignoreDeletes = false") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        Nil,
        s"DELETE FROM tahoe.`$inputDir`")(
        ExpectFailure[UnsupportedOperationException] { e =>
          for (msg <- Seq("Detected deleted data", "not supported", "ignoreDeletes", "true")) {
            assert(e.getMessage.contains(msg))
          }
        }
      )
    }
  }

  Seq("ignoreFileDeletion", "ignoreDeletes").foreach { ignoreDeletes =>
    testQuietly(
      s"allow to delete files after staring a streaming query when $ignoreDeletes is true") {
      withTempDir { inputDir =>
        ignoreOperationsTest(
          inputDir.getAbsolutePath,
          Seq(ignoreDeletes -> "true"),
          s"DELETE FROM tahoe.`$inputDir`")(
          AssertOnQuery { q =>
            Seq("5").toDF().write.format("tahoe").mode("append").save(inputDir.getAbsolutePath)
            q.processAllAvailable()
            true
          },
          CheckAnswer((0 until 6).map(_.toString): _*)
        )
      }
    }
  }

  testQuietly("updating the source table causes failure when ignoreChanges = false") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        Nil,
        s"UPDATE tahoe.`$inputDir` SET value = '10' WHERE value = '3'")(
        ExpectFailure[UnsupportedOperationException] { e =>
          for (msg <- Seq("data update", "not supported", "ignoreChanges", "true")) {
            assert(e.getMessage.contains(msg))
          }
        }
      )
    }
  }

  testQuietly("allow to update the source table when ignoreChanges = true") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        Seq(DeltaOptions.IGNORE_CHANGES_OPTION -> "true"),
        s"UPDATE tahoe.`$inputDir` SET value = '10' WHERE value = '3'")(
        AssertOnQuery { q =>
          Seq("5").toDF().write.format("tahoe").mode("append").save(inputDir.getAbsolutePath)
          q.processAllAvailable()
          true
        },
        CheckAnswer("0", "1", "2", "3", "4", "5", "10")
      )
    }
  }

  testQuietly("deleting files when ignoreChanges = true doesn't fail the query") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        Seq(DeltaOptions.IGNORE_CHANGES_OPTION -> "true"),
        s"DELETE FROM tahoe.`$inputDir`")(
        AssertOnQuery { q =>
          Seq("5").toDF().write.format("tahoe").mode("append").save(inputDir.getAbsolutePath)
          q.processAllAvailable()
          true
        },
        CheckAnswer((0 until 6).map(_.toString): _*)
      )
    }
  }

  testQuietly("updating source table when ignoreDeletes = true fails the query") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        Seq("ignoreDeletes" -> "true"),
        s"UPDATE tahoe.`$inputDir` SET value = '10' WHERE value = '3'")(
        ExpectFailure[UnsupportedOperationException] { e =>
          for (msg <- Seq("data update", "not supported", "ignoreChanges", "true")) {
            assert(e.getMessage.contains(msg))
          }
        }
      )
    }
  }

  testQuietly("transactions must be processed as a whole in order to figure out ignore errors") {
    withTempDir { inputDir =>
      ignoreOperationsTest(
        inputDir.getAbsolutePath,
        Seq(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"),
        s"DELETE FROM tahoe.`$inputDir` where value > '5'")( // does nothing
        AssertOnQuery { q =>
          Seq("5", "6").toDF().write.format("tahoe").mode("append").save(inputDir.getAbsolutePath)
          q.processAllAvailable()
          true
        },
        AssertOnQuery { q =>
          sql(s"UPDATE tahoe.`$inputDir` set value = '4' where value > '5'")
          try {
            q.processAllAvailable()
            false
          } catch {
            case e: StreamingQueryException =>
              e.cause.isInstanceOf[UnsupportedOperationException]
          }
        }
      )
    }
  }

  test("a fast writer should not starve a Delta source") {
    val deltaPath = Utils.createTempDir().getCanonicalPath
    val checkpointPath = Utils.createTempDir().getCanonicalPath
    val writer = spark.readStream
      .format("rate")
      .load()
      .writeStream
      .format("tahoe")
      .option("checkpointLocation", checkpointPath)
      .start(deltaPath)
    try {
      eventually(timeout(streamingTimeout)) {
        assert(sql(s"select * from tahoe.`$deltaPath`").count() > 0)
      }
      val testTableName = "delta_source_test"
      withTable(testTableName) {
        val reader = spark.readStream
          .format("tahoe")
          .load(deltaPath)
          .writeStream
          .format("memory")
          .queryName(testTableName)
          .start()
        try {
          eventually(timeout(streamingTimeout)) {
            assert(spark.table(testTableName).count() > 0)
          }
        } finally {
          reader.stop()
        }
      }
    } finally {
      writer.stop()
    }
  }

  test("start from corrupt checkpoint") {
    withTempDir { inputDir =>
      val path = inputDir.getAbsolutePath
      for (i <- 1 to 5) {
        Seq(i).toDF("id").write.mode("append").format("tahoe").save(path)
      }
      val deltaLog = DeltaLog.forTable(spark, path)
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "1") {
        deltaLog.checkpoint()
      }
      Seq(6).toDF("id").write.mode("append").format("tahoe").save(path)
      val checkpoints = new File(deltaLog.logPath.toUri).listFiles()
        .filter(f => FileNames.isCheckpointFile(new Path(f.getAbsolutePath)))
      assert(checkpoints.length == 5)
      checkpoints.last.delete()

      val df = spark.readStream
        .format("tahoe")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer(1, 2, 3, 4, 5, 6),
        StopStream
      )
    }
  }

  testQuietly(
      "SC-11438: better error message when a stream query cannot construct full Delta history") {
    withTempDir { inputDir =>
      withTable("delta_test") {
        val path = inputDir.getAbsolutePath
        sql(s"CREATE TABLE delta_test (id INT) USING tahoe LOCATION '$path'")
        DeltaLog.clearCache()
        val clock = new ManualClock(System.currentTimeMillis())
        val deltaLog = DeltaLog.forTable(spark, path, clock)
        // Tune the table properties to reproduce this issue fast.
        sql(
          """
            |ALTER TABLE delta_test
            |SET TBLPROPERTIES (
            |  'delta.checkpointRetentionDuration' = '1 millisecond',
            |  'delta.logRetentionDuration' = '1 millisecond',
            |  'delta.checkpointInterval' = '1'
            |)""".stripMargin)
        Seq(1).toDF("id").write.format("tahoe").mode("append").saveAsTable("delta_test")
        val df = spark.readStream
          .format("tahoe")
          .option("maxFilesPerTrigger", "1")
          .load(path)
        val checkpointPath = Utils.createTempDir().getCanonicalPath

        val q = df.writeStream
          .option("checkpointLocation", checkpointPath)
          .foreach(new ForeachWriter[Row] {
            override def open(partitionId: Long, version: Long): Boolean = {
              // Block to avoid moving forward. We just need to materialize the offset
              Thread.sleep(10000)
              true
            }

            override def process(value: Row): Unit = {}

            override def close(errorOrNull: Throwable): Unit = {}
          }).start()
        try {
          // Make sure the offset log has been written
          eventually(timeout(streamingTimeout)) {
            assert(q.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution != null)
          }
        } finally {
          q.stop()
        }

        deltaLog.update()
        val deltaCheckpointFileUsedByStreamQuery =
          new File(FileNames.checkpointFileSingular(
            deltaLog.logPath,
            deltaLog.snapshot.version).toUri)

        // Make sure the clean up task will delete "deltaCheckpointFileUsedByStreamQuery"
        eventually(timeout(streamingTimeout)) {
          assert(
            deltaCheckpointFileUsedByStreamQuery.lastModified() + 1 < System.currentTimeMillis())
        }

        clock.advance(CalendarInterval.fromString("interval 1 day").milliseconds())
        // Write a commit file to delete "deltaCheckpointFileUsedByStreamQuery"
        Seq(1).toDF("id").write.format("tahoe")
          .mode("append").saveAsTable("delta_test")

        // Make sure "deltaCheckpointFileUsedByStreamQuery" has been deleted
        eventually(timeout(streamingTimeout)) {
          assert(!deltaCheckpointFileUsedByStreamQuery.exists)
        }

        // Restarting the query should complaint that it cannot find version 0.
        val q2 = df.writeStream.format("console")
          .option("checkpointLocation", checkpointPath).start()
        try {
          val e = intercept[StreamingQueryException] {
            q2.processAllAvailable()
          }
          assert(e.getCause.isInstanceOf[FileNotFoundException])
          assert(e.getMessage.contains("delta.checkpointRetentionDuratio"))
        } finally {
          q2.stop()
        }
      }
    }
  }

  test("SC-11561: can consume new data without update") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream.format("tahoe").load(inputDir.getCanonicalPath)

      // clear the cache so that the writer creates its own DeltaLog instead of reusing the reader's
      DeltaLog.clearCache()
      (0 until 3).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      // check that reader consumed new data without updating its DeltaLog
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer("0", "1", "2")
      )
      assert(deltaLog.snapshot.version == 0)

      (3 until 5).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("tahoe").save(deltaLog.dataPath.toString)
      }

      // check that reader consumed new data without update despite checkpoint
      val writersLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      writersLog.checkpoint()
      testStream(df)(
        ProcessAllAvailable(),
        CheckAnswer("0", "1", "2", "3", "4")
      )
      assert(deltaLog.snapshot.version == 0)
    }
  }

  test("SC-11561: can delete old files of a snapshot without update") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream.format("tahoe").load(inputDir.getCanonicalPath)

      // clear the cache so that the writer creates its own DeltaLog instead of reusing the reader's
      DeltaLog.clearCache()
      val clock = new ManualClock(System.currentTimeMillis())
      val writersLog = DeltaLog.forTable(spark, new Path(inputDir.toURI), clock)
      (0 until 3).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("tahoe").save(inputDir.getCanonicalPath)
      }

      // Create a checkpoint so that logs before checkpoint can be expired and deleted
      writersLog.checkpoint()

      testStream(df)(
        StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock),
        AdvanceManualClock(10 * 1000L),
        CheckLastBatch("0", "1", "2"),
        Assert {
          clock.advance(
            CalendarInterval.fromString(
              DeltaConfigs.LOG_RETENTION.defaultValue).milliseconds() + 100000000L)

          // Delete all logs before checkpoint
          writersLog.cleanUpExpiredLogs()

          // Check that the first few log files have been deleted
          val logPath = new File(inputDir, "_delta_log")
          val logVersions = logPath.listFiles().map(_.getName)
              .filter(_.endsWith(".json"))
              .map(_.stripSuffix(".json").toInt)

          !logVersions.contains(0) && !logVersions.contains(1)
        },
        Assert {
          (3 until 5).foreach { i =>
            Seq(i.toString).toDF("value")
              .write.mode("append").format("tahoe").save(inputDir.getCanonicalPath)
          }
          true
        },
        // can process new data without update, despite that previous log files have been deleted
        AdvanceManualClock(10 * 1000L),
        AdvanceManualClock(10 * 1000L),
        CheckLastBatch("3", "4")
      )
      assert(deltaLog.snapshot.version == 0)
    }
  }

  testQuietly("SC-13258: Multiple schema changes shouldn't break a stream") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      // Since the schema of the source table is going to change, we need to re-plan the
      // DataFrame
      def getDf: DataFrame = {
        spark.readStream
          .format("tahoe")
          .load(inputDir.getCanonicalPath)
          .filter($"value" contains "keep")
      }

      val chkpoint = new File(inputDir, "_checkpoint").getCanonicalPath
      testStream(getDf)(
        StartStream(checkpointLocation = chkpoint),
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        ProcessAllAvailable(),
        CheckAnswer("keep1", "keep2"),
        // Shouldn't fail stream
        AssertOnQuery { _ =>
          sql(s"ALTER TABLE tahoe.`${inputDir.getCanonicalPath}` CHANGE COLUMN value value " +
            "STRING COMMENT 'this is the value of the row'")
          true
        },
        AddToReservoir(inputDir, Seq("keep3", "drop4").toDF),
        ProcessAllAvailable(),
        CheckAnswer("keep1", "keep2", "keep3"),
        AssertOnQuery { _ =>
          sql(s"ALTER TABLE tahoe.`${inputDir.getCanonicalPath}` ADD COLUMNS (id STRING)")
          sql(s"ALTER TABLE tahoe.`${inputDir.getCanonicalPath}` CHANGE COLUMN id id STRING" +
            " COMMENT 'this is the id of the row'")
          true
        },
        AddToReservoir(inputDir, Seq("keep4" -> "4").toDF("value", "id")),
        ExpectFailure[IllegalStateException]()
      )

      testStream(getDf)(
        StartStream(checkpointLocation = chkpoint),
        ProcessAllAvailable(),
        CheckAnswer[(String, String)]("keep4" -> "4")
      )
    }
  }
}

/**
 * A FileSystem implementation that returns monotonically increasing timestamps for file creation.
 * Note that we may return a different timestamp for the same file. This is okay for the tests
 * where we use this though.
 */
class MonotonicallyIncreasingTimestampFS extends RawLocalFileSystem {
  private var time: Long = System.currentTimeMillis()

  override def getScheme: String = MonotonicallyIncreasingTimestampFS.scheme

  override def getUri: URI = {
    URI.create(s"$getScheme:///")
  }

  override def getFileStatus(f: Path): FileStatus = {
    val original = super.getFileStatus(f)
    time += 1000L
    new FileStatus(original.getLen, original.isDir, 0, 0, time, f)
  }
}

object MonotonicallyIncreasingTimestampFS {
  val scheme = s"MonotonicallyIncreasingTimestampFS"
}
