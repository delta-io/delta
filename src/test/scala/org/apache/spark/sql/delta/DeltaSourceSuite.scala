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

import java.io.{File, FileInputStream, OutputStream}
import java.net.URI
import java.util.UUID

import scala.concurrent.duration._
import scala.language.implicitConversions

import org.apache.spark.sql.delta.actions.{AddFile, InvalidProtocolVersionException, Protocol}
import org.apache.spark.sql.delta.sources.{DeltaSourceOffset, DeltaSQLConf}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ManualClock, Utils}

class DeltaSourceSuite extends DeltaSourceSuiteBase with DeltaSQLCommandTest {

  import testImplicits._

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
          .format("delta")
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
          .format("delta")
          .load(inputDir.getCanonicalPath)
      }
      for (msg <- Seq("Delta does not support specifying the schema at read time")) {
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("basic") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AddToReservoir(inputDir, Seq("drop4", "keep5", "keep6").toDF),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2", "keep5", "keep6"),
        AddToReservoir(inputDir, Seq("keep7", "drop8", "keep9").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2", "keep5", "keep6", "keep7", "keep9")
      )
    }
  }

  test("allow to change schema before staring a streaming query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF("id")
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      withMetadata(deltaLog, StructType.fromDDL("id STRING, value STRING"))

      (5 until 10).foreach { i =>
        val v = Seq(i.toString -> i.toString).toDF("id", "value")
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      val expected = (
          (0 until 5).map(_.toString -> null) ++ (5 until 10).map(_.toString).map(x => x -> x)
        ).toDF("id", "value").collect()
      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer(expected: _*)
      )
    }
  }

  testQuietly("disallow to change schema after starting a streaming query") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer((0 until 5).map(_.toString): _*),
        AssertOnQuery { _ =>
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
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
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
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
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

  test("maxFilesPerTrigger: change and restart") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 10).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
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
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 10).map(_.toString).toDF())
      } finally {
        q.stop()
      }

      (10 until 20).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q2 = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
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
          spark.read.format("delta").load(outputDir.getAbsolutePath),
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
            .format("delta")
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
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      def runTriggerOnceAndVerifyResult(expected: Seq[Int]): Unit = {
        val q = spark.readStream
          .format("delta")
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
          checkAnswer(sql("SELECT * from triggerOnceTest"), expected.map(_.toString).toDF)
        } finally {
          q.stop()
        }
      }

      runTriggerOnceAndVerifyResult(0 until 5)

      // Write more data and start a second batch.
      (5 until 10).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }
      // Verify we can see all of latest data.
      runTriggerOnceAndVerifyResult(0 until 10)
    }
  }

  test("maxBytesPerTrigger: process at least one file") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxBytesPerTrigger: metadata checkpoint") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 20).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 20)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 20).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxBytesPerTrigger: change and restart") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 10).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
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
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 10).map(_.toString).toDF())
      } finally {
        q.stop()
      }

      (10 until 20).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q2 = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "100g")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q2.processAllAvailable()
        val progress = q2.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 1)
        progress.foreach { p =>
          assert(p.numInputRows === 10)
        }

        checkAnswer(
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 20).map(_.toString).toDF())
      } finally {
        q2.stop()
      }
    }
  }

  testQuietly("maxBytesPerTrigger: invalid parameter") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      Seq(0, -1, "string").foreach { invalidMaxBytesPerTrigger =>
        val e = intercept[StreamingQueryException] {
          spark.readStream
            .format("delta")
            .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, invalidMaxBytesPerTrigger.toString)
            .load(inputDir.getCanonicalPath)
            .writeStream
            .format("console")
            .start()
            .processAllAvailable()
        }
        assert(e.getCause.isInstanceOf[IllegalArgumentException])
        for (msg <- Seq("Invalid", DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "size")) {
          assert(e.getCause.getMessage.contains(msg))
        }
      }
    }
  }

  test("maxBytesPerTrigger: max bytes and max files together") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1") // should process a file at a time
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "100gb")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }

      val q2 = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q2.processAllAvailable()
        val progress = q2.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q2.stop()
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
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AssertOnQuery { _ =>
          Utils.deleteRecursively(inputDir)
          val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
          // All Delta tables in tests use the same tableId by default. Here we pass a new tableId
          // to simulate a new table creation in production
          withMetadata(deltaLog, StructType.fromDDL("value STRING"), tableId = Some("tableId-1234"))
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

  test("excludeRegex works and doesn't mess up offsets across restarts - parquet version") {
    withTempDir { inputDir =>
      val chk = new File(inputDir, "_checkpoint").toString

      def excludeReTest(s: Option[String], expected: String*): Unit = {
        val dfr = spark.readStream
          .format("delta")
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

      def writeFile(name: String, content: String): AddFile = {
        FileUtils.write(new File(inputDir, name), content)
        AddFile(name, Map.empty, content.length, System.currentTimeMillis(), dataChange = true)
      }

      def commitFiles(files: AddFile*): Unit = {
        deltaLog.startTransaction().commit(files, DeltaOperations.ManualUpdate)
      }

      Seq("abc", "def").toDF().write.format("delta").save(inputDir.getAbsolutePath)
      commitFiles(
        writeFile("batch1-ignore-file1", "ghi"),
        writeFile("batch1-ignore-file2", "jkl")
      )
      excludeReTest(Some("ignore"), "abc", "def")
    }
  }

  testQuietly("excludeRegex throws good error on bad regex pattern") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val e = intercept[StreamingQueryException] {
        spark.readStream
          .format("delta")
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

  test("a fast writer should not starve a Delta source") {
    val deltaPath = Utils.createTempDir().getCanonicalPath
    val checkpointPath = Utils.createTempDir().getCanonicalPath
    val writer = spark.readStream
      .format("rate")
      .load()
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .start(deltaPath)
    try {
      eventually(timeout(streamingTimeout)) {
        assert(spark.read.format("delta").load(deltaPath).count() > 0)
      }
      val testTableName = "delta_source_test"
      withTable(testTableName) {
        val reader = spark.readStream
          .format("delta")
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
        Seq(i).toDF("id").write.mode("append").format("delta").save(path)
      }
      val deltaLog = DeltaLog.forTable(spark, path)
      deltaLog.checkpoint()
      Seq(6).toDF("id").write.mode("append").format("delta").save(path)
      val checkpoints = new File(deltaLog.logPath.toUri).listFiles()
        .filter(f => FileNames.isCheckpointFile(new Path(f.getAbsolutePath)))
      checkpoints.last.delete()

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer(1, 2, 3, 4, 5, 6),
        StopStream
      )
    }
  }

  test("SC-11561: can consume new data without update") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream.format("delta").load(inputDir.getCanonicalPath)

      // clear the cache so that the writer creates its own DeltaLog instead of reusing the reader's
      DeltaLog.clearCache()
      (0 until 3).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      // check that reader consumed new data without updating its DeltaLog
      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("0", "1", "2")
      )
      assert(deltaLog.snapshot.version == 0)

      (3 until 5).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      // check that reader consumed new data without update despite checkpoint
      val writersLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      writersLog.checkpoint()
      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("0", "1", "2", "3", "4")
      )
      assert(deltaLog.snapshot.version == 0)
    }
  }

  test("SC-11561: can delete old files of a snapshot without update") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream.format("delta").load(inputDir.getCanonicalPath)

      // clear the cache so that the writer creates its own DeltaLog instead of reusing the reader's
      DeltaLog.clearCache()
      val clock = new ManualClock(System.currentTimeMillis())
      val writersLog = DeltaLog.forTable(spark, new Path(inputDir.toURI), clock)
      (0 until 3).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("delta").save(inputDir.getCanonicalPath)
      }

      // Create a checkpoint so that logs before checkpoint can be expired and deleted
      writersLog.checkpoint()

      testStream(df)(
        StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock),
        AdvanceManualClock(10 * 1000L),
        CheckLastBatch("0", "1", "2"),
        Assert {
          val defaultLogRetentionMillis = DeltaConfigs.getMilliSeconds(
            IntervalUtils.safeStringToInterval(
              UTF8String.fromString(DeltaConfigs.LOG_RETENTION.defaultValue)))
          clock.advance(defaultLogRetentionMillis + 100000000L)

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
              .write.mode("append").format("delta").save(inputDir.getCanonicalPath)
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

  test("Delta sources don't write offsets with null json") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)

      val df = spark.readStream.format("delta").load(inputDir.toString)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
      stream.processAllAvailable()
      val offsetFile = checkpointDir.toString + "/offsets/0"

      // Make sure JsonUtils doesn't serialize it as null
      val deltaSourceOffsetLine =
        scala.io.Source.fromFile(offsetFile).getLines.toSeq.last
      val deltaSourceOffset = JsonUtils.fromJson[DeltaSourceOffset](deltaSourceOffsetLine)
      assert(deltaSourceOffset.json != null, "Delta sources shouldn't write null json field")

      // Make sure OffsetSeqLog won't choke on the offset we wrote
      withTempDir { logPath =>
        new OffsetSeqLog(spark, logPath.toString) {
          val offsetSeq = this.deserialize(new FileInputStream(offsetFile))
          val out = new OutputStream() { override def write(b: Int): Unit = { } }
          this.serialize(offsetSeq, out)
        }
      }

      stream.stop()
    }
  }

  test("Delta source advances with non-data inserts") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1L, 2L, 3L).toDF("x").write.format("delta").save(inputDir.toString)

      val df = spark.readStream.format("delta").load(inputDir.toString)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
      try {
        stream.processAllAvailable()

        val deltaLog = DeltaLog.forTable(spark, inputDir.toString)
        for(i <- 1 to 3) {
          deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
          stream.processAllAvailable()
        }

        val fs = deltaLog.dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        for (version <- 0 to 3) {
          val possibleFiles = Seq(
            f"/$version%020d.checkpoint.parquet",
            f"/$version%020d.json",
            f"/$version%020d.crc"
          ).map { name => new Path(inputDir.toString + "/_delta_log" + name) }
          for (logFilePath <- possibleFiles) {
            if (fs.exists(logFilePath)) {
              // The cleanup logic has an edge case when files for higher versions don't have higher
              // timestamps, so we set the timestamp to scale with version rather than just being 0.
              fs.setTimes(logFilePath, version * 1000, 0)
            }
          }
        }
        deltaLog.cleanUpExpiredLogs()
        stream.processAllAvailable()

        val lastOffset = DeltaSourceOffset(
          deltaLog.tableId,
          SerializedOffset(stream.lastProgress.sources.head.endOffset))

        assert(lastOffset == DeltaSourceOffset(1, deltaLog.tableId, 3, -1, false))
      } finally {
        stream.stop()
      }
    }
  }

  test("Rate limited Delta source advances with non-data inserts") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1L, 2L, 3L).toDF("x").write.format("delta").save(inputDir.toString)

      val df = spark.readStream.format("delta").load(inputDir.toString)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .option("maxFilesPerTrigger", 2)
        .start(outputDir.toString)
      try {
        val deltaLog = DeltaLog.forTable(spark, inputDir.toString)
        for(i <- 1 to 3) {
          deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
        }

        val fs = deltaLog.dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        for (version <- 0 to 3) {
          val possibleFiles = Seq(
            f"/$version%020d.checkpoint.parquet",
            f"/$version%020d.json",
            f"/$version%020d.crc"
          ).map { name => new Path(inputDir.toString + "/_delta_log" + name) }
          for (logFilePath <- possibleFiles) {
            if (fs.exists(logFilePath)) {
              // The cleanup logic has an edge case when files for higher versions don't have higher
              // timestamps, so we set the timestamp to scale with version rather than just being 0.
              fs.setTimes(logFilePath, version * 1000, 0)
            }
          }
        }
        deltaLog.cleanUpExpiredLogs()
        stream.processAllAvailable()

        val lastOffset = DeltaSourceOffset(
          deltaLog.tableId,
          SerializedOffset(stream.lastProgress.sources.head.endOffset))

        assert(lastOffset == DeltaSourceOffset(1, deltaLog.tableId, 3, -1, false))
      } finally {
        stream.stop()
      }
    }
  }

  testQuietly("Delta sources should verify the protocol reader version") {
    withTempDir { tempDir =>
      spark.range(0).write.format("delta").save(tempDir.getCanonicalPath)

      val df = spark.readStream.format("delta").load(tempDir.getCanonicalPath)
      val stream = df.writeStream
        .format("console")
        .start()
      try {
        stream.processAllAvailable()

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        deltaLog.store.write(
          FileNames.deltaFile(deltaLog.logPath, deltaLog.snapshot.version + 1),
          // Write a large reader version to fail the streaming query
          Iterator(Protocol(minReaderVersion = Int.MaxValue).json))

        // The streaming query should fail because its version is too old
        val e = intercept[StreamingQueryException] {
          stream.processAllAvailable()
        }
        assert(e.getCause.isInstanceOf[InvalidProtocolVersionException])
      } finally {
        stream.stop()
      }
    }
  }

  /** Generate commits with the given timestamp in millis. */
  private def generateCommits(location: String, commits: Long*): Unit = {
    val deltaLog = DeltaLog.forTable(spark, location)
    var startVersion = deltaLog.snapshot.version + 1
    commits.foreach { ts =>
      val rangeStart = startVersion * 10
      val rangeEnd = rangeStart + 10
      spark.range(rangeStart, rangeEnd).write.format("delta").mode("append").save(location)
      val file = new File(FileNames.deltaFile(deltaLog.logPath, startVersion).toUri)
      file.setLastModified(ts)
      startVersion += 1
    }
  }

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  /** Disable log cleanup to avoid deleting logs we are testing. */
  private def disableLogCleanup(tablePath: String): Unit = {
    sql(s"alter table delta.`$tablePath` " +
      s"set tblproperties (${DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key} = false)")
  }

  testQuietly("startingVersion") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      val start = 1594795800000L
      generateCommits(tablePath, start, start + 20.minutes)

      def testStartingVersion(startingVersion: Long): Unit = {
        val q = spark.readStream
          .format("delta")
          .option("startingVersion", startingVersion)
          .load(tablePath)
          .writeStream
          .format("memory")
          .queryName("startingVersion_test")
          .start()
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      for ((startingVersion, expected) <- Seq(
        0 -> (0 until 20),
        1 -> (10 until 20))
      ) {
        withTempView("startingVersion_test") {
          testStartingVersion(startingVersion)
          checkAnswer(
            spark.table("startingVersion_test"),
            expected.map(_.toLong).toDF())
        }
      }

      assert(intercept[StreamingQueryException] {
        testStartingVersion(-1)
      }.getMessage.contains("Invalid value '-1' for option 'startingVersion'"))
      assert(intercept[StreamingQueryException] {
        testStartingVersion(2)
      }.getMessage.contains("Cannot time travel Delta table to version 2"))

      // Create a checkpoint at version 2 and delete version 0
      disableLogCleanup(tablePath)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      assert(deltaLog.update().version == 2)
      deltaLog.checkpoint()
      new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri).delete()

      // Cannot start from version 0
      assert(intercept[StreamingQueryException] {
        testStartingVersion(0)
      }.getMessage.contains("Cannot time travel Delta table to version 0"))

      // Can start from version 1 even if it's not recreatable
      withTempView("startingVersion_test") {
        testStartingVersion(1L)
        checkAnswer(
          spark.table("startingVersion_test"),
          (10 until 20).map(_.toLong).toDF())
      }
    }
  }

  testQuietly("startingVersion should be ignored when restarting from a checkpoint") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val start = 1594795800000L
      generateCommits(inputDir.getCanonicalPath, start, start + 20.minutes)

      def testStartingVersion(
          startingVersion: Long,
          checkpointLocation: String = checkpointDir.getCanonicalPath): Unit = {
        val q = spark.readStream
          .format("delta")
          .option("startingVersion", startingVersion)
          .load(inputDir.getCanonicalPath)
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

      testStartingVersion(1L)
      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        (10 until 20).map(_.toLong).toDF())

      // Add two new commits
      generateCommits(inputDir.getCanonicalPath, start + 40.minutes)
      disableLogCleanup(inputDir.getCanonicalPath)
      val deltaLog = DeltaLog.forTable(spark, inputDir.getCanonicalPath)
      assert(deltaLog.update().version == 3)
      deltaLog.checkpoint()

      // Make the streaming query move forward. When we restart here, we still need to touch
      // `DeltaSource.getStartingVersion` because the engine will call `getBatch` that was committed
      // (start is None) during the restart.
      testStartingVersion(1L)
      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        (10 until 30).map(_.toLong).toDF())

      // Add one commit and delete version 0 and version 1
      generateCommits(inputDir.getCanonicalPath, start + 60.minutes)
      (0 to 1).foreach { v =>
        new File(FileNames.deltaFile(deltaLog.logPath, v).toUri).delete()
      }

      // Although version 1 has been deleted, restarting the query should still work as we have
      // processed files in version 1. In other words, query restart should ignore "startingVersion"
      testStartingVersion(1L)
      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        ((10 until 30) ++ (40 until 50)).map(_.toLong).toDF()) // the gap caused by "alter table"

      // But if we start a new query, it should fail.
      val newCheckpointDir = Utils.createTempDir()
      try {
        assert(intercept[StreamingQueryException] {
          testStartingVersion(1L, newCheckpointDir.getCanonicalPath)
        }.getMessage.contains("[2, 4]"))
      } finally {
        Utils.deleteRecursively(newCheckpointDir)
      }
    }
  }

  testQuietly("startingTimestamp") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      val start = 1594795800000L // 2020-07-14 23:50:00 PDT
      generateCommits(tablePath, start, start + 20.minutes)

      def testStartingTimestamp(startingTimestamp: String): Unit = {
        val q = spark.readStream
          .format("delta")
          .option("startingTimestamp", startingTimestamp)
          .load(tablePath)
          .writeStream
          .format("memory")
          .queryName("startingTimestamp_test")
          .start()
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      for ((startingTimestamp, expected) <- Seq(
        "2020-07-14" -> (0 until 20),
        "2020-07-14 23:40:00" -> (0 until 20),
        "2020-07-14 23:50:00" -> (0 until 20), // the timestamp of version 0
        "2020-07-14 23:50:01" -> (10 until 20),
        "2020-07-15" -> (10 until 20),
        "2020-07-15 00:00:00" -> (10 until 20),
        "2020-07-15 00:10:00" -> (10 until 20)) // the timestamp of version 1
      ) {
        withTempView("startingTimestamp_test") {
          testStartingTimestamp(startingTimestamp)
          checkAnswer(
            spark.table("startingTimestamp_test"),
            expected.map(_.toLong).toDF())
        }
      }
      assert(intercept[StreamingQueryException] {
        testStartingTimestamp("2020-07-15 00:10:01")
      }.getMessage.contains("The provided timestamp (2020-07-15 00:10:01.0) " +
        "is after the latest version"))
      assert(intercept[StreamingQueryException] {
        testStartingTimestamp("2020-07-16")
      }.getMessage.contains("The provided timestamp (2020-07-16 00:00:00.0) " +
        "is after the latest version"))
      assert(intercept[StreamingQueryException] {
        testStartingTimestamp("i am not a timestamp")
      }.getMessage.contains("The provided timestamp ('i am not a timestamp') " +
        "cannot be converted to a valid timestamp"))

      // With non-strict parsing this produces null when casted to a timestamp and then parses
      // to 1970-01-01 (unix time 0).
      withSQLConf(DeltaSQLConf.DELTA_TIME_TRAVEL_STRICT_TIMESTAMP_PARSING.key -> "false") {
        withTempView("startingTimestamp_test") {
          testStartingTimestamp("i am not a timestamp")
          checkAnswer(
            spark.table("startingTimestamp_test"),
            (0L until 20L).toDF())
        }
      }

      // Create a checkpoint at version 2 and delete version 0
      disableLogCleanup(tablePath)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      assert(deltaLog.update().version == 2)
      deltaLog.checkpoint()
      new File(FileNames.deltaFile(deltaLog.logPath, 0).toUri).delete()

      // Can start from version 1 even if it's not recreatable
      withTempView("startingTimestamp_test") {
        testStartingTimestamp("2020-07-14")
        checkAnswer(
          spark.table("startingTimestamp_test"),
          (10 until 20).map(_.toLong).toDF())
      }
    }
  }

  testQuietly("startingVersion and startingTimestamp are both set") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      generateCommits(tablePath, 0)
      val q = spark.readStream
        .format("delta")
        .option("startingVersion", 0L)
        .option("startingTimestamp", "2020-07-15")
        .load(tablePath)
        .writeStream
        .format("console")
        .start()
      try {
        assert(intercept[StreamingQueryException] {
          q.processAllAvailable()
        }.getMessage.contains("Please either provide 'startingVersion' or 'startingTimestamp'"))
      } finally {
        q.stop()
      }
    }
  }

  test("startingVersion: user defined start works with mergeSchema") {
    withTempDir { inputDir =>
      withTempView("startingVersionTest") {
        spark.range(10)
          .write
          .format("delta")
          .mode("append")
          .save(inputDir.getCanonicalPath)

        // Change schema at version 1
        spark.range(10, 20)
          .withColumn("id2", 'id)
          .write
          .option("mergeSchema", "true")
          .format("delta")
          .mode("append")
          .save(inputDir.getCanonicalPath)

        // Change schema at version 2
        spark.range(20, 30)
          .withColumn("id2", 'id)
          .withColumn("id3", 'id)
          .write
          .option("mergeSchema", "true")
          .format("delta")
          .mode("append")
          .save(inputDir.getCanonicalPath)

        // check answer from version 1
        val q = spark.readStream
          .format("delta")
          .option("startingVersion", "1")
          .load(inputDir.getCanonicalPath)
          .writeStream
          .format("memory")
          .queryName("startingVersionTest")
          .start()
        try {
          q.processAllAvailable()
          checkAnswer(
            sql("select * from startingVersionTest"),
            ((10 until 20).map(x => (x.toLong, x.toLong, None.toString)) ++
              (20 until 30).map(x => (x.toLong, x.toLong, x.toString)))
              .toDF("id", "id2", "id3")
              .selectExpr("id", "id2", "cast(id3 as long) as id3")
          )
        } finally {
          q.stop()
        }
      }
    }
  }

  test("startingVersion latest") {
    withTempDir { dir =>
      withTempView("startingVersionTest") {
        val path = dir.getAbsolutePath
        spark.range(0, 10).write.format("delta").save(path)
        val q = spark.readStream
          .format("delta")
          .option("startingVersion", "latest")
          .load(path)
          .writeStream
          .format("memory")
          .queryName("startingVersionLatest")
          .start()
        try {
          // Starting from latest shouldn't include any data at first, even the most recent version.
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), Seq.empty)

          // After we add some batches the stream should continue as normal.
          spark.range(10, 15).write.format("delta").mode("append").save(path)
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), (10 until 15).map(Row(_)))
          spark.range(15, 20).write.format("delta").mode("append").save(path)
          spark.range(20, 25).write.format("delta").mode("append").save(path)
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), (10 until 25).map(Row(_)))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("startingVersion latest defined before started") {
    withTempDir { dir =>
      withTempView("startingVersionTest") {
        val path = dir.getAbsolutePath
        spark.range(0, 10).write.format("delta").save(path)
        // Define the stream, but don't start it, before a second write. The startingVersion
        // latest should be resolved when the query *starts*, so there'll be no data even though
        // some was added after the stream was defined.
        val streamDef = spark.readStream
          .format("delta")
          .option("startingVersion", "latest")
          .load(path)
          .writeStream
          .format("memory")
          .queryName("startingVersionLatest")
        spark.range(10, 20).write.format("delta").mode("append").save(path)
        val q = streamDef.start()

        try {
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), Seq.empty)
          spark.range(20, 25).write.format("delta").mode("append").save(path)
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), (20 until 25).map(Row(_)))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("startingVersion latest works on defined but empty table") {
    withTempDir { dir =>
      withTempView("startingVersionTest") {
        val path = dir.getAbsolutePath
        spark.range(0).write.format("delta").save(path)
        val streamDef = spark.readStream
          .format("delta")
          .option("startingVersion", "latest")
          .load(path)
          .writeStream
          .format("memory")
          .queryName("startingVersionLatest")
        val q = streamDef.start()

        try {
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), Seq.empty)
          spark.range(0, 5).write.format("delta").mode("append").save(path)
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), (0 until 5).map(Row(_)))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("startingVersion latest calls update when starting") {
    withTempDir { dir =>
      withTempView("startingVersionTest") {
        val path = dir.getAbsolutePath
        spark.range(0).write.format("delta").save(path)

        val streamDef = spark.readStream
          .format("delta")
          .option("startingVersion", "latest")
          .load(path)
          .writeStream
          .format("memory")
          .queryName("startingVersionLatest")
        val log = DeltaLog.forTable(spark, path)
        val originalSnapshot = log.snapshot

        // We write out some new data, and then do a dirty reflection hack to produce an un-updated
        // Delta log. The stream should still update when started and not produce any data.
        spark.range(10).write.format("delta").mode("append").save(path)
        // The field is actually declared in the SnapshotManagement trait, but because traits don't
        // exist in the JVM DeltaLog is where it ends up in reflection.
        val snapshotField = classOf[DeltaLog].getDeclaredField("currentSnapshot")
        snapshotField.setAccessible(true)
        snapshotField.set(log, originalSnapshot)

        val q = streamDef.start()

        try {
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionLatest"), Seq.empty)
        } finally {
          q.stop()
        }
      }
    }
  }

  test("startingVersion should work with rate time") {
    withTempDir { dir =>
      withTempView("startingVersionWithRateLimit") {
        val path = dir.getAbsolutePath
        // Create version 0 and version 1 and each version has two files
        spark.range(0, 5).repartition(2).write.mode("append").format("delta").save(path)
        spark.range(5, 10).repartition(2).write.mode("append").format("delta").save(path)

        val q = spark.readStream
          .format("delta")
          .option("startingVersion", 1)
          .option("maxFilesPerTrigger", 1)
          .load(path)
          .writeStream
          .format("memory")
          .queryName("startingVersionWithRateLimit")
          .start()
        try {
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionWithRateLimit"), (5 until 10).map(Row(_)))
          val id = DeltaLog.forTable(spark, path).snapshot.metadata.id
          val endOffsets = q.recentProgress
            .map(_.sources(0).endOffset)
            .map(offsetJson => DeltaSourceOffset(id, SerializedOffset(offsetJson)))
          assert(endOffsets.toList ==
            DeltaSourceOffset(DeltaSourceOffset.VERSION, id, 1, 0, isStartingVersion = false)
              // When we reach the end of version 1, we will jump to version 2 with index -1
              :: DeltaSourceOffset(DeltaSourceOffset.VERSION, id, 2, -1, isStartingVersion = false)
              :: Nil)
        } finally {
          q.stop()
        }
      }
    }
  }

  testQuietly("SC-46515: deltaSourceIgnoreChangesError contains removeFile, version") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)
      val df = spark.readStream.format("delta").load(inputDir.toString)
      df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
        .processAllAvailable()

      // Overwrite values, causing AddFile & RemoveFile actions to be triggered
      Seq(1, 2, 3).toDF("x")
        .write
        .mode("overwrite")
        .format("delta")
        .save(inputDir.toString)

      val e = intercept[StreamingQueryException] {
        val q = df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointDir.toString)
          // DeltaOptions.IGNORE_CHANGES_OPTION is false by default
          .start(outputDir.toString)

        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      assert(e.getCause.isInstanceOf[UnsupportedOperationException])
      assert(e.getCause.getMessage.contains(
        "This is currently not supported. If you'd like to ignore updates, set the option " +
          "'ignoreChanges' to 'true'."))
      assert(e.getCause.getMessage.contains("for example"))
      assert(e.getCause.getMessage.contains("version"))
    }
  }

  testQuietly("SC-46515: deltaSourceIgnoreDeleteError contains removeFile, version") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)
      val df = spark.readStream.format("delta").load(inputDir.toString)
      df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
        .processAllAvailable()

      // Delete the table, causing only RemoveFile (not AddFile) actions to be triggered
      io.delta.tables.DeltaTable.forPath(spark, inputDir.getAbsolutePath).delete()

      val e = intercept[StreamingQueryException] {
        val q = df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointDir.toString)
          // DeltaOptions.IGNORE_DELETES_OPTION is false by default
          .start(outputDir.toString)

        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      assert(e.getCause.isInstanceOf[UnsupportedOperationException])
      assert(e.getCause.getMessage.contains(
        "This is currently not supported. If you'd like to ignore deletes, set the option " +
          "'ignoreDeletes' to 'true'."))
      assert(e.getCause.getMessage.contains("for example"))
      assert(e.getCause.getMessage.contains("version"))
    }
  }

  test("fail on data loss - starting from missing files") {
    withTempDirs { case (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = spark.readStream.format("delta").load(srcData.getCanonicalPath)

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Delete the first file
      assert(new File(FileNames.deltaFile(srcLog.logPath, 1).toUri).delete())

      val e = intercept[StreamingQueryException] {
        val q = df.writeStream.format("delta")
          .option("checkpointLocation", chkLocation.getCanonicalPath)
          .start(targetData.getCanonicalPath)
        q.processAllAvailable()
      }
      assert(e.getCause.getMessage === DeltaErrors.failOnDataLossException(1L, 2L).getMessage)
    }
  }

  test("fail on data loss - gaps of files") {
    withTempDirs { case (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = spark.readStream.format("delta").load(srcData.getCanonicalPath)

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Delete the second file
      assert(new File(FileNames.deltaFile(srcLog.logPath, 2).toUri).delete())

      val e = intercept[StreamingQueryException] {
        val q = df.writeStream.format("delta")
          .option("checkpointLocation", chkLocation.getCanonicalPath)
          .start(targetData.getCanonicalPath)
        q.processAllAvailable()
      }
      assert(e.getCause.getMessage === DeltaErrors.failOnDataLossException(2L, 3L).getMessage)
    }
  }

  test("fail on data loss - starting from missing files with option off") {
    withTempDirs { case (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = spark.readStream.format("delta").option("failOnDataLoss", "false")
        .load(srcData.getCanonicalPath)

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Delete the first file
      assert(new File(FileNames.deltaFile(srcLog.logPath, 1).toUri).delete())

      val q2 = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q2.processAllAvailable()
      q2.stop()

      assert(spark.read.format("delta").load(targetData.getCanonicalPath).count() === 30)
    }
  }

  test("fail on data loss - gaps of files with option off") {
    withTempDirs { case (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = spark.readStream.format("delta").option("failOnDataLoss", "false")
        .load(srcData.getCanonicalPath)

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Delete the second file
      assert(new File(FileNames.deltaFile(srcLog.logPath, 2).toUri).delete())

      val q2 = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q2.processAllAvailable()
      q2.stop()

      assert(spark.read.format("delta").load(targetData.getCanonicalPath).count() === 30)
    }
  }

  test("make sure that the delta sources works fine") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>

      import io.delta.implicits._

      Seq(1, 2, 3).toDF().write.delta(inputDir.toString)

      val df = spark.readStream.delta(inputDir.toString)

      val stream = df.writeStream
        .option("checkpointLocation", checkpointDir.toString)
        .delta(outputDir.toString)

      stream.processAllAvailable()
      stream.stop()

      val writtenStreamDf = spark.read.delta(outputDir.toString)
      val expectedRows = Seq(Row(1), Row(2), Row(3))

      checkAnswer(writtenStreamDf, expectedRows)
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
    new FileStatus(original.getLen, original.isDirectory, 0, 0, time, f)
  }
}

object MonotonicallyIncreasingTimestampFS {
  val scheme = s"MonotonicallyIncreasingTimestampFS"
}
