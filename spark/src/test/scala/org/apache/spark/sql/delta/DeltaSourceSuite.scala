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

import java.io.{File, FileInputStream, OutputStream, PrintWriter, StringWriter}
import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
import scala.language.implicitConversions

import org.apache.spark.sql.delta.DataFrameUtils
import org.apache.spark.sql.delta.DeltaTestUtils.modifyCommitTimestamp
import org.apache.spark.sql.delta.Relocated
import org.apache.spark.sql.delta.actions.{AddFile, Protocol}
import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSQLConf, DeltaSource, DeltaSourceOffset}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.test.shims.StreamingTestShims.{MemoryStream, OffsetSeqLog}
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamingQueryException, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{IntegerType, LongType, NullType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ManualClock, Utils}

class DeltaSourceSuite extends DeltaSourceSuiteBase
  with DeltaColumnMappingTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  def testNullTypeColumn(shouldDropNullTypeColumns: Boolean): Unit = {
    withTempPaths(3) { case Seq(sourcePath, sinkPath, checkpointPath) =>
      withSQLConf(
        DeltaSQLConf.DELTA_STREAMING_CREATE_DATAFRAME_DROP_NULL_COLUMNS.key ->
          shouldDropNullTypeColumns.toString) {

        spark.sql("select CAST(null as VOID) as nullTypeCol, id from range(10)")
          .write
          .format("delta")
          .mode("append")
          .save(sourcePath.getCanonicalPath)

        def runStream() = {
          loadStreamWithOptions(sourcePath.getCanonicalPath, Map.empty)
            // Need to drop null type columns because it's not supported by the writer.
            .drop("nullTypeCol")
            .writeStream
            .option("checkpointLocation", checkpointPath.getCanonicalPath)
            .format("delta")
            .start(sinkPath.getCanonicalPath)
            .processAllAvailable()
        }
        if (shouldDropNullTypeColumns) {
          val e = intercept[StreamingQueryException] {
            runStream()
          }
          assert(e.getErrorClass == "STREAM_FAILED")
          // This assertion checks the schema of the source did not change while processing a batch.
          assert(e.getMessage.contains("assertion failed: Invalid batch: nullTypeCol"))
        } else {
          runStream()
        }
      }
    }
  }

  test("streaming delta source should not drop null columns") {
    testNullTypeColumn(shouldDropNullTypeColumns = false)
  }

  test("streaming delta source should drop null columns without feature flag") {
    testNullTypeColumn(shouldDropNullTypeColumns = true)
  }

  test("no schema should throw an exception") {
    withTempDir { inputDir =>
      new File(inputDir, "_delta_log").mkdir()
      val e = intercept[AnalysisException] {
        loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
      }
      for (msg <- Seq("Table schema is not set", "CREATE TABLE")) {
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("disallow user specified schema") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val e = intercept[AnalysisException] {
        spark.readStream
          .schema(StructType.fromDDL("a INT, b STRING"))
          .format("delta")
          .load(inputDir.getCanonicalPath)
      }
      for (
        msg <- Seq(
          "The schema provided for the source read doesn't match the schema of the Delta table")
      ) {
        assert(e.getMessage.contains(msg))
      }

      val e2 = intercept[Exception] {
        spark.readStream
          .schema(StructType.fromDDL("value STRING"))
          .format("delta")
          .load(inputDir.getCanonicalPath)
      }
      assert(e2.getMessage.contains("does not support user-specified schema"))
    }
  }

  test("allow user specified schema if consistent: v1 source") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      import org.apache.spark.sql.execution.datasources.DataSource
      // User-specified schema is allowed if it's consistent with the actual Delta table schema.
      // Here we use Spark internal APIs to trigger v1 source code path. That being said, we
      // are not fixing end-user behavior, but advanced Spark plugins.
      val v1DataSource = DataSource(
        spark,
        userSpecifiedSchema = Some(StructType.fromDDL("value STRING")),
        className = "delta",
        options = Map("path" -> inputDir.getCanonicalPath))
      DataFrameUtils.ofRows(spark, Relocated.StreamingRelation(v1DataSource))
    }
  }

  test("createSource should create source with empty or matching table schema provided") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath

      sql(s"CREATE TABLE delta.`$path` (id INT NOT NULL, name STRING) USING delta")

      val deltaSource = new DeltaDataSource()
      val parameters = Map("path" -> path)
      val metadataPath = tempDir.getCanonicalPath + "/_metadata"

      val tableSchema = StructType(Seq(
        StructField("id", IntegerType, false),
        StructField("name", StringType, true)
      ))
      val emptySchema = new StructType()
      val allowedCreationSchemas = Seq(emptySchema, tableSchema)
      for (schema <- allowedCreationSchemas) {
        val source = deltaSource.createSource(
          sqlContext,
          metadataPath = metadataPath,
          schema = Some(schema),
          providerName = "delta",
          parameters = parameters
        )

        val actualSchema = source.asInstanceOf[DeltaSource].schema
        assert(actualSchema.fields.map(_.name).toSet == Set("id", "name"))
      }

      val conflictingSchemas = Seq(
        StructType(Seq(
          StructField("id", IntegerType, true)
          // missing field "name"
        )),
        StructType(Seq(
          StructField("id", IntegerType, false),
          StructField("name", StringType, true),
          StructField("age", IntegerType, true) // extra field
        ))
      )

      for (schema <- conflictingSchemas) {
        val e = intercept[Exception] {
          deltaSource.createSource(
            sqlContext,
            metadataPath = metadataPath,
            schema = Some(schema),
            providerName = "delta",
            parameters = parameters
          )
        }
        assert(e.getMessage.contains(
          "[DELTA_READ_SOURCE_SCHEMA_CONFLICT] " +
            "The schema provided for the source read doesn't match the schema of the Delta table"))
      }
    }
  }

  test("basic") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
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

  test("initial snapshot ends at base index of next version") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))
      // Add data before creating the stream, so that it becomes part of the initial snapshot.
      Seq("keep1", "keep2", "drop3").toDF.write
        .format("delta").mode("append").save(inputDir.getAbsolutePath)

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
        .filter($"value" contains "keep")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        AssertOnQuery { q =>
          val offset = q.committedOffsets.iterator.next()._2.asInstanceOf[DeltaSourceOffset]
          assert(offset.reservoirVersion === 2)
          assert(offset.index === DeltaSourceOffset.BASE_INDEX)
          true
        },
        CheckAnswer("keep1", "keep2"),
        StopStream
      )
    }
  }

  test("allow to change schema before starting a streaming query") {
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

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

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

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer((0 until 5).map(_.toString): _*),
        AssertOnQuery { _ =>
          withMetadata(deltaLog, StructType.fromDDL("id int, value int"))
          true
        },
        ExpectFailure[DeltaIllegalStateException](t =>
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

      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"))
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

      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"))
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

      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"))
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

      val q2 = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "2"))
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
          loadStreamWithOptions(
            inputDir.getCanonicalPath,
            Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> invalidMaxFilesPerTrigger.toString))
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
        val q = loadStreamWithOptions(
          inputDir.getCanonicalPath,
          Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"))
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

  test("maxFilesPerTrigger: Trigger.AvailableNow respects read limits") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, inputDir)
      // Write versions 0, 1, 2, 3, 4.
      (0 to 4).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val stream = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.AvailableNow)
        .queryName("maxFilesPerTriggerTest")

      var q = stream.start(outputDir.getCanonicalPath)
      try {
        assert(q.awaitTermination(streamingTimeout.toMillis))
        assert(q.recentProgress.length === 5)
        // The first 5 versions each contain one file. They are processed as part of the initial
        // snapshot (reservoir version 4) with one index per file.
        (0 to 3).foreach { i =>
          val p = q.recentProgress(i)
          assert(p.numInputRows === 1)
          val endOffset = JsonUtils.fromJson[DeltaSourceOffset](p.sources.head.endOffset)
          assert(endOffset == DeltaSourceOffset(
            endOffset.reservoirId, reservoirVersion = 4, index = i, isInitialSnapshot = true))
        }
        // The last batch ends at the base index of the next reservoir version (5).
        val p4 = q.recentProgress(4)
        assert(p4.numInputRows === 1)
        val endOffset = JsonUtils.fromJson[DeltaSourceOffset](p4.sources.head.endOffset)
        assert(endOffset == DeltaSourceOffset(
          endOffset.reservoirId,
          reservoirVersion = 5,
          index = DeltaSourceOffset.BASE_INDEX,
          isInitialSnapshot = false))

        checkAnswer(
          sql(s"SELECT * from delta.`${outputDir.getCanonicalPath}`"),
          (0 to 4).map(_.toString).toDF)

        // Restarting the stream should immediately terminate with no progress because no more data
        q = stream.start(outputDir.getCanonicalPath)
        assert(q.awaitTermination(streamingTimeout.toMillis))
        // The streaming engine always reports one batch, even if it's empty.
        assert(q.recentProgress.length === 1)
        assert(q.recentProgress(0).sources.head.startOffset ==
          q.recentProgress(0).sources.head.endOffset)

        // Write versions 5, 6, 7.
        (5 to 7).foreach { i =>
          val v = Seq(i.toString).toDF
          v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }

        q = stream.start(outputDir.getCanonicalPath)
        assert(q.awaitTermination(streamingTimeout.toMillis))
        // These versions are processed one by one outside the initial snapshot.
        assert(q.recentProgress.length === 3)

        (5 to 7).foreach { i =>
          val p = q.recentProgress(i - 5)
          assert(p.numInputRows === 1)
          val endOffset = JsonUtils.fromJson[DeltaSourceOffset](p.sources.head.endOffset)
          assert(endOffset == DeltaSourceOffset(
            endOffset.reservoirId,
            reservoirVersion = i + 1,
            index = DeltaSourceOffset.BASE_INDEX,
            isInitialSnapshot = false))
        }

        // Restarting the stream should immediately terminate with no progress because no more data
        q = stream.start(outputDir.getCanonicalPath)
        assert(q.awaitTermination(streamingTimeout.toMillis))
        assert(q.recentProgress.length === 1)
        assert(q.recentProgress(0).sources.head.startOffset ==
          q.recentProgress(0).sources.head.endOffset)
      } finally {
        q.stop()
      }
    }
  }

  test("Trigger.AvailableNow with an empty table") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (value STRING) USING delta")

      val stream = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"))
        .writeStream
        .format("memory")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.AvailableNow)
        .queryName("emptyTableTriggerAvailableNow")

      var q = stream.start()
      try {
        assert(q.awaitTermination(10000))
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 0)
      } finally {
        q.stop()
      }
    }
  }

  test("maxBytesPerTrigger: process at least one file") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "1b"))
        .writeStream
        .format("memory")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
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
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 20).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "1b"))
        .writeStream
        .format("memory")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
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

      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "1b"))
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

      val q2 = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "100g"))
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
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      Seq(0, -1, "string").foreach { invalidMaxBytesPerTrigger =>
        val e = intercept[StreamingQueryException] {
          loadStreamWithOptions(
            inputDir.getCanonicalPath,
            Map(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> invalidMaxBytesPerTrigger.toString))
            .writeStream
            .format("console")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
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

  test("maxBytesPerTrigger: Trigger.AvailableNow respects read limits") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, inputDir)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val stream = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "1b"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.AvailableNow)
        .queryName("maxBytesPerTriggerTest")

      var q = stream.start(outputDir.getCanonicalPath)
      try {
        assert(q.awaitTermination(streamingTimeout.toMillis))
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(
          sql(s"SELECT * from delta.`${outputDir.getCanonicalPath}`"),
          (0 until 5).map(_.toString).toDF)

        // Restarting the stream should immediately terminate with no progress because no more data
        q = stream.start(outputDir.getCanonicalPath)
        assert(q.awaitTermination(streamingTimeout.toMillis))
        assert(q.recentProgress.length === 1)
        assert(q.recentProgress(0).sources.head.startOffset ==
          q.recentProgress(0).sources.head.endOffset)
      } finally {
        q.stop()
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

      // should process a file at a time due to MAX_FILES_PER_TRIGGER_OPTION
      val q = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(
          DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1",
          DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "100gb"))
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

      val q2 = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(
          DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "2",
          DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION -> "1b"))
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

  // DeltaSourceOffset unit tests have been moved to DeltaSourceOffsetSuite.

  testQuietly("streaming query should fail when table is deleted and recreated with new id") {
    withTempDir { inputDir =>
      val tablePath = new Path(inputDir.toURI)
      val deltaLog = DeltaLog.forTable(spark, tablePath)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AssertOnQuery { _ =>
          Utils.deleteRecursively(inputDir)
          // This test deletes and recreates a table at the same path. The InMemoryCommitCoordinator
          // keys on logPath, so stale coordinator state from the old table must be cleared before
          // the new table is created. In production, UC handles table lifecycle management, so
          // explicit coordinator cleanup is not needed.
          if (catalogOwnedDefaultCreationEnabledInTests) {
            deleteCatalogOwnedTableFromCommitCoordinator(tablePath)
          }
          val deltaLog = DeltaLog.forTable(spark, tablePath)
          // All Delta tables in tests use the same tableId by default. Here we pass a new tableId
          // to simulate a new table creation in production
          withMetadata(deltaLog, StructType.fromDDL("value STRING"), tableId = Some("tableId-1234"))
          true
        },
        StartStream(),
        ExpectFailure[DeltaIllegalStateException] { e =>
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
        val options = s.map(regex =>
          Map(DeltaOptions.EXCLUDE_REGEX_OPTION -> regex)).getOrElse(Map.empty)
        val df = loadStreamWithOptions(inputDir.getCanonicalPath, options).groupBy('value).count
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
        // CatalogManaged (CCv2) tables auto-enable row tracking, which requires numRecords in
        // AddFile stats to assign base row IDs. Without numRecords, the commit fails with
        // DELTA_ROW_ID_ASSIGNMENT_WITHOUT_STATS.
        AddFile(name, Map.empty, content.length, System.currentTimeMillis(), dataChange = true,
          stats = s"""{"numRecords": 1}""")
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
        loadStreamWithOptions(
          inputDir.getCanonicalPath,
          Map(DeltaOptions.EXCLUDE_REGEX_OPTION -> "[abc"))
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
        val reader = loadStreamWithOptions(deltaPath, Map.empty)
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

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

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

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

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

  test("startingVersion specific version: new commits arrive after stream initialization") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // Add version 0 and version 1
      Seq(1, 2, 3).toDF("value").write.format("delta").save(inputDir.getCanonicalPath)
      Seq(4, 5, 6).toDF("value").write
        .format("delta").mode("append").save(inputDir.getCanonicalPath)

      // Start streaming from version 1
      val df = loadStreamWithOptions(
        inputDir.getCanonicalPath,
        Map(
          "startingVersion" -> "1",
          DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION -> "1"
        )
      )

      val q = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)

      try {
        // Process version 1 only
        q.processAllAvailable()
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq(4, 5, 6).toDF("value"))

        // Add version 2 and version 3 (after snapshotAtSourceInit was captured)
        Seq(7, 8, 9).toDF("value").write
          .format("delta").mode("append").save(inputDir.getCanonicalPath)
        Seq(10, 11, 12).toDF("value").write
          .format("delta").mode("append").save(inputDir.getCanonicalPath)

        q.processAllAvailable()
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          (4 to 12).toDF("value"))
      } finally {
        q.stop()
      }
    }
  }

  test(
      "can delete old files of a snapshot without update"
  ) {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

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
      // This isn't stable, but it shouldn't change during the test.
      val tahoeId = deltaLog.unsafeVolatileTableId

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
          writersLog.cleanUpExpiredLogs(writersLog.snapshot)

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
        CheckNewAnswer("3", "4")
      )
      assert(deltaLog.snapshot.version == 0)
    }
  }

  test("Delta sources don't write offsets with null json") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(inputDir.toString, Map.empty)
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

  test("Delta source advances with non-data inserts and generates empty dataframe for " +
    "non-data operations") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // Version 0
      Seq(1L, 2L, 3L).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(inputDir.toString, Map.empty)

      val stream = df
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .foreachBatch(
          (outputDf: DataFrame, bid: Long) => {
              // Apart from first batch, rest of batches work with non-data operations
              // for which we expect an empty dataframe to be generated.
              if (bid > 0) {
                assert(outputDf.isEmpty)
              }
              outputDf
                .write
                .format("delta")
                .mode("append")
                .save(outputDir.toString)
            }
        )
        .start()

      val deltaLog = DeltaLog.forTable(spark, inputDir.toString)
      def expectLatestOffset(offset: DeltaSourceOffset) {
          val lastOffset = DeltaSourceOffset(
            deltaLog.unsafeVolatileTableId,
            SerializedOffset(stream.lastProgress.sources.head.endOffset)
          )

          assert(lastOffset == offset)
      }

      try {
        stream.processAllAvailable()
        expectLatestOffset(DeltaSourceOffset(
          deltaLog.unsafeVolatileTableId,
          reservoirVersion = 1,
          DeltaSourceOffset.BASE_INDEX,
          isInitialSnapshot = false))

        deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
        stream.processAllAvailable()
        expectLatestOffset(DeltaSourceOffset(
          deltaLog.unsafeVolatileTableId,
          reservoirVersion = 2,
          DeltaSourceOffset.BASE_INDEX,
          isInitialSnapshot = false))

        deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
        stream.processAllAvailable()
        expectLatestOffset(DeltaSourceOffset(
          deltaLog.unsafeVolatileTableId,
          reservoirVersion = 3,
          DeltaSourceOffset.BASE_INDEX,
          isInitialSnapshot = false))
      } finally {
        stream.stop()
      }
    }
  }

  test("Rate limited Delta source advances with non-data inserts") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // Version 0
      Seq(1L, 2L, 3L).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(inputDir.toString, Map.empty)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .option("maxFilesPerTrigger", 2)
        .start(outputDir.toString)

      try {
        val deltaLog = DeltaLog.forTable(spark, inputDir.toString)
        def waitForOffset(offset: DeltaSourceOffset) {
          eventually(timeout(streamingTimeout)) {
            val lastOffset = DeltaSourceOffset(
              deltaLog.unsafeVolatileTableId,
              SerializedOffset(stream.lastProgress.sources.head.endOffset)
            )

            assert(lastOffset == offset)
          }
        }

        // Process the initial snapshot (version 0) and end up at the start of version 1 which
        // does not exist yet.
        stream.processAllAvailable()
        waitForOffset(DeltaSourceOffset(
          deltaLog.unsafeVolatileTableId, 1, DeltaSourceOffset.BASE_INDEX, false))

        // Add Versions 1, 2, 3, and 4
        for(i <- 1 to 4) {
          deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
        }

        // The manual commits don't have any files in them, but they do have indexes: BASE_INDEX
        // and END_INDEX. Neither of those indexes are counted for rate limiting. We end up at
        // v4[END_INDEX] which is then rounded up to v5[BASE_INDEX] even though v5 does not exist
        // yet.
        stream.processAllAvailable()
        waitForOffset(
          DeltaSourceOffset(deltaLog.unsafeVolatileTableId, 5, DeltaSourceOffset.BASE_INDEX, false))

        // Add Version 5
        deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)

        // The stream progresses to v5[END_INDEX] which is rounded up to v6[BASE_INDEX]. (In prior
        // versions of the code we did not have END_INDEX. In that case the stream would not have
        // moved forward from v5, because there were no indexes after v5[BASE_INDEX].
        stream.processAllAvailable()
        waitForOffset(
          DeltaSourceOffset(deltaLog.unsafeVolatileTableId, 6, DeltaSourceOffset.BASE_INDEX, false))
      } finally {
        stream.stop()
      }
    }
  }

  testQuietly("Delta sources should verify the protocol reader version") {
    withTempDir { tempDir =>
      spark.range(0).write.format("delta").save(tempDir.getCanonicalPath)

      val df = loadStreamWithOptions(tempDir.getCanonicalPath, Map.empty)
      val stream = df.writeStream
        .format("console")
        .start()
      try {
        stream.processAllAvailable()

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        deltaLog.store.write(
          FileNames.unsafeDeltaFile(deltaLog.logPath, deltaLog.snapshot.version + 1),
          // Write a large reader version to fail the streaming query
          Iterator(Protocol(minReaderVersion = Int.MaxValue).json),
          overwrite = false,
          deltaLog.newDeltaHadoopConf())

        // The streaming query should fail because its version is too old
        val e = intercept[StreamingQueryException] {
          stream.processAllAvailable()
        }
        val cause = e.getCause
        val sw = new StringWriter()
        cause.printStackTrace(new PrintWriter(sw))
        assert(
          cause.isInstanceOf[InvalidProtocolVersionException] ||
          // When coordinated commits are enabled, the following assertion error coming from
          // CoordinatedCommitsUtils.getCommitCoordinatorClient may get hit
          (cause.isInstanceOf[AssertionError] &&
           e.getCause.getMessage.contains("coordinated commits table feature is not supported")),
          s"Caused by: ${sw.toString}")
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
      modifyCommitTimestamp(deltaLog, startVersion, ts)
      startVersion += 1
    }
  }

  private implicit def durationToLong(duration: FiniteDuration): Long = {
    duration.toMillis
  }

  /** Disable log cleanup to avoid deleting logs we are testing. */
  protected def disableLogCleanup(tablePath: String): Unit = {
    sql(s"alter table delta.`$tablePath` " +
      s"set tblproperties (${DeltaConfigs.ENABLE_EXPIRED_LOG_CLEANUP.key} = false)")
  }

  testQuietly("startingVersion") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      val start = 1594795800000L
      generateCommits(tablePath, start, start + 20.minutes)

      def testStartingVersion(startingVersion: Long): Unit = {
        val df = loadStreamWithOptions(
          tablePath, Map("startingVersion" -> startingVersion.toString))
        val q = df.writeStream
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
      new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()

      // Cannot start from version 0
      assert(intercept[StreamingQueryException] {
        testStartingVersion(0)
      }.getMessage.contains("Cannot time travel Delta table to version 0"))

      // Can start from version 1 even if it's not recreatable
      // TODO: currently we would error out if we couldn't construct the snapshot to check column
      //  mapping enable tables. Unblock this once we roll out the proper semantics.
      withStreamingReadOnColumnMappingTableEnabled {
        withTempView("startingVersion_test") {
          testStartingVersion(1L)
          checkAnswer(
            spark.table("startingVersion_test"),
            (10 until 20).map(_.toLong).toDF())
        }
      }
    }
  }

  // Row tracking forces actions to appear after AddFiles within commits. This will verify that
  // we correctly skip processed commits, even when an AddFile is not the last action within a
  // commit.
  Seq(true, false).foreach { withRowTracking =>
    testQuietly(s"startingVersion should be ignored when restarting from a checkpoint, " +
      s"withRowTracking = $withRowTracking") {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        val start = 1594795800000L
        withSQLConf(
          DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey -> withRowTracking.toString) {
          generateCommits(inputDir.getCanonicalPath, start, start + 20.minutes)
        }

        def testStartingVersion(
            startingVersion: Long,
            checkpointLocation: String = checkpointDir.getCanonicalPath): Unit = {
          val q = loadStreamWithOptions(
            inputDir.getCanonicalPath,
            Map("startingVersion" -> startingVersion.toString))
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
        // `DeltaSource.getStartingVersion` because the engine will call `getBatch`
        // that was committed (start is None) during the restart.
        testStartingVersion(1L)
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          (10 until 30).map(_.toLong).toDF())

        // Add one commit and delete version 0 and version 1
        generateCommits(inputDir.getCanonicalPath, start + 60.minutes)
        (0 to 1).foreach { v =>
          new File(FileNames.unsafeDeltaFile(deltaLog.logPath, v).toUri).delete()
        }

        // Although version 1 has been deleted, restarting the query should still work as we have
        // processed files in version 1.
        // In other words, query restart should ignore "startingVersion"
        // TODO: currently we would error out if we couldn't construct the snapshot to check column
        //  mapping enable tables. Unblock this once we roll out the proper semantics.
        withStreamingReadOnColumnMappingTableEnabled {
          testStartingVersion(1L)
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            // the gap caused by "alter table"
            ((10 until 30) ++ (40 until 50)).map(_.toLong).toDF())

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
    }
  }

  testQuietly("startingTimestamp") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      val start = 1594795800000L // 2020-07-14 23:50:00 PDT
      generateCommits(tablePath, start, start + 20.minutes)

      def testStartingTimestamp(startingTimestamp: String): Unit = {
        val q = loadStreamWithOptions(
          tablePath,
          Map("startingTimestamp" -> startingTimestamp))
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
      new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 0).toUri).delete()

      // Can start from version 1 even if it's not recreatable
      // TODO: currently we would error out if we couldn't construct the snapshot to check column
      //  mapping enable tables. Unblock this once we roll out the proper semantics.
      withStreamingReadOnColumnMappingTableEnabled {
        withTempView("startingTimestamp_test") {
          testStartingTimestamp("2020-07-14")
          checkAnswer(
            spark.table("startingTimestamp_test"),
            (10 until 20).map(_.toLong).toDF())
        }
      }
    }
  }

  testQuietly("startingVersion and startingTimestamp are both set") {
    withTempDir { tableDir =>
      val tablePath = tableDir.getCanonicalPath
      generateCommits(tablePath, 0)
      val q = loadStreamWithOptions(
        tablePath,
        Map("startingVersion" -> "0", "startingTimestamp" -> "2020-07-15"))
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
        val q = loadStreamWithOptions(
          inputDir.getCanonicalPath,
          Map("startingVersion" -> "1")
        ).writeStream
          .format("memory")
          .queryName("startingVersionTest")
          .start()
        try {
          q.processAllAvailable()
          checkAnswer(
            sql("select * from startingVersionTest"),
            ((10 until 20).map(x => (x.toLong, x.toLong, "null")) ++
              (20 until 30).map(x => (x.toLong, x.toLong, x.toString)))
              .toDF("id", "id2", "id3")
              .selectExpr(
                "id",
                "id2",
                "CASE WHEN id3 = 'null' THEN NULL ELSE cast(id3 as long) END as id3")
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
        val q = loadStreamWithOptions(path, Map("startingVersion" -> "latest"))
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
        val streamDef = loadStreamWithOptions(path, Map("startingVersion" -> "latest"))
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
        val streamDef = loadStreamWithOptions(path, Map("startingVersion" -> "latest"))
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

        val streamDef = loadStreamWithOptions(
          path,
          Map("startingVersion" -> "latest")
        ).writeStream
          .format("memory")
          .queryName("startingVersionLatest")
        val log = DeltaLog.forTable(spark, path)
        val originalSnapshot = log.snapshot
        val timestamp = System.currentTimeMillis()

        // We write out some new data, and then do a dirty reflection hack to produce an un-updated
        // Delta log. The stream should still update when started and not produce any data.
        spark.range(10).write.format("delta").mode("append").save(path)
        // The field is actually declared in the SnapshotManagement trait, but because traits don't
        // exist in the JVM DeltaLog is where it ends up in reflection.
        val snapshotField = classOf[DeltaLog].getDeclaredField("currentSnapshot")
        snapshotField.setAccessible(true)
        snapshotField.set(log, CapturedSnapshot(originalSnapshot, timestamp))

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

        val q = loadStreamWithOptions(
          path,
          Map(
            "startingVersion" -> "1",
            "maxFilesPerTrigger" -> "1"
          )
        ).writeStream
          .format("memory")
          .queryName("startingVersionWithRateLimit")
          .start()
        try {
          q.processAllAvailable()
          checkAnswer(sql("select * from startingVersionWithRateLimit"), (5 until 10).map(Row(_)))
          val id = DeltaLog.forTable(spark, path).snapshot.metadata.id
          val endOffsets = q.recentProgress
            .map(_.sources(0).endOffset)
            .map(offsetJson => DeltaSourceOffset(
              id,
              SerializedOffset(offsetJson)
            ))
          assert(endOffsets.toList ==
            DeltaSourceOffset(id, 1, 0, isInitialSnapshot = false)
              // When we reach the end of version 1, we will jump to version 2 with index -1
              :: DeltaSourceOffset(id, 2, DeltaSourceOffset.BASE_INDEX, isInitialSnapshot = false)
              :: Nil)
        } finally {
          q.stop()
        }
      }
    }
  }

  testQuietly("deltaSourceIgnoreChangesError contains removeFile, version, tablePath") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)
      val df = loadStreamWithOptions(inputDir.toString, Map.empty)
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
        "This is currently not supported. If this is going to happen regularly and you are okay" +
          " to skip changes, set the option 'skipChangeCommits' to 'true'."
      ))
      assert(e.getCause.getMessage.contains("for example"))
      assert(e.getCause.getMessage.contains("version"))
      assert(e.getCause.getMessage.matches(s".*$inputDir.*"))
    }
  }

  testQuietly("deltaSourceIgnoreDeleteError contains removeFile, version, tablePath") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)
      val df = loadStreamWithOptions(inputDir.toString, Map.empty)
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
      assert(e.getCause.getMessage.matches(s".*$inputDir.*"))
    }
  }

  test("streaming with ignoreDeletes = true skips delete-only commits") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // Write initial data
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(
        inputDir.toString, Map(DeltaOptions.IGNORE_DELETES_OPTION -> "true",
          "startingVersion" -> "0"))

      val q = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
      try {
        q.processAllAvailable()
        checkAnswer(
          spark.read.format("delta").load(outputDir.toString),
          Seq(1, 2, 3).map(Row(_)))

        // Delete all rows: produces only RemoveFile actions
        io.delta.tables.DeltaTable.forPath(spark, inputDir.getAbsolutePath).delete()

        // Append new data after the delete
        Seq(4, 5).toDF("x").write.format("delta").mode("append").save(inputDir.toString)

        q.processAllAvailable()

        // The delete commit should be silently skipped; only inserts are processed
        checkAnswer(
          spark.read.format("delta").load(outputDir.toString),
          Seq(1, 2, 3, 4, 5).map(Row(_)))
      } finally {
        q.stop()
      }
    }
  }

  testQuietly("streaming with ignoreDeletes = true still fails on change commits") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(
        inputDir.toString, Map(DeltaOptions.IGNORE_DELETES_OPTION -> "true"))
      df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
        .processAllAvailable()

      // Overwrite produces both AddFile and RemoveFile actions (a change commit)
      Seq(4, 5, 6).toDF("x")
        .write
        .mode("overwrite")
        .format("delta")
        .save(inputDir.toString)

      val e = intercept[StreamingQueryException] {
        val q = df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpointDir.toString)
          .start(outputDir.toString)

        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }

      assert(e.getCause.isInstanceOf[UnsupportedOperationException])
      assert(e.getCause.getMessage.contains(
        "This is currently not supported. If this is going to happen regularly and you are okay" +
          " to skip changes, set the option 'skipChangeCommits' to 'true'."
      ))
    }
  }

  test("streaming with skipChangeCommits = true skips both delete and change commits") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(
        inputDir.toString, Map(DeltaOptions.SKIP_CHANGE_COMMITS_OPTION -> "true"))

      val q = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
      try {
        q.processAllAvailable()
        checkAnswer(
          spark.read.format("delta").load(outputDir.toString),
          Seq(1, 2, 3).map(Row(_)))

        // Delete all rows: produces only RemoveFile actions (delete-only commit)
        io.delta.tables.DeltaTable.forPath(spark, inputDir.getAbsolutePath).delete()

        Seq(4, 5).toDF("x").write.format("delta").mode("append").save(inputDir.toString)

        // Overwrite produces both AddFile and RemoveFile actions (change commit)
        Seq(6, 7, 8).toDF("x")
          .write
          .mode("overwrite")
          .format("delta")
          .save(inputDir.toString)

        Seq(9, 10).toDF("x").write.format("delta").mode("append").save(inputDir.toString)

        q.processAllAvailable()

        // Both the delete and overwrite commits are silently skipped; only inserts are processed
        checkAnswer(
          spark.read.format("delta").load(outputDir.toString),
          Seq(1, 2, 3, 4, 5, 9, 10).map(Row(_)))
      } finally {
        q.stop()
      }
    }
  }

  test("fail on data loss - starting from missing files") {
    withTempDirs { (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = loadStreamWithOptions(srcData.getCanonicalPath, Map.empty)

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Create a checkpoint so that we can create a snapshot without json files before version 3
      srcLog.checkpoint()
      // Delete the first file
      assert(new File(FileNames.unsafeDeltaFile(srcLog.logPath, 1).toUri).delete())

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
    withTempDirs { (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = loadStreamWithOptions(srcData.getCanonicalPath, Map.empty)

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Create a checkpoint so that we can create a snapshot without json files before version 3
      srcLog.checkpoint()
      // Delete the second file
      assert(new File(FileNames.unsafeDeltaFile(srcLog.logPath, 2).toUri).delete())

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
    withTempDirs { (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = loadStreamWithOptions(
        srcData.getCanonicalPath,
        Map("failOnDataLoss" -> "false"))

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Create a checkpoint so that we can create a snapshot without json files before version 3
      srcLog.checkpoint()
      // Delete the first file
      assert(new File(FileNames.unsafeDeltaFile(srcLog.logPath, 1).toUri).delete())

      val q2 = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q2.processAllAvailable()
      q2.stop()

      assert(spark.read.format("delta").load(targetData.getCanonicalPath).count() === 30)
    }
  }

  test("fail on data loss - gaps of files with option off") {
    withTempDirs { (srcData, targetData, chkLocation) =>
      def addData(): Unit = {
        spark.range(10).write.format("delta").mode("append").save(srcData.getCanonicalPath)
      }

      addData()
      val df = loadStreamWithOptions(
        srcData.getCanonicalPath,
        Map("failOnDataLoss" -> "false"))

      val q = df.writeStream.format("delta")
        .option("checkpointLocation", chkLocation.getCanonicalPath)
        .start(targetData.getCanonicalPath)
      q.processAllAvailable()
      q.stop()

      addData()
      addData()
      addData()

      val srcLog = DeltaLog.forTable(spark, srcData)
      // Create a checkpoint so that we can create a snapshot without json files before version 3
      srcLog.checkpoint()
      // Delete the second file
      assert(new File(FileNames.unsafeDeltaFile(srcLog.logPath, 2).toUri).delete())

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


  test("should not attempt to read a non exist version") {
    withTempDirs { (inputDir1, inputDir2, checkpointDir) =>
      spark.range(1, 2).write.format("delta").save(inputDir1.getCanonicalPath)
      spark.range(1, 2).write.format("delta").save(inputDir2.getCanonicalPath)

      def startQuery(): StreamingQuery = {
        val df1 = loadStreamWithOptions(inputDir1.getCanonicalPath, Map.empty)
        val df2 = loadStreamWithOptions(inputDir2.getCanonicalPath, Map.empty)
        df1.union(df2).writeStream
          .format("noop")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start()
      }

      var q = startQuery()
      try {
        q.processAllAvailable()
        // current offsets:
        // source1: DeltaSourceOffset(reservoirVersion=1,index=0,isInitialSnapshot=true)
        // source2: DeltaSourceOffset(reservoirVersion=1,index=0,isInitialSnapshot=true)

        spark.range(1, 2).write.format("delta").mode("append").save(inputDir1.getCanonicalPath)
        spark.range(1, 2).write.format("delta").mode("append").save(inputDir2.getCanonicalPath)
        q.processAllAvailable()
        // current offsets:
        // source1: DeltaSourceOffset(reservoirVersion=2,index=-1,isInitialSnapshot=false)
        // source2: DeltaSourceOffset(reservoirVersion=2,index=-1,isInitialSnapshot=false)
        // Note: version 2 doesn't exist in source1

        spark.range(1, 2).write.format("delta").mode("append").save(inputDir2.getCanonicalPath)
        q.processAllAvailable()
        // current offsets:
        // source1: DeltaSourceOffset(reservoirVersion=2,index=-1,isInitialSnapshot=false)
        // source2: DeltaSourceOffset(reservoirVersion=3,index=-1,isInitialSnapshot=false)
        // Note: version 2 doesn't exist in source1

        q.stop()
        // Restart the query. It will call `getBatch` on the previous two offsets of `source1` which
        // are both DeltaSourceOffset(reservoirVersion=2,index=-1,isInitialSnapshot=false)
        // As version 2 doesn't exist, we should not try to load version 2 in this case.
        q = startQuery()
        q.processAllAvailable()
      } finally {
        q.stop()
      }
    }
  }

  test("self union a Delta table should pass the catalog table assert") {
    withTable("self_union_delta") {
      spark.range(10).write.format("delta").saveAsTable("self_union_delta")
      val df = spark.readStream.format("delta").table("self_union_delta")
      val q = df.union(df).writeStream.format("noop").start()
      try {
        q.processAllAvailable()
      } finally {
        q.stop()
      }
    }
  }

  test("ES-445863: delta source should not hang or reprocess data when using AvailableNow") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      def runQuery(): Unit = {
        val q = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
          // Require a partition filter. The max index of files matching the partition filter must
          // be less than the number of files in the second commit.
          .where("part = 0")
          .writeStream
          .format("delta")
          .trigger(Trigger.AvailableNow)
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start(outputDir.getCanonicalPath)
        try {
          if (!q.awaitTermination(60000)) {
            throw new TimeoutException("the query didn't stop in 60 seconds")
          }
        } finally {
          q.stop()
        }
      }

      spark.range(0, 1)
        .selectExpr("id", "id as part")
        .repartition(10)
        .write
        .partitionBy("part")
        .format("delta")
        .mode("append")
        .save(inputDir.getCanonicalPath)
      runQuery()

      spark.range(1, 10)
        .selectExpr("id", "id as part")
        .repartition(9)
        .write
        .partitionBy("part")
        .format("delta")
        .mode("append")
        .save(inputDir.getCanonicalPath)
      runQuery()

      checkAnswer(
        spark.read.format("delta").load(outputDir.getCanonicalPath),
        Row(0, 0) :: Nil)
    }
  }

  test("add column: restarting with new DataFrame should recover") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 10).foreach { i =>
        val v = Seq(i.toString).toDF("id")
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      def startQuery(): StreamingQuery = {
        loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("mergeSchema", "true")
          // use delta sink because we need to check the result
          .format("delta")
          .start(outputDir.getCanonicalPath)
      }

      var q = startQuery()
      try {
        q.processAllAvailable()
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          (0 until 10).map(i => Row(i.toString)))

        // Clear delta log cache
        DeltaLog.clearCache()
        // Change the table schema using the non-cached `DeltaLog` to mimic the case that the
        // table schema change happens on a different cluster
        withMetadata(deltaLog, StructType.fromDDL("id STRING, newcol STRING"))
        Seq(("10", "a")).toDF("id", "newcol")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)

        // The streaming query should fail when detecting a schema change
        val e = intercept[StreamingQueryException] {
          q.processAllAvailable()
        }
        assert(e.getMessage.contains("Detected schema change"))

        // Restarting the query with a new DataFrame should recover from the schema change
        q = startQuery()
        q.processAllAvailable()
        // Verify the output schema includes the new column
        val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
        assert(outputDf.schema.fieldNames.toSet == Set("id", "newcol"),
          s"Expected schema with {id, newcol} but got ${outputDf.schema.fieldNames.mkString(", ")}")
        checkAnswer(outputDf,
          (0 until 10).map(i => Row(i.toString, null)) :+ Row("10", "a"))
      } finally {
        q.stop()
      }
    }
  }

  test("add column: restarting with stale DataFrame should fail") {
    withTempDir { inputDir =>
      withTempDir { checkpointDir =>
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        (0 until 2).foreach { i =>
          val v = Seq(i.toString).toDF("id")
          v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }
        val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

        // First run: stream detects schema change and fails
        testStream(df)(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          ProcessAllAvailable(),
          CheckAnswer("0", "1"),
          Execute { _ =>
            withMetadata(deltaLog, StructType.fromDDL("id STRING, newcol STRING"))
            Seq(("2", "a")).toDF("id", "newcol")
              .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
          },
          ExpectFailure[DeltaIllegalStateException](t =>
            assert(t.getMessage.contains("Detected schema change"))),
          Execute { q =>
            assert(!q.isActive)
          }
        )

        // Restarting with the same DataFrame cannot recover from column addition because the
        // plan's read schema is fixed at analysis time. User must create a new DataFrame.
        val e = intercept[StreamingQueryException] {
          val q = df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("noop")
            .start()
          try q.processAllAvailable() finally q.stop()
        }
        // V1 fails with an internal batch schema assertion ("Invalid batch"),
        // V2 fails with DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART.
        assert(
          e.getMessage.contains("Invalid batch") ||
            e.getMessage.contains("DELTA_STREAMING_SCHEMA_MISMATCH_ON_RESTART"),
          s"Expected schema mismatch error but got: ${e.getMessage}"
        )
      }
    }
  }

  test("relax nullability: restarting with new DataFrame should recover") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` " +
        "(id STRING NOT NULL) USING DELTA")
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 2).foreach { i =>
        Seq(i.toString).toDF("id")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      def startQuery(): StreamingQuery = {
        loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("mergeSchema", "true")
          // use delta sink because we need to check the result
          .format("delta")
          .start(outputDir.getCanonicalPath)
      }

      var q = startQuery()
      try {
        q.processAllAvailable()

        // Clear delta log cache
        DeltaLog.clearCache()
        // Relax nullability from "id STRING NOT NULL" to "id STRING" using the non-cached
        // `DeltaLog` to mimic the case that the schema change happens on a different cluster
        withMetadata(deltaLog, StructType.fromDDL("id STRING"))
        // Insert a null row
        Seq(Option.empty[String]).toDF("id")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)

        // The streaming query should fail when detecting a schema change
        val e = intercept[StreamingQueryException] {
          q.processAllAvailable()
        }
        assert(e.getMessage.contains("Detected schema change"))

        // Restarting the query with a new DataFrame should recover from the schema change
        q = startQuery()
        q.processAllAvailable()
        val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
        assert(outputDf.schema("id").nullable,
          "Expected 'id' column to be nullable after relaxing nullability")
        checkAnswer(outputDf.orderBy("id"),
          Seq(Row(null), Row("0"), Row("1")))
      } finally {
        q.stop()
      }
    }
  }

  test("relax nullability: restarting with stale DataFrame should recover") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` " +
        "(id STRING NOT NULL) USING DELTA")
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      (0 until 2).foreach { i =>
        Seq(i.toString).toDF("id")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }
      val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

      var q = df.writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("mergeSchema", "true")
        // use delta sink because we need to check the result
        .format("delta")
        .start(outputDir.getCanonicalPath)
      try {
        q.processAllAvailable()

        // Clear delta log cache
        DeltaLog.clearCache()
        // Relax nullability from "id STRING NOT NULL" to "id STRING" using the non-cached
        // `DeltaLog` to mimic the case that the schema change happens on a different cluster
        withMetadata(deltaLog, StructType.fromDDL("id STRING"))
        // Insert a null row
        Seq(Option.empty[String]).toDF("id")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)

        // The streaming query should fail when detecting a schema change
        val e = intercept[StreamingQueryException] {
          q.processAllAvailable()
        }
        assert(e.getMessage.contains("Detected schema change"))

        // Restarting the query with the stale DataFrame should still recover
        q = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("mergeSchema", "true")
          .format("delta")
          .start(outputDir.getCanonicalPath)
        q.processAllAvailable()
        val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
        assert(outputDf.schema("id").nullable,
          "Expected 'id' column to be nullable after relaxing nullability")
        checkAnswer(outputDf.orderBy("id"),
          Seq(Row(null), Row("0"), Row("1")))
      } finally {
        q.stop()
      }
    }
  }

  test("type widening: restarting with new DataFrame should recover") {
    withSQLConf(
      DeltaSQLConf.DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING.key -> "false",
      DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> "true") {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id INT) " +
          "USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        (0 until 2).foreach { i =>
          Seq(i).toDF("id")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }

        def startQuery(): StreamingQuery = {
          loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .option("mergeSchema", "true")
            // use delta sink because we need to check the result
            .format("delta")
            .start(outputDir.getCanonicalPath)
        }

        var q = startQuery()
        try {
          q.processAllAvailable()

          // Clear delta log cache
          DeltaLog.clearCache()
          // Widen the column type from INT to BIGINT using the non-cached `DeltaLog` to mimic
          // the case that the schema change happens on a different cluster
          withMetadata(deltaLog, StructType.fromDDL("id BIGINT"))
          // 2^31 cannot be represented as an int, so it will be inserted as a bigint
          Seq(2147483648L).toDF("id")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)

          // The streaming query should fail when detecting a schema change
          val e = intercept[StreamingQueryException] {
            q.processAllAvailable()
          }
          assert(e.getMessage.contains("Detected schema change"))

          // Restarting the query with a new DataFrame should recover from the schema change
          q = startQuery()
          q.processAllAvailable()
          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          assert(outputDf.schema("id").dataType === LongType,
            s"Expected 'id' column to be LongType after type widening but got " +
              s"${outputDf.schema("id").dataType}")
          checkAnswer(outputDf.orderBy("id"),
            Seq(Row(0L), Row(1L), Row(2147483648L)))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("type widening: restarting with stale DataFrame should recover") {
    withSQLConf(
      DeltaSQLConf.DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING.key -> "false",
      DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey -> "true",
      DeltaSQLConf.DELTA_STREAMING_SINK_ALLOW_IMPLICIT_CASTS.key -> "true") {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id INT) " +
          "USING DELTA TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        (0 until 2).foreach { i =>
          Seq(i).toDF("id")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }
        val df = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)

        var q = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .option("mergeSchema", "true")
          // use delta sink because we need to check the result
          .format("delta")
          .start(outputDir.getCanonicalPath)
        try {
          q.processAllAvailable()

          // Clear delta log cache
          DeltaLog.clearCache()
          // Widen the column type from INT to BIGINT using the non-cached `DeltaLog` to mimic
          // the case that the schema change happens on a different cluster
          withMetadata(deltaLog, StructType.fromDDL("id BIGINT"))
          // 2^31 cannot be represented as an int, so it will be inserted as a bigint
          Seq(2147483648L).toDF("id")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)

          // The streaming query should fail when detecting a schema change
          val e = intercept[StreamingQueryException] {
            q.processAllAvailable()
          }
          assert(e.getMessage.contains("Detected schema change"))

          // Restarting the query with the stale DataFrame should still recover
          q = df.writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .option("mergeSchema", "true")
            .format("delta")
            .start(outputDir.getCanonicalPath)
          q.processAllAvailable()
          val outputDf = spark.read.format("delta").load(outputDir.getCanonicalPath)
          assert(outputDf.schema("id").dataType === LongType,
            s"Expected 'id' column to be LongType after type widening but got " +
              s"${outputDf.schema("id").dataType}")
          checkAnswer(outputDf.orderBy("id"),
            Seq(Row(0L), Row(1L), Row(2147483648L)))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("drop column: should fail with non-additive schema change error") {
    withTempDir { inputDir =>
      withTempDir { checkpointDir =>
        // Create a table with column mapping enabled (required for drop/rename column)
        sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id STRING, value STRING) " +
          "USING DELTA " +
          "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        (0 until 5).foreach { i =>
          Seq((i.toString, s"val$i")).toDF("id", "value")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }

        def startQuery(): StreamingQuery = {
          loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("noop")
            .start()
        }

        val q = startQuery()
        try {
          q.processAllAvailable()

          DeltaLog.clearCache()
          // Simulate dropping "value" column by committing new metadata with only "id".
          // This is a non-additive schema change (column removal).
          withMetadata(deltaLog, StructType.fromDDL("id STRING"))

          val e = intercept[StreamingQueryException] {
            q.processAllAvailable()
          }
          assert(e.getMessage.contains(
            "Streaming read is not supported on tables with read-incompatible schema changes"))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("drop column: should succeed with unsafe column mapping schema change flag enabled") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // Enable the unsafe flag that allows streaming to continue past column mapping
      // schema changes (drop/rename) instead of failing.
      withSQLConf(
        DeltaSQLConf
          .DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES
          .key -> "true"
      ) {
        sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id STRING, value STRING) " +
          "USING DELTA " +
          "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        (0 until 5).foreach { i =>
          Seq((i.toString, s"val$i")).toDF("id", "value")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }

        val q = loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("delta")
          .start(outputDir.getCanonicalPath)
        try {
          q.processAllAvailable()

          DeltaLog.clearCache()
          // Simulate dropping "value" column by committing new metadata with only "id".
          withMetadata(deltaLog, StructType.fromDDL("id STRING"))

          // Write more data after the drop column schema change
          (5 until 10).foreach { i =>
            Seq(i.toString).toDF("id")
              .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
          }

          // With the unsafe flag, the stream should process all data without failing.
          // Post-drop rows have null for the removed "value" column.
          q.processAllAvailable()

          checkAnswer(
            spark.read.format("delta").load(outputDir.getAbsolutePath),
            (0 until 5).map(i => (i.toString, s"val$i")).toDF("id", "value") union
              (5 until 10).map(i => (i.toString, null: String)).toDF("id", "value"))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("rename column: should fail with non-additive schema change error") {
    withTempDir { inputDir =>
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id STRING, value STRING) " +
          "USING DELTA " +
          "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        (0 until 5).foreach { i =>
          Seq((i.toString, s"val$i")).toDF("id", "value")
            .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
        }

        def startQuery(): StreamingQuery = {
          loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("noop")
            .start()
        }

        var q = startQuery()
        try {
          q.processAllAvailable()

          DeltaLog.clearCache()
          // Simulate renaming "value" -> "renamed_value" by committing new metadata.
          // Column rename is a non-additive schema change under column mapping.
          withMetadata(deltaLog, StructType.fromDDL("id STRING, renamed_value STRING"))

          val e = intercept[StreamingQueryException] {
            q.processAllAvailable()
          }
          assert(e.getMessage.contains(
            "Streaming read is not supported on tables with read-incompatible schema changes"))
        } finally {
          q.stop()
        }
      }
    }
  }

  test("rename column: should throw schema change error with unsafe flag enabled") {
    withTempDir { inputDir =>
      withTempDir { checkpointDir =>
        // The unsafe flag only helps with drop-column; rename is still blocked because
        // it changes logical column identity, which can silently corrupt data.
        withSQLConf(
          DeltaSQLConf
            .DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES
            .key -> "true"
        ) {
          sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id STRING, value STRING) " +
            "USING DELTA " +
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
          val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
          (0 until 5).foreach { i =>
            Seq((i.toString, s"val$i")).toDF("id", "value")
              .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
          }

          def startQuery(): StreamingQuery = {
            loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .format("noop")
              .start()
          }

          var q = startQuery()
          try {
            q.processAllAvailable()

            DeltaLog.clearCache()
            // Simulate renaming "value" -> "renamed_value" by committing new metadata.
            withMetadata(deltaLog, StructType.fromDDL("id STRING, renamed_value STRING"))

            // Even with the unsafe flag, rename still fails - but with a different error
            // ("Detected schema change") rather than the non-additive schema change error.
            val e = intercept[StreamingQueryException] {
              q.processAllAvailable()
            }
            assert(e.getMessage.contains("Detected schema change in version 6"))
          } finally {
            q.stop()
          }
        }
      }
    }
  }

  test("type widening: should fail with non-additive schema change error "
    + "when enable schema tracking") {
    withTempDir { inputDir =>
      withTempDir { checkpointDir =>
        withSQLConf(
          DeltaSQLConf.DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING.key -> "true"
        ) {
          // Table with type widening enabled so that type changes are allowed but tracked.
          sql(s"CREATE TABLE delta.`${inputDir.getCanonicalPath}` (id INT, value STRING) " +
            "USING DELTA " +
            "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
          val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
          (0 until 5).foreach { i =>
            Seq((i, s"val$i")).toDF("id", "value")
              .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
          }

          def startQuery(): StreamingQuery = {
            loadStreamWithOptions(inputDir.getCanonicalPath, Map.empty)
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .format("noop")
              .start()
          }

          var q = startQuery()
          try {
            q.processAllAvailable()

            DeltaLog.clearCache()
            // Simulate widening "id" from INT -> BIGINT by committing new metadata.
            // Type widening is a non-additive schema change for streaming reads.
            withMetadata(deltaLog, StructType.fromDDL("id BIGINT, value STRING"))

            val e = intercept[StreamingQueryException] {
              q.processAllAvailable()
            }
            assert(e.getMessage.contains(
              "Streaming read is not supported on tables with read-incompatible schema changes"))
          } finally {
            q.stop()
          }
        }
      }
    }
  }

  test("handling nullability schema changes") {
    withTable("srcTable") {
      withTempDirs { (srcTblDir, checkpointDir, checkpointDir2) =>
        def readStream(startingVersion: Option[Long] = None): DataFrame = {
          var dsr = spark.readStream
          startingVersion.foreach { v =>
            dsr = dsr.option("startingVersion", v)
          }
          dsr.table("srcTable")
        }

        sql(s"""
             |CREATE TABLE srcTable (
             |  a STRING NOT NULL,
             |  b STRING NOT NULL
             |) USING DELTA LOCATION '${srcTblDir.getCanonicalPath}'
             |""".stripMargin)
        sql("""
            |INSERT INTO srcTable
            | VALUES ("a", "b")
            |""".stripMargin)

        // Initialize the stream to pass the initial snapshot
        testStream(readStream())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          ProcessAllAvailable(),
          CheckAnswer(("a", "b"))
        )

        // It is ok to relax nullability during streaming post analysis, and restart would fix it.
        var v1 = 0L
        val clock = new StreamManualClock(System.currentTimeMillis())
        testStream(readStream())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath,
            trigger = ProcessingTimeTrigger(1000), triggerClock = clock),
          ProcessAllAvailable(),
          // Write more data and drop NOT NULL constraint
          Execute { _ =>
            // A batch of Delta actions
            sql("""
              |INSERT INTO srcTable
              |VALUES ("c", "d")
              |""".stripMargin)
            sql("ALTER TABLE srcTable ALTER COLUMN a DROP NOT NULL")
            sql("""
              |INSERT INTO srcTable
              |VALUES ("e", "f")
              |""".stripMargin)
            v1 = DeltaLog.forTable(spark, TableIdentifier("srcTable")).update().version
          },
          // Process next trigger
          AdvanceManualClock(1 * 1000L),
          // The query would fail because the read schema has nullable=false but the schema change
          // tries to relax it, we cannot automatically move ahead with it.
          ExpectFailure[DeltaIllegalStateException](t =>
            assert(t.getMessage.contains("Detected schema change"))),
          Execute { q =>
            assert(!q.isActive)
          },
          // Upon restart, the backfill can work with relaxed nullability read schema
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          ProcessAllAvailable(),
          // See how it loads data from across the nullability change without a problem
          CheckAnswer(("c", "d"), ("e", "f"))
        )

        // However, it is NOT ok to read data with relaxed nullability during backfill, and restart
        // would NOT fix it.
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier("srcTable"))
        deltaLog.withNewTransaction { txn =>
          val schema = txn.snapshot.metadata.schema
          val newSchema = StructType(schema("a").copy(nullable = false) :: schema("b") :: Nil)
          txn.commit(txn.metadata.copy(schemaString = newSchema.json) :: Nil,
            DeltaOperations.ManualUpdate)
        }
        sql("""
            |INSERT INTO srcTable
            |VALUES ("g", "h")
            |""".stripMargin)
        // Backfill from the ADD file action prior to the nullable=false, the latest schema has
        // nullable = false, but the ADD file has nullable = true, which is not allowed as we don't
        // want to show any nulls.
        // It queries [INSERT (e, f), nullable=false schema change, INSERT (g, h)]
        testStream(readStream(startingVersion = Some(v1)))(
          StartStream(checkpointLocation = checkpointDir2.getCanonicalPath),
          // See how it is:
          // 1. a non-retryable exception as it is a backfill.
          // 2. it comes from the new stream start check we added, before this, verifyStreamHygiene
          //    could not detect because the most recent schema change looks exactly like the latest
          //    schema.
          ExpectFailure[DeltaIllegalStateException](t =>
            assert(t.getMessage.contains("Detected schema change") &&
              t.getStackTrace.exists(
                _.toString.contains("checkReadIncompatibleSchemaChangeOnStreamStartOnce"))))
        )
      }
    }
  }

  test("streaming processes 100 sequential single-value commits and contains all values 0 to 99") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      // TODO(#6339): enable batch size 2 after fix PR merged
      assume(!catalogOwnedCoordinatorBackfillBatchSize.contains(2),
        "Test cannot pass with batch size 2 due to issue #6339")
      // Write the first value to initialize the Delta table
      Seq(0).toDF("x").write.format("delta").save(inputDir.toString)

      val df = loadStreamWithOptions(inputDir.toString, Map.empty[String, String])

      val q = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)

      try {
        // Process the initial commit (value 0)
        q.processAllAvailable()

        // Append values 1 through 99, one commit each
        (1 until 100).foreach { i =>
          Seq(i).toDF("x")
            .write
            .format("delta")
            .mode("append")
            .save(inputDir.toString)
        }

        q.processAllAvailable()

        // Verify the output contains exactly all values from 0 to 99
        checkAnswer(
          spark.read.format("delta").load(outputDir.toString),
          (0 until 100).map(Row(_))
        )
      } finally {
        q.stop()
      }
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

// Batch sizes 1, 2, and 100 exercise different backfill behaviors in the commit coordinator.
// Batch size 1 triggers a backfill on every commit (commitVersion % 1 == 0), testing the most
// granular backfill path. Batch size 2 triggers backfill every other commit, testing the boundary
// between backfilled and unbackfilled commits. Batch size 100 leaves most commits unbackfilled,
// testing the production-like path where streaming must read from both the commit coordinator
// and the filesystem. This follows the same pattern as other CatalogManaged (CCv2) test suites
// (DeltaLogSuite, DeltaCDCStreamSuite, etc.).

class DeltaSourceWithCatalogManagedBatch1Suite extends DeltaSourceSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(1)
}

class DeltaSourceWithCatalogManagedBatch2Suite extends DeltaSourceSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)
}

class DeltaSourceWithCatalogManagedBatch100Suite extends DeltaSourceSuite {
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(100)
}
