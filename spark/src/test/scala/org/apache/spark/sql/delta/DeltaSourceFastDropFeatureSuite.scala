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
import java.text.SimpleDateFormat

import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, StreamingQueryException}

class DeltaSourceFastDropFeatureSuite
  extends DeltaSourceSuiteBase
  with DeltaColumnMappingTestUtils
  with DeltaSQLCommandTest {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaSQLConf.FAST_DROP_FEATURE_ENABLED.key, "true")
  }

  protected def dropUnsupportedFeature(dir: File): Unit =
    sql(
      s"""ALTER TABLE delta.`${dir.getCanonicalPath}`
         |DROP FEATURE  ${TestUnsupportedReaderWriterFeature.name}
         |""".stripMargin)

  protected def addUnsupportedFeature(dir: File): Unit =
    sql(
      s"""ALTER TABLE delta.`${dir.getCanonicalPath}` SET TBLPROPERTIES (
         |delta.feature.${TestUnsupportedReaderWriterFeature.name} = 'supported'
         |)""".stripMargin)

  protected def getReadOnlyStream(
      dir: File,
      cdcReadEnabled: Boolean = false): DataStreamWriter[Row] =
    spark.readStream
      .option(DeltaOptions.CDC_READ_OPTION, cdcReadEnabled)
      .format("delta")
      .load(dir.getCanonicalPath)
      .writeStream
      .format("noop")

  protected def addData(dir: File, value: Int): Unit =
    Seq(value).toDF.write.mode("append").format("delta").save(dir.getCanonicalPath)

  protected lazy val cdcReadEnabled =
    spark.conf.getOption(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey)
      .map(_.toBoolean)
      .getOrElse(false)

  test("Latest protocol is checked for unsupported features") {
    withTempDir { inputDir =>
      addData(inputDir, value = 1)
      addUnsupportedFeature(inputDir)

      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> true.toString) {
        DeltaLog.clearCache()
        val e = intercept[DeltaUnsupportedTableFeatureException] {
          getReadOnlyStream(inputDir, cdcReadEnabled).start()
        }
        assert(e.getErrorClass === "DELTA_UNSUPPORTED_FEATURES_FOR_READ")
      }
    }
  }

  for (useStartingTS <- DeltaTestUtils.BOOLEAN_DOMAIN)
  test(s"Protocol is checked when using startingVersion - useStartingTS: $useStartingTS.") {
    withTempDir { inputDir =>
      def getTimestampForVersion(version: Long): String = {
        val logPath = new Path(inputDir.getCanonicalPath, "_delta_log")
        val file = new File(new Path(logPath, f"$version%020d.json").toString)
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        sdf.format(file.lastModified())
      }

      addData(inputDir, value = 1)
      addUnsupportedFeature(inputDir)
      addData(inputDir, value = 2) // More data.
      val versionAfterProtocolUpgrade = DeltaLog.forTable(spark, inputDir).update().version
      dropUnsupportedFeature(inputDir)

      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> true.toString) {
        // No problem loading from the latest version. Feature is dropped.
        DeltaLog.clearCache()
        getReadOnlyStream(inputDir, cdcReadEnabled).start()

        // Start a stream to a version the feature was active.
        val e = intercept[StreamingQueryException] {
          val stream = spark.readStream
            .option(DeltaOptions.CDC_READ_OPTION, cdcReadEnabled)
            .format("delta")

          if (useStartingTS) {
            stream.option("startingTimestamp", getTimestampForVersion(versionAfterProtocolUpgrade))
          } else {
            stream.option("startingVersion", versionAfterProtocolUpgrade)
          }

          val q = stream
            .load(inputDir.getCanonicalPath)
            .writeStream
            .format("noop")
            .start()

          // At initialization get attempt to get a snapshot at the starting version.
          // This will validate whether the client supports the protocol at that version.
          // Note, the protocol upgrade happened before the startingVersion. Therefore,
          // we are certain the exception here does not stem from coming across the protocol
          // bump while processing the stream.
          q.processAllAvailable()
        }
        assert(e.getCause.getMessage.contains("DELTA_UNSUPPORTED_FEATURES_FOR_READ"))
      }
    }
  }

  test("Protocol check at startingVersion is skipped when config is disabled") {
    withTempDir { inputDir =>
      addData(inputDir, value = 1)
      addUnsupportedFeature(inputDir)
      addData(inputDir, value = 2) // More data.
      val versionAfterProtocolUpgrade = DeltaLog.forTable(spark, inputDir).update().version
      dropUnsupportedFeature(inputDir)

      withSQLConf(
          DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL.key -> false.toString,
          DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> true.toString) {
        // Start a stream to a version the feature was active.
        val q = spark.readStream
          .option(DeltaOptions.CDC_READ_OPTION, cdcReadEnabled)
          .format("delta")
          .option("startingVersion", versionAfterProtocolUpgrade)
          .load(inputDir.getCanonicalPath)
          .writeStream
          .format("noop")
          .start()

        try {
          // Should had produced an exception but the check is disabled.
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }
    }
  }

  test("Protocol is checked when coming across an action with a protocol upgrade") {
    withTempDir { inputDir =>
      addData(inputDir, value = 1)
      addData(inputDir, value = 2) // More data. Optional.
      val versionBeforeProtocolUpgrade = DeltaLog.forTable(spark, inputDir).update().version
      addUnsupportedFeature(inputDir)
      dropUnsupportedFeature(inputDir)

      // Latest version looks clean. Feature is dropped.
      val stream = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, cdcReadEnabled)
        .format("delta")
        .option("startingVersion", versionBeforeProtocolUpgrade)
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("noop")

      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> true.toString) {
        val q = stream.start()
        val e = intercept[StreamingQueryException] {
          // We come across the protocol upgrade commit and fail.
          q.processAllAvailable()
        }
        q.stop()
        assert(e.getCause.getMessage.contains("DELTA_UNSUPPORTED_FEATURES_FOR_READ"))
      }
    }
  }

  test("Protocol validations after restarting from a checkpoint") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      addData(inputDir, value = 1)
      addData(inputDir, value = 2) // More data. Optional.
      addUnsupportedFeature(inputDir)

      val stream = spark.readStream
        .option(DeltaOptions.CDC_READ_OPTION, cdcReadEnabled)
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .drop(CDCReader.CDC_TYPE_COLUMN_NAME)
        .drop(CDCReader.CDC_COMMIT_VERSION)
        .drop(CDCReader.CDC_COMMIT_TIMESTAMP)
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .format("delta")

      val q = stream.start(outputDir.getCanonicalPath)
      q.processAllAvailable()

      // Validate progress so far.
      val progress = q.recentProgress.filter(_.numInputRows != 0)
      assert(progress.length === 2)
      progress.foreach { p => assert(p.numInputRows === 1) }
      checkAnswer(
        spark.read.format("delta").load(outputDir.getAbsolutePath),
        (1 until 3).toDF())

      q.stop()

      // More stuff happened since the stream stopped.
      addData(inputDir, value = 3) // More data. Optional.
      addData(inputDir, value = 4) // More data. Optional.
      addData(inputDir, value = 5) // More data. Optional.
      dropUnsupportedFeature(inputDir)

      // Query is restarted from checkpoint. Latest protocol looks clean because we dropped the
      // unsupported feature. Furthermore, the protocol upgrade is before the checkpoint, thus
      // we cannot come across it while streaming.
      // The initial state of the stream is null because it was stopped. As a result, the client
      // attempts to create a snapshot at the checkpoint version. This version contains the
      // unsupported feature and fails.
      withSQLConf(DeltaSQLConf.UNSUPPORTED_TESTING_FEATURES_ENABLED.key -> true.toString) {
        DeltaLog.clearCache()
        val q2 = stream.start(outputDir.getCanonicalPath)

        val e = intercept[StreamingQueryException] {
          // We come across the protocol upgrade commit and fail.
          q2.processAllAvailable()
        }
        assert(e.getCause.getMessage.contains("DELTA_UNSUPPORTED_FEATURES_FOR_READ"))
        q2.stop()
      }
    }
  }

  test("Protocol validations supress errors when snapshot cannot be reconstructed") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir)

      // Add some data.
      addData(inputDir, value = 0) // Version 0.
      addData(inputDir, value = 1) // Version 1.
      addData(inputDir, value = 2) // Version 2.
      addData(inputDir, value = 3) // Version 3.
      deltaLog.checkpoint(deltaLog.update()) // Version 3.
      addData(inputDir, value = 4) // Version 4.

      // Delete version 1.
      new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 1).toUri).delete()

      withSQLConf(
          DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL.key -> "true") {
        DeltaLog.clearCache()
        val q = spark.readStream
          .option(DeltaOptions.CDC_READ_OPTION, cdcReadEnabled)
          .format("delta")
          // Starting version exists but we cannot reconstruct a snapshot because version 1
          // is missing.
          .option("startingVersion", 2)
          .load(inputDir.getCanonicalPath)
          .writeStream
          .format("noop")
          .start()
        try {
          if (cdcReadEnabled) {
            // With CDC enabled, this scenario always produces an exception. In that sense,
            // CDC is more restrictive. This exception is produced in changesToDF when trying
            // to construct a snapshot at the starting version. This is existing
            // behaviour.
            assert(intercept[StreamingQueryException] {
              q.processAllAvailable()
            }.getCause.getMessage.contains("DELTA_VERSIONS_NOT_CONTIGUOUS"))
          } else {
            q.processAllAvailable()
          }
        } finally {
          q.stop()
        }
      }
    }
  }
}

class DeltaSourceFastDropFeatureCDCSuite extends DeltaSourceFastDropFeatureSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")
  }

  override protected def excluded: Seq[String] =
    super.excluded ++ Seq(
      // Excluded because in CDC streaming the current behaviour is to always check the protocol at
      // the starting version.
      "Protocol check at startingVersion is skipped when config is disabled")
}
