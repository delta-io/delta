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

package io.delta.sharing.spark

import java.time.LocalDateTime

import org.apache.spark.sql.delta.{DeltaIllegalStateException, DeltaLog}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.DeltaOptions.{
  IGNORE_CHANGES_OPTION,
  IGNORE_DELETES_OPTION,
  SKIP_CHANGE_COMMITS_OPTION
}
import org.apache.spark.sql.delta.sources.{DeltaSourceOffset, DeltaSQLConf}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.sharing.client.DeltaSharingRestClient
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import io.delta.sharing.spark.test.shims.SharingStreamingTestShims.{
  CheckpointFileManager,
  CommitMetadata,
  OffsetSeqLog,
  SerializedOffset,
  StreamingCheckpointConstants,
  StreamMetadata
}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamTest}
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  LongType,
  StringType,
  StructType,
  TimestampType
}

class DeltaFormatSharingSourceSuite
    extends StreamTest
    with DeltaSQLCommandTest
    with DeltaSharingTestSparkUtils
    with DeltaSharingDataSourceDeltaTestUtils {

  import testImplicits._

  private def getSource(parameters: Map[String, String]): DeltaFormatSharingSource = {
    val options = new DeltaSharingOptions(parameters)
    val path = options.options.getOrElse(
      "path",
      throw DeltaSharingErrors.pathNotSpecifiedException
    )
    val parsedPath = DeltaSharingRestClient.parsePath(path, Map.empty)
    val client = DeltaSharingRestClient(
      profileFile = parsedPath.profileFile,
      shareCredentialsOptions = Map.empty,
      forStreaming = true,
      responseFormat = "delta",
      readerFeatures = DeltaSharingUtils.STREAMING_SUPPORTED_READER_FEATURES.mkString(",")
    )
    val dsTable = DeltaSharingTable(
      share = parsedPath.share,
      schema = parsedPath.schema,
      name = parsedPath.table
    )
    DeltaFormatSharingSource(
      spark = spark,
      client = client,
      table = dsTable,
      options = options,
      parameters = parameters,
      sqlConf = sqlContext.sparkSession.sessionState.conf,
      metadataPath = ""
    )
  }

  private def assertBlocksAreCleanedUp(): Unit = {
    val blockManager = SparkEnv.get.blockManager
    val matchingBlockIds = blockManager.getMatchingBlockIds(
      _.name.startsWith(DeltaSharingLogFileSystem.DELTA_SHARING_LOG_BLOCK_ID_PREFIX)
    )
    assert(matchingBlockIds.isEmpty, "delta sharing blocks are not cleaned up.")
  }

  private def cleanUpDeltaSharingBlocks(): Unit = {
    val blockManager = SparkEnv.get.blockManager
    val matchingBlockIds = blockManager.getMatchingBlockIds(
      _.name.startsWith(
        DeltaSharingLogFileSystem.DELTA_SHARING_LOG_BLOCK_ID_PREFIX)
    )
    matchingBlockIds.foreach(blockManager.removeBlock(_))
  }

  test("DeltaFormatSharingSource able to get schema") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_schema"
      withTable(deltaTableName) {
        createTable(deltaTableName)
        val sharedTableName = "shared_table_schema"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        val profileFile = prepareProfileFile(tempDir)
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val deltaSharingSource = getSource(
            Map("path" -> s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          )
          val expectedSchema: StructType = new StructType()
            .add("c1", IntegerType)
            .add("c2", StringType)
            .add("c3", DateType)
            .add("c4", TimestampType)
          assert(deltaSharingSource.schema == expectedSchema)

          // CDF schema
          val cdfDeltaSharingSource = getSource(
            Map(
              "path" -> s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
              "readChangeFeed" -> "true"
            )
          )
          val expectedCdfSchema: StructType = expectedSchema
            .copy()
            .add("_change_type", StringType)
            .add("_commit_version", LongType)
            .add("_commit_timestamp", TimestampType)
          assert(cdfDeltaSharingSource.schema == expectedCdfSchema)
        }
      }
    }
  }

  test("DeltaFormatSharingSource do not support cdc") {
    withTempDir { tempDir =>
      val sharedTableName = "shared_streaming_table_nocdc"
      val profileFile = prepareProfileFile(tempDir)
      withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
        val e = intercept[Exception] {
          val df = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("readChangeFeed", "true")
            .load(tablePath)
          testStream(df)(
            AssertOnQuery { q =>
              q.processAllAvailable(); true
            }
          )
        }
        assert(e.getMessage.contains("Delta sharing cdc streaming is not supported"))
      }
    }
  }

  test("DeltaFormatSharingSource getTableVersion error") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_version_error"
      withTable(deltaTableName) {
        sql(
          s"""
             |CREATE TABLE $deltaTableName (value STRING)
             |USING DELTA
             |""".stripMargin)
        val sharedTableName = "shared_streaming_table_version_error"
        val profileFile = prepareProfileFile(tempDir)
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName, Some(-1L))
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
          val e = intercept[Exception] {
            val df = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
            testStream(df)(
              AssertOnQuery { q =>
                q.processAllAvailable(); true
              }
            )
          }
          assert(
            e.getMessage.contains("Delta Sharing Server returning negative table version:-1,")
          )
        }
      }
    }
  }

  // Test forceToDeltaSourceOffset directly: pass DeltaSharingSourceOffset JSON, call util.
  // Source construction requires getMetadata; use a real delta table and prepare mocks for
  // shared table "some_table". Flag on -> (DeltaSourceOffset, true); flag off -> throw.
  Seq(true, false).foreach { case autoResolve: Boolean =>
    test(s"forceToDeltaSourceOffset: DeltaSharingSourceOffset JSON with flag " +
      s"autoResolve=$autoResolve") {
      withTempDir { tempDir =>
        val deltaTableName = "delta_table_util_offset"
        withTable(deltaTableName) {
          createTable(deltaTableName)
          val sharedTableName = "some_table"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          val profileFile = prepareProfileFile(tempDir)
          val tableId = "test-table-id"
          val autoResolveKey = DeltaSQLConf
            .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT
            .key
          withSQLConf(
            (getDeltaSharingClassesSQLConf ++ Seq(
              autoResolveKey -> autoResolve.toString
            )).toSeq: _*
          ) {
            val source = getSource(
              Map("path" -> s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
            )
            val tableIdField = source.getClass.getDeclaredField("tableId")
            tableIdField.setAccessible(true)
            tableIdField.set(source, tableId)
            val legacyJson = "{\"sourceVersion\":1," +
              s""""tableId":"$tableId",""" +
              "\"tableVersion\":1," +
              "\"index\":-1," +
              "\"isStartingVersion\":true}"
            val serializedOffset = SerializedOffset(legacyJson)
            if (autoResolve) {
              val (deltaOffset, fromLegacy) = source.forceToDeltaSourceOffset(serializedOffset)
              assert(fromLegacy, "fromLegacy should be true for DeltaSharingSourceOffset JSON")
              assert(deltaOffset.reservoirId === tableId)
              assert(deltaOffset.reservoirVersion === 1L)
              assert(deltaOffset.index === DeltaSourceOffset.BASE_INDEX)
              assert(deltaOffset.isInitialSnapshot)
            } else {
              intercept[Exception](source.forceToDeltaSourceOffset(serializedOffset))
            }
            cleanUpDeltaSharingBlocks()
          }
        }
      }
    }
  }

  // E2E: Custom checkpoint with legacy DeltaSharingSourceOffset format;
  // restart with delta streaming using that checkpoint.
  // Flag on/off. Mocks use delta table only.
  Seq(
    (true, "flag on: restart with delta succeeds"),
    (false, "flag off: restart fails parsing legacy checkpoint")
  ).foreach { case (autoResolve, desc) =>
    test(s"E2E: parquet streaming checkpoint then restart " +
      s"with delta streaming [$desc]") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_e2e_parquet_then_delta"
      withTable(deltaTableName) {
        sql(s"""
               |CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA
               |""".stripMargin)
        sql(s"INSERT INTO $deltaTableName VALUES ('p1'), ('p2')")
        val tableId = DeltaLog.forTable(spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_streaming_table_e2e"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds",
          "10s"
        )

        // Build custom checkpoint with legacy DeltaSharingSourceOffset (no parquet stream run).
        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir = StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(new Path(checkpointPath, commitsDir))
        val metadataPath = new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(
          StreamMetadata(streamId), metadataPath, hadoopConf)
        val legacyOffsetJson =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":1," +
            "\"index\":-1," +
            "\"isStartingVersion\":true}"
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,""" +
            """"batchTimestampMs":0,""" +
            """"conf":{},""" +
            """"sourceMetadataInfo":{}}"""
        val offsetContent =
          s"v1\n$offsetMetadataJson\n$legacyOffsetJson"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offsetBatchPath = new Path(
          new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_OFFSETS), "0")
        val offsetOut = fileManager.createAtomic(offsetBatchPath, overwriteIfPossible = true)
        offsetOut.write(offsetContent)
        offsetOut.close()
        val commitContent = s"v1\n${CommitMetadata(0).json}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commitBatchPath = new Path(
          new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_COMMITS), "0")
        val commitOut = fileManager.createAtomic(commitBatchPath, overwriteIfPossible = true)
        commitOut.write(commitContent)
        commitOut.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT
          .key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> autoResolve.toString
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Snapshot getFiles(versionAsOf=1) for initial batch when resuming from legacy offset
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          if (autoResolve) {
            val q = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("p1", "p2").toDF())
          } else {
            var q: StreamingQuery = null
            val e = intercept[Exception] {
              q = spark.readStream
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .writeStream
                .format("delta")
                .option("checkpointLocation", checkpointDir.toString)
                .start(outputDir.toString)
              try {
                q.processAllAvailable()
              } finally {
                if (q != null) q.stop()
              }
            }
            assert(e.getMessage != null && (
              e.getMessage.contains("legacy") || e.getMessage.contains("checkpoint") ||
              e.getCause != null && (e.getCause.getMessage.contains("legacy") ||
                e.getCause.getMessage.contains("checkpoint"))),
              s"Expected legacy/checkpoint-related error, got: $e")
          }
        }
      }
    }
    }
  }

  // E2E: Legacy checkpoint with isStartingVersion=false (incremental
  // mode). The stream already processed through version 2, so on
  // restart it should pick up version 3 data.
  Seq(
    (true, "flag on: restart succeeds"),
    (false, "flag off: restart fails parsing legacy checkpoint")
  ).foreach { case (autoResolve, desc) =>
    test(s"E2E: legacy checkpoint isStartingVersion=false " +
      s"then restart with delta streaming [$desc]") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName =
        "delta_table_e2e_not_starting_version"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        sql(
          s"INSERT INTO $deltaTableName VALUES ('p1'), ('p2')")
        sql(
          s"INSERT INTO $deltaTableName VALUES ('p3'), ('p4')")
        sql(
          s"INSERT INTO $deltaTableName VALUES ('p5'), ('p6')")
        val tableId = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName =
          "shared_streaming_table_e2e_nsv"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming" +
            ".queryTableVersionIntervalSeconds",
          "10s"
        )

        // Two committed batches so that populateStartOffsets
        // calls getBatch(offset_0, offset_1) with a valid
        // startOffset instead of None.
        val checkpointPath =
          new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager =
          CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir =
          StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir =
          StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir =
          StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(
          new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(
          new Path(checkpointPath, commitsDir))
        val metadataPath =
          new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(
          StreamMetadata(streamId), metadataPath, hadoopConf)
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,""" +
            """"batchTimestampMs":0,""" +
            """"conf":{},""" +
            """"sourceMetadataInfo":{}}"""

        // Batch 0: legacy offset at version 1
        val legacyOffset0Json =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":1," +
            "\"index\":-1," +
            "\"isStartingVersion\":false}"
        val offset0Content =
          s"v1\n$offsetMetadataJson\n$legacyOffset0Json"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset0Path = new Path(new Path(
          checkpointPath, offsetsDir), "0")
        val offset0Out = fileManager.createAtomic(
          offset0Path, overwriteIfPossible = true)
        offset0Out.write(offset0Content)
        offset0Out.close()
        val commit0Content =
          s"v1\n${CommitMetadata(0).json}"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit0Path = new Path(new Path(
          checkpointPath, commitsDir), "0")
        val commit0Out = fileManager.createAtomic(
          commit0Path, overwriteIfPossible = true)
        commit0Out.write(commit0Content)
        commit0Out.close()

        // Batch 1: legacy offset at version 2
        val legacyOffset1Json =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":2," +
            "\"index\":-1," +
            "\"isStartingVersion\":false}"
        val offset1Content =
          s"v1\n$offsetMetadataJson\n$legacyOffset1Json"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset1Path = new Path(new Path(
          checkpointPath, offsetsDir), "1")
        val offset1Out = fileManager.createAtomic(
          offset1Path, overwriteIfPossible = true)
        offset1Out.write(offset1Content)
        offset1Out.close()
        val commit1Content =
          s"v1\n${CommitMetadata(1).json}"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit1Path = new Path(new Path(
          checkpointPath, commitsDir), "1")
        val commit1Out = fileManager.createAtomic(
          commit1Path, overwriteIfPossible = true)
        commit1Out.write(commit1Content)
        commit1Out.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT
          .key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> autoResolve.toString
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(
            deltaTableName, sharedTableName)
          // getBatch(offset_0, offset_1) uses offset_0 as
          // startingOffset (isInitialSnapshot=false) so the
          // streaming API is used from version 1 to 3.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 3L)
          prepareMockedClientGetTableVersion(
            deltaTableName, sharedTableName)

          if (autoResolve) {
            val q = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .load(tablePath)
              .writeStream
              .format("delta")
              .option("checkpointLocation",
                checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
            checkAnswer(
              spark.read.format("delta")
                .load(outputDir.getCanonicalPath),
              Seq("p3", "p4", "p5", "p6").toDF())
          } else {
            var q: StreamingQuery = null
            val e = intercept[Exception] {
              q = spark.readStream
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
                .writeStream
                .format("delta")
                .option("checkpointLocation",
                  checkpointDir.toString)
                .start(outputDir.toString)
              try {
                q.processAllAvailable()
              } finally {
                if (q != null) q.stop()
              }
            }
            assert(e.getMessage != null && (
              e.getMessage.contains("legacy") ||
              e.getMessage.contains("checkpoint") ||
              e.getCause != null && (
                e.getCause.getMessage.contains("legacy") ||
                e.getCause.getMessage
                  .contains("checkpoint"))),
              s"Expected legacy/checkpoint error, got: $e")
          }
        }
      }
    }
    }
  }

  test("DeltaFormatSharingSource simple query works") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_simple"
      withTable(deltaTableName) {
        sql(s"""
               |CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA
               |""".stripMargin)

        val sharedTableName = "shared_streaming_table_simple"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)

        val profileFile = prepareProfileFile(tempDir)
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

          def InsertToDeltaTable(values: String): Unit = {
            sql(s"INSERT INTO $deltaTableName VALUES $values")
          }

          InsertToDeltaTable("""("keep1"), ("keep2"), ("drop3")""")
          prepareMockedClientAndFileSystemResult(deltaTableName, sharedTableName, Some(1L))
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          val df = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .filter($"value" contains "keep")

          spark.sessionState.conf.setConfString(
            "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds",
            "9s"
          )
          val e = intercept[Exception] {
            testStream(df)(
              AssertOnQuery { q =>
                q.processAllAvailable(); true
              }
            )
          }
          assert(e.getMessage.contains("must not be less than 10 seconds"))

          spark.sessionState.conf.setConfString(
            "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds",
            "10s"
          )
          testStream(df)(
            AssertOnQuery { q =>
              q.processAllAvailable(); true
            },
            CheckAnswer("keep1", "keep2"),
            StopStream
          )
        }
      }
    }
  }

  // Mirror of batch auto-resolve test: grid over flag. When ON, getMetadata is used and we send
  // its format (delta or parquet); when OFF, user's responseFormat is used.
  Seq(
    (true, "shared_streaming_table_auto_resolve", "delta"),
    (true, "shared_parquet_table_auto_resolve", "parquet"),
    (false, "shared_parquet_table_streaming", "parquet"),
    (false, "shared_streaming_table_delta", "delta")
  ).foreach { case (autoResolve, sharedTableName, expectedFormat) =>
    test(s"streaming auto-resolve [flag=$autoResolve, " +
      s"format=$expectedFormat]") {
      withTempDir { tempDir =>
        val deltaTableName = "delta_table_auto_resolve"
        withTable(deltaTableName) {
          sql(s"DROP TABLE IF EXISTS $deltaTableName")
          sql(
            s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
          val profileFile = prepareProfileFile(tempDir)
          val tablePath =
            profileFile.getCanonicalPath +
              s"#share1.default.$sharedTableName"
          sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
          spark.sessionState.conf.setConfString(
            "spark.delta.sharing.streaming" +
              ".queryTableVersionIntervalSeconds",
            "10s"
          )
          if (autoResolve) {
            prepareMockedClientMetadata(
              deltaTableName, sharedTableName)
            if (expectedFormat == "delta") {
              prepareMockedClientAndFileSystemResult(
                deltaTableName, sharedTableName, Some(1L))
            } else {
              prepareMockedClientAndFileSystemResultForParquet(
                deltaTableName, sharedTableName)
              prepareMockedClientAndFileSystemResultForParquet(
                deltaTableName, sharedTableName,
                versionAsOf = Some(1L))
              prepareMockedClientAndFileSystemResultForStreaming(
                deltaTableName, sharedTableName, 1L, 1L)
            }
          } else {
            if (expectedFormat == "parquet") {
              prepareMockedClientAndFileSystemResultForParquet(
                deltaTableName, sharedTableName)
              prepareMockedClientAndFileSystemResultForParquet(
                deltaTableName, sharedTableName,
                versionAsOf = Some(1L))
              prepareMockedClientAndFileSystemResultForStreaming(
                deltaTableName, sharedTableName, 1L, 1L)
            } else {
              prepareMockedClientMetadata(
                deltaTableName, sharedTableName)
              prepareMockedClientAndFileSystemResult(
                deltaTableName, sharedTableName, Some(1L))
            }
          }
          prepareMockedClientGetTableVersion(
            deltaTableName, sharedTableName)
          val userResponseFormat =
            if (autoResolve) "parquet" else expectedFormat
          val autoResolveKey =
            DeltaSQLConf
              .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT
              .key
          withSQLConf(
            (getDeltaSharingClassesSQLConf +
              (autoResolveKey -> autoResolve.toString))
              .toSeq: _*
          ) {
            val df = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", userResponseFormat)
              .load(tablePath)
            testStream(df)(
              AssertOnQuery { q =>
                q.processAllAvailable(); true
              },
              CheckAnswer("a", "b"),
              StopStream
            )
            assertRequestedFormat(
              s"share1.default.$sharedTableName",
              Seq(expectedFormat))
          }
        }
      }
    }
  }

    test(
      "restart works sharing"
    ) {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        val deltaTableName = "delta_table_restart"
        withTable(deltaTableName) {
          createTableForStreaming(deltaTableName)
          val sharedTableName = "shared_streaming_table_restart"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          val profileFile = prepareProfileFile(inputDir)
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            def InsertToDeltaTable(values: String): Unit = {
              sql(s"INSERT INTO $deltaTableName VALUES $values")
            }

            // TODO: check testStream() function helper
            def processAllAvailableInStream(): Unit = {
              val q =
                  spark.readStream
                    .format("deltaSharing")
                    .option("responseFormat", "delta")
                    .load(tablePath)
                    .filter($"value" contains "keep")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", checkpointDir.toString)
                    .start(outputDir.toString)

              try {
                q.processAllAvailable()
              } finally {
                q.stop()
              }
            }

            // Able to stream snapshot at version 1.
            InsertToDeltaTable("""("keep1"), ("keep2"), ("drop1")""")
            prepareMockedClientAndFileSystemResult(
              deltaTable = deltaTableName,
              sharedTable = sharedTableName,
              versionAsOf = Some(1L)
            )
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2").toDF()
            )

            // No new data, so restart will not process any new data.
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2").toDF()
            )

            // Able to stream new data at version 2.
            InsertToDeltaTable("""("keep3"), ("keep4"), ("drop2")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              2,
              2
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            sql(s"""OPTIMIZE $deltaTableName""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              2,
              3
            )
            // Optimize doesn't produce new data, so restart will not process any new data.
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            // Able to stream new data at version 3.
            InsertToDeltaTable("""("keep5"), ("keep6"), ("drop3")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              3,
              4
            )

            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4", "keep5", "keep6").toDF()
            )
            assertBlocksAreCleanedUp()
          }
        }
      }
    }

    test(
      "restart works sharing with special chars"
    ) {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        val deltaTableName = "delta_table_restart_special"
        withTable(deltaTableName) {
          // scalastyle:off nonascii
          sql(s"""CREATE TABLE $deltaTableName (`第一列` STRING) USING DELTA""".stripMargin)
          val sharedTableName = "shared_streaming_table_special"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          val profileFile = prepareProfileFile(inputDir)
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            def InsertToDeltaTable(values: String): Unit = {
              sql(s"INSERT INTO $deltaTableName VALUES $values")
            }

            // TODO: check testStream() function helper
            def processAllAvailableInStream(): Unit = {
              val q =
                  spark.readStream
                    .format("deltaSharing")
                    .option("responseFormat", "delta")
                    .load(tablePath)
                    .filter($"第一列" contains "keep")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", checkpointDir.toString)
                    .start(outputDir.toString)
                  // scalastyle:on nonascii

              try {
                q.processAllAvailable()
              } finally {
                q.stop()
              }
            }

            // Able to stream snapshot at version 1.
            InsertToDeltaTable("""("keep1"), ("keep2"), ("drop1")""")
            prepareMockedClientAndFileSystemResult(
              deltaTable = deltaTableName,
              sharedTable = sharedTableName,
              versionAsOf = Some(1L)
            )
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2").toDF()
            )

            // No new data, so restart will not process any new data.
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2").toDF()
            )

            // Able to stream new data at version 2.
            InsertToDeltaTable("""("keep3"), ("keep4"), ("drop2")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              2,
              2
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            sql(s"""OPTIMIZE $deltaTableName""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              2,
              3
            )
            // Optimize doesn't produce new data, so restart will not process any new data.
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            // Able to stream new data at version 3.
            InsertToDeltaTable("""("keep5"), ("keep6"), ("drop3")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              3,
              4
            )

            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4", "keep5", "keep6").toDF()
            )
            assertBlocksAreCleanedUp()
          }
        }
      }
    }

  test("streaming works with deletes on basic table") {
    withTempDir { inputDir =>
      val deltaTableName = "delta_table_deletes"
      withTable(deltaTableName) {
        createTableForStreaming(deltaTableName)
        val sharedTableName = "shared_streaming_table_deletes"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          def InsertToDeltaTable(values: String): Unit = {
            sql(s"INSERT INTO $deltaTableName VALUES $values")
          }

          def processAllAvailableInStream(
              sourceOptions: Map[String, String],
              expectations: StreamAction*): Unit = {
            val df = spark.readStream
              .format("deltaSharing")
              .options(sourceOptions)
              .load(tablePath)

            val base = Seq(StartStream(), ProcessAllAvailable())
            testStream(df)((base ++ expectations): _*)
          }

          // Insert at version 1 and 2.
          InsertToDeltaTable("""("keep1")""")
          InsertToDeltaTable("""("keep2")""")
          // delete at version 3.
          sql(s"""DELETE FROM $deltaTableName WHERE value = "keep1" """)
          // update at version 4.
          sql(s"""UPDATE $deltaTableName SET value = "keep3" WHERE value = "keep2" """)

          prepareMockedClientAndFileSystemResult(
            deltaTable = deltaTableName,
            sharedTable = sharedTableName,
            versionAsOf = Some(4L)
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          processAllAvailableInStream(
            Map("responseFormat" -> "delta"),
            CheckAnswer("keep3")
          )

          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            0,
            4
          )

          // The streaming query will fail because changes detected in version 4.
          // This is the original delta behavior.
          val e = intercept[Exception] {
            processAllAvailableInStream(
              Map("responseFormat" -> "delta", "startingVersion" -> "0")
            )
          }
          for (msg <- Seq(
              "Detected",
              "not supported",
              "true"
            )) {
            assert(e.getMessage.contains(msg))
          }

          // The streaming query will fail because changes detected in version 4.
          // This is the original delta behavior.
          val e2 = intercept[Exception] {
            processAllAvailableInStream(
              Map(
                "responseFormat" -> "delta",
                "startingVersion" -> "0",
                IGNORE_DELETES_OPTION -> "true"
              )
            )
          }
          for (msg <- Seq(
              "Detected",
              "not supported",
              "true"
            )) {
            assert(e2.getMessage.contains(msg))
          }

          // The streaming query will succeed because ignoreChanges helps to ignore the updates, but
          // added updated data "keep3".
          processAllAvailableInStream(
            Map(
              "responseFormat" -> "delta",
              "startingVersion" -> "0",
              IGNORE_CHANGES_OPTION -> "true"
            ),
            CheckAnswer("keep1", "keep2", "keep3")
          )

          // The streaming query will succeed because skipChangeCommits helps to ignore the whole
          // commit with data update, so updated data is not produced either.
          processAllAvailableInStream(
            Map(
              "responseFormat" -> "delta",
              "startingVersion" -> "0",
              SKIP_CHANGE_COMMITS_OPTION -> "true"
            ),
            CheckAnswer("keep1", "keep2")
          )
          assertBlocksAreCleanedUp()
        }
      }
    }
  }

  test("streaming works with DV") {
    withTempDir { inputDir =>
      val deltaTableName = "delta_table_dv"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = false)
        spark.sql(
          s"ALTER TABLE $deltaTableName SET TBLPROPERTIES('delta.enableDeletionVectors' = true)"
        )
        val sharedTableName = "shared_streaming_table_dv"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          def InsertToDeltaTable(values: String): Unit = {
            sql(s"INSERT INTO $deltaTableName VALUES $values")
          }

          def processAllAvailableInStream(
              sourceOptions: Map[String, String],
              expectations: StreamAction*): Unit = {
            val df = spark.readStream
              .format("deltaSharing")
              .options(sourceOptions)
              .load(tablePath)
              .filter($"c2" contains "keep")
              .select("c1")

            val base = Seq(StartStream(), ProcessAllAvailable())
            testStream(df)((base ++ expectations): _*)
          }

          // Insert at version 2.
          InsertToDeltaTable("""(1, "keep1"),(2, "keep1"),(3, "keep1"),(1,"drop1")""")
          // delete at version 3.
          sql(s"""DELETE FROM $deltaTableName WHERE c1 >= 2 """)

          prepareMockedClientAndFileSystemResult(
            deltaTable = deltaTableName,
            sharedTable = sharedTableName,
            versionAsOf = Some(3L)
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          processAllAvailableInStream(
            Map("responseFormat" -> "delta"),
            CheckAnswer(1)
          )

          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            startingVersion = 0,
            endingVersion = 3,
            assertDVExists = true
          )

          // The streaming query will fail because deletes detected in version 3. And there are no
          // options provided to ignore the deletion.
          val e = intercept[Exception] {
            processAllAvailableInStream(
              Map("responseFormat" -> "delta", "startingVersion" -> "0")
            )
          }
          for (msg <- Seq(
              "Detected a data update",
              "not supported",
              SKIP_CHANGE_COMMITS_OPTION,
              "true"
            )) {
            assert(e.getMessage.contains(msg))
          }

          // The streaming query will fail because deletes detected in version 3, and it's
          // recognized as updates and ignoreDeletes doesn't help. This is the original delta
          // behavior.
          val e2 = intercept[Exception] {
            processAllAvailableInStream(
              Map(
                "responseFormat" -> "delta",
                "startingVersion" -> "0",
                IGNORE_DELETES_OPTION -> "true"
              )
            )
          }
          for (msg <- Seq(
              "Detected a data update",
              "not supported",
              SKIP_CHANGE_COMMITS_OPTION,
              "true"
            )) {
            assert(e2.getMessage.contains(msg))
          }

          // The streaming query will succeed because ignoreChanges helps to ignore the delete, but
          // added duplicated data 1.
          processAllAvailableInStream(
            Map(
              "responseFormat" -> "delta",
              "startingVersion" -> "0",
              IGNORE_CHANGES_OPTION -> "true"
            ),
            CheckAnswer(1, 2, 3, 1)
          )

          // The streaming query will succeed because skipChangeCommits helps to ignore the whole
          // commit with data update, so no duplicated data is produced either.
          processAllAvailableInStream(
            Map(
              "responseFormat" -> "delta",
              "startingVersion" -> "0",
              SKIP_CHANGE_COMMITS_OPTION -> "true"
            ),
            CheckAnswer(1, 2, 3)
          )
          assertBlocksAreCleanedUp()
        }
      }
    }
  }

  test("streaming works with timestampNTZ") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_timestampNTZ"
      withTable(deltaTableName) {
        sql(s"CREATE TABLE $deltaTableName(c1 TIMESTAMP_NTZ) USING DELTA")
        val sharedTableName = "shared_table_timestampNTZ"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(tempDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          def InsertToDeltaTable(values: String): Unit = {
            sql(s"INSERT INTO $deltaTableName VALUES $values")
          }

          def processAllAvailableInStream(
            sourceOptions: Map[String, String],
            expectations: StreamAction*): Unit = {
            val df = spark.readStream
              .format("deltaSharing")
              .options(sourceOptions)
              .load(tablePath)
              .select("c1")

            val base = Seq(StartStream(), ProcessAllAvailable())
            testStream(df)((base ++ expectations): _*)
          }

          // Insert at version 1.
          InsertToDeltaTable("""('2022-01-01 02:03:04.123456')""")
          // Insert at version 2.
          InsertToDeltaTable("""('2022-02-02 03:04:05.123456')""")

          prepareMockedClientAndFileSystemResult(
            deltaTable = deltaTableName,
            sharedTable = sharedTableName,
            versionAsOf = Some(2L)
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          processAllAvailableInStream(
            Map("responseFormat" -> "delta"),
            CheckAnswer(
              LocalDateTime.parse("2022-01-01T02:03:04.123456"),
              LocalDateTime.parse("2022-02-02T03:04:05.123456")
            )
          )

          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            startingVersion = 2,
            endingVersion = 2
          )
          processAllAvailableInStream(
            Map(
              "responseFormat" -> "delta",
              "startingVersion" -> "2"
            ),
            CheckAnswer(LocalDateTime.parse("2022-02-02T03:04:05.123456"))
          )
          assertBlocksAreCleanedUp()
        }
      }
    }
  }

    test(
      "startingVersion works"
    ) {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        val deltaTableName = "delta_table_startVersion"
        withTable(deltaTableName) {
          createTableForStreaming(deltaTableName)
          val sharedTableName = "shared_streaming_table_startVersion"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          val profileFile = prepareProfileFile(inputDir)
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            def InsertToDeltaTable(values: String): Unit = {
              sql(s"INSERT INTO $deltaTableName VALUES $values")
            }

            def processAllAvailableInStream(): Unit = {
              val q =
                  spark.readStream
                    .format("deltaSharing")
                    .option("responseFormat", "delta")
                    .option("startingVersion", 0)
                    .load(tablePath)
                    .filter($"value" contains "keep")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", checkpointDir.toString)
                    .start(outputDir.toString)

              try {
                q.processAllAvailable()
              } finally {
                q.stop()
              }
            }

            // Able to stream snapshot at version 1.
            InsertToDeltaTable("""("keep1"), ("keep2"), ("drop1")""")
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTable = deltaTableName,
              sharedTable = sharedTableName,
              startingVersion = 0L,
              endingVersion = 1L
            )
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2").toDF()
            )

            // No new data, so restart will not process any new data.
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2").toDF()
            )

            // Able to stream new data at version 2.
            InsertToDeltaTable("""("keep3"), ("keep4"), ("drop2")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              0,
              2
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            sql(s"""OPTIMIZE $deltaTableName""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              2,
              3
            )
            // Optimize doesn't produce new data, so restart will not process any new data.
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            // No new data, so restart will not process any new data. It will ask for the
            // last commit so that it can figure out that there's nothing to do.
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              3,
              3
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4").toDF()
            )

            // Able to stream new data at version 3.
            InsertToDeltaTable("""("keep5"), ("keep6"), ("drop3")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              3,
              4
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4", "keep5", "keep6").toDF()
            )

            // No new data, so restart will not process any new data. It will ask for the
            // last commit so that it can figure out that there's nothing to do.
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              4,
              4
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq("keep1", "keep2", "keep3", "keep4", "keep5", "keep6").toDF()
            )
            assertBlocksAreCleanedUp()
          }
        }
      }
    }

  test(
    "files are in a stable order for streaming"
  ) {
    // This test function is to check that DeltaSharingLogFileSystem puts the files in the delta log
    // in a stable order for each commit, regardless of the returning order from the server, so that
    // the DeltaSource can produce a stable file index.
    // We are using maxBytesPerTrigger which causes the streaming to stop in the middle of a commit
    // to be able to test this behavior.
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      withTempDirs { (_, outputDir2, checkpointDir2) =>
        val deltaTableName = "delta_table_order"
        withTable(deltaTableName) {
          createSimpleTable(deltaTableName, enableCdf = false)
          val sharedTableName = "shared_streaming_table_order"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          val profileFile = prepareProfileFile(inputDir)
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

          def InsertToDeltaTable(values: String): Unit = {
            sql(s"INSERT INTO $deltaTableName VALUES $values")
          }

          // Able to stream snapshot at version 1.
          InsertToDeltaTable("""(1, "one"), (2, "two"), (3, "three")""")

          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            def processAllAvailableInStream(
                outputDirStr: String,
                checkpointDirStr: String): Unit = {
              val q = spark.readStream
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .option("maxBytesPerTrigger", "1b")
                .load(tablePath)
                .writeStream
                .format("delta")
                .option("checkpointLocation", checkpointDirStr)
                .start(outputDirStr)

              try {
                q.processAllAvailable()
                val progress = q.recentProgress.filter(_.numInputRows != 0)
                assert(progress.length === 3)
                progress.foreach { p =>
                  assert(p.numInputRows === 1)
                }
              } finally {
                q.stop()
              }
            }

            // First output, without reverseFileOrder
            prepareMockedClientAndFileSystemResult(
              deltaTable = deltaTableName,
              sharedTable = sharedTableName,
              versionAsOf = Some(1L)
            )
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            processAllAvailableInStream(outputDir.toString, checkpointDir.toString)
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              Seq((1, "one"), (2, "two"), (3, "three")).toDF()
            )

            // Second output, with reverseFileOrder = true
            prepareMockedClientAndFileSystemResult(
              deltaTable = deltaTableName,
              sharedTable = sharedTableName,
              versionAsOf = Some(1L),
              reverseFileOrder = true
            )
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            processAllAvailableInStream(outputDir2.toString, checkpointDir2.toString)
            checkAnswer(
              spark.read.format("delta").load(outputDir2.getCanonicalPath),
              Seq((1, "one"), (2, "two"), (3, "three")).toDF()
            )

            // Check each version of the two output are the same, which means the files are sorted
            // by DeltaSharingLogFileSystem, and are processed in a deterministic order by the
            // DeltaSource.
            val deltaLog = DeltaLog.forTable(spark, new Path(outputDir.toString))
            Seq(0, 1, 2).foreach { v =>
              val version = deltaLog.snapshot.version - v
              val df1 = spark.read
                .format("delta")
                .option("versionAsOf", version)
                .load(outputDir.getCanonicalPath)
              val df2 = spark.read
                .format("delta")
                .option("versionAsOf", version)
                .load(outputDir2.getCanonicalPath)
              checkAnswer(df1, df2)
              assert(df1.count() == (3 - v))
            }
            assertBlocksAreCleanedUp()
          }
        }
      }
    }
  }

    test(
      "DeltaFormatSharingSource query with two delta sharing tables works"
    ) {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        val deltaTableName = "delta_table_two"

        def InsertToDeltaTable(values: String): Unit = {
          sql(s"INSERT INTO $deltaTableName VALUES $values")
        }

        withTable(deltaTableName) {
          createSimpleTable(deltaTableName, enableCdf = false)
          val sharedTableName = "shared_streaming_table_two"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)

          val profileFile = prepareProfileFile(inputDir)
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            InsertToDeltaTable("""(1, "one"), (2, "one")""")
            InsertToDeltaTable("""(1, "two"), (2, "two")""")
            InsertToDeltaTable("""(1, "three"), (2, "three")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResult(
              deltaTableName,
              sharedTableName,
              Some(3L)
            )
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              startingVersion = 1,
              endingVersion = 3
            )

            def processAllAvailableInStream(): Unit = {
              val dfLatest = spark.readStream
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .load(tablePath)
              val dfV1 = spark.readStream
                .format("deltaSharing")
                .option("responseFormat", "delta")
                .option("startingVersion", 1)
                .load(tablePath)
                .select(col("c2"), col("c1").as("v1c1"))
                .filter(col("v1c1") === 1)

              val q =
                  dfLatest
                    .join(dfV1, "c2")
                    .writeStream
                    .format("delta")
                    .option("checkpointLocation", checkpointDir.toString)
                    .start(outputDir.toString)

              try {
                q.processAllAvailable()
              } finally {
                q.stop()
              }
            }

            // c1 from dfLatest, c2 from dfLatest, c1 from dfV1
            var expected = Seq(
              Row("one", 1, 1),
              Row("one", 2, 1),
              Row("two", 1, 1),
              Row("two", 2, 1),
              Row("three", 1, 1),
              Row("three", 2, 1)
            )
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              expected
            )

            InsertToDeltaTable("""(1, "four"), (2, "four")""")
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              startingVersion = 4,
              endingVersion = 4
            )
            prepareMockedClientAndFileSystemResultForStreaming(
              deltaTableName,
              sharedTableName,
              startingVersion = 1,
              endingVersion = 4
            )

            expected = expected ++ Seq(Row("four", 1, 1), Row("four", 2, 1))
            processAllAvailableInStream()
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              expected
            )
            assertBlocksAreCleanedUp()
          }
        }
      }
    }

    Seq(
      ("add a partition column", Seq("part"), Seq("is_even", "part")),
      ("change partition order", Seq("part", "is_even"), Seq("is_even", "part")),
      ("different partition column", Seq("part"), Seq("is_even"))
    ).foreach {
      case (repartitionTestCase, initPartitionCols, overwritePartitionCols) =>
        test(
          "deltaSharing - repartition delta source should fail by default " +
          s"unless unsafe flag is set - $repartitionTestCase"
        ) {
          withTempDirs { (inputDir, outputDir, checkpointDir) =>
            val deltaTableName = "basic_delta_table_partition_check"
            withTable(deltaTableName) {
              spark.sql(
                s"""CREATE TABLE $deltaTableName (id LONG, part INT, is_even BOOLEAN)
                   |USING DELTA PARTITIONED BY (${initPartitionCols.mkString(", ")})
                   |""".stripMargin
              )
              val sharedTableName = "shared_streaming_table_partition_check_" +
                s"${repartitionTestCase.replace(' ', '_')}"
              prepareMockedClientMetadata(deltaTableName, sharedTableName)
              val profileFile = prepareProfileFile(inputDir)
              val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

              withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {

                def processAllAvailableInStream(startingVersion: Int): Unit = {
                  val q =
                      spark.readStream
                        .format("deltaSharing")
                        .option("responseFormat", "delta")
                        .option("skipChangeCommits", "true")
                        .option("startingVersion", startingVersion)
                        .load(tablePath)
                        .writeStream
                        .format("delta")
                        .option("checkpointLocation", checkpointDir.toString)
                        .start(outputDir.toString)

                  try {
                    q.processAllAvailable()
                  } finally {
                    q.stop()
                  }
                }

                spark.range(10).withColumn("part", lit(1))
                  .withColumn("is_even", $"id" % 2 === 0).write
                  .format("delta").partitionBy(initPartitionCols: _*)
                  .mode("append")
                  .saveAsTable(deltaTableName)
                spark.range(2).withColumn("part", lit(2))
                  .withColumn("is_even", $"id" % 2 === 0).write
                  .format("delta").partitionBy(initPartitionCols: _*)
                  .mode("append").saveAsTable(deltaTableName)
                spark.range(10).withColumn("part", lit(1))
                  .withColumn("is_even", $"id" % 2 === 0).write
                  .format("delta").partitionBy(overwritePartitionCols: _*)
                  .option("overwriteSchema", "true").mode("overwrite")
                  .saveAsTable(deltaTableName)
                spark.range(2).withColumn("part", lit(2))
                  .withColumn("is_even", $"id" % 2 === 0).write
                  .format("delta").partitionBy(overwritePartitionCols: _*)
                  .mode("append").saveAsTable(deltaTableName)

                prepareMockedClientAndFileSystemResultForStreaming(
                  deltaTable = deltaTableName,
                  sharedTable = sharedTableName,
                  startingVersion = 0L,
                  endingVersion = 4L
                )
                prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
                var e = intercept[StreamingQueryException] {
                  processAllAvailableInStream(0)
                }
                assert(e.getCause.asInstanceOf[DeltaIllegalStateException].getErrorClass
                  == "DELTA_SCHEMA_CHANGED_WITH_STARTING_OPTIONS")
                assert(e.getMessage.contains("Detected schema change in version 3"))

                // delta table created using sql with specified partition col
                // will construct their initial snapshot on the initial definition
                prepareMockedClientAndFileSystemResultForStreaming(
                  deltaTable = deltaTableName,
                  sharedTable = sharedTableName,
                  startingVersion = 4L,
                  endingVersion = 4L
                )
                prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
                e = intercept[StreamingQueryException] {
                  processAllAvailableInStream(4)
                }
                assert(e.getMessage.contains("Detected schema change in version 4"))

                // Streaming query made progress without throwing error when
                // unsafe flag is set to true
                withSQLConf(
                  DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_PARTITION_COLUMN_CHANGE.key -> "true"
                ) {
                  processAllAvailableInStream(0)
                }
              }
            }
          }
        }
    }

    test("streaming variant query works") {
      withTempDirs { (inputDir, outputDir, checkpointDir) =>
        val deltaTableName = "variant_table"
        withTable(deltaTableName) {
          sql(s"create table $deltaTableName (v VARIANT) using delta")

          val sharedTableName = "shared_variant_table"
          prepareMockedClientMetadata(deltaTableName, sharedTableName)

          val profileFile = prepareProfileFile(inputDir)
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

            sql(s"""insert into table $deltaTableName
                select parse_json(format_string('{"key": %s}', id))
                from range(0, 10)
            """)

            prepareMockedClientAndFileSystemResult(
              deltaTableName,
              sharedTableName,
              versionAsOf = Some(1L)
            )
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

            val q =
                spark.readStream
                  .format("deltaSharing")
                  .option("responseFormat", "delta")
                  .load(tablePath)
                  .writeStream
                  .format("delta")
                  .option("checkpointLocation", checkpointDir.toString)
                  .start(outputDir.toString)

            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }

            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath),
              spark.sql(s"select * from $deltaTableName")
            )
          }
        }
      }
    }

  // E2E test: Legacy checkpoint at a version boundary (index=-1, i.e., the
  // "lucky case") with isStartingVersion=true. The stream should transition
  // fully to DeltaSourceOffset and use SHA256 file IDs.
  test("E2E: legacy checkpoint at version boundary (lucky case) transitions cleanly") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_lucky_case"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('c'), ('d')")
        val tableId = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_lucky_case"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds", "10s")

        // Build checkpoint: legacy offset at version boundary (index=-1)
        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir = StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(new Path(checkpointPath, commitsDir))
        val metadataPath = new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(StreamMetadata(streamId), metadataPath, hadoopConf)
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""

        // Batch 0: version 1, at boundary (index=-1), initial snapshot
        val legacyOffset0Json =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":1," +
            "\"index\":-1," +
            "\"isStartingVersion\":true}"
        val offset0Content =
          s"v1\n$offsetMetadataJson\n$legacyOffset0Json"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset0Path = new Path(new Path(checkpointPath, offsetsDir), "0")
        val offset0Out = fileManager.createAtomic(offset0Path, overwriteIfPossible = true)
        offset0Out.write(offset0Content)
        offset0Out.close()
        val commit0Content = s"v1\n${CommitMetadata(0).json}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit0Path = new Path(new Path(checkpointPath, commitsDir), "0")
        val commit0Out = fileManager.createAtomic(commit0Path, overwriteIfPossible = true)
        commit0Out.write(commit0Content)
        commit0Out.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // getBatch(None, offset_0) with isInitialSnapshot=true uses
          // snapshot at the same version.
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Since the legacy offset is at a version boundary (index=-1 -> BASE_INDEX),
          // this is the "lucky case": the stream should use SHA256 file IDs and
          // fetch multiple versions normally.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 2L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          val q = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.toString)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }
          // Replayed batch 0 produces v1 snapshot, then version 2
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq("a", "b", "c", "d").toDF())

          // Validate the final offset is in DeltaSourceOffset format
          // (has reservoirVersion, not tableVersion).
          val offsetLog = new OffsetSeqLog(
            spark, s"${checkpointDir.getCanonicalPath}/offsets")
          val (latestBatchId, latestOffsetSeq) = offsetLog.getLatest().get
          val offsetJson = latestOffsetSeq.offsets.head.get.json()
          assert(offsetJson.contains("reservoirVersion"),
            s"Expected DeltaSourceOffset (reservoirVersion) but got: $offsetJson")
          assert(!offsetJson.contains("tableVersion"),
            s"Expected no legacy tableVersion in final offset but got: $offsetJson")
        }
      }
    }
  }

  // E2E: Legacy checkpoint mid-version (index != -1, the "unlucky case")
  // with isStartingVersion=false. The stream should restrict to 1 version
  // with MD5 file IDs, then transition after reaching a boundary.
  test("E2E: legacy checkpoint mid-version (unlucky case) " +
    "finishes version before transitioning") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_unlucky_case"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        sql(s"INSERT INTO $deltaTableName VALUES ('v1a'), ('v1b')")
        // Version 2: exactly 2 files (1 row each) for the mid-version case.
        // parallelize with numSlices=2 places each row in its own partition.
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row("v2a"), Row("v2b")), 2),
          new StructType().add("value", StringType)
        ).write.mode("append").insertInto(deltaTableName)
        sql(s"INSERT INTO $deltaTableName VALUES ('v3a'), ('v3b')")
        val tableId = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_unlucky_case"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds", "10s")

        // Build checkpoint: legacy offset mid-version (index=0, not -1)
        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir = StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(new Path(checkpointPath, commitsDir))
        val metadataPath = new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(StreamMetadata(streamId), metadataPath, hadoopConf)
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""

        // Batch 0: mid-version at version 2, index 0 (processed 1 of 2 files)
        val legacyOffset0Json =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":2," +
            "\"index\":0," +
            "\"isStartingVersion\":false}"
        val offset0Content =
          s"v1\n$offsetMetadataJson\n$legacyOffset0Json"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset0Path = new Path(new Path(checkpointPath, offsetsDir), "0")
        val offset0Out = fileManager.createAtomic(offset0Path, overwriteIfPossible = true)
        offset0Out.write(offset0Content)
        offset0Out.close()
        val commit0Content = s"v1\n${CommitMetadata(0).json}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit0Path = new Path(new Path(checkpointPath, commitsDir), "0")
        val commit0Out = fileManager.createAtomic(commit0Path, overwriteIfPossible = true)
        commit0Out.write(commit0Content)
        commit0Out.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // getBatch(None, offset_0) computes starting offset at
          // version 1 as initial snapshot, so we need this mock.
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // For mid-version case, the stream should restrict to version 2
          // only and use MD5 file IDs.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
          // After transitioning at version boundary, stream fetches
          // remaining versions normally.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 3L, 3L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          val q = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.toString)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }
          // The legacy checkpoint was at version 2, index 0 (1 of 2 files
          // processed). The stream finishes the remaining file in version 2,
          // transitions to DeltaSourceOffset at the version boundary, then
          // processes version 3 normally.
          val outputValues = spark.read.format("delta")
            .load(outputDir.getCanonicalPath)
            .collect().map(_.getString(0)).toSet
          assert(outputValues.size == 3,
            s"Expected 3 rows (1 remaining v2 + 2 v3) but got: $outputValues")
          assert(outputValues.contains("v3a") && outputValues.contains("v3b"),
            s"Expected v3a and v3b in output but got: $outputValues")
          assert(outputValues.intersect(Set("v2a", "v2b")).size == 1,
            s"Expected exactly one v2 row from the unprocessed file but got: $outputValues")

          // Validate the final offset has transitioned to DeltaSourceOffset format.
          val offsetLog = new OffsetSeqLog(
            spark, s"${checkpointDir.getCanonicalPath}/offsets")
          val (latestBatchId, latestOffsetSeq) = offsetLog.getLatest().get
          val offsetJson = latestOffsetSeq.offsets.head.get.json()
          assert(offsetJson.contains("reservoirVersion"),
            s"Expected DeltaSourceOffset (reservoirVersion) but got: $offsetJson")
          assert(!offsetJson.contains("tableVersion"),
            s"Expected no legacy tableVersion in final offset but got: $offsetJson")
        }
      }
    }
  }

  // Test that fileIdHash is correctly passed to the server: SHA256 for
  // version-boundary legacy offsets (lucky case), MD5 for mid-version
  // legacy offsets (unlucky case), and SHA256 after transition.
  test("fileIdHash: SHA256 for boundary legacy offset") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_fileidhash"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('c'), ('d')")
        val tableId = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_fileidhash"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds", "10s")

        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir = StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(new Path(checkpointPath, commitsDir))
        val metadataPath = new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(StreamMetadata(streamId), metadataPath, hadoopConf)
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""

        // Version boundary (lucky case) should use SHA256
        val boundaryOffsetJson =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":1," +
            "\"index\":-1," +
            "\"isStartingVersion\":true}"
        val offset0Content =
          s"v1\n$offsetMetadataJson\n$boundaryOffsetJson"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset0Path = new Path(new Path(checkpointPath, offsetsDir), "0")
        val offset0Out = fileManager.createAtomic(offset0Path, overwriteIfPossible = true)
        offset0Out.write(offset0Content)
        offset0Out.close()
        val commit0Content = s"v1\n${CommitMetadata(0).json}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit0Path = new Path(new Path(checkpointPath, commitsDir), "0")
        val commit0Out = fileManager.createAtomic(commit0Path, overwriteIfPossible = true)
        commit0Out.write(commit0Content)
        commit0Out.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 2L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          TestClientForDeltaFormatSharing.clearFileIdHashHistory()

          val q = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.toString)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }

          // All streaming getFiles calls should use SHA256 for the
          // lucky case (version boundary).
          val history = TestClientForDeltaFormatSharing.getFileIdHashHistory
          val streamingCalls = history.filter { case (name, qt, _) =>
            name == sharedTableName && qt.startsWith("getFiles_streaming")
          }
          assert(streamingCalls.nonEmpty, "Expected at least one streaming getFiles call")
          streamingCalls.foreach { case (_, queryType, fileIdHash) =>
            assert(fileIdHash.contains(DeltaSharingRestClient.FILEIDHASH_SHA256),
              s"Expected SHA256 for boundary legacy offset but got $fileIdHash in $queryType")
          }
        }
      }
    }
  }

  test("fileIdHash: MD5 for mid-version legacy offset") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_fileidhash_mid"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        sql(s"INSERT INTO $deltaTableName VALUES ('e'), ('f')")
        sql(s"INSERT INTO $deltaTableName VALUES ('g'), ('h')")
        sql(s"INSERT INTO $deltaTableName VALUES ('i'), ('j')")
        val tableId = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_fileidhash_mid"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds", "10s")

        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir = StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(new Path(checkpointPath, commitsDir))
        val metadataPath = new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(StreamMetadata(streamId), metadataPath, hadoopConf)
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""

        // Mid-version offset: version 2, index 0 (not at boundary)
        val midVersionOffsetJson =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":2," +
            "\"index\":0," +
            "\"isStartingVersion\":false}"
        val offset0Content =
          s"v1\n$offsetMetadataJson\n$midVersionOffsetJson"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset0Path = new Path(new Path(checkpointPath, offsetsDir), "0")
        val offset0Out = fileManager.createAtomic(offset0Path, overwriteIfPossible = true)
        offset0Out.write(offset0Content)
        offset0Out.close()
        val commit0Content = s"v1\n${CommitMetadata(0).json}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit0Path = new Path(new Path(checkpointPath, commitsDir), "0")
        val commit0Out = fileManager.createAtomic(commit0Path, overwriteIfPossible = true)
        commit0Out.write(commit0Content)
        commit0Out.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // getBatch recovery loads snapshot at version 2-1=1
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Mid-version: restricted to version 2 only (MD5)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
          // After transition: fetches from version 2 onwards normally (SHA256)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 3L, 3L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          TestClientForDeltaFormatSharing.clearFileIdHashHistory()

          val q = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .load(tablePath)
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.toString)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }

          val history = TestClientForDeltaFormatSharing.getFileIdHashHistory
          val streamingCalls = history.filter { case (name, qt, _) =>
            name == sharedTableName && qt.startsWith("getFiles_streaming")
          }
          assert(streamingCalls.nonEmpty, "Expected at least one streaming getFiles call")
          // The first streaming call should use MD5 (mid-version legacy),
          // subsequent calls after transition should use SHA256.
          val firstCall = streamingCalls.head
          assert(firstCall._3.contains(DeltaSharingRestClient.FILEIDHASH_MD5),
            s"Expected MD5 for mid-version legacy offset but got ${firstCall._3}")
          if (streamingCalls.size > 1) {
            streamingCalls.tail.foreach { case (_, queryType, fileIdHash) =>
              assert(fileIdHash.contains(DeltaSharingRestClient.FILEIDHASH_SHA256),
                s"Expected SHA256 after transition but got $fileIdHash in $queryType")
            }
          }
        }
      }
    }
  }

  // E2E: Legacy checkpoint mid-version with multiple files per version.
  // Uses maxFilesPerTrigger=1 so that deltaSource.latestOffset returns a
  // mid-version end offset, which triggers convertDeltaSourceOffsetToLegacyOffset.
  // Validates that intermediate batch offsets stay in legacy format and
  // the final batch transitions to DeltaSourceOffset.
  test("E2E: convertDeltaSourceOffsetToLegacyOffset - intermediate batches " +
    "stay legacy until version boundary") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_legacy_convert_e2e"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        // Version 1: 1 file (2 rows)
        sql(s"INSERT INTO $deltaTableName VALUES ('v1a'), ('v1b')")
        // Version 2: 3 files via repartition (1 row each)
        Seq("v2a", "v2b", "v2c").toDF("value")
          .repartition(3)
          .write.insertInto(deltaTableName)
        // Version 3: 1 file (2 rows)
        sql(s"INSERT INTO $deltaTableName VALUES ('v3a'), ('v3b')")
        val deltaLog = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
        val tableId = deltaLog.update().metadata.id

        // Verify version 2 has multiple files (added in that version)
        val v2Changes = deltaLog.getChanges(2).next()._2
          .collect { case a: AddFile if a.dataChange => a }
        assert(v2Changes.size >= 2,
          s"Expected multiple files in version 2 but got ${v2Changes.size}")

        val sharedTableName = "shared_legacy_convert_e2e"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds", "10s")

        // Build checkpoint: legacy offset mid-version at version 2, index 0
        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
        val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
        val metaDir = StreamingCheckpointConstants.DIR_NAME_METADATA
        fileManager.mkdirs(new Path(checkpointPath, offsetsDir))
        fileManager.mkdirs(new Path(checkpointPath, commitsDir))
        val metadataPath = new Path(checkpointPath, metaDir)
        val streamId = java.util.UUID.randomUUID.toString
        StreamMetadata.write(StreamMetadata(streamId), metadataPath, hadoopConf)
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""

        // Batch 0: mid-version at version 2, index 0 (processed 1 of 3 files)
        val legacyOffset0Json =
          "{\"sourceVersion\":1," +
            s""""tableId":"$tableId",""" +
            "\"tableVersion\":2," +
            "\"index\":0," +
            "\"isStartingVersion\":false}"
        val offset0Content =
          s"v1\n$offsetMetadataJson\n$legacyOffset0Json"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offset0Path = new Path(new Path(checkpointPath, offsetsDir), "0")
        val offset0Out = fileManager.createAtomic(offset0Path, overwriteIfPossible = true)
        offset0Out.write(offset0Content)
        offset0Out.close()
        val commit0Content = s"v1\n${CommitMetadata(0).json}"
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val commit0Path = new Path(new Path(checkpointPath, commitsDir), "0")
        val commit0Out = fileManager.createAtomic(commit0Path, overwriteIfPossible = true)
        commit0Out.write(commit0Content)
        commit0Out.close()

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Mid-version: restrict to version 2 only with MD5 file IDs
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
          // After transitioning at version boundary, fetch remaining versions
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 3L, 3L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

          // maxFilesPerTrigger=1 forces deltaSource.latestOffset to return
          // mid-version end offsets, triggering convertDeltaSourceOffsetToLegacyOffset
          val q = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("maxFilesPerTrigger", "1")
            .load(tablePath)
            .writeStream
            .format("delta")
            .option("checkpointLocation", checkpointDir.toString)
            .start(outputDir.toString)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }

          val offsetLog = new OffsetSeqLog(
            spark, s"${checkpointDir.getCanonicalPath}/offsets")

          // Validate intermediate batch offsets are in legacy format.
          // Batch 0 is the synthetic legacy checkpoint we created.
          // Batch 1 is the first micro-batch after restart; since both
          // start (index=0) and end (index=1) are mid-version,
          // convertDeltaSourceOffsetToLegacyOffset should fire.
          val batch1OffsetOpt = offsetLog.get(1)
          assert(batch1OffsetOpt.isDefined, "Expected batch 1 in offset log")
          val batch1Json = batch1OffsetOpt.get.offsets.head.get.json()
          assert(batch1Json.contains("tableVersion"),
            s"Expected legacy offset (tableVersion) in batch 1 but got: $batch1Json")
          assert(!batch1Json.contains("reservoirVersion"),
            s"Expected no DeltaSourceOffset in batch 1 but got: $batch1Json")

          // Validate the final offset has transitioned to DeltaSourceOffset format.
          val (_, finalOffsetSeq) = offsetLog.getLatest().get
          val finalJson = finalOffsetSeq.offsets.head.get.json()
          assert(finalJson.contains("reservoirVersion"),
            s"Expected DeltaSourceOffset (reservoirVersion) in final offset but got: $finalJson")
          assert(!finalJson.contains("tableVersion"),
            s"Expected no legacy tableVersion in final offset but got: $finalJson")
        }
      }
    }
  }

  // Test convertDeltaSourceOffsetToLegacyOffset: verify that a DeltaSourceOffset
  // is correctly converted back to a legacy DeltaSharingSourceOffset.
  test("convertDeltaSourceOffsetToLegacyOffset produces valid legacy offset") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_convert_back"
      withTable(deltaTableName) {
        createTable(deltaTableName)
        val sharedTableName = "shared_convert_back"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(tempDir)
        val tableId = "test-table-convert-back"
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val source = getSource(
            Map("path" -> s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          )
          val tableIdField = source.getClass.getDeclaredField("tableId")
          tableIdField.setAccessible(true)
          tableIdField.set(source, tableId)

          // Use reflection to call the private convertDeltaSourceOffsetToLegacyOffset
          val method = source.getClass.getDeclaredMethod(
            "convertDeltaSourceOffsetToLegacyOffset",
            classOf[DeltaSourceOffset]
          )
          method.setAccessible(true)

          val deltaOffset = DeltaSourceOffset(
            reservoirId = tableId,
            reservoirVersion = 5L,
            index = 3L,
            isInitialSnapshot = false
          )
          val result = method.invoke(source, deltaOffset)
          // Call json() via the connector Offset interface
          val jsonMethod = result.getClass.getMethod("json")
          val json = jsonMethod.invoke(result).asInstanceOf[String]

          // Verify the legacy offset JSON contains expected fields
          assert(json.contains("\"sourceVersion\":1"), s"Expected sourceVersion:1 in $json")
          assert(json.contains(s""""tableId":"$tableId""""), s"Expected tableId in $json")
          assert(json.contains("\"tableVersion\":5"), s"Expected tableVersion:5 in $json")
          assert(json.contains("\"index\":3"), s"Expected index:3 in $json")
          assert(json.contains("\"isStartingVersion\":false"),
            s"Expected isStartingVersion:false in $json")

          // Round-trip: the legacy offset JSON should be parseable back via
          // forceToDeltaSourceOffset
          val autoResolveKey = DeltaSQLConf
            .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
          withSQLConf(autoResolveKey -> "true") {
            val serialized = SerializedOffset(json)
            val (roundTripped, fromLegacy) = source.forceToDeltaSourceOffset(serialized)
            assert(fromLegacy, "Should be detected as legacy")
            assert(roundTripped.reservoirId === tableId)
            assert(roundTripped.reservoirVersion === 5L)
            // Index -1 maps to BASE_INDEX, but index 3 should stay 3
            assert(roundTripped.index === 3L)
            assert(!roundTripped.isInitialSnapshot)
          }
          cleanUpDeltaSharingBlocks()
        }
      }
    }
  }
}
