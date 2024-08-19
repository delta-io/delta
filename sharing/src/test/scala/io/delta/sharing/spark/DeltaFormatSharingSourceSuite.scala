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

import org.apache.spark.sql.delta.DeltaIllegalStateException
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOptions.{
  IGNORE_CHANGES_OPTION,
  IGNORE_DELETES_OPTION,
  SKIP_CHANGE_COMMITS_OPTION
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.sharing.client.DeltaSharingRestClient
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.Row
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.streaming.StreamTest
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
    val parsedPath = DeltaSharingRestClient.parsePath(path)
    val client = DeltaSharingRestClient(
      profileFile = parsedPath.profileFile,
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
            e.getMessage.contains("Delta Sharing Server returning negative table version: -1.")
          )
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

  test("restart works sharing") {
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
            val q = spark.readStream
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

  test("restart works sharing with special chars") {
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
            val q = spark.readStream
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

  test("startingVersion works") {
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
            val q = spark.readStream
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

          // No new data, so restart will not process any new data. It will ask for the last commit
          // so that it can figure out that there's nothing to do.
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

          // No new data, so restart will not process any new data. It will ask for the last commit
          // so that it can figure out that there's nothing to do.
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

  test("files are in a stable order for streaming") {
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

  test("DeltaFormatSharingSource query with two delta sharing tables works") {
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

            val q = dfLatest
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
  ).foreach { case (repartitionTestCase, initPartitionCols, overwritePartitionCols) =>
    test("deltaSharing - repartition delta source should fail by default " +
      s"unless unsafe flag is set - $repartitionTestCase") {
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
              val q = spark.readStream
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
              DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_PARTITION_COLUMN_CHANGE.key -> "true") {
              processAllAvailableInStream(0)
            }
          }
        }
      }
    }
  }
}
