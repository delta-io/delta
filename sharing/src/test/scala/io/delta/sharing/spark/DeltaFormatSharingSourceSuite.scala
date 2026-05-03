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
import org.apache.spark.sql.delta.DeltaOptions.{
  IGNORE_CHANGES_OPTION,
  IGNORE_DELETES_OPTION,
  SKIP_CHANGE_COMMITS_OPTION
}
import org.apache.spark.sql.delta.actions.AddFile
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
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamTest, Trigger}
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

  /**
   * Initialize a streaming checkpoint directory with offsets, commits, and metadata subdirs.
   * Returns (checkpointPath, fileManager) for use with writeLegacyOffsetAndCommit.
   */
  private def initCheckpointDirs(
      checkpointDir: java.io.File): (Path, CheckpointFileManager) = {
    val checkpointPath = new Path(checkpointDir.getCanonicalPath)
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
    fileManager.mkdirs(
      new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_OFFSETS))
    fileManager.mkdirs(
      new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_COMMITS))
    StreamMetadata.write(
      StreamMetadata(java.util.UUID.randomUUID.toString),
      new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_METADATA),
      hadoopConf)
    (checkpointPath, fileManager)
  }

  /**
   * Write a legacy DeltaSharingSourceOffset and its corresponding commit entry
   * into a streaming checkpoint directory.
   */
  private def writeLegacyOffsetAndCommit(
      fileManager: CheckpointFileManager,
      checkpointPath: Path,
      batchId: Long,
      tableId: String,
      tableVersion: Long,
      index: Long,
      isStartingVersion: Boolean): Unit = {
    val offsetMetadataJson =
      """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""
    val legacyOffsetJson =
      s"""{"sourceVersion":1,"tableId":"$tableId",""" +
        s""""tableVersion":$tableVersion,"index":$index,""" +
        s""""isStartingVersion":$isStartingVersion}"""
    val offsetsDir = StreamingCheckpointConstants.DIR_NAME_OFFSETS
    val commitsDir = StreamingCheckpointConstants.DIR_NAME_COMMITS
    val offsetContent =
      s"v1\n$offsetMetadataJson\n$legacyOffsetJson"
        .getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val offsetPath = new Path(new Path(checkpointPath, offsetsDir), batchId.toString)
    val offsetOut = fileManager.createAtomic(offsetPath, overwriteIfPossible = true)
    offsetOut.write(offsetContent)
    offsetOut.close()
    val commitContent = s"v1\n${CommitMetadata(batchId).json}"
      .getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val commitPath = new Path(new Path(checkpointPath, commitsDir), batchId.toString)
    val commitOut = fileManager.createAtomic(commitPath, overwriteIfPossible = true)
    commitOut.write(commitContent)
    commitOut.close()
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

  test("DeltaFormatSharingSource CDF streaming: initial snapshot tags all rows as insert") {
    withTempDirs { (_, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_cdf_initial_snapshot"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA
               |TBLPROPERTIES (delta.enableChangeDataFeed = true)""".stripMargin)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "a"), (2, "b")""")

        val sharedTableName = "shared_cdf_initial_snapshot"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        // No startingVersion: DeltaSource uses isInitialSnapshot=true at the latest version (1),
        // so the server is queried via getFiles(versionAsOf=1), not getCDFFiles.
        prepareMockedClientAndFileSystemResult(
          deltaTableName, sharedTableName, versionAsOf = Some(1L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        withTempDir { tempDir =>
          val profileFile = prepareProfileFile(tempDir)
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
            val q = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .load(tablePath)
              .select("c1", "c2", "_change_type", "_commit_version", "_commit_timestamp")
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
            val output = spark.read.format("delta").load(outputDir.getCanonicalPath)
            val result = output.select("c1", "c2", "_change_type", "_commit_version").orderBy("c1")
            checkAnswer(result, Seq(
              Row(1, "a", "insert", 1L),
              Row(2, "b", "insert", 1L)
            ))
            assert(
              output.select("_commit_timestamp").collect().forall(r => !r.isNullAt(0)),
              "_commit_timestamp should be non-null for all rows"
            )
          }
        }
      }
    }
  }

  test("DeltaFormatSharingSource CDF streaming: incremental inserts updates and deletes") {
    withTempDirs { (_, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_cdf_incremental"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA
               |TBLPROPERTIES (delta.enableChangeDataFeed = true)""".stripMargin)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "a"), (2, "b")""") // version 1
        sql(s"""UPDATE $deltaTableName SET c2 = "updated" WHERE c1 = 1""") // version 2
        sql(s"""DELETE FROM $deltaTableName WHERE c1 = 2""") // version 3

        val sharedTableName = "shared_cdf_incremental"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        // initial snapshot at version 0 (empty)
        prepareMockedClientAndFileSystemResult(
          deltaTableName, sharedTableName, versionAsOf = Some(0L))
        // incremental CDF from version 1 onwards
        prepareMockedClientAndFileSystemResultForCdf(deltaTableName, sharedTableName, 1L)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        withTempDir { tempDir =>
          val profileFile = prepareProfileFile(tempDir)
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
            val q = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .load(tablePath)
              .select("c1", "c2", "_change_type", "_commit_version", "_commit_timestamp")
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
            // Expect inserts from v1, update preimage/postimage from v2, delete from v3
            val result = spark.read.format("delta").load(outputDir.getCanonicalPath)
              .select("c1", "c2", "_change_type", "_commit_version", "_commit_timestamp")
              .orderBy("c1", "_change_type")
            val expected = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .table(deltaTableName)
              .select("c1", "c2", "_change_type", "_commit_version", "_commit_timestamp")
              .orderBy("c1", "_change_type")
            checkAnswer(result, expected)
          }
        }
      }
    }
  }

  test("DeltaFormatSharingSource CDF streaming: DV-based delete produces delete change rows") {
    // Verifies that when a CDF-enabled table also has deletion vectors enabled, a DELETE
    // operation (which writes a DV file on the data file alongside the CDC file) is correctly
    // processed by applyTableFileChanges and produces _change_type=delete rows in the output.
    withTempDirs { (_, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_cdf_dv"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA
               |TBLPROPERTIES (
               |  'delta.enableChangeDataFeed' = 'true',
               |  'delta.enableDeletionVectors' = 'true'
               |)""".stripMargin)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "a"), (2, "b"), (3, "c")""") // v1
        sql(s"""DELETE FROM $deltaTableName WHERE c1 = 2""") // v2: DV-based delete

        val sharedTableName = "shared_cdf_dv"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientAndFileSystemResult(
          deltaTableName, sharedTableName, versionAsOf = Some(0L))
        prepareMockedClientAndFileSystemResultForCdf(deltaTableName, sharedTableName, 1L)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        withTempDir { tempDir =>
          val profileFile = prepareProfileFile(tempDir)
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
            val q = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .load(tablePath)
              .select("c1", "c2", "_change_type", "_commit_version")
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
            // v1 inserts + v2 DV-based delete should all appear as CDF rows
            val result = spark.read.format("delta").load(outputDir.getCanonicalPath)
              .select("c1", "c2", "_change_type", "_commit_version")
              .orderBy("c1", "_change_type")
            val expected = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .table(deltaTableName)
              .select("c1", "c2", "_change_type", "_commit_version")
              .orderBy("c1", "_change_type")
            checkAnswer(result, expected)
          }
        }
      }
    }
  }

  test("DeltaFormatSharingSource CDF streaming: sourceSchema includes CDF columns") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_cdf_schema_check"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA
               |TBLPROPERTIES (delta.enableChangeDataFeed = true)""".stripMargin)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "a")""")

        val sharedTableName = "shared_cdf_schema_check"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(tempDir)
        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
          val df = spark.readStream
            .format("deltaSharing")
            .option("responseFormat", "delta")
            .option("readChangeFeed", "true")
            .load(tablePath)
          val fieldTypes = df.schema.fields.map(f => f.name -> f.dataType).toMap
          assert(fieldTypes.contains("c1") && fieldTypes.contains("c2"))
          assert(fieldTypes.get("_change_type").contains(StringType),
            s"_change_type should be StringType in schema: ${df.schema}")
          assert(fieldTypes.get("_commit_version").contains(LongType),
            s"_commit_version should be LongType in schema: ${df.schema}")
          assert(fieldTypes.get("_commit_timestamp").contains(TimestampType),
            s"_commit_timestamp should be TimestampType in schema: ${df.schema}")
        }
      }
    }
  }

  test("DeltaFormatSharingSource CDF streaming: schema change within CDF range") {
    // Verifies that includeHistoricalMetadata=true causes Metadata actions for schema changes
    // to be included in the getCDFFiles response, allowing DeltaSource to handle backward-
    // compatible schema changes (e.g., ADD COLUMN) gracefully: the stream continues and the
    // new column is null for rows written before the schema change.
    // Non-backward-compatible changes (e.g., DROP COLUMN) instead fail the stream; see
    // "non-backward-compatible schema change fails stream" below.
    withTempDirs { (_, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_cdf_schema_change"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA
               |TBLPROPERTIES (delta.enableChangeDataFeed = true)""".stripMargin)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "a"), (2, "b")""") // version 1

        // version 2: backward-compatible schema change - add nullable column
        sql(s"""ALTER TABLE $deltaTableName ADD COLUMN c3 STRING""")

        // version 3: insert with the new column present
        sql(s"""INSERT INTO $deltaTableName VALUES (3, "c", "new_c3")""")

        val sharedTableName = "shared_cdf_schema_change"
        // prepareMockedClientMetadata uses the latest snapshot (schema c1, c2, c3).
        // sourceSchema() for CDF streaming will return (c1, c2, c3, CDF cols).
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        // Initial empty snapshot used when DeltaSource initialises at v0.
        prepareMockedClientAndFileSystemResult(
          deltaTableName, sharedTableName, versionAsOf = Some(0L))
        // CDF from v1: prepareMockedClientAndFileSystemResultForCdf includes
        //   - Protocol + Metadata at v1 (schema c1, c2)
        //   - Metadata at v2 (schema c1, c2, c3) - the mid-range schema change
        //   - AddFile / AddCDCFile actions at v1 and v3
        prepareMockedClientAndFileSystemResultForCdf(deltaTableName, sharedTableName, 1L)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        withTempDir { tempDir =>
          val profileFile = prepareProfileFile(tempDir)
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

            // Run CDF stream with startingVersion=1
            val q = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .load(tablePath)
              .select("c1", "c2", "_change_type")
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }

            // All rows from v1 (inserts before schema change) and v3 (inserts after schema change)
            // should be present. c3 is projected away in the select. The stream must NOT fail even
            // though a Metadata action for the schema change was in the CDF response.
            val result = spark.read.format("delta").load(outputDir.getCanonicalPath)
              .select("c1", "c2", "_change_type").orderBy("c1")
            val expected = spark.read
              .format("delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .table(deltaTableName)
              .select("c1", "c2", "_change_type").orderBy("c1")
            checkAnswer(result, expected)
          }
        }
      }
    }
  }

  test("DeltaFormatSharingSource CDF streaming: non-backward-compatible schema change fails") {
    // Verifies that a non-backward-compatible Metadata action within the CDF range causes
    // the stream to fail on restart. Structure:
    //   Run 1: process v1 inserts, checkpoint cleanly.
    //   Then add v2 (DROP COLUMN) and v3 (insert with reduced schema) to the mocked server.
    //   Run 2: restart from checkpoint; DeltaSource sees the Metadata action at v2 in the
    //          getCDFFiles response (delivered via includeHistoricalMetadata=true) and fails.
    // Verifies:
    //   - Run 1 successfully writes v1 rows to the sink.
    //   - Run 2 throws DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG.
    //   - v1 rows remain in the sink after Run 2 (nothing new is committed).
    // This is the counterpart to the backward-compatible ADD COLUMN test above.
    withTempDirs { (_, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_cdf_schema_break"
      withTable(deltaTableName) {
        // Column mapping (CM) mode is required for DROP COLUMN in Delta; regular Delta tables do
        // not support dropping columns. minReaderVersion=2 and minWriterVersion=5 are the minimum
        // protocol versions required by CM mode.
        sql(s"""CREATE TABLE $deltaTableName (c1 INT, c2 STRING) USING DELTA
               |TBLPROPERTIES (
               |  'delta.enableChangeDataFeed' = 'true',
               |  'delta.columnMapping.mode' = 'name',
               |  'delta.minReaderVersion' = '2',
               |  'delta.minWriterVersion' = '5'
               |)""".stripMargin)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "a"), (2, "b")""") // version 1

        val sharedTableName = "shared_cdf_schema_break"
        // Initial mocks reflect only v1 (pre-schema-change state).
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientAndFileSystemResult(
          deltaTableName, sharedTableName, versionAsOf = Some(0L))
        prepareMockedClientAndFileSystemResultForCdf(deltaTableName, sharedTableName, 1L)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        withTempDir { tempDir =>
          val profileFile = prepareProfileFile(tempDir)
          withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
            val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

            // Run 1: process v1 inserts and checkpoint cleanly (no schema change yet).
            val q1 = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .load(tablePath)
              .select("c1", "_change_type", "_commit_version")
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            try {
              q1.processAllAvailable()
            } finally {
              q1.stop()
            }

            // Verify v1 rows were successfully written in Run 1.
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath)
                .select("c1", "_change_type", "_commit_version").orderBy("c1"),
              Seq(Row(1, "insert", 1L), Row(2, "insert", 1L))
            )

            // Now introduce the non-backward-compatible schema change and a post-change insert.
            sql(s"""ALTER TABLE $deltaTableName DROP COLUMN c2""") // version 2
            sql(s"""INSERT INTO $deltaTableName VALUES (3)""") // version 3

            // Re-mock so the server now reports v1+v2+v3. prepareMockedClientMetadata reflects
            // the latest snapshot (c1 only, after DROP COLUMN). The CDF response at
            // startingVersion=1 now includes Metadata at v1 (c1, c2) and Metadata at v2 (c1
            // only) + data files.
            prepareMockedClientMetadata(deltaTableName, sharedTableName)
            prepareMockedClientAndFileSystemResultForCdf(deltaTableName, sharedTableName, 1L)
            prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

            // Run 2: restart from the same checkpoint. DeltaSource detects the v2 schema
            // change as non-backward-compatible and fails the stream.
            val q2 = spark.readStream
              .format("deltaSharing")
              .option("responseFormat", "delta")
              .option("readChangeFeed", "true")
              .option("startingVersion", "1")
              .load(tablePath)
              .select("c1", "_change_type", "_commit_version")
              .writeStream
              .format("delta")
              .option("checkpointLocation", checkpointDir.toString)
              .start(outputDir.toString)
            val e = intercept[Exception] {
              try {
                q2.processAllAvailable()
              } finally {
                q2.stop()
              }
            }
            assert(
              e.getMessage.contains("DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG"),
              s"Expected schema change error, got: ${e.getMessage}")

            // v1 rows are still in the sink; no v3 rows (stream stopped at v2).
            checkAnswer(
              spark.read.format("delta").load(outputDir.getCanonicalPath)
                .select("c1", "_change_type", "_commit_version").orderBy("c1"),
              Seq(Row(1, "insert", 1L), Row(2, "insert", 1L))
            )
          }
        }
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
        // Version 1: 2 files (1 row each) via repartition so index=0
        // represents a mid-version offset with isStartingVersion=true.
        spark.createDataFrame(
          spark.sparkContext.parallelize(Seq(Row("p1"), Row("p2")), 2),
          new StructType().add("value", StringType)
        ).write.insertInto(deltaTableName)
        sql(s"INSERT INTO $deltaTableName VALUES ('p3'), ('p4')")
        val tableId = DeltaLog.forTable(spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_parquet_table_e2e_legacy"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds",
          "10s"
        )

        // Build custom checkpoint with legacy DeltaSharingSourceOffset (no parquet stream run).
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)
        // Mid-version legacy offset: index=0, isStartingVersion=true
        // means we're in the middle of processing the initial snapshot
        // at v1 (processed 1 of 2 files).
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = 0,
          isStartingVersion = true)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT
          .key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> autoResolve.toString
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Legacy source needs parquet-format metadata at v1 for delegation
          // to RemoteDeltaLog.
          prepareMockedClientAndFileSystemResultForParquet(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Mid-version: restricted to v1 only (MD5) to finish
          // remaining files in the initial snapshot.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // After transition at v1 boundary, stream fetches v2 normally.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 2L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
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
            // The stream finishes the remaining v1 file (1 of p1/p2),
            // then processes v2 (p3, p4).
            val outputValues = spark.read.format("delta")
              .load(outputDir.getCanonicalPath)
              .collect().map(_.getString(0)).toSet
            assert(outputValues.size == 3,
              s"Expected 3 rows (1 remaining v1 + 2 v2) but got: $outputValues")
            assert(outputValues.contains("p3") && outputValues.contains("p4"),
              s"Expected p3 and p4 in output but got: $outputValues")
            assert(outputValues.intersect(Set("p1", "p2")).size == 1,
              s"Expected exactly one of p1/p2 from remaining v1 file but got: $outputValues")
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
        // Batch 0: legacy offset at version 1
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = -1,
          isStartingVersion = false)
        // Batch 1: legacy offset at version 2
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

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
          // Priming getBatch(offset_0, offset_1): endOffset is
          // legacy (v2, index=-1 -> BASE_INDEX), so
          // endVersion = 2 - 1 = 1. Fetches v1 only (MD5).
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // After priming, latestOffset starts from v2 and
          // fetches v2-v3.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
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
  // "lucky case") with isStartingVersion=false. The stream should transition
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

        // Build checkpoint: legacy offset at version boundary (index=-1),
        // isStartingVersion=false because index=-1 means past initial snapshot.
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)
        // Batch 0: version 2, at boundary (index=-1), not initial snapshot
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // getBatch(None, offset_0) with isInitialSnapshot=false:
          // getStartingOffset returns (v2-1=v1, isInitialSnapshot=true).
          // endVersion = 2-1 = 1. Priming fetches snapshot at v1.
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Since the legacy offset is at a version boundary (index=-1 -> BASE_INDEX),
          // this is the "lucky case": latestOffset uses SHA256 file IDs and
          // fetches from v2 onwards.
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
          // Priming replays v1 snapshot (already committed), then v2 is processed.
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq("c", "d").toDF())

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
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)

        // Batch 0: finished processing version 0 (index=-1 means starting version 1)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = -1,
          isStartingVersion = false)
        // Batch 1: mid-version at version 2, index 0 (processed 1 of 2 files)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = 0,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Priming getBatch(offset_0, offset_1) fetches streaming v1-v2.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 2L)
          // Mid-version: restricted to version 2 only (MD5).
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

        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)

        // Version boundary (lucky case) with isStartingVersion=false
        // (index=-1 means past initial snapshot)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Priming: getStartingOffset returns (v2-1=v1, true),
          // endVersion = 2-1 = 1. Snapshot at v1.
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Lucky case: latestOffset uses SHA256 from v2 onwards.
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
            assert(fileIdHash.contains(DeltaSharingRestClient.FILEIDHASH_DELTA),
              s"Expected DELTA for boundary legacy offset but got $fileIdHash in $queryType")
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

        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)

        // Batch 0: finished processing version 0 (index=-1 means starting version 1)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = -1,
          isStartingVersion = false)
        // Batch 1: mid-version at version 2, index 0 (not at boundary)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = 0,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Priming getBatch(offset_0, offset_1) fetches streaming v1-v2.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 2L)
          // Mid-version: restricted to version 2 only (MD5).
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 2L)
          // After transition: fetches from version 2 onwards normally (SHA256).
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
          assert(firstCall._3.contains(DeltaSharingRestClient.FILEIDHASH_PARQUET),
            s"Expected PARQUET for mid-version legacy offset but got ${firstCall._3}")
          if (streamingCalls.size > 1) {
            streamingCalls.tail.foreach { case (_, queryType, fileIdHash) =>
              assert(fileIdHash.contains(DeltaSharingRestClient.FILEIDHASH_DELTA),
                s"Expected DELTA after transition but got $fileIdHash in $queryType")
            }
          }
        }
      }
    }
  }

  // E2E: On restart the engine primes with getBatch(start=legacy, end=legacy) before
  // calling latestOffset. When both offsets are from a legacy checkpoint, the file
  // fetch must use MD5 fileIdHash to match the legacy source's ordering. This test
  // creates two committed legacy batches (both at version boundary), restarts, and
  // verifies that the priming getBatch uses MD5 for the initial file fetch.
  test("fileIdHash: MD5 for priming getBatch when both start and end are legacy") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_fileidhash_priming"
      withTable(deltaTableName) {
        sql(s"""CREATE TABLE $deltaTableName (value STRING)
               |USING DELTA""".stripMargin)
        sql(s"INSERT INTO $deltaTableName VALUES ('a1'), ('a2')")
        sql(s"INSERT INTO $deltaTableName VALUES ('b1'), ('b2')")
        sql(s"INSERT INTO $deltaTableName VALUES ('c1'), ('c2')")
        val tableId = DeltaLog.forTable(
          spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_fileidhash_priming"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath +
          s"#share1.default.$sharedTableName"
        spark.sessionState.conf.setConfString(
          "spark.delta.sharing.streaming.queryTableVersionIntervalSeconds", "10s")

        // Two committed legacy batches: batch 0 at version 1, batch 1 at version 2.
        // On restart, the engine calls getBatch(offset_0, offset_1) as priming -- both legacy.
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)

        // Batch 0: legacy offset at version 1 (boundary)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = -1,
          isStartingVersion = false)
        // Batch 1: legacy offset at version 2 (boundary)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Priming getBatch(offset_0, offset_1): endOffset is legacy
          // (v2, index=-1 -> BASE_INDEX), so endVersion = 2 - 1 = 1.
          // Fetches version 1 only (MD5).
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // After priming, latestOffset starts from v2 and fetches v2-v3.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
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

          // Verify data correctness: the priming getBatch replays version 1 to 2
          // data. Then latestOffset picks up version 3.
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq("b1", "b2", "c1", "c2").toDF())

          // The first streaming getFiles call (from priming getBatch) should use MD5
          // because both start and end offsets are from legacy checkpoints.
          val history = TestClientForDeltaFormatSharing.getFileIdHashHistory
          val streamingCalls = history.filter { case (name, qt, _) =>
            name == sharedTableName && qt.startsWith("getFiles_streaming")
          }
          assert(streamingCalls.nonEmpty, "Expected at least one streaming getFiles call")
          val firstCall = streamingCalls.head
          assert(firstCall._3.contains(DeltaSharingRestClient.FILEIDHASH_PARQUET),
            s"Expected PARQUET for priming getBatch with both-legacy offsets but got " +
            s"${firstCall._3}")
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
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)

        // Batch 0: finished processing version 0 (index=-1 means starting version 1)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = -1,
          isStartingVersion = false)
        // Batch 1: mid-version at version 2, index 0 (processed 1 of 3 files)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = 0,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Priming getBatch(offset_0, offset_1) fetches streaming v1-v2.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 2L)
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
          // Batch 0 and 1 are the synthetic legacy checkpoint we created.
          // Batch 2 is the first micro-batch after restart; since both
          // start (index=0) and end (index=1) are mid-version,
          // convertDeltaSourceOffsetToLegacyOffset should fire.
          val batch2OffsetOpt = offsetLog.get(2)
          assert(batch2OffsetOpt.isDefined, "Expected batch 2 in offset log")
          val batch2Json = batch2OffsetOpt.get.offsets.head.get.json()
          assert(batch2Json.contains("tableVersion"),
            s"Expected legacy offset (tableVersion) in batch 2 but got: $batch2Json")
          assert(!batch2Json.contains("reservoirVersion"),
            s"Expected no DeltaSourceOffset in batch 2 but got: $batch2Json")

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

  // ---- Legacy initial snapshot offset transition tests ----

  // Legacy stream already finished the initial snapshot in the first batch.
  // On restart, start=None and end is the legacy offset (version boundary),
  // so the stream transitions to new format immediately.
  test("legacy offset transition: initial snapshot completed in first batch") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_legacy_transition_1"
      withTable(deltaTableName) {
        sql(s"CREATE TABLE $deltaTableName (value STRING) USING DELTA")
        sql(s"INSERT INTO $deltaTableName VALUES ('v1a'), ('v1b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('v2a'), ('v2b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('v3a'), ('v3b')")
        val tableId = DeltaLog.forTable(spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_legacy_transition_1"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        // Single legacy offset at batch 0: version=2, index=-1 (version boundary),
        // isStartingVersion=false. The legacy stream finished the initial snapshot
        // and processed through version 2 in one batch.
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Priming getBatch(None, offset_0): endOffset is legacy
          // (v2, index=-1 -> BASE_INDEX), so endVersion = 2 - 1 = 1.
          // Needs snapshot at v1 for getFiles.
          prepareMockedClientAndFileSystemResult(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // After priming, latestOffset starts from v2 and fetches v2-v3.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
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

          // Should get v2 and v3 data (v1 was already processed by legacy stream).
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq("v2a", "v2b", "v3a", "v3b").toDF())

          // Validate the final offset has transitioned to DeltaSourceOffset format.
          val offsetLog = new OffsetSeqLog(
            spark, s"${checkpointDir.getCanonicalPath}/offsets")
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

  // Legacy stream is still processing the initial snapshot (mid-version).
  // On restart, the start offset is a legacy initial snapshot with index > BASE_INDEX,
  // so the stream should delegate to the legacy DeltaSharingSource to continue
  // processing the remaining snapshot files.
  test("legacy offset transition: snapshot still in progress, delegates to legacy source") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_legacy_transition_2"
      withTable(deltaTableName) {
        sql(s"CREATE TABLE $deltaTableName (value STRING) USING DELTA")
        // Version 1: 4 files (1 row each) so index 0, 1 and 2 are mid-version.
        spark.createDataFrame(
          spark.sparkContext.parallelize(
            Seq(Row("v1a"), Row("v1b"), Row("v1c"), Row("v1d")), 4),
          new StructType().add("value", StringType)
        ).write.insertInto(deltaTableName)
        sql(s"INSERT INTO $deltaTableName VALUES ('v2a'), ('v2b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('v3a'), ('v3b')")
        val tableId = DeltaLog.forTable(spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_parquet_table_legacy_transition_2"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        // Two legacy offsets still in the initial snapshot:
        // Batch 0: v1, index=0, isStartingVersion=true (processed 1 of 4 files)
        // Batch 1: v1, index=1, isStartingVersion=true (processed 2 of 4 files)
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = 0,
          isStartingVersion = true)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 1, index = 1,
          isStartingVersion = true)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Legacy source needs parquet-format metadata at v1 for delegation
          // to RemoteDeltaLog.
          prepareMockedClientAndFileSystemResultForParquet(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Mid-version: restricted to v1 only (MD5) to finish
          // remaining files in the initial snapshot.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // After transition at v1 boundary, stream fetches v2-v3.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 3L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 3L, 3L)
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

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

          // The stream finishes the remaining v1 files (2 of v1a/v1b/v1c/v1d),
          // then processes v2 and v3.
          val outputValues = spark.read.format("delta")
            .load(outputDir.getCanonicalPath)
            .collect().map(_.getString(0)).toSet
          assert(outputValues.size == 6,
            s"Expected 6 rows (2 remaining v1 + 2 v2 + 2 v3) but got: $outputValues")
          assert(outputValues.intersect(Set("v1a", "v1b", "v1c", "v1d")).size == 2,
            s"Expected exactly two of v1a/v1b/v1c/v1d from remaining v1 files " +
              s"but got: $outputValues")
          assert(outputValues.contains("v2a") && outputValues.contains("v2b"),
            s"Expected v2a and v2b in output but got: $outputValues")
          assert(outputValues.contains("v3a") && outputValues.contains("v3b"),
            s"Expected v3a and v3b in output but got: $outputValues")

          // Validate offsets 0, 1, 2, 3 are in legacy format (all v1 files)
          // and offset 4 is in the new DeltaSourceOffset format.
          val offsetLog = new OffsetSeqLog(
            spark, s"${checkpointDir.getCanonicalPath}/offsets")
          for (batchId <- 0 to 3) {
            val legacyOffsetSeq = offsetLog.get(batchId).get
            val legacyJson = legacyOffsetSeq.offsets.head.get.json()
            assert(legacyJson.contains("tableVersion"),
              s"Expected legacy offset (tableVersion) at batch $batchId " +
                s"but got: $legacyJson")
            assert(!legacyJson.contains("reservoirVersion"),
              s"Expected no reservoirVersion at batch $batchId but got: $legacyJson")
          }
          val newOffsetSeq = offsetLog.get(4).get
          val newJson = newOffsetSeq.offsets.head.get.json()
          assert(newJson.contains("reservoirVersion"),
            s"Expected DeltaSourceOffset (reservoirVersion) at batch 4 " +
              s"but got: $newJson")
          assert(!newJson.contains("tableVersion"),
            s"Expected no legacy tableVersion at batch 4 but got: $newJson")
        }
      }
    }
  }

  // Legacy stream start offset is still an initial snapshot, but the snapshot
  // finishes within the last batch so the end offset reaches the version boundary
  // (startVersion+1, index=-1). The stream transitions to new format.
  test("legacy offset transition: snapshot finishes in last batch, transitions to new format") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_legacy_transition_3"
      withTable(deltaTableName) {
        sql(s"CREATE TABLE $deltaTableName (value STRING) USING DELTA")
        sql(s"INSERT INTO $deltaTableName VALUES ('v1a'), ('v1b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('v2a'), ('v2b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('v3a'), ('v3b')")
        val tableId = DeltaLog.forTable(spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_parquet_table_legacy_transition_3"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        // Batch 0: v1, index=0, isStartingVersion=true (mid-snapshot at v1)
        // Batch 1: v2, index=-1, isStartingVersion=false (snapshot finished,
        //   end offset is startVersion+1 at the version boundary)
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = 0,
          isStartingVersion = true)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Legacy source needs parquet-format metadata at v1 for delegation
          // to RemoteDeltaLog.
          prepareMockedClientAndFileSystemResultForParquet(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Priming getBatch(offset_0, offset_1): start is legacy initial
          // snapshot so delegates to legacy source, then latestOffset
          // transitions to new format.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // After priming, latestOffset starts from v2 and fetches v2-v3.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 2L, 3L)
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

          // Should get v2 and v3 data (v1 was already processed by legacy stream).
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq("v2a", "v2b", "v3a", "v3b").toDF())

          // Validate the final offset has transitioned to DeltaSourceOffset format.
          val offsetLog = new OffsetSeqLog(
            spark, s"${checkpointDir.getCanonicalPath}/offsets")
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

  // Legacy stream start offset is still an initial snapshot (v1, index=0,
  // isStartingVersion=true). The parquet delegation finishes the snapshot and
  // reaches the version boundary (v2, index=-1, isStartingVersion=false).
  // However, the table only has v1 -- no newer versions exist.
  // latestOffset should detect that the converted offset's reservoirVersion exceeds
  // the table's latest version and return null (no new data), so the query finishes.
  test("legacy offset transition: snapshot delegates to parquet, table only has v1") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_legacy_transition_4"
      withTable(deltaTableName) {
        sql(s"CREATE TABLE $deltaTableName (value STRING) USING DELTA")
        sql(s"INSERT INTO $deltaTableName VALUES ('v1a'), ('v1b')")
        val tableId = DeltaLog.forTable(spark, new TableIdentifier(deltaTableName))
          .update().metadata.id
        val sharedTableName = "shared_parquet_table_legacy_transition_4"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        // Batch 0: v1, index=0, isStartingVersion=true (mid-snapshot at v1)
        // Batch 1: v2, index=-1, isStartingVersion=false (snapshot finished,
        //   end offset is startVersion+1 at the version boundary)
        val (checkpointPath, fileManager) = initCheckpointDirs(checkpointDir)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 0, tableId, tableVersion = 1, index = 0,
          isStartingVersion = true)
        writeLegacyOffsetAndCommit(fileManager, checkpointPath,
          batchId = 1, tableId, tableVersion = 2, index = -1,
          isStartingVersion = false)

        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf(
          (getDeltaSharingClassesSQLConf ++ Seq(
            autoResolveKey -> "true"
          )).toSeq: _*
        ) {
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          // Legacy source needs parquet-format metadata at v1 for delegation
          // to RemoteDeltaLog.
          prepareMockedClientAndFileSystemResultForParquet(
            deltaTableName, sharedTableName, versionAsOf = Some(1L))
          // Priming getBatch(offset_0, offset_1): start is legacy initial
          // snapshot so delegates to legacy source. Only v1 exists.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName, sharedTableName, 1L, 1L)
          // Table only has v1, so report v1 as the latest version.
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName, Some(1L))

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

          // latestOffset returned null (no new data), so no batch was
          // executed and the output directory has no Delta log.
          assert(!new java.io.File(outputDir, "_delta_log").exists(),
            "Expected no Delta log in output dir since no batch should have run")
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Targeted unit tests for determineVersionAndHashFromGetBatch and
  // determineVersionAndHashFromLatestOffset.
  // ---------------------------------------------------------------------------

  private def makeOffset(
      version: Long,
      index: Long,
      isInitialSnapshot: Boolean = false): DeltaSourceOffset = {
    DeltaSourceOffset(
      reservoirId = "test-table-id",
      reservoirVersion = version,
      index = index,
      isInitialSnapshot = isInitialSnapshot
    )
  }

  private val MD5 = DeltaSharingRestClient.FILEIDHASH_PARQUET
  private val SHA256 = DeltaSharingRestClient.FILEIDHASH_DELTA

  test("determineVersionAndHashFromGetBatch: all branches") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_determine_gb"
      withTable(deltaTableName) {
        createTable(deltaTableName)
        val sharedTableName = "shared_determine_gb"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(tempDir)
        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf((getDeltaSharingClassesSQLConf ++ Seq(
          autoResolveKey -> "true"
        )).toSeq: _*) {
          val source = getSource(
            Map("path" -> s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          )

          // (description, start, startLegacy, end, endLegacy, latestVer,
          //  expectedVersion, expectedHash)
          Seq(
            // end=legacy with BASE_INDEX -> endVersion = reservoirVersion - 1, MD5
            ("end legacy BASE_INDEX",
              makeOffset(3, DeltaSourceOffset.BASE_INDEX), true,
              makeOffset(5, DeltaSourceOffset.BASE_INDEX), true,
              10L, Some(4L), MD5),
            // end=legacy with non-BASE_INDEX -> endVersion = reservoirVersion, MD5
            ("end legacy non-BASE_INDEX",
              makeOffset(3, DeltaSourceOffset.BASE_INDEX), true,
              makeOffset(5, 2L), true,
              10L, Some(5L), MD5),
            // start=legacy mid-version, end=new -> endVersion = start.reservoirVersion, MD5
            ("start legacy mid-version, end new",
              makeOffset(3, 0L), true,
              makeOffset(4, DeltaSourceOffset.BASE_INDEX), false,
              10L, Some(3L), MD5),
            // start=legacy boundary, end=new -> regular path, SHA256
            ("start legacy boundary, end new",
              makeOffset(3, DeltaSourceOffset.BASE_INDEX), true,
              makeOffset(5, DeltaSourceOffset.BASE_INDEX), false,
              10L, Some(10L), SHA256),
            // regular delta format (no legacy) -> SHA256
            ("regular delta format",
              makeOffset(5, DeltaSourceOffset.BASE_INDEX), false,
              makeOffset(7, DeltaSourceOffset.BASE_INDEX), false,
              10L, Some(10L), SHA256)
          ).foreach { case (desc, start, startLegacy, end, endLegacy,
              latestVer, expectedVersion, expectedHash) =>
            val (endVersion, fileIdHash) = source.determineVersionAndHashFromGetBatch(
              startingOffset = start,
              startConvertedFromLegacy = startLegacy,
              endOffset = end,
              endConvertedFromLegacy = endLegacy,
              latestTableVersion = latestVer
            )
            expectedVersion.foreach(v =>
              assert(endVersion === v, s"[$desc] unexpected endVersion"))
            assert(fileIdHash.contains(expectedHash),
              s"[$desc] expected $expectedHash but got $fileIdHash")
          }
          cleanUpDeltaSharingBlocks()
        }
      }
    }
  }

  test("determineVersionAndHashFromLatestOffset: all branches") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_determine_lo"
      withTable(deltaTableName) {
        createTable(deltaTableName)
        val sharedTableName = "shared_determine_lo"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(tempDir)
        val autoResolveKey = DeltaSQLConf
          .DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT.key
        withSQLConf((getDeltaSharingClassesSQLConf ++ Seq(
          autoResolveKey -> "true"
        )).toSeq: _*) {
          val source = getSource(
            Map("path" -> s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName")
          )

          // (description, start, startLegacy, latestVer, expectedVersion, expectedHash)
          Seq(
            // start=legacy mid-version -> endVersion = start.reservoirVersion, MD5
            ("legacy mid-version",
              makeOffset(3, 0L), true, 10L, Some(3L), MD5),
            // start=legacy boundary -> regular path, SHA256
            ("legacy boundary",
              makeOffset(3, DeltaSourceOffset.BASE_INDEX), true, 10L, Some(10L), SHA256),
            // regular delta format -> SHA256
            ("regular delta format",
              makeOffset(3, DeltaSourceOffset.BASE_INDEX), false, 10L, Some(10L), SHA256)
          ).foreach { case (desc, start, startLegacy, latestVer,
              expectedVersion, expectedHash) =>
            val (endVersion, fileIdHash) = source.determineVersionAndHashFromLatestOffset(
              startingOffset = start,
              startConvertedFromLegacy = startLegacy,
              latestTableVersion = latestVer
            )
            expectedVersion.foreach(v =>
              assert(endVersion === v, s"[$desc] unexpected endVersion"))
            assert(fileIdHash.contains(expectedHash),
              s"[$desc] expected $expectedHash but got $fileIdHash")
          }
          cleanUpDeltaSharingBlocks()
        }
      }
    }
  }

  // Tests for Trigger.AvailableNow with native SupportsTriggerAvailableNow implementation.

  private val disableAvailableNowWorkaround = Map.empty[String, String]

  /**
   * Helper to set up a Delta Sharing streaming test with AvailableNow trigger.
   * Creates a delta table and shared table, prepares mock client metadata, and runs the
   * provided test body with the workaround disabled.
   */
  private def withAvailableNowSharingStream(testName: String)(
      testBody: (String, String, String, java.io.File, java.io.File) => Unit): Unit = {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = s"delta_table_available_now_$testName"
      withTable(deltaTableName) {
        createTableForStreaming(deltaTableName)
        val sharedTableName = s"shared_available_now_$testName"
        prepareMockedClientMetadata(deltaTableName, sharedTableName)
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf((getDeltaSharingClassesSQLConf ++ disableAvailableNowWorkaround).toSeq: _*) {
          testBody(deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir)
        }
      }
    }
  }

  /** Run an AvailableNow streaming query to completion, with optional extra read options. */
  private def runAvailableNowQuery(
      tablePath: String,
      outputDir: java.io.File,
      checkpointDir: java.io.File,
      extraOptions: Map[String, String] = Map.empty): Unit = {
    var builder = spark.readStream
      .format("deltaSharing")
      .option("responseFormat", "delta")
    extraOptions.foreach { case (k, v) => builder = builder.option(k, v) }
    val q = builder
      .load(tablePath)
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointDir.toString)
      .trigger(Trigger.AvailableNow())
      .start(outputDir.toString)
    try {
      q.processAllAvailable()
    } finally {
      q.stop()
    }
  }

  test("Trigger.AvailableNow processes all data across multiple micro-batches") {
    withAvailableNowSharingStream("basic") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('c'), ('d')")
        sql(s"INSERT INTO $deltaTableName VALUES ('e'), ('f')")

        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(3L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        // Use maxFilesPerTrigger=1 to force multiple micro-batches.
        runAvailableNowQuery(tablePath, outputDir, checkpointDir,
          extraOptions = Map("maxFilesPerTrigger" -> "1"))

        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b", "c", "d", "e", "f").toDF())

        // Exclude .crc sidecar files written by Hadoop; count only the numeric batch files.
        def countBatchFiles(dir: String): Int =
          new java.io.File(checkpointDir, dir)
            .listFiles(f => !f.isDirectory && !f.getName.startsWith(".")).length
        val numOffsets = countBatchFiles("offsets")
        val numCommits = countBatchFiles("commits")
        // With maxFilesPerTrigger=1, one micro-batch per source file; snapshot has 3 files.
        val expectedBatches = DeltaLog
          .forTable(spark, TableIdentifier(deltaTableName))
          .getSnapshotAt(3L).allFiles.count().toInt
        assert(numOffsets == expectedBatches,
          s"Expected $expectedBatches offset files (one per source file), got $numOffsets")
        assert(numCommits == expectedBatches,
          s"Expected $expectedBatches commit files (one per source file), got $numCommits")
        assertBlocksAreCleanedUp()
    }
  }

  test("Trigger.AvailableNow excludes data arriving after query start") {
    withAvailableNowSharingStream("snapshot") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        // Insert data at versions 1-3 (to exercise multiple micro-batches with
        // maxFilesPerTrigger=1), then version 4 which arrives after the query starts.
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('c'), ('d')")
        sql(s"INSERT INTO $deltaTableName VALUES ('e'), ('f')")
        sql(s"INSERT INTO $deltaTableName VALUES ('g'), ('h')") // arrives after query start

        // Mock getTableVersion to return version 3 (simulating the frozen version),
        // even though version 4 exists. This simulates data arriving after query start.
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName, Some(3L))
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(3L))

        // Use maxFilesPerTrigger=1 to exercise multiple micro-batches (one per version file).
        runAvailableNowQuery(tablePath, outputDir, checkpointDir,
          extraOptions = Map("maxFilesPerTrigger" -> "1"))

        // Only versions 1-3 data should be processed; version 4 data is excluded.
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b", "c", "d", "e", "f").toDF())
        assertBlocksAreCleanedUp()
    }
  }

  test("Trigger.AvailableNow restart resumes from checkpoint") {
    withAvailableNowSharingStream("restart") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(1L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        // First run: process initial data.
        runAvailableNowQuery(tablePath, outputDir, checkpointDir)
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b").toDF())

        // Insert more data.
        sql(s"INSERT INTO $deltaTableName VALUES ('c'), ('d')")
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        prepareMockedClientAndFileSystemResultForStreaming(
          deltaTableName, sharedTableName, 2, 2)

        // Second run: should resume from checkpoint and only process new data.
        runAvailableNowQuery(tablePath, outputDir, checkpointDir)
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b", "c", "d").toDF())
        assertBlocksAreCleanedUp()
    }
  }

  test("Trigger.AvailableNow terminates when no new data after initial run") {
    withAvailableNowSharingStream("nodata") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(1L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        runAvailableNowQuery(tablePath, outputDir, checkpointDir)
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b").toDF())

        // Second run with no new data -- should terminate immediately.
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        runAvailableNowQuery(tablePath, outputDir, checkpointDir)

        // Same data as before -- no new data processed.
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b").toDF())
        assertBlocksAreCleanedUp()
    }
  }

  test("Trigger.AvailableNow processes incremental versions after snapshot") {
    withAvailableNowSharingStream("incremental") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(1L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        // First run: process snapshot.
        runAvailableNowQuery(tablePath, outputDir, checkpointDir)
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b").toDF())

        // Insert more data across multiple versions.
        sql(s"INSERT INTO $deltaTableName VALUES ('c')")
        sql(s"INSERT INTO $deltaTableName VALUES ('d')")
        sql(s"INSERT INTO $deltaTableName VALUES ('e')")
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
        prepareMockedClientAndFileSystemResultForStreaming(
          deltaTableName, sharedTableName, 2, 4)

        // Second run: process all incremental versions in multiple micro-batches and terminate.
        runAvailableNowQuery(tablePath, outputDir, checkpointDir,
          extraOptions = Map("maxFilesPerTrigger" -> "1"))
        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b", "c", "d", "e").toDF())
        assertBlocksAreCleanedUp()
    }
  }

  test("Trigger.ProcessingTime is not affected by AvailableNow changes") {
    withAvailableNowSharingStream("processing_time") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(1L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

        // Use default trigger (ProcessingTime) -- should work unchanged.
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
          Seq("a", "b").toDF())
        assertBlocksAreCleanedUp()
    }
  }

  test("Trigger.AvailableNow fails when server returns negative version") {
    withAvailableNowSharingStream("negative_version") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        sql(s"INSERT INTO $deltaTableName VALUES ('a'), ('b')")
        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(1L))

        // Mock server returning negative version -- should fail fast.
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName, Some(-1L))

        val e = intercept[StreamingQueryException] {
          runAvailableNowQuery(tablePath, outputDir, checkpointDir)
        }
        assert(e.getMessage.contains("negative table version"))
    }
  }

  test("Trigger.AvailableNow restart handles mid-version checkpoint") {
    withAvailableNowSharingStream("mid_index") {
      (deltaTableName, sharedTableName, tablePath, outputDir, checkpointDir) =>
        // Create 3 files in the initial snapshot (one per INSERT = one file per version).
        sql(s"INSERT INTO $deltaTableName VALUES ('a')")
        sql(s"INSERT INTO $deltaTableName VALUES ('b')")
        sql(s"INSERT INTO $deltaTableName VALUES ('c')")

        prepareMockedClientAndFileSystemResult(
          deltaTable = deltaTableName, sharedTable = sharedTableName, versionAsOf = Some(3L))
        prepareMockedClientGetTableVersion(deltaTableName, sharedTableName, Some(3L))

        // The reservoir ID is embedded in DeltaSourceOffset and must match what
        // DeltaFormatSharingSource uses internally (copied from actual table metadata).
        val reservoirId = DeltaLog.forTable(spark, TableIdentifier(deltaTableName)).tableId

        // Seed the checkpoint to simulate a prior run that committed batch 0 mid-snapshot
        // (index=0 of 3 files processed, with files at index 1 and 2 still remaining).
        // This lets us test the restart path without timing-dependent query interruption.
        val checkpointPath = new Path(checkpointDir.getCanonicalPath)
        // scalastyle:off deltahadoopconfiguration
        val hadoopConf = spark.sessionState.newHadoopConf()
        // scalastyle:on deltahadoopconfiguration
        val fileManager = CheckpointFileManager.create(checkpointPath, hadoopConf)
        fileManager.mkdirs(
          new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_OFFSETS))
        fileManager.mkdirs(
          new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_COMMITS))
        StreamMetadata.write(
          StreamMetadata(java.util.UUID.randomUUID.toString),
          new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_METADATA),
          hadoopConf)

        // Offset: batch 0 ended at index=0, isStartingVersion=true (mid initial snapshot).
        val offsetMetadataJson =
          """{"batchWatermarkMs":0,"batchTimestampMs":0,"conf":{},"sourceMetadataInfo":{}}"""
        val midIndexOffsetJson =
          s"""{"sourceVersion":1,"reservoirId":"$reservoirId",""" +
          s""""reservoirVersion":3,"index":0,"isStartingVersion":true}"""
        val offsetContent =
          s"v1\n$offsetMetadataJson\n$midIndexOffsetJson"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        val offsetOut = fileManager.createAtomic(
          new Path(
            new Path(checkpointPath, StreamingCheckpointConstants.DIR_NAME_OFFSETS), "0"),
          overwriteIfPossible = true)
        offsetOut.write(offsetContent)
        offsetOut.close()

        // No commits/0: simulates a crash after the source committed offsets/0 but
        // before the sink committed batch 0. On restart the engine re-runs batch 0
        // (index=0 -> row 'a'), then batch 1 (index=1 -> row 'b'), batch 2 (index=2 ->
        // row 'c'). All 3 rows appear in the output.
        runAvailableNowQuery(tablePath, outputDir, checkpointDir,
          extraOptions = Map("maxFilesPerTrigger" -> "1"))

        checkAnswer(
          spark.read.format("delta").load(outputDir.getCanonicalPath),
          Seq("a", "b", "c").toDF())
        assertBlocksAreCleanedUp()
    }
  }
}
