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

import java.io.File

import org.apache.spark.sql.delta.{
  BatchCDFSchemaEndVersion,
  BatchCDFSchemaLatest,
  BatchCDFSchemaLegacy,
  DeltaUnsupportedOperationException
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.sharing.DeltaSharingTestSparkUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.{StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// Unit tests to verify that delta format sharing support column mapping (CM).
class DeltaSharingDataSourceCMSuite
    extends StreamTest
    with DeltaSQLCommandTest
    with DeltaSharingTestSparkUtils
    with DeltaSharingDataSourceDeltaTestUtils {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileSystem.closeAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "false")
  }


  private def testReadCMTable(
      deltaTableName: String,
      sharedTablePath: String,
      dropC1: Boolean = false): Unit = {
    val expectedSchema: StructType = if (deltaTableName == "cm_id_table") {
      spark.read.format("delta").table(deltaTableName).schema
    } else {
      if (dropC1) {
        new StructType()
          .add("c2rename", StringType)
      } else {
        new StructType()
          .add("c1", IntegerType)
          .add("c2rename", StringType)
      }
    }
    assert(
      expectedSchema == spark.read
        .format("deltaSharing")
        .option("responseFormat", "delta")
        .load(sharedTablePath)
        .schema
    )

    val sharingDf =
      spark.read.format("deltaSharing").option("responseFormat", "delta").load(sharedTablePath)
    val deltaDf = spark.read.format("delta").table(deltaTableName)
    checkAnswer(sharingDf, deltaDf)
    assert(sharingDf.count() > 0)

    val filteredSharingDf =
      spark.read
        .format("deltaSharing")
        .option("responseFormat", "delta")
        .load(sharedTablePath)
        .filter(col("c2rename") === "one")
    val filteredDeltaDf =
      spark.read
        .format("delta")
        .table(deltaTableName)
        .filter(col("c2rename") === "one")
    checkAnswer(filteredSharingDf, filteredDeltaDf)
    assert(filteredSharingDf.count() > 0)
  }

  private def testReadCMCdf(
      deltaTableName: String,
      sharedTablePath: String,
      startingVersion: Int): Unit = {
    val schema = spark.read
      .format("deltaSharing")
      .option("responseFormat", "delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", startingVersion)
      .load(sharedTablePath)
      .schema
    val expectedSchema = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", startingVersion)
      .table(deltaTableName)
      .schema
    assert(expectedSchema == schema)

    val deltaDf = spark.read
      .format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", startingVersion)
      .table(deltaTableName)
    val sharingDf = spark.read
      .format("deltaSharing")
      .option("responseFormat", "delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", startingVersion)
      .load(sharedTablePath)
    if (startingVersion <= 2) {
      Seq(BatchCDFSchemaEndVersion, BatchCDFSchemaLatest, BatchCDFSchemaLegacy).foreach { m =>
        withSQLConf(
          DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key ->
          m.name
        ) {
          val deltaException = intercept[DeltaUnsupportedOperationException] {
            deltaDf.collect()
          }
          assert(
            deltaException.getMessage.contains("Retrieving table changes between") &&
            deltaException.getMessage.contains("failed because of an incompatible")
          )
          val sharingException = intercept[DeltaUnsupportedOperationException] {
            sharingDf.collect()
          }
          assert(
            sharingException.getMessage.contains("Retrieving table changes between") &&
            sharingException.getMessage.contains("failed because of an incompatible")
          )
        }
      }
    } else {
      checkAnswer(sharingDf, deltaDf)
      assert(sharingDf.count() > 0)
    }
  }

  private def testReadingSharedCMTable(
      tempDir: File,
      deltaTableName: String,
      sharedTableName: String): Unit = {
    prepareMockedClientAndFileSystemResult(
      deltaTable = deltaTableName,
      sharedTable = sharedTableName
    )
    prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)

    withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
      val profileFile = prepareProfileFile(tempDir)
      testReadCMTable(
        deltaTableName = deltaTableName,
        sharedTablePath = s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName"
      )
    }

    // Test CM and CDF
    // Error when reading cdf with startingVersion <= 2, matches delta behavior.
    prepareMockedClientAndFileSystemResultForCdf(
      deltaTableName,
      sharedTableName,
      startingVersion = 0
    )
    prepareMockedClientAndFileSystemResultForCdf(
      deltaTableName,
      sharedTableName,
      startingVersion = 2
    )
    prepareMockedClientAndFileSystemResultForCdf(
      deltaTableName,
      sharedTableName,
      startingVersion = 3
    )

    withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
      val profileFile = prepareProfileFile(tempDir)
      testReadCMCdf(
        deltaTableName,
        s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
        0
      )
      testReadCMCdf(
        deltaTableName,
        s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
        2
      )
      testReadCMCdf(
        deltaTableName,
        s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
        3
      )
    }

    // DROP COLUMN
    sql(s"ALTER TABLE $deltaTableName DROP COLUMN c1")
    prepareMockedClientAndFileSystemResult(
      deltaTable = deltaTableName,
      sharedTable = sharedTableName
    )
    prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
    withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
      val profileFile = prepareProfileFile(tempDir)
      testReadCMTable(
        deltaTableName = deltaTableName,
        sharedTablePath = s"${profileFile.getCanonicalPath}#share1.default.$sharedTableName",
        dropC1 = true
      )
    }
  }

  /**
   * column mapping tests
   */
  test(
    "DeltaSharingDataSource able to read data for cm name mode"
  ) {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_cm_name"
      withTable(deltaTableName) {
        createSimpleTable(deltaTableName, enableCdf = true)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "one"), (2, "one")""")
        spark.sql(
          s"""ALTER TABLE $deltaTableName SET TBLPROPERTIES('delta.minReaderVersion' = '2',
             |'delta.minWriterVersion' = '5',
             |'delta.columnMapping.mode' = 'name')""".stripMargin
        )
        sql(s"""ALTER TABLE $deltaTableName RENAME COLUMN c2 TO c2rename""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "two"), (2, "two")""")

        sql(s"""DELETE FROM $deltaTableName where c1=1""")
        sql(s"""UPDATE $deltaTableName set c1="3" where c2rename="one"""")

        val sharedTableName = "shared_table_cm_name"
        testReadingSharedCMTable(tempDir, deltaTableName, sharedTableName)
      }
    }
  }

  test("DeltaSharingDataSource able to read data for cm id mode") {
    withTempDir { tempDir =>
      val deltaTableName = "delta_table_cm_id"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "one"), (2, "one")""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "two"), (2, "two")""")

        sql(s"""ALTER TABLE $deltaTableName RENAME COLUMN c2 TO c2rename""")
        sql(s"""INSERT INTO $deltaTableName VALUES (1, "two"), (2, "two")""")

        sql(s"""DELETE FROM $deltaTableName where c1=1""")
        sql(s"""UPDATE $deltaTableName set c1="3" where c2rename="one"""")

        val sharedTableName = "shared_table_cm_id"
        testReadingSharedCMTable(tempDir, deltaTableName, sharedTableName)
      }
    }
  }

  /**
   * Streaming Test
   */
  private def InsertToDeltaTable(tableName: String, values: String): Unit = {
    sql(s"INSERT INTO $tableName VALUES $values")
  }

  private def processAllAvailableInStream(
      tablePath: String,
      checkpointDirStr: String,
      outputDirStr: String): Unit = {
    val q = spark.readStream
      .format("deltaSharing")
      .option("responseFormat", "delta")
      .load(tablePath)
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointDirStr)
      .option("mergeSchema", "true")
      .start(outputDirStr)

    try {
      q.processAllAvailable()
    } finally {
      q.stop()
    }
  }

  private def processStreamWithSchemaTracking(
      tablePath: String,
      checkpointDirStr: String,
      outputDirStr: String,
      trigger: Option[Trigger] = None,
      maxFilesPerTrigger: Option[Int] = None): Unit = {
    var dataStreamReader = spark.readStream
      .format("deltaSharing")
      .option("schemaTrackingLocation", checkpointDirStr)
      .option("responseFormat", "delta")
    if (maxFilesPerTrigger.isDefined || trigger.isDefined) {
      // When trigger.Once is defined, maxFilesPerTrigger is ignored -- this is the
      // behavior of the streaming engine. And AvailableNow is converted as Once for delta sharing.
      dataStreamReader =
        dataStreamReader.option("maxFilesPerTrigger", maxFilesPerTrigger.getOrElse(1))
    }
    var dataStreamWriter = dataStreamReader
      .load(tablePath)
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointDirStr)
      .option("mergeSchema", "true")
    if (trigger.isDefined) {
      dataStreamWriter = dataStreamWriter.trigger(trigger.get)
    }

    val q = dataStreamWriter.start(outputDirStr)

    try {
      q.processAllAvailable()
      if (maxFilesPerTrigger.isDefined && trigger.isEmpty) {
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        // 2 batches -- 2 files are processed, this is how the delta table is constructed.
        assert(progress.length === 2)
        progress.foreach { p =>
          assert(p.numInputRows === 2) // 2 rows per batch -- 2 rows in each file.
        }
      }
    } finally {
      q.stop()
    }
  }

  private def prepareProcessAndCheckInitSnapshot(
      deltaTableName: String,
      sharedTableName: String,
      sharedTablePath: String,
      checkpointDirStr: String,
      outputDir: File,
      useSchemaTracking: Boolean,
      trigger: Option[Trigger] = None
  ): Unit = {
    InsertToDeltaTable(deltaTableName, """(1, "one"), (2, "one"), (1, "two")""")
    prepareMockedClientAndFileSystemResult(
      deltaTable = deltaTableName,
      sharedTable = sharedTableName,
      versionAsOf = Some(1L)
    )
    prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
    prepareMockedClientMetadata(deltaTableName, sharedTableName)
    if (useSchemaTracking) {
      processStreamWithSchemaTracking(
        sharedTablePath,
        checkpointDirStr,
        outputDir.toString,
        trigger
      )
    } else {
      processAllAvailableInStream(
        sharedTablePath,
        checkpointDirStr,
        outputDir.toString
      )
    }

    checkAnswer(
      spark.read.format("delta").load(outputDir.getCanonicalPath),
      Seq((1, "one"), (2, "one"), (1, "two")).toDF()
    )
  }

  def prepareNewInsert(
      deltaTableName: String,
      sharedTableName: String,
      values: String,
      startingVersion: Long,
      endingVersion: Long): Unit = {
    InsertToDeltaTable(deltaTableName, values)
    prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
    prepareMockedClientAndFileSystemResultForStreaming(
      deltaTableName,
      sharedTableName,
      startingVersion,
      endingVersion
    )
  }

  private def renameColumnAndPrepareRpcResponse(
      deltaTableName: String,
      sharedTableName: String,
      startingVersion: Long,
      endingVersion: Long,
      insertAfterRename: Boolean): Unit = {
    // Rename on the original delta table.
    sql(s"""ALTER TABLE $deltaTableName RENAME COLUMN c2 TO c2rename""")
    if (insertAfterRename) {
      InsertToDeltaTable(deltaTableName, """(1, "three")""")
      InsertToDeltaTable(deltaTableName, """(2, "three")""")
    }
    // Prepare all the delta sharing rpcs.
    prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
    prepareMockedClientMetadata(deltaTableName, sharedTableName)
    prepareMockedClientAndFileSystemResultForStreaming(
      deltaTableName,
      sharedTableName,
      startingVersion,
      endingVersion
    )
  }

  private def expectUseSchemaLogException(
      tablePath: String,
      checkpointDirStr: String,
      outputDirStr: String): Unit = {
    val error = intercept[StreamingQueryException] {
      processAllAvailableInStream(
        tablePath,
        checkpointDirStr,
        outputDirStr
      )
    }.toString
    assert(error.contains("DELTA_STREAMING_INCOMPATIBLE_SCHEMA_CHANGE_USE_SCHEMA_LOG"))
    assert(error.contains("Please provide a 'schemaTrackingLocation'"))
  }

  private def expectMetadataEvolutionException(
      tablePath: String,
      checkpointDirStr: String,
      outputDirStr: String,
      trigger: Option[Trigger] = None,
      maxFilesPerTrigger: Option[Int] = None): Unit = {
    val error = intercept[StreamingQueryException] {
      processStreamWithSchemaTracking(
        tablePath,
        checkpointDirStr,
        outputDirStr,
        trigger,
        maxFilesPerTrigger
      )
    }.toString
    assert(error.contains("DELTA_STREAMING_METADATA_EVOLUTION"))
    assert(error.contains("Please restart the stream to continue"))
  }

  private def expectSqlConfException(
      tablePath: String,
      checkpointDirStr: String,
      outputDirStr: String,
      trigger: Option[Trigger] = None,
      maxFilesPerTrigger: Option[Int] = None): Unit = {
    val error = intercept[StreamingQueryException] {
      processStreamWithSchemaTracking(
        tablePath,
        checkpointDirStr,
        outputDirStr,
        trigger,
        maxFilesPerTrigger
      )
    }.toString
    assert(error.contains("DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION"))
    assert(error.contains("delta.streaming.allowSourceColumnRenameAndDrop"))
  }

  private def processWithSqlConf(
      tablePath: String,
      checkpointDirStr: String,
      outputDirStr: String,
      trigger: Option[Trigger] = None,
      maxFilesPerTrigger: Option[Int] = None): Unit = {
    // Using allowSourceColumnRenameAndDrop instead of
    // allowSourceColumnRenameAndDrop.[checkpoint_hash] because the checkpointDir changes
    // every test.
    spark.conf
      .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "always")
    processStreamWithSchemaTracking(
      tablePath,
      checkpointDirStr,
      outputDirStr,
      trigger,
      maxFilesPerTrigger
    )
  }

  private def testRestartStreamingFourTimes(
      tablePath: String,
      checkpointDir: java.io.File,
      outputDirStr: String): Unit = {
    val checkpointDirStr = checkpointDir.toString

    // 1. Followed the previous error message to use schemaTrackingLocation, but received
    // error suggesting restart.
    expectMetadataEvolutionException(tablePath, checkpointDirStr, outputDirStr)

    // 2. Followed the previous error message to restart, but need to restart again for
    // DeltaSource to handle offset movement, this is the SAME behavior as stream reading from
    // the delta table directly.
    expectMetadataEvolutionException(tablePath, checkpointDirStr, outputDirStr)

    // 3. Followed the previous error message to restart, but cannot write to the dest table.
    expectSqlConfException(tablePath, checkpointDirStr, outputDirStr)

    // 4. Restart with new sqlConf, able to process new data and writing to a new column.
    // Not using allowSourceColumnRenameAndDrop.[checkpoint_hash] because the checkpointDir
    // changes every test, using allowSourceColumnRenameAndDrop=always instead.
    processWithSqlConf(tablePath, checkpointDirStr, outputDirStr)
  }

  test("cm streaming works with newly added schemaTrackingLocation") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_cm_streaming_basic"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        val sharedTableName = "shared_table_cm_streaming_basic"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          // 1. Able to stream snapshot at version 1.
          // The streaming is started without schemaTrackingLocation.
          prepareProcessAndCheckInitSnapshot(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            sharedTablePath = tablePath,
            checkpointDirStr = checkpointDir.toString,
            outputDir = outputDir,
            useSchemaTracking = false
          )

          // 2. Able to stream new data at version 2.
          // The streaming is continued without schemaTrackingLocation.
          prepareNewInsert(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            values = """(2, "two")""",
            startingVersion = 2,
            endingVersion = 2
          )
          processAllAvailableInStream(
            tablePath,
            checkpointDir.toString,
            outputDir.toString
          )
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq((1, "one"), (2, "one"), (1, "two"), (2, "two")).toDF()
          )

          // 3. column renaming at version 3, and expect exception.
          renameColumnAndPrepareRpcResponse(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            startingVersion = 2,
            endingVersion = 3,
            insertAfterRename = false
          )
          expectUseSchemaLogException(tablePath, checkpointDir.toString, outputDir.toString)

          // 4. insert new data at version 4.
          prepareNewInsert(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            values = """(1, "three"), (2, "three")""",
            startingVersion = 2,
            endingVersion = 4
          )
          // Additional preparation for rpc because deltaSource moved the offset to (3, -20) and
          // (3, -19) after restart.
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            3,
            4
          )

          // 5. with 4 restarts, able to continue the streaming
          // The streaming is re-started WITH schemaTrackingLocation, and it's able to capture the
          // schema used in previous version, based on the initial call of getBatch for the latest
          // offset, which pulls the metadata from the server.
          testRestartStreamingFourTimes(tablePath, checkpointDir, outputDir.toString)

          // An additional column is added to the output table.
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq(
              (1, "one", null),
              (2, "one", null),
              (1, "two", null),
              (2, "two", null),
              (1, null, "three"),
              (2, null, "three")
            ).toDF()
          )
        }
      }
    }
  }

  test("cm streaming works with restart on snapshot query") {
    // The main difference in this test is the rename happens after processing the initial snapshot,
    // (instead of after making continuous progress), to test that the restart could fetch the
    // latest metadata and the metadata from lastest offset.
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_streaming_restart"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        val sharedTableName = "shared_table_streaming_restart"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          // 1. Able to stream snapshot at version 1.
          prepareProcessAndCheckInitSnapshot(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            sharedTablePath = tablePath,
            checkpointDirStr = checkpointDir.toString,
            outputDir = outputDir,
            useSchemaTracking = false
          )

          // 2. column renaming at version 2, and expect exception.
          renameColumnAndPrepareRpcResponse(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            startingVersion = 2,
            endingVersion = 2,
            insertAfterRename = false
          )
          expectUseSchemaLogException(tablePath, checkpointDir.toString, outputDir.toString)

          // 3. insert new data at version 3.
          prepareNewInsert(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            values = """(1, "three"), (2, "three")""",
            startingVersion = 2,
            endingVersion = 3
          )

          // 4. with 4 restarts, able to continue the streaming
          testRestartStreamingFourTimes(tablePath, checkpointDir, outputDir.toString)

          // An additional column is added to the output table.
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq(
              (1, "one", null),
              (2, "one", null),
              (1, "two", null),
              (1, null, "three"),
              (2, null, "three")
            ).toDF()
          )
        }
      }
    }
  }

  test("cm streaming works with schemaTracking used at start") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_streaming_schematracking"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        val sharedTableName = "shared_table_streaming_schematracking"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          // 1. Able to stream snapshot at version 1.
          prepareProcessAndCheckInitSnapshot(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            sharedTablePath = tablePath,
            checkpointDirStr = checkpointDir.toString,
            outputDir = outputDir,
            useSchemaTracking = true
          )

          // 2. Able to stream new data at version 2.
          prepareNewInsert(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            values = """(2, "two")""",
            startingVersion = 2,
            endingVersion = 2
          )
          processStreamWithSchemaTracking(
            tablePath,
            checkpointDir.toString,
            outputDir.toString
          )
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq((1, "one"), (2, "one"), (1, "two"), (2, "two")).toDF()
          )

          // 3. column renaming at version 3, and expect exception.
          renameColumnAndPrepareRpcResponse(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            startingVersion = 2,
            endingVersion = 3,
            insertAfterRename = false
          )
          expectMetadataEvolutionException(tablePath, checkpointDir.toString, outputDir.toString)

          // 4. First see exception, then with sql conf, able to stream new data at version 4.
          prepareNewInsert(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            values = """(1, "three"), (2, "three")""",
            startingVersion = 3,
            endingVersion = 4
          )
          expectSqlConfException(tablePath, checkpointDir.toString, outputDir.toString)
          processWithSqlConf(tablePath, checkpointDir.toString, outputDir.toString)

          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq(
              (1, "one", null),
              (2, "one", null),
              (1, "two", null),
              (2, "two", null),
              (1, null, "three"),
              (2, null, "three")
            ).toDF()
          )
        }
      }
    }
  }

  test("cm streaming works with restart with accumulated inserts after rename") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_streaming_accumulate"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        val sharedTableName = "shared_table_streaming_accumulate"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          // 1. Able to stream snapshot at version 1.
          prepareProcessAndCheckInitSnapshot(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            sharedTablePath = tablePath,
            checkpointDirStr = checkpointDir.toString,
            outputDir = outputDir,
            useSchemaTracking = false
          )

          // 2. column renaming at version 2, and expect exception.
          renameColumnAndPrepareRpcResponse(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            startingVersion = 2,
            endingVersion = 4,
            insertAfterRename = true
          )
          expectUseSchemaLogException(tablePath, checkpointDir.toString, outputDir.toString)

          // 4. with 4 restarts, able to continue the streaming
          testRestartStreamingFourTimes(tablePath, checkpointDir, outputDir.toString)

          // An additional column is added to the output table.
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq(
              (1, "one", null),
              (2, "one", null),
              (1, "two", null),
              (1, null, "three"),
              (2, null, "three")
            ).toDF()
          )
        }
      }
    }
  }

  test("cm streaming works with column drop and add") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_column_drop"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        val sharedTableName = "shared_table_column_drop"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          // 1. Able to stream snapshot at version 1.
          prepareProcessAndCheckInitSnapshot(
            deltaTableName = deltaTableName,
            sharedTableName = sharedTableName,
            sharedTablePath = tablePath,
            checkpointDirStr = checkpointDir.toString,
            outputDir = outputDir,
            useSchemaTracking = true
          )

          // 2. drop column c1 at version 2
          sql(s"ALTER TABLE $deltaTableName DROP COLUMN c1")
          // 3. add column c3 at version 3
          sql(s"ALTER TABLE $deltaTableName ADD COLUMN (c3 int)")
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            2,
            3
          )
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            3,
            3
          )

          // Needs a 3 restarts for deltaSource to catch up.
          expectMetadataEvolutionException(tablePath, checkpointDir.toString, outputDir.toString)
          expectSqlConfException(tablePath, checkpointDir.toString, outputDir.toString)
          spark.conf
            .set("spark.databricks.delta.streaming.allowSourceColumnRenameAndDrop", "always")
          expectMetadataEvolutionException(tablePath, checkpointDir.toString, outputDir.toString)
          processWithSqlConf(tablePath, checkpointDir.toString, outputDir.toString)

          // 4. insert at version 4
          InsertToDeltaTable(deltaTableName, """("four", 4)""")
          // 5. insert at version 5
          InsertToDeltaTable(deltaTableName, """("five", 5)""")
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            3,
            5
          )

          processStreamWithSchemaTracking(
            tablePath,
            checkpointDir.toString,
            outputDir.toString
          )
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq[(java.lang.Integer, String, java.lang.Integer)](
              (1, "one", null),
              (2, "one", null),
              (1, "two", null),
              (null, "four", 4),
              (null, "five", 5)
            ).toDF()
          )
        }
      }
    }
  }


  test("cm streaming works with MaxFilesPerTrigger") {
    withTempDirs { (inputDir, outputDir, checkpointDir) =>
      val deltaTableName = "delta_table_maxfiles"
      withTable(deltaTableName) {
        createCMIdTableWithCdf(deltaTableName)
        val sharedTableName = "shared_table_maxfiles"
        val profileFile = prepareProfileFile(inputDir)
        val tablePath = profileFile.getCanonicalPath + s"#share1.default.$sharedTableName"

        withSQLConf(getDeltaSharingClassesSQLConf.toSeq: _*) {
          // 1. Able to stream snapshot at version 1.
          InsertToDeltaTable(deltaTableName, """(1, "one"), (2, "one"), (1, "two"), (2, "two")""")
          prepareMockedClientAndFileSystemResult(
            deltaTable = deltaTableName,
            sharedTable = sharedTableName,
            versionAsOf = Some(1L)
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          prepareMockedClientMetadata(deltaTableName, sharedTableName)

          // process with maxFilesPerTrigger.
          processStreamWithSchemaTracking(
            tablePath,
            checkpointDir.toString,
            outputDir.toString,
            trigger = None,
            maxFilesPerTrigger = Some(1)
          )
          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq((1, "one"), (2, "one"), (1, "two"), (2, "two")).toDF()
          )

          // 2. column renaming at version 2, no exception because of Trigger.Once.
          sql(s"""ALTER TABLE $deltaTableName RENAME COLUMN c2 TO c2rename""")

          // Prepare all the delta sharing rpcs.
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          prepareMockedClientMetadata(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            startingVersion = 1,
            endingVersion = 2
          )
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            startingVersion = 2,
            endingVersion = 2
          )

          // maxFilesPerTrigger doesn't change whether exception is thrown or not.
          expectMetadataEvolutionException(
            tablePath,
            checkpointDir.toString,
            outputDir.toString,
            trigger = None,
            maxFilesPerTrigger = Some(1)
          )

          // 4. First see exception, then with sql conf, able to stream new data at version 4 and 5.
          InsertToDeltaTable(
            deltaTableName,
            """(1, "three"), (2, "three"), (1, "four"), (2, "four")"""
          )
          prepareMockedClientGetTableVersion(deltaTableName, sharedTableName)
          prepareMockedClientAndFileSystemResultForStreaming(
            deltaTableName,
            sharedTableName,
            2,
            3
          )

          expectSqlConfException(
            tablePath,
            checkpointDir.toString,
            outputDir.toString,
            trigger = None,
            maxFilesPerTrigger = Some(1)
          )
          processWithSqlConf(
            tablePath,
            checkpointDir.toString,
            outputDir.toString,
            trigger = None,
            maxFilesPerTrigger = Some(1)
          )

          checkAnswer(
            spark.read.format("delta").load(outputDir.getCanonicalPath),
            Seq(
              (1, "one", null),
              (2, "one", null),
              (1, "two", null),
              (2, "two", null),
              (1, null, "three"),
              (2, null, "three"),
              (1, null, "four"),
              (2, null, "four")
            ).toDF()
          )
        }
      }
    }
  }
}
