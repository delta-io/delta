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
import java.nio.charset.Charset

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.sources._
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

trait StreamingSchemaEvolutionSuiteBase extends ColumnMappingStreamingTestUtils
  with DeltaSourceSuiteBase with DeltaColumnMappingSelectedTestMixin with DeltaSQLCommandTest {

  override protected def runOnlyTests: Seq[String] = Seq(
    "schema log initialization with additive schema changes",
    "detect incompatible schema change while streaming",
    "trigger.Once with deferred commit should work",
    "trigger.AvailableNow should work",
    "consecutive schema evolutions",
    "latestOffset should not progress before schema evolved"
  )

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    // Enable for testing
    conf.set(DeltaSQLConf.DELTA_STREAMING_ENABLE_SCHEMA_TRACKING.key, "true")
    conf.set(
      s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming.allowSourceColumnRenameAndDrop", "always")
    if (isCdcTest) {
      conf.set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")
    } else {
      conf
    }
  }

  protected def testWithoutAllowStreamRestart(testName: String)(f: => Unit): Unit = {
    test(testName) {
      withSQLConf(s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming" +
        s".allowSourceColumnRenameAndDrop" -> "false") {
        f
      }
    }
  }

  import testImplicits._

  protected val ExpectSchemaLogInitializationFailedException =
    ExpectFailure[DeltaRuntimeException](e =>
      assert(e.asInstanceOf[DeltaRuntimeException].getErrorClass ==
        "DELTA_STREAMING_SCHEMA_LOG_INIT_FAILED_INCOMPATIBLE_SCHEMA"))

  protected val ExpectSchemaEvolutionException =
    ExpectFailure[DeltaRuntimeException](e =>
      assert(
        e.asInstanceOf[DeltaRuntimeException].getErrorClass == "DELTA_STREAMING_SCHEMA_EVOLUTION" &&
          !e.getStackTrace.exists(_.toString.contains("checkReadIncompatibleSchemaChanges"))
      )
    )

  protected val indexWhenSchemaLogIsUpdated = DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX

  protected val AwaitTermination = AssertOnQuery { q =>
    q.awaitTermination(600 * 1000) // 600 seconds
    true
  }

  protected val AwaitTerminationIgnoreError = AssertOnQuery { q =>
    try {
      q.awaitTermination(600 * 1000) // 600 seconds
    } catch {
      case _: Throwable =>
        // ignore
    }
    true
  }

  protected def allowSchemaLocationOutsideCheckpoint(f: => Unit): Unit = {
    val allowSchemaLocationOutSideCheckpointConf =
      DeltaSQLConf.DELTA_STREAMING_ALLOW_SCHEMA_LOCATION_OUTSIDE_CHECKPOINT_LOCATION.key
    withSQLConf(allowSchemaLocationOutSideCheckpointConf -> "true") {
      f
    }
  }

  protected def testSchemaEvolution(
      testName: String,
      columnMapping: Boolean = true)(f: DeltaLog => Unit): Unit = {
    super.test(testName) {
      if (columnMapping) {
        withStarterTable { log =>
          f(log)
        }
      } else {
        withColumnMappingConf("none") {
          withStarterTable { log =>
            f(log)
          }
        }
      }
    }
  }

  /**
   * Initialize a starter table with 6 rows and schema STRUCT<a STRING, b STRING>
   */
  protected def withStarterTable(f: DeltaLog => Unit): Unit = {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath
      // Write 6 versions, the first version 0 will contain data -1 and will come with the default
      // schema initialization actions.
      (-1 until 5).foreach { i =>
        Seq((i.toString, i.toString)).toDF("a", "b")
          .write.mode("append").format("delta")
          .save(tablePath)
      }
      val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      deltaLog.update()
      f(deltaLog)
    }
  }

  protected def addData(
      data: Seq[Int],
      userSpecifiedSchema: Option[StructType] = None)(implicit log: DeltaLog): Unit = {
    val schema = userSpecifiedSchema.getOrElse(log.update().schema)
    data.foreach { i =>
      val data = Seq(Row(schema.map(_ => i.toString): _*))
      spark.createDataFrame(data.asJava, schema)
        .write.format("delta").mode("append").save(log.dataPath.toString)
    }
  }

  protected def readStream(
      schemaLocation: Option[String] = None,
      sourceTrackingId: Option[String] = None,
      startingVersion: Option[Long] = None,
      maxFilesPerTrigger: Option[Int] = None,
      ignoreDeletes: Option[Boolean] = None)(implicit log: DeltaLog): DataFrame = {
    var dsr = spark.readStream.format("delta")
    if (isCdcTest) {
      dsr = dsr.option(DeltaOptions.CDC_READ_OPTION, "true")
    }
    schemaLocation.foreach { loc => dsr = dsr.option(DeltaOptions.SCHEMA_TRACKING_LOCATION, loc) }
    sourceTrackingId.foreach { name =>
      dsr = dsr.option(DeltaOptions.STREAMING_SOURCE_TRACKING_ID, name)
    }
    startingVersion.foreach { v => dsr = dsr.option("startingVersion", v) }
    maxFilesPerTrigger.foreach { f => dsr = dsr.option("maxFilesPerTrigger", f) }
    ignoreDeletes.foreach{ i => dsr.option("ignoreDeletes", i) }
    val df = {
        dsr.load(log.dataPath.toString)
    }
    if (isCdcTest) {
      dropCDCFields(df)
    } else {
      df
    }
  }

  protected def getDefaultSchemaLog(
      sourceTrackingId: Option[String] = None
  )(implicit log: DeltaLog): DeltaSourceSchemaTrackingLog =
    DeltaSourceSchemaTrackingLog.create(
      spark, getDefaultSchemaLocation.toString, log.update(), sourceTrackingId)

  protected def getDefaultCheckpoint(implicit log: DeltaLog): Path =
    new Path(log.dataPath, "_checkpoint")

  protected def getDefaultSchemaLocation(implicit log: DeltaLog): Path =
    new Path(getDefaultCheckpoint, "_schema_location")

  protected def addColumn(column: String, dt: String = "STRING")(implicit log: DeltaLog): Unit = {
    sql(s"ALTER TABLE delta.`${log.dataPath}` ADD COLUMN ($column $dt)")
  }

  protected def renameColumn(oldColumn: String, newColumn: String)(implicit log: DeltaLog): Unit = {
    sql(s"ALTER TABLE delta.`${log.dataPath}` RENAME COLUMN $oldColumn TO $newColumn")
  }

  protected def dropColumn(column: String)(implicit log: DeltaLog): Unit = {
    sql(s"ALTER TABLE delta.`${log.dataPath}` DROP COLUMN $column")
  }

  protected def overwriteSchema(
      schema: StructType,
      partitionColumns: Seq[String] = Nil)(implicit log: DeltaLog): Unit = {
    spark.sqlContext.internalCreateDataFrame(spark.sparkContext.emptyRDD[InternalRow], schema)
      .write.format("delta")
      .mode("overwrite")
      .partitionBy(partitionColumns: _*)
      .option("overwriteSchema", "true")
      .save(log.dataPath.toString)
  }

  protected def upgradeToNameMode(implicit log: DeltaLog): Unit = {
    sql(
      s"""ALTER TABLE delta.`${log.dataPath}` SET TBLPROPERTIES (
         |'delta.columnMapping.mode' = "name",
         |'delta.minReaderVersion' = '2',
         |'delta.minWriterVersion' = '5'
         |)
         |""".stripMargin)
  }

  protected def makeMetadata(
      schema: StructType,
      partitionSchema: StructType)(implicit log: DeltaLog): Metadata = {
    log.update().metadata.copy(
      schemaString = schema.json,
      partitionColumns = partitionSchema.fieldNames
    )
  }

  protected def testSchemasLocationMustBeUnderCheckpoint(implicit log: DeltaLog): Unit = {
    val dest = Utils.createTempDir().getCanonicalPath
    val ckpt = getDefaultCheckpoint.toString
    val invalidSchemaLocation = Utils.createTempDir().getCanonicalPath

    // By default it should fail
    val e = intercept[DeltaAnalysisException] {
      readStream(schemaLocation = Some(invalidSchemaLocation))
        .writeStream.option("checkpointLocation", ckpt).start(dest)
    }
    assert(e.getErrorClass == "DELTA_STREAMING_SCHEMA_LOCATION_NOT_UNDER_CHECKPOINT")

    // But can be lifted with the flag
    allowSchemaLocationOutsideCheckpoint {
      testStream(readStream(schemaLocation = Some(invalidSchemaLocation)))(
        StartStream(checkpointLocation = ckpt),
        ProcessAllAvailable(),
        CheckAnswer((-1 until 5).map(i => (i.toString, i.toString)): _*)
      )
    }
  }

  testSchemaEvolution(s"schema location must be placed under checkpoint location") { implicit log =>
    testSchemasLocationMustBeUnderCheckpoint
  }

  testSchemaEvolution("multiple delta source sharing same schema log is blocked") { implicit log =>
    allowSchemaLocationOutsideCheckpoint {
      val dest = Utils.createTempDir().getCanonicalPath
      val ckpt = getDefaultCheckpoint.toString
      val schemaLocation = getDefaultSchemaLocation.toString

      // Two INSTANCES of Delta sources sharing same schema location should be blocked
      val df1 = readStream(schemaLocation = Some(schemaLocation))
      val df2 = readStream(schemaLocation = Some(schemaLocation))
      val sdf = df1 union df2

      val e = intercept[DeltaAnalysisException] {
        sdf.writeStream.option("checkpointLocation", ckpt).start(dest)
      }
      assert(e.getErrorClass == "DELTA_STREAMING_SCHEMA_LOCATION_CONFLICT")


      // But providing an additional source name can differentiate
      val df3 = readStream(schemaLocation = Some(schemaLocation), sourceTrackingId = Some("a"))
      val df4 = readStream(schemaLocation = Some(schemaLocation), sourceTrackingId = Some("b"))
      val sdf2 = df3 union df4
      testStream(sdf2)(
        StartStream(checkpointLocation = ckpt),
        ProcessAllAvailable(),
        CheckAnswer(((-1 until 5) union (-1 until 5)).map(i => (i.toString, i.toString)): _*)
      )

      // But if they are the same instance it should not be blocked, because they will be
      // unified to the same source during execution.
      val sdf3 = df1 union df1
      testStream(sdf3)(
        StartStream(checkpointLocation = ckpt),
        ProcessAllAvailable(),
        AssertOnQuery { q =>
          // Just one source being executed
          q.committedOffsets.size == 1
        }
      )
    }
  }

  // Disable column mapping for this test so we could save some schema metadata manipulation hassle
  testSchemaEvolution("schema log is applied", columnMapping = false) { implicit log =>
    withSQLConf(
      DeltaSQLConf.DELTA_STREAMING_SCHEMA_TRACKING_METADATA_PATH_CHECK_ENABLED.key -> "false") {
      // Schema log's schema is respected
      val schemaLog = getDefaultSchemaLog()
      val newSchema = PersistedSchema(log.tableId, 0,
        makeMetadata(
          new StructType().add("a", StringType, true)
            .add("b", StringType, true)
            .add("c", StringType, true),
          partitionSchema = new StructType()
        ),
        sourceMetadataPath = ""
      )
      schemaLog.writeNewSchema(newSchema)

      testStream(readStream(schemaLocation = Some(getDefaultSchemaLocation.toString)))(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailable(),
        // See how the schema returns one more dimension for `c`
        CheckAnswer((-1 until 5).map(_.toString).map(i => (i, i, null)): _*)
      )

      // Cannot use schema from another table
      val newSchemaWithTableId = PersistedSchema(
        "some_random_id", 0,
        makeMetadata(
          new StructType().add("a", StringType, true)
          .add("b", StringType, true),
          partitionSchema = new StructType()
        ),
        sourceMetadataPath = ""
      )
      schemaLog.writeNewSchema(newSchemaWithTableId)
      assert {
        val e = intercept[DeltaAnalysisException] {
          val q = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
            .writeStream
            .option("checkpointLocation", getDefaultCheckpoint.toString)
            .outputMode("append")
            .format("console")
            .start()
          q.processAllAvailable()
          q.stop()
        }
        ExceptionUtils.getRootCause(e).asInstanceOf[DeltaAnalysisException]
          .getErrorClass == "DELTA_STREAMING_SCHEMA_LOG_INCOMPATIBLE_DELTA_TABLE_ID"
      }
    }
  }

  test("concurrent schema log modification should be detected") {
    withStarterTable { implicit log =>
      // Note: this test assumes schema log files are written one after another, which is majority
      // of the case; True concurrent execution would require commit service to protected against.
      val schemaLocation = getDefaultSchemaLocation.toString
      val snapshot = log.update()
      val schemaLog1 = DeltaSourceSchemaTrackingLog.create(spark, schemaLocation, snapshot)
      val schemaLog2 = DeltaSourceSchemaTrackingLog.create(spark, schemaLocation, snapshot)
      val newSchema =
        PersistedSchema("1", 1,
          makeMetadata(new StructType(), partitionSchema = new StructType()),
          sourceMetadataPath = "")

      schemaLog1.writeNewSchema(newSchema)
      val e = intercept[DeltaAnalysisException] {
        schemaLog2.writeNewSchema(newSchema)
      }
      assert(e.getErrorClass == "DELTA_STREAMING_SCHEMA_LOCATION_CONFLICT")
    }
  }

  /**
   * Manually create a new offset with targeted reservoirVersion by copying it from the previous
   * offset.
   * @param checkpoint Checkpoint location
   * @param version Target version
   */
  protected def manuallyCreateStreamingBatchUntilReservoirVersion(
      checkpoint: String, version: Long): Unit = {
    // manually create another offset to latest version
    val offsetDir = new File(checkpoint.stripPrefix("file:") + "/offsets")
    val previousOffset = offsetDir.listFiles().filter(!_.getName.endsWith(".crc"))
      .maxBy(_.getName.toInt)
    val reservoirVersionRegex = """"reservoirVersion":[0-9]+""".r
    val previousOffsetContent = FileUtils
      .readFileToString(previousOffset, Charset.defaultCharset())
    val updatedOffsetContent = reservoirVersionRegex
      .replaceAllIn(previousOffsetContent, s""""reservoirVersion":$version""")
    val newOffsetFile = new File(previousOffset.getParent,
      (previousOffset.getName.toInt + 1).toString)
    FileUtils.writeStringToFile(newOffsetFile, updatedOffsetContent, Charset.defaultCharset())
  }

  testSchemaEvolution("schema log initialization with additive schema changes") { implicit log =>
    // Provide a schema log by default
    def createNewDf(): DataFrame =
      readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    // Initialize snapshot schema same as latest, no need to fail stream
    testStream(createNewDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((-1 until 5).map(_.toString).map(i => (i, i)): _*)
    )

    val v0 = log.update().version

    // And schema log is initialized already, even though there aren't schema evolution exceptions
    assert(getDefaultSchemaLog().getCurrentTrackedSchema.get.deltaCommitVersion == v0)

    // Add a column and some data
    addColumn("c")
    val v1 = log.update().version

    addData(5 until 10)

    // Update schema log to v1
    testStream(createNewDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer(Nil: _*),
      ExpectSchemaEvolutionException
    )
    assert(getDefaultSchemaLog().getCurrentTrackedSchema.get.deltaCommitVersion == v1)

    var v2: Long = -1
    testStream(createNewDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      // Process successfully
      CheckAnswer((5 until 10).map(_.toString).map(i => (i, i, i)): _*),
      // Trigger additive schema change would evolve schema as well
      Execute { _ =>
        addColumn("d")
        v2 = log.update().version
      },
      Execute { _ => addData(10 until 15) },
      ExpectSchemaEvolutionException,
      AssertOnQuery { q =>
        val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
        offset.sourceVersion == 3 && offset.index == indexWhenSchemaLogIsUpdated
      }
    )
    assert(getDefaultSchemaLog().getCurrentTrackedSchema.get.deltaCommitVersion == v2)
    testStream(createNewDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((10 until 15).map(_.toString).map(i => (i, i, i, i)): _*)
    )
  }

  testSchemaEvolution("detect incompatible schema change while streaming") { implicit log =>
    // Rename as part of initial snapshot
    renameColumn("b", "c")
    // Write more data
    addData(5 until 10)
    // Source df without schema location
    val df = readStream()
    var schemaChangeDeltaVersion: Long = -1
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      // schema change inside initial snapshot should not throw error
      CheckAnswer((-1 until 10).map(i => (i.toString, i.toString)): _*),
      // This new rename should throw the legacy error because we have not provided a schema
      // location
      Execute {_ =>
        renameColumn("c", "d")
        schemaChangeDeltaVersion = log.update().version
      },
      // Add some data in new schema
      Execute {_ => addData(10 until 15) },
      ProcessAllAvailableIgnoreError,
      // No more data should've been processed
      CheckAnswer((-1 until 10).map(i => (i.toString, i.toString)): _*),
      // Detected by the in stream check
      ExpectInStreamSchemaChangeFailure
    )
    // Start the stream again with a schema location
    val df2 = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    assert(getDefaultSchemaLog().getLatestSchema.isEmpty)
    testStream(df2)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      // No data should've been processed
      CheckAnswer(Nil: _*),
      // Schema evolution exception!
      ExpectSchemaEvolutionException
    )
    // We should've updated the schema to the version just before the schema change version
    // because that's the previous version's schema we left with. To be safe and in case there
    // are more file actions to process, we saved that schema instead of the renamed schema.
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion ==
      schemaChangeDeltaVersion - 1)
    // Start the stream again with the same schema location
    val df3 = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    testStream(df3)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      // Again, no data should've been processed because the next version has a rename
      CheckAnswer(Nil: _*),
      // And schema will be evolved again
      ExpectSchemaEvolutionException
    )
    // Now finally the schema log is up to date
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == schemaChangeDeltaVersion)
    // Start the stream again should process the rest of the data without a problem
    val df4 = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    val v1 = log.update().version
    testStream(df4)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((10 until 15).map(i => (i.toString, i.toString)): _*),
      AssertOnQuery { q =>
        val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
        // bumped from file action, no pending schema change
        offset.reservoirVersion == v1 + 1 &&
          offset.index == DeltaSourceOffset.BASE_INDEX &&
          // base index uses latest VERSION_3 as well for consistency
          offset.sourceVersion == 3 &&
          // but serialized should use version 1 & -1 index for backward compatibility
          offset.json.contains(s""""sourceVersion":1""") &&
          offset.json.contains(s""""index":-1""")
      },
      // Trigger another schema change
      Execute { _ =>
        addColumn("e")
        addData(15 until 20)
      },
      ProcessAllAvailableIgnoreError,
      // No more new data
      CheckAnswer((10 until 15).map(i => (i.toString, i.toString)): _*),
      AssertOnQuery { q =>
        val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
        // latest offset should have a schema attached and evolved set to true
        // note the reservoir version has not changed
        offset.reservoirVersion == v1 + 1 &&
          offset.index == indexWhenSchemaLogIsUpdated &&
          offset.sourceVersion == 3
      },
      ExpectSchemaEvolutionException
    )

    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v1 + 1)

    val df5 = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    // Process the rest
    testStream(df5)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((15 until 20).map(i => (i.toString, i.toString, i.toString)): _*)
    )
  }

  testSchemaEvolution("detect incompatible schema change during first getBatch") { implicit log =>
    renameColumn("b", "c")
    val schemaChangeVersion = log.update().version
    // Source df without schema location, and start at version 1 to ignore initial snapshot
    // We also use maxFilePerTrigger=1 so that the first getBatch will conduct the check instead
    // of latestOffset() scanning far ahead and throw the In-Stream version of the exception.
    val df = readStream(startingVersion = Some(1), maxFilesPerTrigger = Some(1))
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      // Add more data
      Execute { _ => addData(5 until 10) },
      // Try processing
      ProcessAllAvailableIgnoreError,
      // No data should've been processed :)
      CheckAnswer(Nil: _*),
      // The first getBatch should fail
      if (isCdcTest) {
        ExpectGenericSchemaIncompatibleFailure
      } else {
        ExpectStreamStartInCompatibleSchemaFailure
      }
    )
    // Restart with a schema location, note that maxFilePerTrigger is not needed now
    // because a schema location is provided and any exception would evolve the schema.
    val df2 = readStream(startingVersion = Some(1),
      schemaLocation = Some(getDefaultSchemaLocation.toString))
    assert(getDefaultSchemaLog().getLatestSchema.isEmpty)
    testStream(df2)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      // Again, no data is processed
      CheckLastBatch(Nil: _*),
      // Schema evolution exception!
      ExpectSchemaEvolutionException
    )
    // Since the error happened during the first getBatch, we initialize schema log to schema@v1
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == 1)
    // Restart again with a schema location
    val df3 = readStream(startingVersion = Some(1),
      schemaLocation = Some(getDefaultSchemaLocation.toString))
    testStream(df3)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      // Note that the default maxFilePerTrigger is 1000, so this shows that the batch has been
      // split and the available data prior to schema change should've been served.
      // Also since we started at v1, -1 is not included.
      CheckAnswer((0 until 5).map(i => (i.toString, i.toString)): _*),
      // Schema evolution exception!
      ExpectSchemaEvolutionException
    )
    // Now the schema is up to date
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == schemaChangeVersion)
    // Restart again should pick up the new schema and process the rest without a problem.
    // Note that startingVersion is ignored when we have existing progress to work with.
    val df4 = readStream(startingVersion = Some(1),
      schemaLocation = Some(getDefaultSchemaLocation.toString))
    testStream(df4)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((5 until 10).map(i => (i.toString, i.toString)): _*)
    )
  }

  Seq("rename", "drop").foreach { invalidAction =>
    testSchemaEvolution(s"detect invalid offset during getBatch before " +
      s"initializing schema log - $invalidAction") { implicit log =>
      // start a stream to initialize checkpoint
      val ckpt = getDefaultCheckpoint.toString
      val df = readStream(startingVersion = Some(1))
      testStream(df)(
        StartStream(checkpointLocation = ckpt),
        ProcessAllAvailable(),
        CheckAnswer((0 until 5).map(i => (i.toString, i.toString)): _*),
        StopStream
      )
      if (invalidAction == "rename") {
        renameColumn("b", "c")
      } else if (invalidAction == "drop") {
        addColumn("c")
      }
      // write more data
      addData(5 until 10)
      // Add a rename or drop commit that reverses the previous change, to ensure that our check
      // has validated all the schema changes, instead of just checking the start schema.
      if (invalidAction == "rename") {
        renameColumn("c", "b")
      } else if (invalidAction == "drop") {
        dropColumn("c")
      }
      // write more data
      addData(10 until 15)
      val latestVersion = log.update().version
      // Manually create another offset to latest version to simulate the situation that an end
      // offset is somehow generated that bypasses the block, e.g. they were upgrading from an
      // super old version that did not have the block logic, and is left with a constructed
      // batch that bypasses a schema change.
      // There should be at MOST one such trailing batch as of today's streaming engine semantics.
      manuallyCreateStreamingBatchUntilReservoirVersion(ckpt, latestVersion)

      // rerun the stream should detect that and fail, even with schema location
      val schemaLocation = getDefaultSchemaLocation.toString
      testStream(readStream(schemaLocation = Some(schemaLocation)))(
        StartStream(checkpointLocation = ckpt),
        ProcessAllAvailableIgnoreError,
        CheckAnswer(Nil: _*),
        ExpectSchemaLogInitializationFailedException
      )
    }
  }

  testSchemaEvolution("resolve the most encompassing schema during getBatch " +
    "to initialize schema log") { implicit log =>
    // start a stream to initialize checkpoint
    val ckpt = getDefaultCheckpoint.toString
    val df = readStream(startingVersion = Some(1))
    testStream(df)(
      StartStream(checkpointLocation = ckpt),
      ProcessAllAvailable()
    )
    val v1 = log.update().version
    // add a new column
    addColumn("c")
    // write more data
    addData(5 until 6)
    // add another column
    addColumn("d")
    val secondAddColumnVersion = log.update().version
    addData(6 until 10)
    // add an invalid commit so we could fail directly
    renameColumn("d", "d2")
    val renamedVersion = log.update().version
    // v2 should include the two add column change but not the renamed version
    val v2 = v1 + 5
    // manually create another offset to latest version
    manuallyCreateStreamingBatchUntilReservoirVersion(ckpt, v2)
    // rerun the stream should detect rename with the stream start check, but since within the
    // offsets the schema changes are all additive, we could use the encompassing schema <a,b,c,d>.
    val schemaLocation = getDefaultSchemaLocation.toString
    testStream(readStream(schemaLocation = Some(schemaLocation)))(
      StartStream(checkpointLocation = ckpt),
      ProcessAllAvailableIgnoreError,
      CheckAnswer(Nil: _*),
      // Schema can be evolved
      ExpectSchemaEvolutionException
    )
    // Schema log is ready and populated with <a,b,c,d>
    assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
      .sameElements(Array("a", "b", "c", "d")))
    // ... which is the schema from the second add column schema change
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == secondAddColumnVersion)
    // Keep going until rename is found
    testStream(readStream(schemaLocation = Some(schemaLocation)))(
      StartStream(checkpointLocation = ckpt),
      ProcessAllAvailableIgnoreError,
      CheckAnswer((Seq(5).map(i => (i.toString, i.toString, i.toString, null)) ++
        (6 until 10).map(i => (i.toString, i.toString, i.toString, i.toString))): _*),
      ExpectSchemaEvolutionException
    )
    // Schema log is evolved with <a,b,c,d2>
    assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
      .sameElements(Array("a", "b", "c", "d2")))
    // ... which is the renamed version
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == renamedVersion)
  }

  test("trigger.Once with deferred commit should work") {
    withStarterTable { implicit log =>
      dropColumn("b")
      val schemaChangeVersion = log.update().version
      addData(5 until 10)

      val ckpt = getDefaultCheckpoint.toString
      val schemaLoc = getDefaultSchemaLocation.toString

      // Use starting version to ignore initial snapshot
      def read: DataFrame = readStream(schemaLocation = Some(schemaLoc), startingVersion = Some(1))

      // Use once trigger to execute streaming one step a time
      val StartThisStream = StartStream(trigger = Trigger.Once, checkpointLocation = ckpt)
      // This trigger:
      // 1. The stream starts with an uninitialized schema log.
      // 2. The stream schema is taken from the latest version of the Delta table.
      // 3. The schema tracking log must initialized immediately, in this case from latestOffset
      //    because this is the first time the stream starts. The schema is initialized to the
      //    schema at version 1.
      // 4. Because the schema at version 1 is not equal to the stream schema, the stream must be
      //    restarted.
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer(Nil: _*),
        ExpectSchemaEvolutionException
      )
      // Latest schema in schema log has been initialized
      assert(getDefaultSchemaLog().getLatestSchema.exists(_.deltaCommitVersion == 1))

      // This trigger:
      // 1. Finds the latest offset that ends with the schema change
      // 2. Serve all batches prior to the schema change
      // Note that the schema has NOT evolved yet because the batch ending at the schema change has
      // not being committed, and thus we have not triggered the schema evolution and will need an
      // extra restart.
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer((0 until 5).map(i => (i.toString, i.toString)): _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          // bumped from file action
          offset.reservoirVersion == schemaChangeVersion &&
            offset.index == DeltaSourceOffset.SCHEMA_CHANGE_INDEX &&
            // deserialized has latest version 3
            offset.sourceVersion == 3 &&
            // serialized has latest version 3 as well
            offset.json.contains(s""""sourceVersion":3""")
        }
      )
      assert(getDefaultSchemaLog().getLatestSchema.exists(_.deltaCommitVersion == 1))
      // This trigger:
      // 1. Finds a NEW latest offset that sets the dummy offset index post schema change
      // 2. The previous valid batch can be committed
      // 3. The commit evolves the schema and exit the stream.
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer(Nil: _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          // still stuck, but the pending schema change is marked as evolved
          offset.reservoirVersion == schemaChangeVersion &&
            offset.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX &&
            offset.sourceVersion == 3
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema
        .exists(_.deltaCommitVersion == schemaChangeVersion))

      // This trigger:
      // 1. GetBatch for the empty batch because it was constructed and now no schema mismatches
      testStream(read)(
        StartThisStream,
        AwaitTermination,
        CheckAnswer(Nil: _*)
      )

      // This trigger:
      // 1. Find the latest offset till end of data
      // 2. Commits the previous empty batch (with no schema change), so no schema evolution
      // 3. GetBatch of all data
      val v2 = log.update().version
      testStream(read)(
        StartThisStream,
        AwaitTermination,
        CheckAnswer((5 until 10).map(i => (i.toString)): _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          // bumped by file action, and since it's an non schema change, just clear schema change
          offset.reservoirVersion == v2 + 1 &&
            offset.index == DeltaSourceOffset.BASE_INDEX &&
            // Base index still uses VERSION_1
            offset.sourceVersion == 3
        }
      )

      // Create a new schema change
      addColumn("b")
      val v3 = log.update().version
      addData(10 until 11)

      // This trigger:
      // 1. Finds a new offset ending with the schema change index
      // 2. Commits previous batch (no schema change, thus no schema evolution)
      // 3. GetBatch of this empty batch
      testStream(read)(
        StartThisStream,
        AwaitTermination,
        CheckAnswer(Nil: _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          offset.reservoirVersion == v2 + 1 &&
            offset.index == DeltaSourceOffset.SCHEMA_CHANGE_INDEX &&
            offset.sourceVersion == 3
        }
      )

      // This trigger:
      // 1. Again, finds an empty batch but now ending at the dummy post schema change index.
      // 2. Commits the previous batch, evolve the schema and fail the stream.
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer(Nil: _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          offset.reservoirVersion == v3 &&
            offset.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX &&
            offset.sourceVersion == 3
        },
        ExpectSchemaEvolutionException
      )
    }
  }

  test("trigger.AvailableNow should work") {
    withStarterTable { implicit log =>
      dropColumn("b")
      val schemaChangeVersion = log.update().version
      addData(5 until 10)

      val ckpt = getDefaultCheckpoint.toString
      val schemaLoc = getDefaultSchemaLocation.toString

      // Use starting version to ignore initial snapshot
      def read: DataFrame = readStream(schemaLocation = Some(schemaLoc), startingVersion = Some(1))

      // Use trigger available now
      val StartThisStream = StartStream(trigger = Trigger.AvailableNow(), checkpointLocation = ckpt)

      // Similar to once trigger, this:
      // 1. Detects the schema change right-away from computing latest offset
      // 2. Initialize the schema log and exit stream
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer(Nil: _*),
        ExpectSchemaEvolutionException
      )
      // Latest schema in schema log has been updated
      assert(getDefaultSchemaLog().getLatestSchema.exists(_.deltaCommitVersion == 1))

      // Now, this trigger:
      // 1. Finds the latest offset RIGHT AT the schema change ending at schema change index
      // 2. GetBatch till that offset
      // 3. Finds ANOTHER the latest offset ending at the dummy post schema change index
      // 4. GetBatch for this empty batch
      // 5. Commits the previous batch
      // 6. Triggers schema evolution
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer((0 until 5).map(_.toString).map(i => (i, i)): _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          offset.reservoirVersion == schemaChangeVersion &&
            // schema change marked as evolved
            offset.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX &&
            offset.sourceVersion == 3
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema
        .exists(_.deltaCommitVersion == schemaChangeVersion))

      // This trigger:
      // 1. Finds the next latest offset, which is the end of data
      // 2. Commit previous empty batch with no pending schema change
      // 3. GetBatch with the remaining data
      val latestVersion = log.update().version
      testStream(read)(
        StartThisStream,
        AwaitTermination,
        CheckAnswer((5 until 10).map(i => (i.toString)): _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          // schema change cleared because it's a non-schema change offset
          offset.reservoirVersion == latestVersion + 1 &&
            offset.index == DeltaSourceOffset.BASE_INDEX
        }
      )

      // Create a new schema change
      addColumn("b")
      val v3 = log.update().version
      addData(10 until 11)

      // This trigger:
      // 1. Finds the latest offset, again ending at the schema change index
      // 2. Commits previous batch
      // 3. GetBatch with empty data and schema change ending offset
      // 4. Finds another latest offset, ending at the dummy post schema change index
      // 5. Commits the empty batch at 3, evolves schema log and restart stream.
      testStream(read)(
        StartThisStream,
        AwaitTerminationIgnoreError,
        CheckAnswer(Nil: _*),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          offset.reservoirVersion == v3 &&
            offset.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX &&
            offset.sourceVersion == 3
        },
        ExpectSchemaEvolutionException
      )

      // Finish the rest
      testStream(read)(
        StartThisStream,
        AwaitTermination,
        CheckAnswer((10 until 11).map(_.toString).map(i => (i, i)): _*)
      )
    }
  }

  testSchemaEvolution("consecutive schema evolutions without schema merging") { implicit log =>
    withSQLConf(
      DeltaSQLConf.DELTA_STREAMING_ENABLE_SCHEMA_TRACKING_MERGE_CONSECUTIVE_CHANGES.key
        -> "false") {
      val v5 = log.update().version // v5 has an ADD file action with value (4, 4)
      renameColumn("b", "c") // v6
      renameColumn("c", "b") // v7
      dropColumn("b") // v9
      addColumn("b") // v10

      def df: DataFrame = readStream(
        schemaLocation = Some(getDefaultSchemaLocation.toString), startingVersion = Some(v5))

      // The schema log initializes @ v1 with schema <a, b>
      testStream(df)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        AssertOnQuery { q =>
          // initialization does not generate any offsets
          q.availableOffsets.isEmpty
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5)
      assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
        .sameElements(Array("a", "b")))
      // Encounter next schema change <a, c>
      testStream(df)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        CheckAnswer(Seq(4).map(_.toString).map(i => (i, i)): _*),
        AssertOnQuery { q =>
          q.availableOffsets.size == 1 && {
            val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.head)
            offset.reservoirVersion == v5 + 1 && offset.index == indexWhenSchemaLogIsUpdated &&
              offset.sourceVersion == 3
          }
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5 + 1)
      assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
        .sameElements(Array("a", "c")))
      // Encounter next schema change <a, b> again
      testStream(df)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        AssertOnQuery { q =>
          // size is 1 because commit removes previous offset
          q.availableOffsets.size == 1 && {
            val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.head)
            offset.reservoirVersion == v5 + 2 && offset.index == indexWhenSchemaLogIsUpdated &&
              offset.sourceVersion == 3
          }
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5 + 2)
      assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
        .sameElements(Array("a", "b")))
      // Encounter next schema change <a>
      testStream(df)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        AssertOnQuery { q =>
          q.availableOffsets.size == 1 && {
            val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.head)
            offset.reservoirVersion == v5 + 3 && offset.index == indexWhenSchemaLogIsUpdated &&
              offset.sourceVersion == 3
          }
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5 + 3)
      assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
        .sameElements(Array("a")))
      // Encounter next schema change <a, b> again
      testStream(df)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        AssertOnQuery { q =>
          q.availableOffsets.size == 1 && {
            val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.head)
            offset.reservoirVersion == v5 + 4 && offset.index == indexWhenSchemaLogIsUpdated &&
              offset.sourceVersion == 3
          }
        },
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5 + 4)
      assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
        .sameElements(Array("a", "b")))
    }
  }

  testSchemaEvolution("consecutive schema evolutions") { implicit log =>
    // By default we have consecutive schema merging turned on
    val v5 = log.update().version // v5 has an ADD file action with value (4, 4)
    renameColumn("b", "c") // v6
    renameColumn("c", "b") // v7
    dropColumn("b") // v9
    addColumn("b") // v10
    val v10 = log.update().version
    // Write some more data post the consecutive schema changes
    addData(5 until 6)

    def df: DataFrame = readStream(
      schemaLocation = Some(getDefaultSchemaLocation.toString), startingVersion = Some(v5))

    // The schema log initializes @ v1 with schema <a, b>
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      AssertOnQuery { q =>
        // initialization does not generate any offsets
        q.availableOffsets.isEmpty
      },
      ExpectSchemaEvolutionException
    )
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5)
    assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
      .sameElements(Array("a", "b")))
    // Encounter next schema change <a, c>
    // This still fails schema evolution exception and won't scan ahead
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer(Seq(4).map(_.toString).map(i => (i, i)): _*),
      AssertOnQuery { q =>
        q.availableOffsets.size == 1 && {
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.head)
          offset.reservoirVersion == v5 + 1 && offset.index == indexWhenSchemaLogIsUpdated &&
            offset.sourceVersion == 3
        }
      },
      ExpectSchemaEvolutionException
    )
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5 + 1)
    assert(getDefaultSchemaLog().getLatestSchema.get.dataSchema.fieldNames
      .sameElements(Array("a", "c")))

    // Now the next restart would scan over the consecutive schema changes and use the last one
    // to initialize the schema again.
    val latestDf = df
    assert(latestDf.schema.fieldNames.sameElements(Array("a", "b")))
    // The analysis phase should've already updated schema log
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v10)
    // Processing should ignore the intermediary schema changes and process the data using the
    // merged schema.
    testStream(latestDf)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((5 until 6).map(i => (i.toString, i.toString)): _*)
    )
  }

  testSchemaEvolution("upgrade and downgrade") { implicit log =>
    val ckpt = getDefaultCheckpoint.toString
    val df = readStream(startingVersion = Some(1))
    val v0 = log.update().version
    // Initialize a stream
    testStream(df)(
      StartStream(checkpointLocation = ckpt),
      ProcessAllAvailable(),
      CheckAnswer((0 until 5).map(_.toString).map(i => (i, i)): _*),
      AssertOnQuery { q =>
        assert(q.availableOffsets.size == 1)
        val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
        // Still using the legacy offset format, bumped from file action
        offset.reservoirVersion == v0 + 1 &&
          offset.index == DeltaSourceOffset.BASE_INDEX &&
          offset.sourceVersion == 3
      }
    )

    addData(Seq(5))
    val v1 = log.update().version
    dropColumn("b")
    val v2 = log.update().version

    // Restart with schema location should initialize
    val df2 = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    testStream(df2)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      AssertOnQuery { q =>
        // initialization does not generate any more offsets
        q.availableOffsets.size == 1
      },
      ExpectSchemaEvolutionException
    )
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v0)

    // Restart again should be able to use the new offset version
    val df3 = readStream(schemaLocation = Some(getDefaultSchemaLocation.toString))
    val logAppenderUpgrade = new LogAppender("Should convert legacy offset", maxEvents = 1e6.toInt)
    logAppenderUpgrade.setThreshold(Level.DEBUG)

    withLogAppender(logAppenderUpgrade, level = Some(Level.DEBUG)) {
      testStream(df3)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        CheckAnswer(("5", "5")),
        AssertOnQuery { q =>
          val offset = DeltaSourceOffset(log.tableId, q.availableOffsets.values.last)
          // Upgraded to the new offset version
          offset.reservoirVersion == v2 &&
            offset.index == indexWhenSchemaLogIsUpdated &&
            offset.sourceVersion == 3
        },
        ExpectSchemaEvolutionException
      )
    }
    assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v2)
    // Should've upgraded the legacy offset
    val target = logAppenderUpgrade.loggingEvents.find(
      _.getMessage.toString.contains("upgrading offset "))
    assert(target.isDefined)

    // Add more data
    addData(Seq(6))

    // Suppose now the user doesn't want to use schema tracking any more, and whats to downgrade
    // to use latest schema again, it should be able to do that.
    val df4 = readStream() // without schema location
    val logAppenderDowngrade = new LogAppender("Should convert new offset", maxEvents = 1e6.toInt)
    logAppenderDowngrade.setThreshold(Level.DEBUG)

    withSQLConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES_DURING_STREAM_START
        .key -> "true",
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES
        .key -> "true") {
      withLogAppender(logAppenderDowngrade, level = Some(Level.DEBUG)) {
        testStream(df4)(
          StartStream(checkpointLocation = getDefaultCheckpoint.toString),
          ProcessAllAvailable(),
          // See the next read just falls back to use latest schema
          CheckAnswer(("6"))
        )
      }
    }
  }

  testSchemaEvolution("multiple sources with schema evolution") { implicit log =>
    val v5 = log.update().version // v5 has an ADD file action with value (4, 4)
    renameColumn("b", "c")
    addData(5 until 10)

    val schemaLog1Location = new Path(getDefaultCheckpoint, "_schema_log1").toString
    val schemaLog2Location = new Path(getDefaultCheckpoint, "_schema_log2").toString

    // Join two individual sources with two schema log
    // Each source should return an identical batch and therefore the output batch should also be
    // identical, we are just using join to create a multi-source situation.
    def df: DataFrame =
      readStream(schemaLocation =
        Some(schemaLog1Location),
        startingVersion = Some(v5))
        .unionByName(
          readStream(schemaLocation =
            Some(schemaLog2Location),
            startingVersion = Some(v5)), allowMissingColumns = true)

    // Both schema log initialized
    def schemaLog1: DeltaSourceSchemaTrackingLog = DeltaSourceSchemaTrackingLog.create(
      spark, schemaLog1Location, log.update())
    def schemaLog2: DeltaSourceSchemaTrackingLog = DeltaSourceSchemaTrackingLog.create(
      spark, schemaLog2Location, log.update())

    // The schema log initializes @ v5 with schema <a, b>
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      AssertOnQuery { q =>
        // initialization does not generate any offsets
        q.availableOffsets.isEmpty
      },
      ExpectSchemaEvolutionException
    )

    // But takes another restart for the other Delta source
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      AssertOnQuery { q =>
        // initialization does not generate any offsets
        q.availableOffsets.isEmpty
      },
      ExpectSchemaEvolutionException
    )

    // Both schema log should be initialized
    assert(schemaLog1.getCurrentTrackedSchema.map(_.deltaCommitVersion) ==
      schemaLog2.getCurrentTrackedSchema.map(_.deltaCommitVersion))

    // One of the source will commit and fail
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      // The data prior to schema change is served
      // Two rows in schema [a, b]
      CheckAnswer(("4", "4"), ("4", "4")),
      ExpectSchemaEvolutionException
    )

    // Restart should fail the other commit
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer(Nil: _*),
      ExpectSchemaEvolutionException
    )

    assert(schemaLog1.getCurrentTrackedSchema.map(_.deltaCommitVersion) ==
      schemaLog2.getCurrentTrackedSchema.map(_.deltaCommitVersion))

    // Restart stream should proceed on loading the rest of data
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      // Unioned data is served
      // 10 rows in schema [a, c]
      CheckAnswer((5 until 10).map(_.toString).flatMap(i => Seq((i, i), (i, i))): _*)
    )

    // Attempt to use the wrong schema log for each source will be detected
    val wrongDf = readStream(schemaLocation =
      // instead of using schemaLog1Location
      Some(schemaLog2Location),
      startingVersion = Some(v5))
      .unionByName(
        readStream(schemaLocation =
          // instead of using schemaLog2Location
          Some(schemaLog1Location),
          startingVersion = Some(v5)), allowMissingColumns = true)

    testStream(wrongDf)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      ExpectFailure[IllegalArgumentException](t =>
        assert(t.getMessage.contains("The Delta source metadata path used for execution")))
    )
  }

  testSchemaEvolution("schema evolution with Delta sink") { implicit log =>
    val v5 = log.update().version // v5 has an ADD file action with value (4)
    renameColumn("b", "c")
    val renameVersion1 = log.update().version
    addData(5 until 10)
    renameColumn("c", "b")
    val renameVersion2 = log.update().version
    addData(10 until 15)
    dropColumn("b")
    val dropVersion = log.update().version
    addData(15 until 20)
    addColumn("b")
    val addVersion = log.update().version
    addData(20 until 25)

    withTempDir { sink =>
      def writeStream(df: DataFrame): Unit = {
        val q = df.writeStream
          .format("delta")
          .option("checkpointLocation", getDefaultCheckpoint.toString)
          .option("mergeSchema", "true") // for automatically adding columns
          .start(sink.getCanonicalPath)
        q.processAllAvailable()
        q.stop()
      }

      def df: DataFrame = readStream(
        schemaLocation = Some(getDefaultSchemaLocation.toString), startingVersion = Some(v5))
      def readSink: DataFrame = spark.read.format("delta").load(sink.getCanonicalPath)

      val e1 = ExceptionUtils.getRootCause {
        intercept[StreamingQueryException] {
          writeStream(df)
        }
      }
      ExpectSchemaEvolutionException.assertFailure(e1)
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == v5)

      val e2 = ExceptionUtils.getRootCause {
        intercept[StreamingQueryException] {
          writeStream(df)
        }
      }
      assert(readSink.schema.fieldNames sameElements Array("a", "b"))
      checkAnswer(readSink, Seq(4).map(_.toString).map(i => Row(i, i)))
      ExpectSchemaEvolutionException.assertFailure(e2)
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == renameVersion1)

      val e3 = ExceptionUtils.getRootCause {
        intercept[StreamingQueryException] {
          writeStream(df)
        }
      }
      // c added as a new column
      assert(readSink.schema.fieldNames sameElements Array("a", "b", "c"))
      checkAnswer(readSink, Seq(4).map(_.toString).map(i => Row(i, i, null)) ++
        (5 until 10).map(_.toString).map(i => Row(i, null, i)))
      ExpectSchemaEvolutionException.assertFailure(e3)
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == renameVersion2)

      val e4 = ExceptionUtils.getRootCause {
        intercept[StreamingQueryException] {
          writeStream(df)
        }
      }
      // c was renamed to b, new data now writes to b
      assert(readSink.schema.fieldNames sameElements Array("a", "b", "c"))
      checkAnswer(readSink, Seq(4).map(_.toString).map(i => Row(i, i, null)) ++
        (5 until 10).map(_.toString).map(i => Row(i, null, i)) ++
        (10 until 15).map(_.toString).map(i => Row(i, i, null)))
      ExpectSchemaEvolutionException.assertFailure(e4)
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == dropVersion)

      val e5 = ExceptionUtils.getRootCause {
        intercept[StreamingQueryException] {
          writeStream(df)
        }
      }
      // b was dropped, but sink remains the same
      assert(readSink.schema.fieldNames sameElements Array("a", "b", "c"))
      checkAnswer(readSink, Seq(4).map(_.toString).map(i => Row(i, i, null)) ++
        (5 until 10).map(_.toString).map(i => Row(i, null, i)) ++
        (10 until 15).map(_.toString).map(i => Row(i, i, null)) ++
        (15 until 20).map(_.toString).map(i => Row(i, null, null)))
      ExpectSchemaEvolutionException.assertFailure(e5)
      assert(getDefaultSchemaLog().getLatestSchema.get.deltaCommitVersion == addVersion)

      // Finish the stream without errors
      writeStream(df)
      // b was added back, sink remains the same
      assert(readSink.schema.fieldNames sameElements Array("a", "b", "c"))
      checkAnswer(readSink, Seq(4).map(_.toString).map(i => Row(i, i, null)) ++
        (5 until 10).map(_.toString).map(i => Row(i, null, i)) ++
        (10 until 15).map(_.toString).map(i => Row(i, i, null)) ++
        (15 until 20).map(_.toString).map(i => Row(i, null, null)) ++
        (20 until 25).map(_.toString).map(i => Row(i, i, null)))
    }
  }

  testSchemaEvolution("latestOffset should not progress before schema evolved") { implicit log =>
    val s0 = log.update()
    // Change schema
    renameColumn("b", "c")
    val v0 = log.update().version
    addData(Seq(5))
    val v1 = log.update().version

    // Manually construct a Delta source since it's hard to test multiple (2+) latestOffset() calls
    // with the current streaming engine without incurring the schema evolution failure.
    def getSource: DeltaSource = DeltaSource(
      spark, log,
      new DeltaOptions(Map("startingVersion" -> "0"), spark.sessionState.conf),
      log.update(),
      metadataPath = "",
      Some(getDefaultSchemaLog()))

    def getLatestOffset(source: DeltaSource, start: Option[Offset] = None): DeltaSourceOffset =
      DeltaSourceOffset(log.tableId,
        source.latestOffset(start.orNull, source.getDefaultReadLimit))

    // Initialize the schema log to skip initialization failure
    getDefaultSchemaLog().writeNewSchema(
      PersistedSchema(
        log.tableId,
        0L,
        s0.metadata,
        sourceMetadataPath = ""
      )
    )

    val source1 = getSource

    // 1st call, land at INDEX_SCHEMA_CHANGE
    val ofs1 = getLatestOffset(source1)
    assert(ofs1.index == DeltaSourceOffset.SCHEMA_CHANGE_INDEX)
    source1.getBatch(startOffsetOption = None, ofs1)
    // 2nd call, land at INDEX_POST_SCHEMA_CHANGE
    val ofs2 = getLatestOffset(source1, Some(ofs1))
    assert(ofs2.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX)
    source1.getBatch(Some(ofs1), ofs2)
    // 3rd call, still land at INDEX_POST_SCHEMA_CHANGE, because schema evolution has not happened
    val ofs3 = getLatestOffset(source1, Some(ofs2))
    assert(ofs3.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX)
    // Commit and restart
    val e = intercept[DeltaRuntimeException] {
      source1.commit(ofs2)
    }
    ExpectSchemaEvolutionException.assertFailure(e)
    assert(getDefaultSchemaLog().getCurrentTrackedSchema.get.deltaCommitVersion == v0)

    val source2 = getSource
    // restore previousOffset
    source2.getBatch(Some(ofs3), ofs3)
    // 4th call, should move on to latest version + 1 (bumped by file action)
    val ofs4 = getLatestOffset(source2, Some(ofs3))
    assert(ofs4.index == DeltaSourceOffset.BASE_INDEX &&
      ofs4.reservoirVersion == v1 + 1)
  }

  protected def expectSqlConfException(opType: String, ver: Long, checkpointHash: Int) = {
    ExpectFailure[DeltaRuntimeException] { e =>
      val se = e.asInstanceOf[DeltaRuntimeException]
      assert {
        se.getErrorClass == "DELTA_STREAMING_CANNOT_CONTINUE_PROCESSING_POST_SCHEMA_EVOLUTION" &&
          se.messageParameters(0) == opType && se.messageParameters(1) == ver.toString &&
          se.messageParameters.exists(_.contains(checkpointHash.toString))
      }
    }
  }

  /**
   * Initialize a simple streaming DF for a simple table with just one (0, 0) entry for schema <a,b>
   * We also prepare an initialized schema log to skip the initialization phase.
   */
  protected def withSimpleStreamingDf(f: (() => DataFrame, DeltaLog) => Unit): Unit = {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath
      Seq(("0", "0")).toDF("a", "b")
        .write.mode("append").format("delta").save(tablePath)
      implicit val log = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val s0 = log.update()
      val schemaLog = getDefaultSchemaLog()
      schemaLog.writeNewSchema(
        PersistedSchema(log.tableId, s0.version, s0.metadata, sourceMetadataPath = "")
      )

      def read(): DataFrame =
        readStream(
          Some(getDefaultSchemaLocation.toString),
          startingVersion = Some(s0.version))

      // Initialize checkpoint
      withSQLConf(
        DeltaSQLConf.DELTA_STREAMING_SCHEMA_TRACKING_METADATA_PATH_CHECK_ENABLED.key -> "false") {
        testStream(read())(
          StartStream(checkpointLocation = getDefaultCheckpoint.toString),
          ProcessAllAvailable(),
          CheckAnswer(("0", "0")),
          StopStream
        )
        f(read, log)
      }
    }
  }

  testWithoutAllowStreamRestart("unblock with sql conf") {
    def testStreamFlow(
        changeSchema: DeltaLog => Unit,
        schemaChangeType: String,
        getConfKV: (Int, Long) => (String, String)): Unit = {
      withSimpleStreamingDf { (readDf, log) =>
        val ckptHash = (getDefaultCheckpoint(log).toString + "/sources/0").hashCode
        changeSchema(log)
        val v1 = log.update().version
        addData(Seq(1))(log)
        // Encounter schema evolution exception
        testStream(readDf())(
          StartStream(checkpointLocation = getDefaultCheckpoint(log).toString),
          ProcessAllAvailableIgnoreError,
          CheckAnswer(Nil: _*),
          ExpectSchemaEvolutionException
        )
        // Restart would fail due to SQL conf validation
        testStream(readDf())(
          StartStream(checkpointLocation = getDefaultCheckpoint(log).toString),
          ProcessAllAvailableIgnoreError,
          CheckAnswer(Nil: _*),
          expectSqlConfException(schemaChangeType, v1, ckptHash)
        )
        // Another restart still fails
        testStream(readDf())(
          StartStream(checkpointLocation = getDefaultCheckpoint(log).toString),
          ProcessAllAvailableIgnoreError,
          CheckAnswer(Nil: _*),
          expectSqlConfException(schemaChangeType, v1, ckptHash)
        )
        // With SQL Conf set we can move on
        val (k, v) = getConfKV(ckptHash, v1)
        withSQLConf(k -> v) {
          testStream(readDf())(
            StartStream(checkpointLocation = getDefaultCheckpoint(log).toString),
            ProcessAllAvailable()
          )
        }
      }
    }

    // Test drop column
    Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnDrop").foreach { allow =>
      Seq(
        (
          (log: DeltaLog) => {
            dropColumn("a")(log)
            // Revert the drop to test consecutive schema changes won't affect sql conf validation
            // the new column will show up with different physical name so it can trigger the
            // DROP COLUMN detection logic
            addColumn("a")(log)
          },
          (ckptHash: Int, _: Long) =>
            (s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming.$allow.ckpt_$ckptHash", "always")
        ),
        (
          (log: DeltaLog) => {
            dropColumn("a")(log)
            // Ditto
            addColumn("a")(log)
          },
          (ckptHash: Int, ver: Long) =>
            (s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming.$allow.ckpt_$ckptHash", ver.toString)
        )
      ).foreach { case (changeSchema, getConfKV) =>
        testStreamFlow(changeSchema, NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_DROP, getConfKV)
      }
    }

    // Test rename column
    Seq("allowSourceColumnRenameAndDrop", "allowSourceColumnRename").foreach { allow =>
      Seq(
        (
          (log: DeltaLog) => {
            renameColumn("b", "c")(log)
          },
          (ckptHash: Int, _: Long) =>
            (s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming.$allow.ckpt_$ckptHash", "always")
        ),
        (
          (log: DeltaLog) => {
            renameColumn("b", "c")(log)
          },
          (ckptHash: Int, ver: Long) =>
            (s"${DeltaSQLConf.SQL_CONF_PREFIX}.streaming.$allow.ckpt_$ckptHash", ver.toString)
        )
      ).foreach { case (changeSchema, getConfKV) =>
        testStreamFlow(changeSchema, NonAdditiveSchemaChangeTypes.SCHEMA_CHANGE_RENAME, getConfKV)
      }
    }
  }

  testSchemaEvolution(
    "schema tracking interacting with unsafe escape flag") { implicit log =>
    renameColumn("b", "c")
    // Even when schema location is provided, it won't be initialized because the unsafe
    // flag is turned on.
    val df = readStream(
      schemaLocation = Some(getDefaultSchemaLocation.toString), startingVersion = Some(1L))
    withSQLConf(
      DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES.key
        -> "true") {
      testStream(df)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailable(),
        CheckAnswer((0 until 5).map(_.toString).map(i => (i, i)): _*)
      )
    }
    assert(getDefaultSchemaLog().getCurrentTrackedSchema.isEmpty)
  }

  testSchemaEvolution(
    "streaming with a column mapping upgrade", columnMapping = false) { implicit log =>
    upgradeToNameMode
    val v0 = log.update().version
    renameColumn("b", "c")
    val v1 = log.update().version
    addData(5 until 10)

    // Start schema tracking from prior to upgrade
    // Initialize schema tracking log
    def readDf(): DataFrame =
      readStream(
        schemaLocation = Some(getDefaultSchemaLocation.toString),
        startingVersion = Some(1))

    testStream(readDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer(Nil: _*),
      ExpectSchemaEvolutionException
    )
    assert {
      val schemaEntry = getDefaultSchemaLog().getCurrentTrackedSchema.get
      schemaEntry.deltaCommitVersion == 1 &&
        // no physical name entry
        !DeltaColumnMapping.hasPhysicalName(schemaEntry.dataSchema.head)
    }

    testStream(readDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer((0 until 5).map(_.toString).map(i => (i, i)): _*),
      ExpectSchemaEvolutionException
    )
    assert {
      val schemaEntry = getDefaultSchemaLog().getCurrentTrackedSchema.get
      // stopped at the upgrade commit
      schemaEntry.deltaCommitVersion == v0 &&
        // now with physical name entry
        DeltaColumnMapping.hasPhysicalName(schemaEntry.dataSchema.head)
    }

    // Note that since we have schema merging, we won't need to fail again at the rename column
    // schema change, the rest of the data can be served altogether.
    testStream(readDf())(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      CheckAnswer((5 until 10).map(_.toString).map(i => (i, i)): _*)
    )

    assert {
      val schemaEntry = getDefaultSchemaLog().getCurrentTrackedSchema.get
      // schema log updated implicitly
      schemaEntry.deltaCommitVersion == v1 &&
        schemaEntry.dataSchema.fieldNames.sameElements(Array("a", "c"))
    }

  }

  test("backward-compat: persisted schema can read back entry without configurations") {
    // scalastyle:off line.size.limit
    val serialized = """{"tableId":"test","deltaCommitVersion":1,"dataSchemaJson":"{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionSchemaJson":"{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","sourceMetadataPath":""}"""
    // scalastyle:on line.size.limit

    val schemaFromJson = PersistedSchema.fromJson(serialized)
    assert(schemaFromJson == PersistedSchema(
      tableId = "test",
      deltaCommitVersion = 1,
      StructType.fromDDL("a INT").json,
      StructType.fromDDL("a INT").json,
      sourceMetadataPath = "",
      tableConfigurations = None
    ))
  }

  test("forward-compat: older version cannot read back newer JSON") {
    val newSchema = PersistedSchema(
      tableId = "test",
      deltaCommitVersion = 1,
      StructType.fromDDL("a INT").json,
      StructType.fromDDL("a INT").json,
      sourceMetadataPath = "/path",
      tableConfigurations = Some(Map("a" -> "b"))
    )

    case class OldPersistedSchema(
      tableId: String,
      deltaCommitVersion: Long,
      dataSchemaJson: String,
      partitionSchemaJson: String,
      sourceMetadataPath: String
    )

    intercept[Exception] {
      JsonUtils.fromJson[OldPersistedSchema](newSchema.toJson)
    }
  }

  testSchemaEvolution("partition evolution") { implicit log =>
    // Same schema but different partition
    overwriteSchema(log.update().schema, partitionColumns = Seq("a"))
    val v0 = log.update().version
    addData(5 until 10)
    overwriteSchema(log.update().schema, partitionColumns = Seq("b"))
    val v1 = log.update().version
    def readDf: DataFrame =
      readStream(schemaLocation = Some(getDefaultSchemaLocation.toString),
        startingVersion = Some(1),
        // ignoreDeletes because overwriteSchema would generate RemoveFiles.
        ignoreDeletes = Some(true))

    // Init schema log
    testStream(readDf)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      AwaitTerminationIgnoreError,
      CheckAnswer(Nil: _*),
      ExpectSchemaEvolutionException
    )
    // Latest schema in schema log has been updated
    assert(getDefaultSchemaLog().getLatestSchema.exists(_.deltaCommitVersion == 1))

    // Process the first batch before overwrite
    testStream(readDf)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer((0 until 5).map(_.toString).map(i => (i, i)): _*),
      ExpectSchemaEvolutionException
    )
    assert(getDefaultSchemaLog().getLatestSchema.exists(_.deltaCommitVersion == v0))

    // Process until the next overwrite
    testStream(readDf)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailableIgnoreError,
      CheckAnswer(
        // TODO: since we did an overwrite, the previous RemoveFiles are also captured, but they are
        //  using the old physical schema, we cannot read them back correctly. This is a corner case
        //  with schema overwrite + CDC, although technically CDC should not worry about overwrite
        //  because that means the downstream table needs to be truncated after applying CDC.
        // Note that since we support reuse physical name across overwrite, the value of partition
        // can still be read.
        (if (isCdcTest) (-1 until 5).map(_.toString).map(i => (null, i)) else Nil) ++
        (5 until 10).map(_.toString).map(i => (i, i)): _*),
      ExpectSchemaEvolutionException
    )
    assert(getDefaultSchemaLog().getLatestSchema.exists(_.deltaCommitVersion == v1))
  }
}

class DeltaSourceSchemaEvolutionNameColumnMappingSuite
  extends StreamingSchemaEvolutionSuiteBase
    with DeltaColumnMappingEnableNameMode {
  override def isCdcTest: Boolean = false
}

class DeltaSourceSchemaEvolutionIdColumnMappingSuite
  extends StreamingSchemaEvolutionSuiteBase
    with DeltaColumnMappingEnableIdMode {
  override def isCdcTest: Boolean = false
}

trait CDCStreamingSchemaEvolutionSuiteBase extends StreamingSchemaEvolutionSuiteBase {
  override def isCdcTest: Boolean = true

  import testImplicits._

  // This test will generate AddCDCFiles
  test("CDC streaming with schema evolution") {
    withTempDir { dir =>
      spark.range(10).toDF("id").write.format("delta").save(dir.getCanonicalPath)
      implicit val log: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

      {
        withTable("merge_source") {
          spark.range(10).filter(_ % 2 == 0)
            .toDF("id").withColumn("age", lit("string"))
            .createOrReplaceTempView("data")

          spark.sql(s"CREATE TABLE merge_source USING delta AS SELECT * FROM data")

          // Use merge to trigger schema evolution as well (add column age)
          withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
            spark.sql(
              s"""
                 |MERGE INTO delta.`${log.dataPath}` t
                 |USING merge_source s
                 |ON t.id = s.id
                 |WHEN MATCHED
                 |  THEN UPDATE SET *
                 |WHEN NOT MATCHED
                 |  THEN INSERT *
                 |""".stripMargin)
          }
        }
      }
      val v1 = log.update().version

      def readDf: DataFrame =
        readStream(schemaLocation = Some(getDefaultSchemaLocation.toString),
          startingVersion = Some(0))

      // Init schema log
      testStream(readDf)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        CheckAnswer(Nil: _*),
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getCurrentTrackedSchema.get.deltaCommitVersion == 0L)

      // Streaming CDC until the MERGE invoked schema change
      testStream(readDf)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailableIgnoreError,
        // The first 10 inserts
        CheckAnswer((0L until 10L): _*),
        ExpectSchemaEvolutionException
      )
      assert(getDefaultSchemaLog().getCurrentTrackedSchema.get.deltaCommitVersion == v1 &&
        getDefaultSchemaLog().getCurrentTrackedSchema.get.dataSchema.fieldNames.sameElements(
          Array("id", "age")))

      // Streaming CDC of the MERGE
      testStream(readDf)(
        StartStream(checkpointLocation = getDefaultCheckpoint.toString),
        ProcessAllAvailable(),
        CheckAnswer(
          // odd numbers have UPDATE actions (preimage and postimage)
          (0L until 10L).filter(_ % 2 == 0).flatMap(i => Seq((i, null), (i, "string"))): _*
        )
      )
    }
  }
}

class DeltaSourceSchemaEvolutionCDCNameColumnMappingSuite
  extends CDCStreamingSchemaEvolutionSuiteBase
    with DeltaColumnMappingEnableNameMode {
  override def isCdcTest: Boolean = true
}

class DeltaSourceSchemaEvolutionCDCIdColumnMappingSuite
  extends CDCStreamingSchemaEvolutionSuiteBase
    with DeltaColumnMappingEnableIdMode {
  override def isCdcTest: Boolean = true
}
