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

import org.apache.spark.sql.delta.sources._
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.Offset
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

trait StreamingSchemaEvolutionSuiteBase extends ColumnMappingStreamingTestUtils
  with DeltaSourceSuiteBase with DeltaColumnMappingSelectedTestMixin with DeltaSQLCommandTest {

  override protected def runAllTests: Boolean = true

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    // Enable for testing
    conf.set(DeltaSQLConf.DELTA_STREAMING_ENABLE_NON_ADDITIVE_SCHEMA_EVOLUTION.key, "true")
    if (isCdcTest) {
      conf.set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")
    } else {
      conf
    }
  }

  import testImplicits._

  protected val ExpectSchemaLocationUnderCheckpointException =
    ExpectFailure[DeltaRuntimeException](e =>
      assert(e.asInstanceOf[DeltaAnalysisException].getErrorClass ==
        "DELTA_STREAMING_SCHEMA_LOCATION_NOT_UNDER_CHECKPOINT"))

  protected val ExpectConflictingSchemaLocationException =
    ExpectFailure[DeltaAnalysisException](e =>
      assert(e.asInstanceOf[DeltaAnalysisException].getErrorClass ==
        "DELTA_STREAMING_SCHEMA_LOCATION_CONFLICT"))

  protected val ExpectSchemaLogInitializationFailedException =
    ExpectFailure[DeltaRuntimeException](e =>
      assert(e.asInstanceOf[DeltaRuntimeException].getErrorClass ==
        "DELTA_STREAMING_SCHEMA_LOG_INIT_FAILED_INCOMPATIBLE_SCHEMA"))

  protected val ExpectSchemaEvolutionException =
    ExpectFailure[DeltaRuntimeException](e =>
      assert(e.asInstanceOf[DeltaRuntimeException].getErrorClass ==
        "DELTA_STREAMING_SCHEMA_EVOLUTION"))

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

  protected def AssertMetadataLogSchemaMatchesLatest(
      metadataLog: DeltaSourceSchemaTrackingLog,
      deltaLog: DeltaLog): StreamAction = {
    AssertOnQuery { _ =>
      val latestSnapshot = deltaLog.update()
      metadataLog.getLatest().exists(
        _._2.dataSchema == latestSnapshot.schema)
    }
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
          .write.mode("append").format("delta").save(tablePath)
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
      maxFilesPerTrigger: Option[Int] = None)(implicit log: DeltaLog): DataFrame = {
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
    val df = dsr.load(log.dataPath.toString)
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

  testSchemaEvolution("schema location must be placed under checkpoint location") { implicit log =>
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
    // Schema log's schema is respected
    val schemaLog = getDefaultSchemaLog()
    val newSchema = PersistedSchema(log.tableId, 0,
      new StructType().add("a", StringType, true)
        .add("b", StringType, true)
        .add("c", StringType, true), new StructType())
    schemaLog.evolveSchema(newSchema)

    testStream(readStream(schemaLocation = Some(getDefaultSchemaLocation.toString)))(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      // See how the schema returns one more dimension for `c`
      CheckAnswer((-1 until 5).map(_.toString).map(i => (i, i, null)): _*)
    )

    // Cannot use schema with different partition schema than the table
    val newSchemaWithPartition = PersistedSchema(log.tableId, 0,
      new StructType().add("a", StringType, true)
        .add("b", StringType, true),
      new StructType().add("b", StringType, true))
    schemaLog.evolveSchema(newSchemaWithPartition)

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
        .getErrorClass == "DELTA_STREAMING_SCHEMA_LOG_INCOMPATIBLE_PARTITION_SCHEMA"
    }

    // Cannot use schema from another table
    val newSchemaWithTableId = PersistedSchema(
      "some_random_id", 0,
      new StructType().add("a", StringType, true)
        .add("b", StringType, true),
      new StructType())
    schemaLog.evolveSchema(newSchemaWithTableId)
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

  test("concurrent schema log modification should be detected") {
    withStarterTable { implicit log =>
      // Note: this test assumes schema log files are written one after another, which is majority
      // of the case; True concurrent execution would require commit service to protected against.
      val schemaLocation = getDefaultSchemaLocation.toString
      val snapshot = log.update()
      val schemaLog1 = DeltaSourceSchemaTrackingLog.create(spark, schemaLocation, snapshot)
      val schemaLog2 = DeltaSourceSchemaTrackingLog.create(spark, schemaLocation, snapshot)
      val newSchema = PersistedSchema("1", 1, new StructType(), new StructType())

      schemaLog1.evolveSchema(newSchema)
      val e = intercept[DeltaAnalysisException] {
        schemaLog2.evolveSchema(newSchema)
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

  testSchemaEvolution("consecutive schema evolutions") { implicit log =>
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

    // Join two individual sources with two schema log
    // Each source should return an identical batch and therefore the output batch should also be
    // identical, we are just using join to create a multi-source situation.
    def df: DataFrame =
      readStream(schemaLocation =
        Some(new Path(getDefaultCheckpoint, "_schema_log1").toString),
        startingVersion = Some(v5))
        .unionByName(
          readStream(schemaLocation =
            Some(new Path(getDefaultCheckpoint, "_schema_log2").toString),
            startingVersion = Some(v5)), allowMissingColumns = true)

    // Both schema log initialized
    def schemaLog1: DeltaSourceSchemaTrackingLog = DeltaSourceSchemaTrackingLog.create(
      spark, new Path(getDefaultCheckpoint, "_schema_log1").toString, log.update())
    def schemaLog2: DeltaSourceSchemaTrackingLog = DeltaSourceSchemaTrackingLog.create(
      spark, new Path(getDefaultCheckpoint, "_schema_log2").toString, log.update())

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
    assert(schemaLog1.getCurrentTrackedSchema.contains(schemaLog2.getCurrentTrackedSchema.get))

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

    assert(schemaLog1.getCurrentTrackedSchema.contains(schemaLog2.getCurrentTrackedSchema.get))

    // Restart stream should proceed on loading the rest of data
    testStream(df)(
      StartStream(checkpointLocation = getDefaultCheckpoint.toString),
      ProcessAllAvailable(),
      // Unioned data is served
      // 10 rows in schema [a, c]
      CheckAnswer((5 until 10).map(_.toString).flatMap(i => Seq((i, i), (i, i))): _*)
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
      Some(getDefaultSchemaLog()))

    def getLatestOffset(source: DeltaSource, start: Option[Offset] = None): DeltaSourceOffset =
      DeltaSourceOffset(log.tableId,
        source.latestOffset(start.orNull, source.getDefaultReadLimit))

    // Initialize the schema log to skip initialization failure
    getDefaultSchemaLog().evolveSchema(
      PersistedSchema(
        log.tableId,
        0L,
        s0.metadata.schema,
        s0.metadata.partitionSchema
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

// TODO: add CDC tests
