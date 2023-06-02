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
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.{DeltaSource, DeltaSQLConf}
import org.apache.spark.sql.delta.test.DeltaColumnMappingSelectedTestMixin
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingExecutionRelation}
import org.apache.spark.sql.streaming.{DataStreamReader, StreamTest}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

trait ColumnMappingStreamingTestUtils extends StreamTest with DeltaColumnMappingTestUtils {

  // Whether we are requesting CDC streaming changes
  protected def isCdcTest: Boolean

  protected val ProcessAllAvailableIgnoreError = Execute { q =>
    try {
      q.processAllAvailable()
    } catch {
      case _: Throwable =>
        // swallow the errors so we could check answer and failure on the query later
    }
  }

  protected def isColumnMappingSchemaIncompatibleFailure(
      t: Throwable,
      detectedDuringStreaming: Boolean): Boolean = t match {
    case e: DeltaStreamingColumnMappingSchemaIncompatibleException =>
      e.additionalProperties.get("detectedDuringStreaming")
        .exists(_.toBoolean == detectedDuringStreaming)
    case _ => false
  }

  protected val ExpectStreamStartInCompatibleSchemaFailure =
    ExpectFailure[DeltaStreamingColumnMappingSchemaIncompatibleException] { t =>
      assert(isColumnMappingSchemaIncompatibleFailure(t, detectedDuringStreaming = false))
    }

  protected val ExpectInStreamSchemaChangeFailure =
    ExpectFailure[DeltaStreamingColumnMappingSchemaIncompatibleException] { t =>
      assert(isColumnMappingSchemaIncompatibleFailure(t, detectedDuringStreaming = true))
    }

  protected val ExpectGenericSchemaIncompatibleFailure =
    ExpectFailure[DeltaStreamingColumnMappingSchemaIncompatibleException]()

  // Failure thrown by the current DeltaSource schema change incompatible check
  protected val ExistingRetryableInStreamSchemaChangeFailure = Execute { q =>
    // Similar to ExpectFailure but allows more fine-grained checking of exceptions
    failAfter(streamingTimeout) {
      try {
        q.awaitTermination()
      } catch {
        case _: Throwable =>
          // swallow the exception
      }
      val cause = ExceptionUtils.getRootCause(q.exception.get)
      assert(cause.getMessage.contains("Detected schema change"))
    }
  }

  protected def getLatestCommittedDeltaVersion(q: StreamExecution): Long =
    JsonUtils.fromJson[Map[String, Any]](
      q.committedOffsets.values.head.json()
    ).apply("reservoirVersion").asInstanceOf[Number].longValue()

  // Drop CDC fields because they are not useful for testing the blocking behavior
  protected def dropCDCFields(df: DataFrame): DataFrame =
    df.drop(CDCReader.CDC_COMMIT_TIMESTAMP)
      .drop(CDCReader.CDC_TYPE_COLUMN_NAME)
      .drop(CDCReader.CDC_COMMIT_VERSION)
}

trait ColumnMappingStreamingBlockedWorkflowSuiteBase extends ColumnMappingStreamingTestUtils {

  import testImplicits._

  // DataStreamReader to use
  // Set a small max file per trigger to ensure we could catch failures ASAP
  private def dsr: DataStreamReader = if (isCdcTest) {
    spark.readStream.format("delta")
      .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
      .option(DeltaOptions.CDC_READ_OPTION, "true")
  } else {
    spark.readStream.format("delta")
      .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
  }

  private def checkStreamStartBlocked(
      df: DataFrame,
      ckpt: File,
      expectedFailure: StreamAction): Unit = {
    // Restart the stream from the same checkpoint will pick up the dropped schema and our
    // column mapping check will kick in and error out.
    testStream(df)(
      StartStream(checkpointLocation = ckpt.getCanonicalPath),
      ProcessAllAvailableIgnoreError,
      // No batches have been served
      CheckLastBatch(Nil: _*),
      expectedFailure
    )
  }

  protected def writeDeltaData(
      data: Seq[Int],
      deltaLog: DeltaLog,
      userSpecifiedSchema: Option[StructType] = None): Unit = {
    val schema = userSpecifiedSchema.getOrElse(deltaLog.update().schema)
    data.foreach { i =>
      val data = Seq(Row(schema.map(_ => i.toString): _*))
      spark.createDataFrame(data.asJava, schema)
        .write.format("delta").mode("append").save(deltaLog.dataPath.toString)
    }
  }

  test("deltaLog snapshot should not be updated outside of the stream") {
    withTempDir { dir =>
      val tablePath = dir.getCanonicalPath
      // write initial data
      Seq(1).toDF("id").write.format("delta").mode("overwrite").save(tablePath)
      // record initial snapshot version and warm DeltaLog cache
      val initialDeltaLog = DeltaLog.forTable(spark, tablePath)
      // start streaming
      val df = spark.readStream.format("delta").load(tablePath)
      testStream(df)(
        StartStream(),
        ProcessAllAvailable(),
        AssertOnQuery { q =>
          // write more data
          Seq(2).toDF("id").write.format("delta").mode("append").save(tablePath)
          // update deltaLog externally
          initialDeltaLog.update()
          assert(initialDeltaLog.snapshot.version == 1)
          // query start snapshot should not change
          val source = q.logicalPlan.collectFirst {
            case r: StreamingExecutionRelation =>
              r.source.asInstanceOf[DeltaSource]
          }.get
          // same delta log but stream start version not affected
          source.snapshotAtSourceInit.deltaLog == initialDeltaLog &&
            source.snapshotAtSourceInit.version == 0
        }
      )
    }
  }

  test("column mapping + streaming - allowed workflows - column addition") {
    // column addition schema evolution should not be blocked upon restart
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      writeDeltaData(0 until 5, deltaLog, Some(StructType.fromDDL("id string, value string")))

      val checkpointDir = new File(inputDir, "_checkpoint")

      def loadDf(): DataFrame = dropCDCFields(dsr.load(inputDir.getCanonicalPath))

      testStream(loadDf())(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        CheckAnswer((0 until 5).map(i => (i.toString, i.toString)): _*),
        Execute { _ =>
          sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` ADD COLUMN (value2 string)")
        },
        Execute { _ =>
          writeDeltaData(5 until 10, deltaLog)
        },
        ExistingRetryableInStreamSchemaChangeFailure
      )

      testStream(loadDf())(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        // Sink is reinitialized, only 5-10 are ingested
        CheckAnswer(
          (5 until 10).map(i => (i.toString, i.toString, i.toString)): _*)
      )
    }

  }

  test("column mapping + streaming - allowed workflows - upgrade to name mode") {
    // upgrade should not blocked both during the stream AND during stream restart
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      withColumnMappingConf("none") {
        writeDeltaData(0 until 5, deltaLog, Some(StructType.fromDDL("id string, name string")))
      }

      def createNewDf(): DataFrame = dropCDCFields(dsr.load(inputDir.getCanonicalPath))

      val checkpointDir = new File(inputDir, "_checkpoint")

      testStream(createNewDf())(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        CheckAnswer((0 until 5).map(i => (i.toString, i.toString)): _*),
        Execute { _ =>
          sql(
            s"""
               |ALTER TABLE delta.`${inputDir.getCanonicalPath}`
               |SET TBLPROPERTIES (
               |  ${DeltaConfigs.COLUMN_MAPPING_MODE.key} = "name",
               |  ${DeltaConfigs.MIN_READER_VERSION.key} = "2",
               |  ${DeltaConfigs.MIN_WRITER_VERSION.key} = "5")""".stripMargin)
        },
        Execute { _ =>
          writeDeltaData(5 until 10, deltaLog)
        },
        ProcessAllAvailable(),
        CheckAnswer((0 until 10).map(i => (i.toString, i.toString)): _*),
        // add column schema evolution should fail the stream
        Execute { _ =>
          sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` ADD COLUMN (value2 string)")
        },
        Execute { _ =>
          writeDeltaData(10 until 15, deltaLog)
        },
        ExistingRetryableInStreamSchemaChangeFailure
      )

      // but should not block after restarting, now in column mapping mode
      testStream(createNewDf())(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        // Sink is reinitialized, only 10-15 are ingested
        CheckAnswer(
          (10 until 15).map(i => (i.toString, i.toString, i.toString)): _*)
      )

      // use a different checkpoint to simulate a clean stream restart
      val checkpointDir2 = new File(inputDir, "_checkpoint2")

      testStream(createNewDf())(
        StartStream(checkpointLocation = checkpointDir2.getCanonicalPath),
        ProcessAllAvailable(),
        // Since the latest schema contain the additional column, it is null for previous batches.
        // This is fine as it is consistent with the current semantics.
        CheckAnswer((0 until 10).map(i => (i.toString, i.toString, null)) ++
          (10 until 15).map(i => (i.toString, i.toString, i.toString)): _*),
        StopStream
      )

      // Refresh delta log so we could catch the latest schema with column mapping mode
      deltaLog.update()
      // test read prior to upgrade batches with latest metadata should also work
      val checkpointDir3 = new File(inputDir, "_checkpoint3")
      testStream(dropCDCFields(dsr.option("startingVersion", 0).load(inputDir.getCanonicalPath)))(
        StartStream(checkpointLocation = checkpointDir3.getCanonicalPath),
        ProcessAllAvailable(),
        // Since the latest schema contain the additional column, it is null for previous batches.
        // This is fine as it is consistent with the current semantics.
        CheckAnswer((0 until 10).map(i => (i.toString, i.toString, null)) ++
          (10 until 15).map(i => (i.toString, i.toString, i.toString)): _*),
        StopStream
      )

    }
  }

  /**
   * Setup the test table for testing blocked workflow, this will create a id or name mode table
   * based on which tests it is run.
   */
  protected def setupTestTable(deltaLog: DeltaLog): Unit = {
    require(columnMappingModeString != NoMapping.name)
    val tablePath = deltaLog.dataPath.toString

    // For name mapping, we use upgrade to stir things up a little
    if (columnMappingModeString == NameMapping.name) {
      // initialize with no column mapping
      withColumnMappingConf("none") {
        writeDeltaData(0 until 5, deltaLog, Some(StructType.fromDDL("id string, value string")))
      }

      // upgrade to name mode
      val protocol = deltaLog.snapshot.protocol
        val (r, w) = if (protocol.supportsReaderFeatures || protocol.supportsWriterFeatures) {
        (TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION,
          TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
      } else {
        (spark.conf
          .get(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION)
          .max(ColumnMappingTableFeature.minReaderVersion),
          spark.conf
            .get(DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION)
            .max(ColumnMappingTableFeature.minWriterVersion))
      }

      sql(
        s"""
           |ALTER TABLE delta.`${tablePath}`
           |SET TBLPROPERTIES (
           |  ${DeltaConfigs.COLUMN_MAPPING_MODE.key} = "name",
           |  ${DeltaConfigs.MIN_READER_VERSION.key} = "$r",
           |  ${DeltaConfigs.MIN_WRITER_VERSION.key} = "$w")""".stripMargin)

      // write more data post upgrade
      writeDeltaData(5 until 10, deltaLog)
    }
    // For id mapping, we could only create the table from scratch
    else if (columnMappingModeString == IdMapping.name) {
      withColumnMappingConf("id") {
        writeDeltaData(0 until 10, deltaLog, Some(StructType.fromDDL("id string, value string")))
      }
    }
  }

  test("column mapping + streaming: blocking workflow - drop column") {
    val schemaAlterQuery = "DROP COLUMN value"
    val schemaRestoreQuery = "ADD COLUMN (value string)"

    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      setupTestTable(deltaLog)

      // change schema
      sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` $schemaAlterQuery")

      // write more data post change schema
      writeDeltaData(10 until 15, deltaLog)

      // Test the two code paths below
      // Case 1 - Restart did not specify a start version, this will successfully serve the initial
      //          entire existing data based on the initial snapshot's schema, which is basically
      //          the stream schema, all schema changes in between are ignored.
      //          But once the initial snapshot is served, all subsequent batches will fail if
      //          encountering a schema change during streaming, and all restart effort should fail.
      val checkpointDir = new File(inputDir, "_checkpoint")
      val df = dropCDCFields(dsr.load(inputDir.getCanonicalPath))

      testStream(df)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        // Initial data (pre + post upgrade + post change schema) all served
        CheckAnswer((0 until 15).map(i => i.toString): _*),
        Execute { _ =>
          // write more data in new schema during streaming
          writeDeltaData(15 until 20, deltaLog)
        },
        ProcessAllAvailable(),
        // can still work because the schema is still compatible
        CheckAnswer((0 until 20).map(i => i.toString): _*),
        // But a new schema change would cause stream to fail
        // Note here we are restoring back the original schema, see next case for how we test
        // some extra special cases when schemas are reverted.
        Execute { _ =>
          sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` $schemaRestoreQuery")
        },
        // write more data in updated schema again
        Execute { _ =>
          writeDeltaData(20 until 25, deltaLog)
        },
        // The last batch should not be processed and stream should fail
        ProcessAllAvailableIgnoreError,
        // sink data did not change
        CheckAnswer((0 until 20).map(i => i.toString): _*),
        // The schemaRestoreQuery for DROP column is ADD column so it fails a more benign error
        ExistingRetryableInStreamSchemaChangeFailure
      )

      val df2 = dropCDCFields(dsr.load(inputDir.getCanonicalPath))
      // Since the initial snapshot ignores all schema changes, the most recent schema change
      // is just ADD COLUMN, which can be retried.
      testStream(df2)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        // but an additional drop should fail the stream as we are capturing data changes now
        Execute { _ =>
          sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` $schemaAlterQuery")
        },
        ProcessAllAvailableIgnoreError,
        ExpectInStreamSchemaChangeFailure
      )
      // The latest DROP columns blocks the stream.
      if (isCdcTest) {
        checkStreamStartBlocked(df2, checkpointDir, ExpectGenericSchemaIncompatibleFailure)
      } else {
        checkStreamStartBlocked(df2, checkpointDir, ExpectStreamStartInCompatibleSchemaFailure)
      }

      // Case 2 - Specifically we use startingVersion=0 to simulate serving the entire table's data
      //          in a streaming fashion, ignoring the initialSnapshot.
      //          Here we test the special case when the latest schema is "restored".
      val checkpointDir2 = new File(inputDir, "_checkpoint2")
      val dfStartAtZero = dropCDCFields(dsr
        .option(DeltaOptions.STARTING_VERSION_OPTION, "0")
        .load(inputDir.getCanonicalPath))

      if (isCdcTest) {
        checkStreamStartBlocked(
          dfStartAtZero, checkpointDir2, ExpectGenericSchemaIncompatibleFailure)
      } else {
        // In the case when we drop and add a column back
        // the restart should still fail directly because all the historical batches with the same
        // old logical name now will have a different physical name we would have data loss

        // lets add back the column we just dropped before
        sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` $schemaRestoreQuery")
        assert(DeltaLog.forTable(spark, inputDir.getCanonicalPath).snapshot.schema.size == 2)

        // restart should block right away
        checkStreamStartBlocked(
          dfStartAtZero, checkpointDir, ExpectStreamStartInCompatibleSchemaFailure)
      }
    }
  }

  test("column mapping + streaming: blocking workflow - rename column") {
    val schemaAlterQuery = "RENAME COLUMN value TO value2"
    val schemaRestoreQuery = "RENAME COLUMN value2 TO value"

    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      setupTestTable(deltaLog)

      // change schema
      sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` $schemaAlterQuery")

      // write more data post change schema
      writeDeltaData(10 until 15, deltaLog)

      // Test the two code paths below
      // Case 1 - Restart did not specify a start version, this will successfully serve the initial
      //          entire existing data based on the initial snapshot's schema, which is basically
      //          the stream schema, all schema changes in between are ignored.
      //          But once the initial snapshot is served, all subsequent batches will fail if
      //          encountering a schema change during streaming, and all restart effort should fail.
      val checkpointDir = new File(inputDir, "_checkpoint")
      def df: DataFrame = dropCDCFields(dsr.load(inputDir.getCanonicalPath))

      testStream(df)(
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        ProcessAllAvailable(),
        // Initial data (pre + post upgrade + post change schema) all served
        CheckAnswer((0 until 15).map(i => (i.toString, i.toString)): _*),
        Execute { _ =>
          // write more data in new schema during streaming
          writeDeltaData(15 until 20, deltaLog)
        },
        ProcessAllAvailable(),
        // can still work because the schema is still compatible
        CheckAnswer((0 until 20).map(i => (i.toString, i.toString)): _*),
        // stop stream to allow schema change + data update to start in a batch
        StopStream,
        // But a new schema change would cause stream to fail
        // Note here we are restoring back the original schema, see next case for how we test
        // some extra special cases when schemas are reverted.
        Execute { _ =>
          writeDeltaData(20 until 25, deltaLog)
          sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` $schemaRestoreQuery")
        }
      )

      val df2 = dropCDCFields(dsr.load(inputDir.getCanonicalPath))
      testStream(df2)(
        // Restart stream
        StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
        // the last batch should not be processed because the batch cross an incompatible
        // schema change.
        ProcessAllAvailableIgnoreError,
        // no data processed
        CheckAnswer(Nil: _*),
        // detected schema change while trying to generate the next offset
        ExpectStreamStartInCompatibleSchemaFailure
      )

      // Case 2 - Specifically we use startingVersion=0 to simulate serving the entire table's data
      //          in a streaming fashion, ignoring the initialSnapshot.
      //          Here we test the special case when the latest schema is "restored".
      if (isCdcTest) {
        val checkpointDir2 = new File(inputDir, "_checkpoint2")
        val dfStartAtZero = dropCDCFields(dsr
          .option(DeltaOptions.STARTING_VERSION_OPTION, "0")
          .load(inputDir.getCanonicalPath))
        checkStreamStartBlocked(
          dfStartAtZero, checkpointDir2, ExpectGenericSchemaIncompatibleFailure)
      } else {
        // In the trickier case when we rename a column and rename back, we could not
        // immediately detect the schema incompatibility at stream start, so we will move on.
        // This is fine because the batches served will be compatible until the in-stream check
        // finds another schema change action and fail.
        val checkpointDir2 = new File(inputDir, s"_checkpoint_${UUID.randomUUID.toString}")
        val dfStartAtZero = dropCDCFields(dsr
          .option(DeltaOptions.STARTING_VERSION_OPTION, "0")
          .load(inputDir.getCanonicalPath))
        testStream(dfStartAtZero)(
          // The stream could not move past version 10, because batches after which
          // will be incompatible with the latest schema.
          StartStream(checkpointLocation = checkpointDir2.getCanonicalPath),
          ProcessAllAvailableIgnoreError,
          AssertOnQuery { q =>
            val latestCommittedVersion = getLatestCommittedDeltaVersion(q)
            latestCommittedVersion <= 10
          },
          ExpectInStreamSchemaChangeFailure
        )
        // restart won't move forward either
        val df2 = dropCDCFields(dsr.load(inputDir.getCanonicalPath))
        checkStreamStartBlocked(df2, checkpointDir2, ExpectInStreamSchemaChangeFailure)
      }
    }
  }

  test("column mapping + streaming: blocking workflow - " +
    "should not generate latestOffset past schema change") {
    withTempDir { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
      writeDeltaData(0 until 5, deltaLog,
        userSpecifiedSchema = Some(
          new StructType()
          .add("id", StringType, true)
          .add("value", StringType, true)))
      // rename column
      sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` RENAME COLUMN value TO value2")
      val renameVersion = deltaLog.update().version
      // write more data
      writeDeltaData(5 until 10, deltaLog)

      // Case 1 - Stream start failure should not progress new latestOffset
      // Since we had a rename, the data files prior to that should not be served with the renamed
      // schema <id, value2>, but the original schema <id, value>. latestOffset() should not create
      // a new offset moves past the schema change.
      val df1 = dropCDCFields(
        dsr.option("startingVersion", "1") // start from 1 to ignore the initial schema change
           .load(inputDir.getCanonicalPath))
      testStream(df1)(
        StartStream(), // fresh checkpoint
        ProcessAllAvailableIgnoreError,
        AssertOnQuery { q =>
          // This should come from the latestOffset checker
          q.availableOffsets.isEmpty && q.latestOffsets.isEmpty &&
            q.exception.get.cause.getStackTrace.exists(_.toString.contains("latestOffset"))
        },
        ExpectStreamStartInCompatibleSchemaFailure
      )

      // try drop column now
      sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` DROP COLUMN value2")
      val dropVersion = deltaLog.update().version
      // write more data
      writeDeltaData(10 until 15, deltaLog)

      val df2 = dropCDCFields(
        dsr.option("startingVersion", renameVersion + 1) // so we could detect drop column
          .load(inputDir.getCanonicalPath))
      testStream(df2)(
        StartStream(), // fresh checkpoint
        ProcessAllAvailableIgnoreError,
        AssertOnQuery { q =>
          // This should come from the latestOffset stream start checker
          q.availableOffsets.isEmpty && q.latestOffsets.isEmpty &&
            q.exception.get.cause.getStackTrace.exists(_.toString.contains("latestOffset"))
        },
        ExpectStreamStartInCompatibleSchemaFailure
      )

      // Case 2 - in stream failure should not progress latest offset too
      // This is the handle prior to SC-111607, which should cover the major cases.
      def loadDf(): DataFrame = dropCDCFields(
        dsr.option("startingVersion", dropVersion + 1) // so we could move on to in stream failure
           .load(inputDir.getCanonicalPath))

      val ckpt = Utils.createTempDir().getCanonicalPath
      var latestAvailableOffsets: Seq[String] = null
      testStream(loadDf())(
        StartStream(checkpointLocation = ckpt), // fresh checkpoint
        ProcessAllAvailable(),
        CheckAnswer((10 until 15).map(i => (i.toString)): _*),
        Execute { q =>
          latestAvailableOffsets = q.availableOffsets.values.map(_.json()).toSeq
        },
        // add more data and rename column
        Execute { _ =>
          sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` RENAME COLUMN id TO id2")
          writeDeltaData(15 until 16, deltaLog)
        },
        ProcessAllAvailableIgnoreError,
        CheckAnswer((10 until 15).map(i => (i.toString)): _*), // no data processed
        AssertOnQuery { q =>
          // Available offsets should not change
          // This should come from the latestOffset in-stream checker
          q.availableOffsets.values.map(_.json()) == latestAvailableOffsets &&
            q.latestOffsets.isEmpty &&
            q.exception.get.cause.getStackTrace.exists(_.toString.contains("latestOffset"))
        },
        ExpectInStreamSchemaChangeFailure
      )

      // Case 3 - resuming from existing checkpoint, note that getBatch's stream start check
      // should be called instead of latestOffset for recovery.
      // This is also the handle prior to SC-111607, which should cover the major cases.
      testStream(loadDf())(
        StartStream(checkpointLocation = ckpt), // existing checkpoint
        ProcessAllAvailableIgnoreError,
        CheckAnswer(Nil: _*),
        AssertOnQuery { q =>
          // This should come from the latestOffset in-stream checker
          q.availableOffsets.values.map(_.json()) == latestAvailableOffsets &&
            q.latestOffsets.isEmpty &&
            q.exception.get.cause.getStackTrace.exists(_.toString.contains("getBatch"))
        },
        ExpectStreamStartInCompatibleSchemaFailure
      )
    }
  }

  test("unsafe flag can unblock drop or rename column") {
    // upgrade should not blocked both during the stream AND during stream restart
    withTempDir { inputDir =>
      Seq(
        s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` DROP COLUMN value",
        s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` RENAME COLUMN value TO value2"
      ).foreach { schemaChangeQuery =>
        FileUtils.deleteDirectory(inputDir)
        val deltaLog = DeltaLog.forTable(spark, new Path(inputDir.toURI))
        withColumnMappingConf("none") {
          writeDeltaData(0 until 5, deltaLog,
            Some(StructType.fromDDL("id string, value string")))
        }

        def createNewDf(): DataFrame = dropCDCFields(dsr.load(inputDir.getCanonicalPath))

        val checkpointDir = new File(inputDir, s"_checkpoint_${schemaChangeQuery.hashCode}")
        val isRename = schemaChangeQuery.contains("RENAME")
        testStream(createNewDf())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          ProcessAllAvailable(),
          CheckAnswer((0 until 5).map(i => (i.toString, i.toString)): _*),
          Execute { _ =>
            sql(
              s"""
                 |ALTER TABLE delta.`${inputDir.getCanonicalPath}`
                 |SET TBLPROPERTIES (
                 |  ${DeltaConfigs.COLUMN_MAPPING_MODE.key} = "name",
                 |  ${DeltaConfigs.MIN_READER_VERSION.key} = "2",
                 |  ${DeltaConfigs.MIN_WRITER_VERSION.key} = "5")""".stripMargin)
            // Add another schema change to ensure even after enable the flag, we would still hit
            // a schema change with more columns than read schema so `verifySchemaChange` would see
            // that can complain.
            sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` ADD COLUMN (random STRING)")
            sql(schemaChangeQuery)
            writeDeltaData(5 until 10, deltaLog)
          },
          ProcessAllAvailableIgnoreError,
          ExistingRetryableInStreamSchemaChangeFailure
        )

        // Without the flag it would still fail
        testStream(createNewDf())(
          StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
          ProcessAllAvailableIgnoreError,
          CheckAnswer(Nil: _*),
          ExpectStreamStartInCompatibleSchemaFailure
        )

        val checkExpectedResult = if (isRename) {
          CheckAnswer((5 until 10).map(i => (i.toString, i.toString, i.toString)): _*)
        } else {
          CheckAnswer((5 until 10).map(i => (i.toString, i.toString)): _*)
        }

        withSQLConf(DeltaSQLConf
            .DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES
            .key -> "true") {
          testStream(createNewDf())(
            StartStream(checkpointLocation = checkpointDir.getCanonicalPath),
            // The processing will pass, ignoring any schema column missing in the backfill.
            ProcessAllAvailable(),
            // Show up as dropped column
            checkExpectedResult,
            Execute { _ =>
              // But any schema change post the stream analysis would still cause exceptions
              // as usual, which is critical to avoid data loss.
              sql(s"ALTER TABLE delta.`${inputDir.getCanonicalPath}` ADD COLUMN (random2 STRING)")
            },
            ProcessAllAvailableIgnoreError,
            ExistingRetryableInStreamSchemaChangeFailure
          )
        }
      }
    }
  }
}

trait DeltaSourceColumnMappingSuiteBase extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests = Seq(
    "basic",
    "maxBytesPerTrigger: metadata checkpoint",
    "maxFilesPerTrigger: metadata checkpoint",
    "allow to change schema before starting a streaming query",

    // streaming blocking semantics test
    "deltaLog snapshot should not be updated outside of the stream",
    "column mapping + streaming - allowed workflows - column addition",
    "column mapping + streaming - allowed workflows - upgrade to name mode",
    "column mapping + streaming: blocking workflow - drop column",
    "column mapping + streaming: blocking workflow - rename column",
    "column mapping + streaming: blocking workflow - " +
      "should not generate latestOffset past schema change"
  )
}

class DeltaSourceIdColumnMappingSuite extends DeltaSourceSuite
  with ColumnMappingStreamingBlockedWorkflowSuiteBase
  with DeltaColumnMappingEnableIdMode
  with DeltaSourceColumnMappingSuiteBase {

  override protected def isCdcTest: Boolean = false

}

class DeltaSourceNameColumnMappingSuite extends DeltaSourceSuite
  with ColumnMappingStreamingBlockedWorkflowSuiteBase
  with DeltaColumnMappingEnableNameMode
  with DeltaSourceColumnMappingSuiteBase {

  override protected def isCdcTest: Boolean = false

}
