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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.sources._
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

trait StreamingSchemaEvolutionSuiteBase extends ColumnMappingStreamingTestUtils
  with DeltaSourceSuiteBase with DeltaColumnMappingSelectedTestMixin with DeltaSQLCommandTest {

  override protected def runAllTests: Boolean = true

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
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
    ExpectFailure[DeltaRuntimeException] { e =>
      assert(e.asInstanceOf[DeltaRuntimeException].getErrorClass ==
        "DELTA_STREAMING_SCHEMA_LOG_INIT_FAILED")
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
      metadataLog: DeltaSourceSchemaLog,
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
        sourceTrackingId: Option[String] = None)(implicit log: DeltaLog): DeltaSourceSchemaLog =
    DeltaSourceSchemaLog.create(
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
    val newSchema = PersistedSchema(log.tableId, 100,
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
    val newSchemaWithPartition = PersistedSchema(log.tableId, 100,
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
      "some_random_id", 100,
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
      val schemaLog1 = DeltaSourceSchemaLog.create(spark, schemaLocation, snapshot)
      val schemaLog2 = DeltaSourceSchemaLog.create(spark, schemaLocation, snapshot)
      val newSchema = PersistedSchema("1", 1, new StructType(), new StructType())

      schemaLog1.evolveSchema(newSchema)
      val e = intercept[DeltaAnalysisException] {
        schemaLog2.evolveSchema(newSchema)
      }
      assert(e.getErrorClass == "DELTA_STREAMING_SCHEMA_LOCATION_CONFLICT")
    }
  }

  // TODO: more tests
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
