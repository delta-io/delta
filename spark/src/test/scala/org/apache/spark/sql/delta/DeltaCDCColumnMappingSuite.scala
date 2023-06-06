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

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaColumnMappingSelectedTestMixin

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

trait DeltaCDCColumnMappingSuiteBase extends DeltaCDCSuiteBase
  with DeltaColumnMappingTestUtils
  with DeltaColumnMappingSelectedTestMixin {

  import testImplicits._

  implicit class DataFrameDropCDCFields(df: DataFrame) {
    def dropCDCFields: DataFrame =
      df.drop(CDC_COMMIT_TIMESTAMP)
      .drop(CDC_TYPE_COLUMN_NAME)
      .drop(CDC_COMMIT_VERSION)
  }

  test("upgrade to column mapping not blocked") {
    withTempDir { dir =>
      setupInitialDeltaTable(dir, upgradeInNameMode = true)
      implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val v1 = deltaLog.update().version
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v1.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        (0 until 10).map(_.toString).map(i => Row(i, i))
      )
    }
  }

  test("add column batch cdc read not blocked") {
    withTempDir { dir =>
      // Set up an initial table with 10 records in schema <id string, value string>
      setupInitialDeltaTable(dir)
      implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

      // add column should not be blocked
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` ADD COLUMN (name string)")

      // write more data
      writeDeltaData((10 until 15))

      // None of the schema mode should block this use case
      Seq(BatchCDFSchemaLegacy, BatchCDFSchemaLatest, BatchCDFSchemaEndVersion).foreach { mode =>
        checkAnswer(
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            EndingVersion(deltaLog.update().version.toString),
            Some(mode)).dropCDCFields,
          (0 until 10).map(_.toString).toDF("id")
            .withColumn("value", col("id"))
            .withColumn("name", lit(null)) union
            (10 until 15).map(_.toString).toDF("id")
              .withColumn("value", col("id"))
              .withColumn("name", col("id")))
      }
    }
  }

  test("data type and nullability change batch cdc read blocked") {
    withTempDir { dir =>
      // Set up an initial table with 10 records in schema <id string, value string>
      setupInitialDeltaTable(dir)
      implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val s1 = deltaLog.update()
      val v1 = s1.version

      // Change the data type of column
      deltaLog.withNewTransaction { txn =>
        // id was string
        val updatedSchema =
          SchemaMergingUtils.transformColumns(
            StructType.fromDDL("id INT, value STRING")) { (_, field, _) =>
            val refField = s1.metadata.schema(field.name)
            field.copy(metadata = refField.metadata)
          }
        txn.commit(s1.metadata.copy(schemaString = updatedSchema.json) :: Nil, ManualUpdate)
      }
      val v2 = deltaLog.update().version

      // write more data in updated schema
      Seq((10, "10")).toDF("id", "value")
        .write.format("delta").mode("append").save(dir.getCanonicalPath)
      val v3 = deltaLog.update().version

      // query all changes using latest schema blocked
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v3.toString)).collect()
      }

      // query using end version also blocked if cross schema change
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          schemaMode = BatchCDFSchemaEndVersion,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v3.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }

      // query using end version NOT blocked if NOT cross schema change
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v3.toString),
          EndingVersion(v3.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        Row(10, "10") :: Nil
      )

      val s2 = deltaLog.update()

      // Change nullability unsafely
      deltaLog.withNewTransaction { txn =>
        // the schema was nullable, but we want to make it non-nullable
        val updatedSchema =
          SchemaMergingUtils.transformColumns(
            StructType.fromDDL("id INT, value string").asNullable) { (_, field, _) =>
            val refField = s1.metadata.schema(field.name)
            field.copy(metadata = refField.metadata, nullable = false)
          }
        txn.commit(s2.metadata.copy(schemaString = updatedSchema.json) :: Nil, ManualUpdate)
      }
      val v4 = deltaLog.update().version

      // write more data in updated schema
      Seq((11, "11")).toDF("id", "value")
        .write.format("delta").mode("append").save(dir.getCanonicalPath)

      val v5 = deltaLog.update().version

      // query changes using latest schema blocked
      // Note this is not detected as an illegal schema change, but a data violation, because
      // we attempt to read using latest schema @ v5 (nullable=false) to read some past data @ v3
      // (nullable=true), which is unsafe.
      assertBlocked(
          expectedIncompatSchemaVersion = v3,
          expectedReadSchemaVersion = v5,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          // v3 is the first version post the data type schema change
          StartingVersion(v3.toString),
          EndingVersion(v5.toString)).collect()
      }

      // query using end version also blocked if cross schema change
      assertBlocked(
          expectedIncompatSchemaVersion = v3,
          expectedReadSchemaVersion = v5,
          schemaMode = BatchCDFSchemaEndVersion,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v3.toString),
          EndingVersion(v5.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }

      // query using end version NOT blocked if NOT cross schema change
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v5.toString),
          EndingVersion(v5.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        Row(11, "11") :: Nil
      )
    }
  }

  test("overwrite table with invalid schema change in non-column mapping table is blocked") {
    withTempDir { dir =>
      withColumnMappingConf("none") {
        // Create table action sequence
        Seq((1, "a")).toDF("id", "name").write.format("delta").save(dir.getCanonicalPath)
        implicit val log: DeltaLog = DeltaLog.forTable(spark, dir)
        val v1 = log.update().version

        // Overwrite with dropped column
        Seq(2).toDF("id")
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(dir.getCanonicalPath)
        val v2 = log.update().version

        assertBlocked(
            expectedIncompatSchemaVersion = v1,
            expectedReadSchemaVersion = v2,
            bySchemaChange = false) {
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion(v1.toString),
            EndingVersion(v2.toString),
            schemaMode = Some(BatchCDFSchemaEndVersion)).collect()
        }

        // Overwrite with a renamed column
        Seq(3).toDF("id2")
          .write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .save(dir.getCanonicalPath)
        val v3 = log.update().version

        assertBlocked(
            expectedIncompatSchemaVersion = v2,
            expectedReadSchemaVersion = v3,
            bySchemaChange = false) {
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion(v2.toString),
            EndingVersion(v3.toString)).collect()
        }
      }
    }
  }

  test("drop column batch cdc read blocked") {
    withTempDir { dir =>
      // Set up an initial table with 10 records in schema <id string, value string>
      setupInitialDeltaTable(dir)
      implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val v1 = deltaLog.update().version

      // drop column would cause CDC read to be blocked
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` DROP COLUMN value")
      val v2 = deltaLog.update().version

      // write more data
      writeDeltaData(Seq(10))
      val v3 = deltaLog.update().version

      // query all changes using latest schema blocked
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v3.toString)).collect()
      }

      // query just first two versions which have more columns than latest schema is also blocked
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion("1")).collect()
      }

      // query unblocked if force enabled by user
      withSQLConf(
        DeltaSQLConf.DELTA_CDF_UNSAFE_BATCH_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES.key -> "true") {
        checkAnswer(
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            EndingVersion(v3.toString)).dropCDCFields,
          // Note id is dropped because we are using latest schema
          (0 until 11).map(i => Row(i.toString))
        )
      }

      // querying changes using endVersion schema blocked if crossing schema boundary
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          schemaMode = BatchCDFSchemaEndVersion,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v3.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }

      assertBlocked(
          expectedIncompatSchemaVersion = v1,
          expectedReadSchemaVersion = v3,
          schemaMode = BatchCDFSchemaEndVersion,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v1.toString),
          EndingVersion(v3.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }

      // querying changes using endVersion schema NOT blocked if NOT crossing schema boundary
      // with schema <id, value>
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v1.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        (0 until 10).map(_.toString).map(i => Row(i, i)))

      // with schema <id>
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v3.toString),
          EndingVersion(v3.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        Row("10") :: Nil
      )

      // let's add the column back...
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` ADD COLUMN (value string)")
      val v4 = deltaLog.update().version

      // write more data
      writeDeltaData(Seq(11))
      val v5 = deltaLog.update().version

      // The read is still blocked, even schema @ 0 looks the "same" as the latest schema
      // but the added column now maps to a different physical column.
      // Note that this bypasses all the schema change actions in between because:
      // 1. The schema after dropping @ v2 is a subset of the read schema -> this is fine
      // 2. The schema after adding back @ v4 is the same as latest schema -> this is fine
      // but our final check against the starting schema would catch it.
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v5,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v5.toString)).collect()
      }

      // In this case, tho there aren't any read-incompat schema changes in the querying range,
      // the latest schema is not read-compat with the data files @ v0, so we still block.
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v5,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion("1")).collect()
      }
    }
  }

  test("rename column batch cdc read blocked") {
    withTempDir { dir =>
      // Set up an initial table with 10 records in schema <id string, value string>
      setupInitialDeltaTable(dir)
      implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      val v1 = deltaLog.update().version

      // Rename column
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` RENAME COLUMN id TO id2")
      val v2 = deltaLog.update().version

      // write more data
      writeDeltaData(Seq(10))
      val v3 = deltaLog.update().version

      // query all versions using latest schema blocked
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v3.toString)).collect()
      }

      // query unblocked if force enabled by user
      withSQLConf(
        DeltaSQLConf.DELTA_CDF_UNSAFE_BATCH_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES.key -> "true") {
        val df = cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v3.toString)).dropCDCFields
        checkAnswer(df, (0 until 11).map(i => Row(i.toString, i.toString)))
        // Note we serve the batch using the renamed column in the latest schema.
        assert(df.schema.fieldNames.sameElements(Array("id2", "value")))
      }

      // query just the first few versions using latest schema also blocked
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v3,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion("1")).collect()
      }

      // query using endVersion schema across schema boundary also blocked
      assertBlocked(
          expectedIncompatSchemaVersion = 0,
          expectedReadSchemaVersion = v2,
          schemaMode = BatchCDFSchemaEndVersion,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v2.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }

      // query using endVersion schema NOT blocked if NOT crossing schema boundary
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v1.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        (0 until 10).map(_.toString).map(i => Row(i, i))
      )

      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v2.toString),
          EndingVersion(v3.toString),
          Some(BatchCDFSchemaEndVersion)).dropCDCFields,
        Row("10", "10") :: Nil
      )

      // Let's rename the column back
      sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` RENAME COLUMN id2 TO id")
      val v4 = deltaLog.update().version

      // write more data
      writeDeltaData(Seq(11))
      val v5 = deltaLog.update().version

      // query all changes using latest schema would still block because we crossed an
      //   intermediary action with a conflicting schema (the first rename).
      assertBlocked(expectedIncompatSchemaVersion = v2, expectedReadSchemaVersion = v5) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v5.toString)).collect()
      }

      // query all changes using LATEST schema would NOT block if we exclude the first
      //   rename back, because the data schemas before that are now consistent with the latest.
      checkAnswer(
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion(v1.toString)).dropCDCFields,
        (0 until 10).map(_.toString).map(i => Row(i, i)))

      // query using endVersion schema is blocked if we cross schema boundary
      assertBlocked(
          expectedIncompatSchemaVersion = v3,
          expectedReadSchemaVersion = v5,
          schemaMode = BatchCDFSchemaEndVersion,
          bySchemaChange = false) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          // v3 just pass the first schema change
          StartingVersion(v3.toString),
          EndingVersion(v5.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }

      // Note how the conflictingVersion is v2 (the first rename), because v1 matches our end
      // version schema due to renaming back.
      assertBlocked(
          expectedIncompatSchemaVersion = v2,
          expectedReadSchemaVersion = v5,
          schemaMode = BatchCDFSchemaEndVersion) {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion(v1.toString),
          EndingVersion(v5.toString),
          Some(BatchCDFSchemaEndVersion)).collect()
      }
    }
  }

  override def runOnlyTests: Seq[String] = Seq(
    "changes from table by name",
    "changes from table by path",
    "batch write: append, dynamic partition overwrite + CDF",
    // incompatible schema changes & schema mode tests
    "add column batch cdc read not blocked",
    "data type and nullability change batch cdc read blocked",
    "drop column batch cdc read blocked",
    "rename column batch cdc read blocked"
  )

  protected def assertBlocked(
      expectedIncompatSchemaVersion: Long,
      expectedReadSchemaVersion: Long,
      schemaMode: DeltaBatchCDFSchemaMode = BatchCDFSchemaLegacy,
      timeTravel: Boolean = false,
      bySchemaChange: Boolean = true)(f: => Unit)(implicit log: DeltaLog): Unit = {
    val e = intercept[DeltaUnsupportedOperationException] {
      f
    }
    val (end, readSchemaJson) = if (bySchemaChange) {
      assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_SCHEMA_CHANGE")
      val Seq(_, end, readSchemaJson, readSchemaVersion, incompatibleVersion, _, _, _, _) =
        e.getMessageParametersArray.toSeq
      assert(incompatibleVersion.toLong == expectedIncompatSchemaVersion)
      assert(readSchemaVersion.toLong == expectedReadSchemaVersion)
      (end, readSchemaJson)
    } else {
      assert(e.getErrorClass == "DELTA_CHANGE_DATA_FEED_INCOMPATIBLE_DATA_SCHEMA")
      val Seq(_, end, readSchemaJson, readSchemaVersion, incompatibleVersion, config) =
        e.getMessageParametersArray.toSeq
      assert(incompatibleVersion.toLong == expectedIncompatSchemaVersion)
      assert(readSchemaVersion.toLong == expectedReadSchemaVersion)
      assert(config == DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key)
      (end, readSchemaJson)
    }

    val latestSnapshot = log.update()
    schemaMode match {
      case BatchCDFSchemaLegacy if timeTravel =>
        // Read using time travelled schema, it can be arbitrary so nothing to check here
      case BatchCDFSchemaEndVersion =>
        // Read using end version schema
        assert(expectedReadSchemaVersion == end.toLong &&
          log.getSnapshotAt(expectedReadSchemaVersion).schema.json == readSchemaJson)
      case _ =>
        // non time-travel legacy mode and latest mode should both read latest schema
        assert(expectedReadSchemaVersion == latestSnapshot.version &&
          latestSnapshot.schema.json == readSchemaJson)
    }
  }

  /**
   * Write test delta data to test blocking column mapping for CDC batch queries, it takes a
   * sequence and write out as a row of strings, assuming the delta log's schema are all strings.
   */
  protected def writeDeltaData(
      data: Seq[Int],
      userSpecifiedSchema: Option[StructType] = None)(implicit log: DeltaLog): Unit = {
    val schema = userSpecifiedSchema.getOrElse(log.update().schema)
    data.foreach { i =>
      val data = Seq(Row(schema.map(_ => i.toString): _*))
      spark.createDataFrame(data.asJava, schema)
        .write.format("delta").mode("append").save(log.dataPath.toString)
    }
  }

  /**
   * Set up initial table data, considering current column mapping mode
   *
   * The table contains 10 rows, with schema <id, value> both are string
   */
  protected def setupInitialDeltaTable(dir: File, upgradeInNameMode: Boolean = false): Unit = {
    require(columnMappingModeString != NoMapping.name)
    val tablePath = dir.getCanonicalPath
    implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, tablePath)

    if (upgradeInNameMode && columnMappingModeString == NameMapping.name) {
      // For name mode, we do an upgrade then write to test that behavior as well
      // init table with 5 versions without column mapping
      withColumnMappingConf("none") {
        writeDeltaData((0 until 5), userSpecifiedSchema = Some(
          new StructType().add("id", StringType, true).add("value", StringType, true)
        ))
      }
      // upgrade to name mode
      val protocol = deltaLog.snapshot.protocol
      val (r, w) = if (protocol.supportsReaderFeatures || protocol.supportsWriterFeatures) {
        (TableFeatureProtocolUtils.TABLE_FEATURES_MIN_READER_VERSION,
          TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION)
      } else {
        (ColumnMappingTableFeature.minReaderVersion, ColumnMappingTableFeature.minWriterVersion)
      }
      sql(
        s"""
           |ALTER TABLE delta.`${dir.getCanonicalPath}`
           |SET TBLPROPERTIES (
           |  ${DeltaConfigs.COLUMN_MAPPING_MODE.key} = "name",
           |  ${DeltaConfigs.MIN_READER_VERSION.key} = "$r",
           |  ${DeltaConfigs.MIN_WRITER_VERSION.key} = "$w")""".stripMargin)
      // write more data
      writeDeltaData((5 until 10))
    } else {
      // For id mode and non-upgrade name mode, we could just create a table from scratch
      withColumnMappingConf(columnMappingModeString) {
        writeDeltaData((0 until 10), userSpecifiedSchema = Some(
          new StructType().add("id", StringType, true).add("value", StringType, true)
        ))
      }
    }

    checkAnswer(
      cdcRead(
        new TablePath(dir.getCanonicalPath),
        StartingVersion("0"),
        EndingVersion(deltaLog.update().version.toString)).dropCDCFields,
      (0 until 10).map(_.toString).toDF("id").withColumn("value", col("id")))
  }
}

trait DeltaCDCColumnMappingScalaSuiteBase extends DeltaCDCColumnMappingSuiteBase {

  import testImplicits._

  test("time travel with batch cdf is disbaled by default") {
    withTempDir { dir =>
      Seq(1).toDF("id").write.format("delta").save(dir.getCanonicalPath)
      val e = intercept[DeltaAnalysisException] {
        cdcRead(
          new TablePath(dir.getCanonicalPath),
          StartingVersion("0"),
          EndingVersion("1"),
          readerOptions = Map(DeltaOptions.VERSION_AS_OF -> "0")).collect()
      }
      assert(e.getErrorClass == "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS")
    }
  }

  // NOTE: we do not support time travel option with SQL API, so we will just test Scala API suite
  test("cannot specify both time travel options and schema mode") {
    withSQLConf(DeltaSQLConf.DELTA_CDF_ALLOW_TIME_TRAVEL_OPTIONS.key -> "true") {
      withTempDir { dir =>
        Seq(1).toDF("id").write.format("delta").save(dir.getCanonicalPath)
        val e = intercept[DeltaIllegalArgumentException] {
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            EndingVersion("1"),
            Some(BatchCDFSchemaEndVersion),
            readerOptions = Map(DeltaOptions.VERSION_AS_OF -> "0")).collect()
        }
        assert(e.getMessage.contains(
          DeltaSQLConf.DELTA_CDF_DEFAULT_SCHEMA_MODE_FOR_COLUMN_MAPPING_TABLE.key))
      }
    }
  }

  test("time travel option is respected") {
    withSQLConf(DeltaSQLConf.DELTA_CDF_ALLOW_TIME_TRAVEL_OPTIONS.key -> "true") {
      withTempDir { dir =>
        // Set up an initial table with 10 records in schema <id string, value string>
        setupInitialDeltaTable(dir)
        implicit val deltaLog: DeltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val v1 = deltaLog.update().version

        // Add a column
        sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` ADD COLUMN (prop string)")
        val v2 = deltaLog.update().version

        // write more data
        writeDeltaData(Seq(10))
        val v3 = deltaLog.update().version

        // Rename a column
        sql(s"ALTER TABLE delta.`${dir.getCanonicalPath}` RENAME COLUMN id TO id2")
        val v4 = deltaLog.update().version

        // write more data
        writeDeltaData(Seq(11))
        val v5 = deltaLog.update().version

        // query changes between version 0 - v1, not crossing schema boundary
        checkAnswer(
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            EndingVersion(v1.toString),
            readerOptions = Map(DeltaOptions.VERSION_AS_OF -> v1.toString)).dropCDCFields,
          (0 until 10).map(_.toString).map(i => Row(i, i)))

        // query across add column, but not cross the rename, not blocked
        checkAnswer(
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            EndingVersion(v3.toString),
            // v2 is the add column schema change
            readerOptions = Map(DeltaOptions.VERSION_AS_OF -> v2.toString)).dropCDCFields,
          // Note how the first 10 records now misses a column, but it's fine
          (0 until 10).map(_.toString).map(i => Row(i, i, null)) ++
            Seq(Row("10", "10", "10")))

        // query across rename is blocked, if we are still specifying an old version
        // note it failed at v4, because the initial schema does not conflict with schema @ v2
        assertBlocked(
            expectedIncompatSchemaVersion = v4,
            expectedReadSchemaVersion = v2,
            timeTravel = true) {
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            // v5 cross the v4 rename column
            EndingVersion(v5.toString),
            // v2 is the add column schema change
            readerOptions = Map(DeltaOptions.VERSION_AS_OF -> v2.toString)).collect()
        }

        // Even the querying range has no schema change, the data files are still not
        // compatible with the read schema due to arbitrary time travel.
        assertBlocked(
            expectedIncompatSchemaVersion = 0,
            expectedReadSchemaVersion = v4,
            timeTravel = true,
            bySchemaChange = false) {
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion("0"),
            // v1 still uses the schema prior to the rename
            EndingVersion(v1.toString),
            // v4 is the rename column change
            readerOptions = Map(DeltaOptions.VERSION_AS_OF -> v4.toString)).collect()
        }

        // But without crossing schema change boundary (v4 - v5) using v4's renamed schema,
        // we can load the batch.
        checkAnswer(
          cdcRead(
            new TablePath(dir.getCanonicalPath),
            StartingVersion(v4.toString),
            EndingVersion(v5.toString),
            readerOptions = Map(DeltaOptions.VERSION_AS_OF -> v4.toString)).dropCDCFields,
          Seq(Row("11", "11", "11")))
      }
    }
  }
}

class DeltaCDCIdColumnMappingSuite extends DeltaCDCScalaSuite
  with DeltaCDCColumnMappingScalaSuiteBase
  with DeltaColumnMappingEnableIdMode

class DeltaCDCNameColumnMappingSuite extends DeltaCDCScalaSuite
  with DeltaCDCColumnMappingScalaSuiteBase
  with DeltaColumnMappingEnableNameMode

