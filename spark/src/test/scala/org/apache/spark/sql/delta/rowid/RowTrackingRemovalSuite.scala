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

package org.apache.spark.sql.delta.rowid

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaIllegalStateException, DeltaLog, DeltaOperations, MaterializedRowCommitVersion, MaterializedRowId, RowId, RowTrackingFeature}
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.RowId.RowTrackingMetadataDomain
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ManualClock

trait RowTrackingRemovalSuiteBase
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  protected override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ROW_TRACKING_ENABLED.defaultTablePropertyKey, "true")

  def validateRowTrackingState(deltaLog: DeltaLog, isPresent: Boolean): Unit = {
    val snapshot = deltaLog.update()
    val configuration = snapshot.metadata.configuration
    val allFiles = snapshot.allFiles.collect()

    assert(RowId.isSupported(snapshot.protocol) === isPresent)
    assert(!configuration.contains(DeltaConfigs.ROW_TRACKING_SUSPENDED.key))

    if (isPresent) {
      assert(DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(snapshot.metadata))
      assert(configuration.contains(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP))
      assert(configuration.contains(MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP))
      assert(RowTrackingMetadataDomain
        .fromSnapshot(snapshot)
        .forall(_.rowIdHighWaterMark > RowId.MISSING_HIGH_WATER_MARK))
      assert(allFiles.forall(a => a.baseRowId.isDefined && a.defaultRowCommitVersion.isDefined))
    } else {
      assert(!configuration.contains(DeltaConfigs.ROW_TRACKING_ENABLED.key))
      assert(!configuration.contains(DeltaConfigs.ROW_TRACKING_SUSPENDED.key))
      assert(!configuration.contains(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP))
      assert(!configuration.contains(MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP))
      assert(RowTrackingMetadataDomain.fromSnapshot(snapshot).isEmpty)
      assert(allFiles.forall(a => a.baseRowId.isEmpty && a.defaultRowCommitVersion.isEmpty))
    }
  }

  def dropRowTracking(deltaLog: DeltaLog, truncateHistory: Boolean = false): Unit = {
    val sqlText =
      s"""
         |ALTER TABLE delta.`${deltaLog.dataPath}`
         |DROP FEATURE ${RowTrackingFeature.name}
         |${if (truncateHistory) "TRUNCATE HISTORY" else ""}
         |""".stripMargin

    sql(sqlText)
  }

  def addData(path: String, start: Long, end: Long): Unit = {
    spark.range(start, end, step = 1, numPartitions = 2)
      .write
      .format("delta")
      .mode("append")
      .save(path)
  }
}

class RowTrackingRemovalSuite extends RowTrackingRemovalSuiteBase {

  test("Basic row tracking removal") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      addData(dir.getAbsolutePath, 0, 10)
      assert(RowId.isSupported(deltaLog.update().protocol))

      dropRowTracking(deltaLog)
      validateRowTrackingState(deltaLog, isPresent = false)
    }
  }

  test("Remove row tracking and then re-enable it") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      addData(dir.getAbsolutePath, 0, 10)
      addData(dir.getAbsolutePath, 10, 20)

      val table = DeltaTable.forPath(spark, dir.getAbsolutePath)
      table.update(expr("id == 2"), Map("id" -> lit(200)))

      // Store the old materialized column names.
      val configuration = deltaLog.update().metadata.configuration
      val oldMaterializedRowIdName = configuration(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)
      val oldMaterializedRowCommitVersionName =
        configuration(MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP)

      dropRowTracking(deltaLog)
      addData(dir.getAbsolutePath, 20, 30)
      validateRowTrackingState(deltaLog, isPresent = false)

      // Re-enable row tracking.
      sql(
        s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
           |SET TBLPROPERTIES(
           |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'true'
           |)""".stripMargin)

      addData(dir.getAbsolutePath, 30, 40)

      validateRowTrackingState(deltaLog, isPresent = true)

      // Make sure the materialized column names are different.
      val newConfiguration = deltaLog.update().metadata.configuration
      val newMaterializedRowIdName =
        newConfiguration(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)
      val newMaterializedRowCommitVersionName =
        newConfiguration(MaterializedRowCommitVersion.MATERIALIZED_COLUMN_NAME_PROP)
      assert(newMaterializedRowIdName != oldMaterializedRowIdName)
      assert(newMaterializedRowCommitVersionName != oldMaterializedRowCommitVersionName)
    }
  }

  // This test verifies we can recover a drop feature failure. this is for the scenario
  // the user decides to re-enable row tracking instead of retrying drop feature.
  test("Row tracking can recover from suspension") {
    import org.apache.spark.sql.delta.implicits._

    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      def getMaterializedRowId(df: DataFrame, id: Long): Long = {
         df
          .filter(s"id == $id")
          .select(RowId.QUALIFIED_COLUMN_NAME)
          .as[Long]
          .collect()
          .head
      }

      def getHighWatermark(): Long = {
        RowId.extractHighWatermark(deltaLog.update()).getOrElse {
          throw new IllegalStateException("High watermark is missing")
        }
      }

      addData(dir.getAbsolutePath, 0, 10)

      val table = DeltaTable.forPath(spark, dir.getAbsolutePath)

      // These operations should materialize row IDs.
      table.update(expr("id == 2"), Map("id" -> lit(200)))
      table.delete("id == 4")

      val watermarkPreDisablement = getHighWatermark()
      val materializedRowIdPreDisablement = getMaterializedRowId(table.toDF, 200)

      AlterTableSetPropertiesDeltaCommand(
        table = DeltaTableV2(spark, deltaLog.dataPath),
        configuration = Map(
          DeltaConfigs.ROW_TRACKING_ENABLED.key -> "false",
          DeltaConfigs.ROW_TRACKING_SUSPENDED.key -> "true"))
        .run(spark)

      // Should not generate row IDs.
      addData(dir.getAbsolutePath, 10, 15)
      table.update(expr("id == 200"), Map("id" -> lit(300)))
      assert(getHighWatermark() === watermarkPreDisablement)

      // Lift row identity generation suspension.
      AlterTableSetPropertiesDeltaCommand(
        table = DeltaTableV2(spark, deltaLog.dataPath),
        configuration = Map(DeltaConfigs.ROW_TRACKING_SUSPENDED.key -> "false"))
        .run(spark)

      // Backfill.
      AlterTableSetPropertiesDeltaCommand(
        table = DeltaTableV2(spark, deltaLog.dataPath),
        configuration = Map(DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true"))
        .run(spark)

      // Row tracking continued from previous high watermark.
      assert(getHighWatermark() > watermarkPreDisablement)

      // Row tracking does not guarantee the materialized row ID will be the same after
      // re-enablement.
      assert(getMaterializedRowId(table.toDF, 300) != materializedRowIdPreDisablement)

      // All add files should have row IDs.
      assert(deltaLog.update().allFiles.where("baseRowId IS NULL").count() === 0)
    }
  }

  for (dvsEnabled <- BOOLEAN_DOMAIN)
  test(s"Property `delta.rowTrackingSuspended` is respected - dvsEnabled: $dvsEnabled") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
      addData(dir.getAbsolutePath, 0, 10)

      sql(
        s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
           |SET TBLPROPERTIES(
           |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'false',
           |${DeltaConfigs.ROW_TRACKING_SUSPENDED.key} = 'true',
           |${DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.key} = '$dvsEnabled'
           |)""".stripMargin)

      val filesWithRowIds = deltaLog.update().allFiles.collect().toSet

      addData(dir.getAbsolutePath, 10, 15)

      val targetTable = DeltaTable.forPath(dir.getAbsolutePath)
      targetTable.update(expr("id IN (2, 12)"), Map("id" -> lit(200)))
      targetTable.delete("id == 3")

      val allFiles = deltaLog.update()
        .allFiles
        .collect()
        .filterNot(filesWithRowIds.contains)

      assert(allFiles.forall(a => a.baseRowId.isEmpty && a.defaultRowCommitVersion.isEmpty))
    }
  }

  test(s"Cannot enable both configurations at the same time") {
    withTempDir { dir =>
      addData(dir.getAbsolutePath, 0, 10)

      sql(
        s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
           |SET TBLPROPERTIES(
           |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'true'
           |)""".stripMargin)

      assertThrows[IllegalStateException] {
        sql(
          s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
             |SET TBLPROPERTIES(
             |${DeltaConfigs.ROW_TRACKING_SUSPENDED.key} = 'true'
             |)""".stripMargin)
      }

      sql(
        s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
           |SET TBLPROPERTIES(
           |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'false',
           |${DeltaConfigs.ROW_TRACKING_SUSPENDED.key} = 'true'
           |)""".stripMargin)


      assertThrows[IllegalStateException] {
        sql(
          s"""ALTER TABLE delta.`${dir.getAbsolutePath}`
             |SET TBLPROPERTIES(
             |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'true'
             |)""".stripMargin)
      }
    }
  }

  test("Third party writer enables row tracking without disabling suspension property") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      addData(dir.getAbsolutePath, 0, 10)

      // Third party writer messes up the configs.
      val txn = deltaLog.startTransaction(catalogTableOpt = None)
      val newConfiguration = txn.metadata.configuration ++ Map(
        DeltaConfigs.ROW_TRACKING_ENABLED.key -> "true",
        DeltaConfigs.ROW_TRACKING_SUSPENDED.key -> "true"
      )
      txn.updateMetadata(txn.metadata.copy(configuration = newConfiguration))
      txn.commit(Seq.empty, DeltaOperations.ManualUpdate)


      val e = intercept[DeltaIllegalStateException] {
        addData(dir.getAbsolutePath, 10, 20)
      }
      checkError(
        e,
        "DELTA_ROW_TRACKING_ILLEGAL_PROPERTY_COMBINATION",
        parameters = Map(
          "property1" -> DeltaConfigs.ROW_TRACKING_ENABLED.key,
          "property2" -> DeltaConfigs.ROW_TRACKING_SUSPENDED.key))
    }
  }
}
