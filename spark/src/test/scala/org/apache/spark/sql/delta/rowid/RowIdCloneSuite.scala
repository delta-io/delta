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

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaIllegalStateException, DeltaLog, RowId}
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.TABLE_FEATURES_MIN_WRITER_VERSION
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class RowIdCloneSuite
  extends QueryTest
  with SharedSparkSession
  with RowIdTestUtils {


  val numRows = 10

  test("clone assigns fresh row IDs when explicitly adding row IDs support") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = false, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = false, tableState = TableState.NON_EXISTING)) {
      cloneTable(
        targetTableName = "target",
        sourceTableName = "source",
        tblProperties = s"'$rowTrackingFeatureName' = 'supported'" :: Nil)

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreValid(targetLog)
      assert(RowId.isSupported(snapshot.protocol))
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  test("clone a table with row tracking enabled into non-existing target " +
    "enables row tracking even if disabled by default") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = true, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = false, tableState = TableState.NON_EXISTING)) {
      withRowTrackingEnabled(enabled = false) {
        cloneTable(targetTableName = "target", sourceTableName = "source")

        val (targetLog, snapshot) =
          DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
        assertRowIdsAreValid(targetLog)
        assert(RowId.isSupported(targetLog.update().protocol))
        assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))
      }
    }
  }

  for (enableRowIdsForSource <- BOOLEAN_DOMAIN)
  test("self-clone an empty table does not change the table's Row Tracking " +
    s"enablement and does not set Row IDs, enableRowIdsForSource=$enableRowIdsForSource") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = enableRowIdsForSource, tableState = TableState.EMPTY)) {
      cloneTable(targetTableName = "source", sourceTableName = "source")

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("source"))
      assertRowIdsAreNotSet(targetLog)
      assert(RowId.isSupported(targetLog.update().protocol) === enableRowIdsForSource)
      assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata) === enableRowIdsForSource)
    }
  }

  for {
    rowIdsEnabledOnSource <- BOOLEAN_DOMAIN
    targetTableState <- Seq(TableState.EMPTY, TableState.NON_EXISTING)
  } {
    test("clone from empty source into an empty or non-existing target " +
      s"does not assign row IDs, rowIdsEnabledOnSource=$rowIdsEnabledOnSource, " +
      s"targetTableState=$targetTableState") {
      withTables(
        TableSetupInfo(tableName = "source",
          rowIdsEnabled = rowIdsEnabledOnSource, tableState = TableState.EMPTY),
        TableSetupInfo(tableName = "target",
          rowIdsEnabled = false, tableState = targetTableState)) {
        cloneTable(targetTableName = "target", sourceTableName = "source")

        val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
        assertRowIdsAreNotSet(targetLog)
        assert(RowId.isSupported(snapshot.protocol) === rowIdsEnabledOnSource)
        assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata) === rowIdsEnabledOnSource)
      }
    }
  }

  for (targetTableState <- Seq(TableState.EMPTY, TableState.NON_EXISTING))
  test("clone from empty source into an empty or non-existing target " +
    s"using property override does not assign row IDs, targetTableState=$targetTableState") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = false, tableState = TableState.EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = false, tableState = targetTableState)) {

      cloneTable(
        targetTableName = "target",
        sourceTableName = "source",
        tblProperties = s"'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = true" :: Nil)

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreNotSet(targetLog)
      assert(RowId.isSupported(snapshot.protocol))
      assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  test("clone that add row ID feature using table property override " +
    "doesn't enable row IDs on target") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = false, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = false, tableState = TableState.EMPTY)) {

      cloneTable(
        targetTableName = "target",
        sourceTableName = "source",
        tblProperties = s"'$rowTrackingFeatureName' = 'supported'" ::
                        s"'delta.minWriterVersion' = $TABLE_FEATURES_MIN_WRITER_VERSION" :: Nil)

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreValid(targetLog)
      assert(RowId.isSupported(snapshot.protocol))
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  test("clone can disable row IDs using property override") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = true, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = true, tableState = TableState.EMPTY)) {

      cloneTable(
        targetTableName = "target",
        sourceTableName = "source",
        tblProperties = s"'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = false" :: Nil)

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreValid(targetLog)
      assert(RowId.isSupported(snapshot.protocol))
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  test("clone throws error when assigning row IDs without stats") {
    withSQLConf(
      DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {
      withTable("source", "target") {
        withRowTrackingEnabled(enabled = false) {
          spark.range(end = 10)
            .write.format("delta").saveAsTable("source")
        }
        withRowTrackingEnabled(enabled = true) {
          // enable stats to create table with row IDs
          withSQLConf(DeltaSQLConf.DELTA_COLLECT_STATS.key -> "true") {
            spark.range(0).write.format("delta").saveAsTable("target")
          }
          val err = intercept[DeltaIllegalStateException] {
            cloneTable(targetTableName = "target", sourceTableName = "source")
          }
          checkError(err, "DELTA_ROW_ID_ASSIGNMENT_WITHOUT_STATS")
        }
      }
    }
  }

  test("clone can enable row tracking on empty target using property override") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = false, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = false, tableState = TableState.EMPTY)) {

      cloneTable(
        targetTableName = "target",
        sourceTableName = "source",
        tblProperties = s"'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = true" :: Nil)

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreValid(targetLog)
      assert(RowId.isSupported(snapshot.protocol))
      assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  test("clone assigns fresh row IDs for empty target") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = false, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = true, tableState = TableState.EMPTY)) {
      cloneTable(targetTableName = "target", sourceTableName = "source")

      val (targetLog, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreValid(targetLog)
      assert(RowId.isSupported(snapshot.protocol))
      assert(!RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  test("clone can't assign row IDs for non-empty target") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = false, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = true, tableState = TableState.NON_EMPTY)) {
      assert(intercept[DeltaIllegalStateException] {
        cloneTable(targetTableName = "target", sourceTableName = "source")
      }.getErrorClass === "DELTA_UNSUPPORTED_NON_EMPTY_CLONE")
    }
  }

  test("clone from source with row tracking enabled into existing empty target " +
    "without row tracking enables row tracking") {
    withTables(
      TableSetupInfo(tableName = "source",
        rowIdsEnabled = true, tableState = TableState.NON_EMPTY),
      TableSetupInfo(tableName = "target",
        rowIdsEnabled = false, tableState = TableState.EMPTY)) {
      cloneTable(targetTableName = "target", sourceTableName = "source")

      val (targetLog, snapshot) =
        DeltaLog.forTableWithSnapshot(spark, TableIdentifier("target"))
      assertRowIdsAreValid(targetLog)
      assert(RowId.isSupported(targetLog.update().protocol))
      assert(RowId.isEnabled(snapshot.protocol, snapshot.metadata))
    }
  }

  def cloneTable(
      targetTableName: String,
      sourceTableName: String,
      tblProperties: Seq[String] = Seq.empty): Unit = {
    val tblPropertiesStr = if (tblProperties.nonEmpty) {
      s"TBLPROPERTIES ${tblProperties.mkString("(", ",", ")")}"
    } else {
      ""
    }
    sql(
      s"""
         |CREATE OR REPLACE TABLE $targetTableName
         |SHALLOW CLONE $sourceTableName
         |$tblPropertiesStr
         |""".stripMargin)
  }

  final object TableState extends Enumeration {
    type TableState = Value
    val NON_EMPTY, EMPTY, NON_EXISTING = Value
  }

  case class TableSetupInfo(
      tableName: String,
      rowIdsEnabled: Boolean,
      tableState: TableState.TableState)

  private def withTables(tables: TableSetupInfo*)(f: => Unit): Unit = {
    if (tables.isEmpty) {
      f
    } else {
      val firstTable = tables.head
      withTable(firstTable.tableName) {
        firstTable.tableState match {
          case TableState.NON_EMPTY | TableState.EMPTY =>
            val rows = if (firstTable.tableState == TableState.NON_EMPTY) {
              spark.range(start = 0, end = numRows, step = 1, numPartitions = 1)
            } else {
              spark.range(0)
            }
            withRowTrackingEnabled(enabled = firstTable.rowIdsEnabled) {
              rows.write.format("delta").saveAsTable(firstTable.tableName)
            }
          case TableState.NON_EXISTING =>
        }

        withTables(tables.drop(1): _*)(f)
      }
    }
  }
}
