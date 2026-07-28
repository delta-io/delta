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

package org.apache.spark.sql.delta.uniform

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{
  DeltaLog,
  IcebergCompatBase,
  IcebergCompatUtilsBase,
  IcebergCompatV3,
  UniFormWithIcebergCompatV1SuiteBase,
  UniFormWithIcebergCompatV2SuiteBase,
  UniversalFormatMiscSuiteBase,
  UniversalFormatSuiteBase}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.DeltaReorgTableCommand
import org.apache.spark.sql.delta.icebergShaded.IcebergTransactionUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.uniform.UniFormIcebergVerifier
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException

/** Contains shared utils for both IcebergCompatV1, IcebergCompatV2 and MISC suites. */
trait UniversalFormatSuiteUtilsBase
    extends IcebergCompatUtilsBase
    with WriteDeltaHMSReadIceberg {
  override def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { dir =>
      val tablePath = new Path(dir.toString, "table")

      withTable(tableId) {
        f(tableId, s"'$tablePath'")
      }
    }
  }

  override def executeSql(sqlStr: String): DataFrame = write(sqlStr)

  override protected val allReaderWriterVersions: Seq[(Int, Int)] = (1 to 3)
    .flatMap { r => (1 to 7).filter(_ != 6).map(w => (r, w)) }
    // can only be at minReaderVersion >= 3 if minWriterVersion is >= 7
    .filterNot { case (r, w) => w < 7 && r >= 3 }
}

class UniversalFormatSuite
    extends UniversalFormatMiscSuiteBase
    with UniversalFormatSuiteUtilsBase

class UniFormWithIcebergCompatV1Suite
    extends UniversalFormatSuiteUtilsBase
    with UniFormWithIcebergCompatV1SuiteBase

class UniFormWithIcebergCompatV2Suite
    extends UniversalFormatSuiteUtilsBase
    with UniFormWithIcebergCompatV2SuiteBase

class UniFormWithIcebergCompatV3Suite
  extends IcebergCompatUtilsBase
    with WriteDeltaUCCCReadIceberg {

  import org.apache.spark.sql.delta.test.DeltaTestImplicits._

  override val compatObject: IcebergCompatBase = IcebergCompatV3

  override def executeSql(sqlStr: String): DataFrame = write(sqlStr)

  override def withTempTableAndDir(f: (String, String) => Unit): Unit = {
    val tableId = s"testTable${UUID.randomUUID()}".replace("-", "_")
    withTempDir { dir =>
      val tablePath = new Path(dir.toString, "table")

      withTable(tableId) {
        f(tableId, s"'$tablePath'")
      }
    }
  }

  test("row tracking information should be converted") {
    withTempTableAndDir { case (id, _) =>
      executeSql(
        s"""
           |CREATE TABLE $id (ID INT) USING DELTA TBLPROPERTIES (
           |  'delta.universalFormat.enabledFormats' = 'iceberg',
           |  'delta.enableIcebergCompatV$compatVersion' = 'true'
           |  $requiredTableProperties
           |)""".stripMargin)
      // TODO: Iceberg first_row_id is assigned by file add-order, which is non-deterministic for
      // multi-file commits, so per-file baseRowId != firstRowId flakes. Until the converter
      // assigns first_row_id deterministically (sort added files by baseRowId), force one file per
      // txn via optimized writes so the row-tracking conversion is deterministic.
      withSQLConf("spark.databricks.delta.optimizeWrite.enabled" -> "true") {
        executeSql(s"insert into $id values (1), (2), (3)")
        executeSql(s"update $id set id = 100 where id = 1")
      }

      val identifier = TableIdentifier(id)
      val table = DeltaTableV2(spark, identifier)
      val deltaLog = table.deltaLog
      val icebergTable = UniFormIcebergVerifier.loadIcebergTableFromUC(id)

      new UniFormIcebergVerifier(spark, deltaLog, table.catalogTable, icebergTable).verify()
    }
  }

  test("V2 to V3 upgrade preserves Iceberg snapshot lineage") {
    withTempTableAndDir { case (id, _) =>
      withSQLConf(
        DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "1") {
        executeSql(
          s"""
             |CREATE TABLE $id (ID INT) USING DELTA TBLPROPERTIES (
             |  'delta.universalFormat.enabledFormats' = 'iceberg',
             |  'delta.enableIcebergCompatV2' = 'true'
             |  $requiredTableProperties
             |)""".stripMargin)
        executeSql(s"INSERT INTO $id VALUES (1), (2), (3)")
        executeSql(s"INSERT INTO $id VALUES (4), (5), (6)")

        val tableBeforeUpgrade = UniFormIcebergVerifier.loadIcebergTableFromUC(id)
        val snapshotsBeforeUpgrade = tableBeforeUpgrade.snapshots().asScala.toSeq
        val snapshotIdsBeforeUpgrade = snapshotsBeforeUpgrade.map(_.snapshotId()).toSet
        val checkpointSnapshotId = snapshotsBeforeUpgrade.minBy(_.sequenceNumber()).snapshotId()
        val currentSnapshotId = tableBeforeUpgrade.currentSnapshot().snapshotId()
        assert(
          checkpointSnapshotId != currentSnapshotId,
          "Test setup requires a streaming checkpoint behind the current Iceberg snapshot.")

        executeSql(
          s"""
             |ALTER TABLE $id SET TBLPROPERTIES (
             |  'delta.enableIcebergCompatV2' = 'false',
             |  'delta.enableIcebergCompatV3' = 'true'
             |)""".stripMargin)

        val tableAfterUpgrade = UniFormIcebergVerifier.loadIcebergTableFromUC(id)
        val snapshotsAfterUpgrade = tableAfterUpgrade.snapshots().asScala.toSeq
        assert(
          snapshotIdsBeforeUpgrade.subsetOf(snapshotsAfterUpgrade.map(_.snapshotId()).toSet),
          "The upgrade must retain snapshots that may be referenced by streaming checkpoints.")
        assert(
          tableAfterUpgrade.snapshot(checkpointSnapshotId) != null,
          "The snapshot stored in a streaming checkpoint must remain resolvable.")

        val upgradeSnapshots = snapshotsAfterUpgrade
          .filterNot(snapshot => snapshotIdsBeforeUpgrade.contains(snapshot.snapshotId()))
        assert(upgradeSnapshots.nonEmpty, "The upgrade should commit at least one snapshot.")
        assert(
          upgradeSnapshots.forall(_.operation() == "replace"),
          "Row Tracking backfill must use metadata-only replace snapshots.")
        assert(
          upgradeSnapshots.minBy(_.sequenceNumber()).parentId() == currentSnapshotId,
          "The first upgrade snapshot must extend the pre-upgrade snapshot chain.")
      }
    }
  }
}
