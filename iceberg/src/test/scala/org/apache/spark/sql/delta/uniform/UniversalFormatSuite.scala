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

import org.apache.spark.sql.delta.{
  DeltaLog,
  IcebergCompatUtilsBase,
  UniFormWithIcebergCompatV1SuiteBase,
  UniFormWithIcebergCompatV2SuiteBase,
  UniversalFormatMiscSuiteBase,
  UniversalFormatSuiteBase}
import org.apache.spark.sql.delta.commands.DeltaReorgTableCommand
import org.apache.spark.sql.delta.icebergShaded.IcebergTransactionUtils
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

/** Contains shared tests for both IcebergCompatV1 and IcebergCompatV2 suites. */
trait UniversalFormatSuiteTestBase extends UniversalFormatSuiteUtilsBase {
  private def loadIcebergTable(
      spark: SparkSession,
      id: TableIdentifier): Option[shadedForDelta.org.apache.iceberg.Table] = {
    val deltaLog = DeltaLog.forTable(spark, id)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(id)
    val hiveCatalog = IcebergTransactionUtils
      .createHiveCatalog(deltaLog.newDeltaHadoopConf())
    val icebergTableId = IcebergTransactionUtils
      .convertSparkTableIdentifierToIcebergHive(catalogTable.identifier)
    if (hiveCatalog.tableExists(icebergTableId)) {
      Some(hiveCatalog.loadTable(icebergTableId))
    } else {
      None
    }
  }

  protected def getCompatVersionOtherThan(version: Int): Int

  protected def getCompatVersionsOtherThan(version: Int): Seq[Int]

  test("CTAS new UniForm (Iceberg) table without manually enabling column mapping") {
    // These are the versions with column mapping enabled
    allReaderWriterVersions.foreach { case (r, w) =>
      withTempTableAndDir { case (from_id, from_loc) =>
        withTempTableAndDir { case (to_id1, to_loc1) =>
          withTempTableAndDir { case (to_id2, to_loc2) =>
            executeSql(
              s"""
                 |CREATE TABLE $from_id (PID INT, PCODE INT) USING DELTA LOCATION $from_loc
                 | TBLPROPERTIES (
                 |  'delta.minReaderVersion' = $r,
                 |  'delta.minWriterVersion' = $w
                 |)""".stripMargin)
            executeSql(
              s"""
                 |INSERT INTO TABLE $from_id (PID, PCODE)
                 | VALUES (1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8)""".stripMargin)
            executeSql(
              s"""
                 |CREATE TABLE $to_id1 USING DELTA LOCATION $to_loc1 TBLPROPERTIES (
                 |  'delta.universalFormat.enabledFormats' = 'iceberg',
                 |  'delta.enableIcebergCompatV$compatVersion' = 'true',
                 |  'delta.minReaderVersion' = $r,
                 |  'delta.minWriterVersion' = $w
                 |) AS SELECT * FROM $from_id""".stripMargin)
            executeSql(
              s"""
                 |CREATE TABLE $to_id2 USING DELTA LOCATION $to_loc2 TBLPROPERTIES (
                 |  'delta.universalFormat.enabledFormats' = 'iceberg',
                 |  'delta.columnMapping.mode' = 'name',
                 |  'delta.enableIcebergCompatV$compatVersion' = 'true',
                 |  'delta.minReaderVersion' = $r,
                 |  'delta.minWriterVersion' = $w
                 |) AS SELECT * FROM $from_id""".stripMargin)

            val icebergTable1 = loadIcebergTable(spark, new TableIdentifier(to_id1))
            if (icebergTable1.isDefined) {
              assert(icebergTable1.get.schema().asStruct().fields().toArray.length == 2)
            }

            val expected1: Array[Row] = (1 to 7).map { i => Row(i, i + 1) }.toArray
            readAndVerify(to_id1, "PID, PCODE", "PID", expected1)

            val icebergTable2 = loadIcebergTable(spark, new TableIdentifier(to_id2))
            if (icebergTable2.isDefined) {
              assert(icebergTable2.get.schema().asStruct().fields().toArray.length == 2)
            }

            val expected2: Array[Row] = (1 to 7).map { i => Row(i, i + 1) }.toArray
            readAndVerify(to_id2, "PID, PCODE", "PID", expected2)
          }
        }
      }
    }
  }

  test("REORG TABLE: command does not support where clause") {
    withTempTableAndDir { case (id, loc) =>
      val anotherCompatVersion = getCompatVersionOtherThan(compatVersion)
      executeSql(s"""
                    | CREATE TABLE $id (ID INT) USING DELTA LOCATION $loc TBLPROPERTIES (
                    |  'delta.universalFormat.enabledFormats' = 'iceberg',
                    |  'delta.enableIcebergCompatV$anotherCompatVersion' = 'true'
                    |)""".stripMargin)
      val e = intercept[ParseException] {
        spark.sessionState.sqlParser.parsePlan(
          s"""
             | REORG TABLE $id
             | WHERE ID > 0
             | APPLY (UPGRADE UNIFORM (ICEBERGCOMPATVERSION = $compatVersion))
         """.stripMargin).asInstanceOf[DeltaReorgTableCommand]
      }
      assert(e.getErrorClass === "PARSE_SYNTAX_ERROR")
      assert(e.getMessage.contains("Syntax error at or near 'REORG'"))
    }
  }
}

class UniversalFormatSuite
    extends UniversalFormatMiscSuiteBase
    with UniversalFormatSuiteUtilsBase

class UniFormWithIcebergCompatV1Suite
    extends UniversalFormatSuiteTestBase
    with UniFormWithIcebergCompatV1SuiteBase

class UniFormWithIcebergCompatV2Suite
    extends UniversalFormatSuiteTestBase
    with UniFormWithIcebergCompatV2SuiteBase
