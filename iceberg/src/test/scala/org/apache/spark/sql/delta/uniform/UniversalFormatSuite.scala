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
}
