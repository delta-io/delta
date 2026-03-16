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

import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.delta.icebergShaded.{IcebergConverter, UNIFORM_CC_MODE}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession

class IcebergConverterForTest extends IcebergConverter {
  def convertSnapshotAndReturnMetadataPath(
      snapshotToConvert: Snapshot,
      catalogTable: CatalogTable): String = {
    val icebergTxn = convertSnapshotInternal(
      snapshotToConvert,
      readSnapshotOpt = None,
      lastConvertedInfo = LastConvertedIcebergInfo(
        icebergTable = None,
        icebergSnapshotId = None,
        deltaVersionConverted = None,
        baseMetadataLocationOpt = None
      ),
      conversionContext = new ConversionContext(
        conversionMode = UNIFORM_CC_MODE,
        additionalDeltaActionsToCommit = None,
        opType = "delta.iceberg.conversion.convertSnapshot"
      ),
      catalogTable
    )
    icebergTxn.getConvertedIcebergMetadata._1
  }
}

class UniFormConverterSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {
  test("convertSnapshot writes Iceberg metadata and file count matches Delta snapshot") {
    val tableName = "test_iceberg_converter"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT, name STRING) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      // Do write
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      // Trigger conversion
      val converter = new IcebergConverterForTest()
      val metadataPath = converter.convertSnapshotAndReturnMetadataPath(snapshot, catalogTable)
      // Check match
      val icebergTable =
        new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
    }
  }

  test("convertSnapshot file count matches Delta snapshot after multiple inserts") {
    val tableName = "test_iceberg_converter_multi"
    withTable(tableName) {
      spark.sql(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)
      // Do some writes
      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")
      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)
      assert(snapshot.numOfFiles == 3)
      // Trigger conversion
      val converter = new IcebergConverterForTest()
      val metadataPath = converter.convertSnapshotAndReturnMetadataPath(snapshot, catalogTable)
      // Check match
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
    }
  }
}
