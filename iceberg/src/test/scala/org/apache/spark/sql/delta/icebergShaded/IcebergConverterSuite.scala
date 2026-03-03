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

package org.apache.spark.sql.delta.icebergShaded

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables

class IcebergConverterSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  /**
   * Creates a UC-managed Delta table with UniForm Iceberg enabled, calls convertSnapshot
   * directly, and verifies:
   *   1. The returned Iceberg metadata path exists on disk.
   *   2. The number of data files reported by Iceberg equals the number reported by the
   *      Delta snapshot.
   */
  test("convertSnapshot writes Iceberg metadata and file count matches Delta snapshot") {
    val tableName = "test_iceberg_converter"
    withTable(tableName) {
      // Create a catalog-managed Delta table with IcebergCompatV2 + UniForm enabled.
      spark.sql(
        s"""CREATE TABLE $tableName (id INT, name STRING) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |)""".stripMargin)

      // Insert a few rows so the snapshot has a non-trivial file count.
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')")

      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)

      val converter = new IcebergConverter()

      // convertSnapshot returns (icebergMetadataPath, lastConvertedDeltaVersion).
      val (metadataPath, _) = converter.convertSnapshot(snapshot, None, catalogTable)

      // 1. The metadata file must exist.
      assert(metadataPath != null)

      // 2. File count in Iceberg must match the Delta snapshot.
      val icebergTable =
        new HadoopTables(spark.sessionState.newHadoopConf).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt

      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
    }
  }

  /**
   * Same as above but with multiple inserts to verify the converter still produces a
   * consistent file count when the snapshot contains files from several commits.
   */
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

      spark.sql(s"INSERT INTO $tableName VALUES (1)")
      spark.sql(s"INSERT INTO $tableName VALUES (2)")
      spark.sql(s"INSERT INTO $tableName VALUES (3)")

      val tableId = TableIdentifier(tableName)
      val deltaLog = DeltaLog.forTable(spark, tableId)
      val snapshot = deltaLog.update()
      val catalogTable = spark.sessionState.catalog.getTableMetadata(tableId)

      val converter = new IcebergConverter()
      val (metadataPath, _) = converter.convertSnapshot(snapshot, None, catalogTable)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val metadataFile = new Path(metadataPath)
      val fs = metadataFile.getFileSystem(hadoopConf)
      assert(fs.exists(metadataFile), s"Iceberg metadata file should exist at $metadataPath")

      val icebergTable = new HadoopTables(hadoopConf).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt

      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
    }
  }
}
