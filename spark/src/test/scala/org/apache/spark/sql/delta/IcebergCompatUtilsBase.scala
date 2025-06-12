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

import java.util.UUID

import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * The shared utils base to be extended by corresponding suites/traits.
 */
trait IcebergCompatUtilsBase extends QueryTest {
  override protected def spark: SparkSession

  protected val compatObject: IcebergCompatBase = null

  protected def compatVersion: Int = Option(compatObject).map(_.version.toInt).getOrElse(-1)

  protected def enableCompatTableProperty: String = compatObject.config.key

  protected val compatColumnMappingMode: String = "name"

  protected val allCompatTFs = Map(
    IcebergCompatV1 -> IcebergCompatV1TableFeature,
    IcebergCompatV2 -> IcebergCompatV2TableFeature
  )

  protected def compatTableFeature: TableFeature = allCompatTFs(compatObject)

  protected val allReaderWriterVersions: Seq[(Int, Int)] = (1 to 3)
    .flatMap { r => (1 to 7).map(w => (r, w)) }
    // can only be at minReaderVersion >= 3 if minWriterVersion is >= 7
    .filterNot { case (r, w) => w < 7 && r >= 3 }

  protected val defaultSchemaName: String = "default"

  protected val defaultCatalogName: String = "main"

  def getRndTableId: TableIdentifier = {
    val rndTableName = s"testTable${UUID.randomUUID()}"
    TableIdentifier(rndTableName, Some(defaultSchemaName), Some(defaultCatalogName))
  }

  /**
   * Executes `f` with params (tableId, tempPath).
   *
   * We want to use a temp directory in addition to a unique temp table so that when the async
   * iceberg conversion runs and completes, the parent folder is still removed.
   */
  protected def withTempTableAndDir(f: (String, String) => Unit): Unit

  protected def executeSql(sqlStr: String): DataFrame

  protected def getProperties(tableId: String): Map[String, String] = {
    DeltaLog.forTable(spark, TableIdentifier(tableId)).update().getProperties.toMap
  }

  protected def assertIcebergCompatProtocolAndProperties(
      tableId: String,
      compatObj: IcebergCompatBase = compatObject): Unit = {
    val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableId)).update()
    val protocol = snapshot.protocol
    val tblProperties = snapshot.getProperties
    val tableFeature = allCompatTFs(compatObj)

    val expectedMinReaderVersion = Math.max(
      ColumnMappingTableFeature.minReaderVersion,
      tableFeature.minReaderVersion
    )

    val expectedMinWriterVersion = Math.max(
      ColumnMappingTableFeature.minWriterVersion,
      tableFeature.minWriterVersion
    )

    assert(protocol.minReaderVersion >= expectedMinReaderVersion)
    assert(protocol.minWriterVersion >= expectedMinWriterVersion)
    assert(protocol.writerFeatures.get.contains(tableFeature.name))
    assert(tblProperties(compatObj.config.key) === "true")
    assert(Seq("name", "id").contains(tblProperties("delta.columnMapping.mode")))
  }

  protected def parseIcebergVersion(metadataLocation: String): Int = {
    val versionStart = metadataLocation.lastIndexOf('/') + 1
    val versionEnd = metadataLocation.indexOf('-', versionStart)
    if (versionEnd < 0) throw new RuntimeException(
      s"No version end found in $metadataLocation: $versionEnd")
    Integer.valueOf(metadataLocation.substring(versionStart, versionEnd))
  }
}
