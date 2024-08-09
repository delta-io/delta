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
  ColumnMappingTableFeature,
  DeltaLog,
  IcebergCompatV2TableFeature,
  UniversalFormat}
import org.apache.spark.sql.delta.uniform.IcebergCompatV2EnableUniformByAlterTableSuiteBase
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.TableIdentifier

class IcebergCompatV2EnableUniformByAlterTableSuite
    extends IcebergCompatV2EnableUniformByAlterTableSuiteBase
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

  override def executeSql(sqlStr: String): Unit = write(sqlStr)

  override def assertUniFormIcebergProtocolAndProperties(id: String): Unit = {
    val snapshot = DeltaLog.forTable(spark, new TableIdentifier(id)).update()
    val protocol = snapshot.protocol
    val tblProperties = snapshot.getProperties
    val tableFeature = IcebergCompatV2TableFeature

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
    assert(tblProperties(s"delta.enableIcebergCompatV2") === "true")
    assert(Seq("name", "id").contains(tblProperties("delta.columnMapping.mode")))
    assert(UniversalFormat.icebergEnabled(snapshot.metadata))
  }
}
