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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.actions.{AddFile, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class MaterializePartitionColumnsFeatureSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private def validateWriterFeatureEnabled(deltaLog: DeltaLog, isEnabled: Boolean): Unit = {
    val protocol = deltaLog.update().protocol
    assert(protocol.isFeatureSupported(MaterializePartitionColumnsTableFeature) == isEnabled)
    assert(protocol.writerFeatures.getOrElse(Set.empty).contains("materializePartitionColumns")
      == isEnabled)
    assert(!protocol.readerFeatures.getOrElse(Set.empty).contains("materializePartitionColumns"))
  }

  private def validateAddedFilesFromLastOperationMaterializedPartitionColumn(
      deltaLog: DeltaLog, expectedMaterialized: Boolean, partCol: Seq[String]): Unit = {
    val snapshot = deltaLog.update()
    val currentVersion = snapshot.version
    val addedFiles = deltaLog.getChanges(currentVersion).flatMap(_._2).collect {
      case a: AddFile => a
    }
    assert(addedFiles.nonEmpty)
    val logicalToPhysicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(
      snapshot.schema)
    val physicalPartCol = logicalToPhysicalNameMap(partCol).head

    addedFiles.foreach { file =>
      val filePath = DeltaFileOperations.absolutePath(deltaLog.dataPath.toString, file.path)
      val path = new Path(filePath.toString)
      val fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))
      val parquetSchema = try {
        val metaData = fileReader.getFooter
        metaData.getFileMetaData.getSchema
      } finally {
        fileReader.close()
      }
      val fieldNames = parquetSchema.getFields.asScala.map(_.getName).toSet
      assert(fieldNames.contains(physicalPartCol) == expectedMaterialized)
    }
  }

  Seq(true, false).foreach { enable =>
    test("MaterializePartitionColumnsTableFeature is auto-enabled when table property is set - " +
        s"enable=$enable") {
      val tbl = "tbl"
      withTable(tbl) {
        sql(
          s"""CREATE TABLE $tbl (id LONG, partCol INT)
             |USING DELTA
             |PARTITIONED BY (partCol)
             |""".stripMargin)
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('${DeltaConfigs
          .ENABLE_MATERIALIZE_PARTITION_COLUMNS_FEATURE.key}' = '$enable')")

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        validateWriterFeatureEnabled(deltaLog, isEnabled = enable)
      }
    }
  }

  test("DROP / Add back feature for materializePartitionColumns removes / adds feature and " +
      "stops / starts materializing columns") {
    val tbl = "tbl"
    withTable(tbl) {
      // Create table with materializePartitionColumns feature enabled
      sql(
        s"""CREATE TABLE $tbl (id LONG, partCol INT)
           |USING DELTA
           |PARTITIONED BY (partCol)
           |""".stripMargin)
      sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('${DeltaConfigs
        .ENABLE_MATERIALIZE_PARTITION_COLUMNS_FEATURE.key}' = 'true')")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))

      // Verify feature is enabled
      validateWriterFeatureEnabled(deltaLog, isEnabled = true)

      // Insert data - partition columns should be materialized
      sql(s"INSERT INTO $tbl VALUES (1, 100), (2, 200)")
      validateAddedFilesFromLastOperationMaterializedPartitionColumn(
        deltaLog, expectedMaterialized = true, partCol = Seq("partCol"))

      // Drop the feature
      sql(s"ALTER TABLE $tbl DROP FEATURE materializePartitionColumns")

      // Verify feature is removed from protocol
      validateWriterFeatureEnabled(deltaLog, isEnabled = false)

      // Insert more data - new files should NOT have materialized partition columns
      sql(s"INSERT INTO $tbl VALUES (3, 300), (4, 400)")
      validateAddedFilesFromLastOperationMaterializedPartitionColumn(
        deltaLog, expectedMaterialized = false, partCol = Seq("partCol"))

      // Add table feature back and verify partition columns are materialized again
      sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('${DeltaConfigs
        .ENABLE_MATERIALIZE_PARTITION_COLUMNS_FEATURE.key}' = 'true')")
      validateWriterFeatureEnabled(deltaLog, isEnabled = true)

      // Insert data - partition columns should be materialized again, all the files
      // including old and new should have partition columns
      sql(s"INSERT INTO $tbl VALUES (5, 500), (6, 600)")
      validateAddedFilesFromLastOperationMaterializedPartitionColumn(
        deltaLog, expectedMaterialized = true, partCol = Seq("partCol"))
    }
  }
}
