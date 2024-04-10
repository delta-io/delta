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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaLog, MaterializedRowId, RowId}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.rowtracking.RowTrackingTestUtils
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData

import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest

trait RowIdTestUtils extends RowTrackingTestUtils with DeltaSQLCommandTest with ParquetTest {
  val QUALIFIED_BASE_ROW_ID_COLUMN_NAME = s"${FileFormat.METADATA_NAME}.${RowId.BASE_ROW_ID}"

  protected def getRowIdRangeInclusive(f: AddFile): (Long, Long) = {
    val min = f.baseRowId.get
    val max = min + f.numPhysicalRecords.get - 1L
    (min, max)
  }

  def assertRowIdsDoNotOverlap(log: DeltaLog): Unit = {
    val files = log.update().allFiles.collect()

    val sortedRanges = files
      .map(f => (f.path, getRowIdRangeInclusive(f)))
      .sortBy { case (_, (min, _)) => min }

    for (i <- sortedRanges.indices.dropRight(1)) {
      val (curPath, (_, curMax)) = sortedRanges(i)
      val (nextPath, (nextMin, _)) = sortedRanges(i + 1)
      assert(curMax < nextMin, s"$curPath and $nextPath have overlapping row IDs")
    }
  }

  def assertHighWatermarkIsCorrect(log: DeltaLog): Unit = {
    val snapshot = log.update()
    val files = snapshot.allFiles.collect()

    val highWatermarkOpt = RowId.extractHighWatermark(snapshot)
    if (files.isEmpty) {
      assert(highWatermarkOpt.isDefined)
    } else {
      val maxAssignedRowId = files
        .map(a => a.baseRowId.get + a.numPhysicalRecords.get - 1L)
        .max
      assert(highWatermarkOpt.get == maxAssignedRowId)
    }
  }

  def assertRowIdsAreValid(log: DeltaLog): Unit = {
    assertRowIdsDoNotOverlap(log)
    assertHighWatermarkIsCorrect(log)
  }

  def assertHighWatermarkIsCorrectAfterUpdate(
      log: DeltaLog, highWatermarkBeforeUpdate: Long, expectedNumRecordsWritten: Long): Unit = {
    val highWaterMarkAfterUpdate = RowId.extractHighWatermark(log.update()).get
    assert((highWatermarkBeforeUpdate + expectedNumRecordsWritten) === highWaterMarkAfterUpdate)
    assertRowIdsAreValid(log)
  }

  def assertRowIdsAreNotSet(log: DeltaLog): Unit = {
    val snapshot = log.update()

    val highWatermarks = RowId.extractHighWatermark(snapshot)
    assert(highWatermarks.isEmpty)

    val files = snapshot.allFiles.collect()
    assert(files.forall(_.baseRowId.isEmpty))
  }

  def assertRowIdsAreLargerThanValue(log: DeltaLog, value: Long): Unit = {
    log.update().allFiles.collect().foreach { f =>
      val minRowId = getRowIdRangeInclusive(f)._1
      assert(minRowId > value, s"${f.toString} has a row id smaller or equal than $value")
    }
  }

  def extractMaterializedRowIdColumnName(log: DeltaLog): Option[String] = {
    log.update().metadata.configuration.get(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)
  }

  protected def readRowGroupsPerFile(dir: File): Seq[Seq[BlockMetaData]] = {
    assert(dir.isDirectory)
    readAllFootersWithoutSummaryFiles(
      // scalastyle:off deltahadoopconfiguration
      new Path(dir.getAbsolutePath), spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      .map(_.getParquetMetadata.getBlocks.asScala.toSeq)
  }

  protected def checkFileLayout(
      dir: File,
      numFiles: Int,
      numRowGroupsPerFile: Int,
      rowCountPerRowGroup: Int): Unit = {
    val rowGroupsPerFile = readRowGroupsPerFile(dir)
    assert(numFiles === rowGroupsPerFile.size)
    for (rowGroups <- rowGroupsPerFile) {
      assert(numRowGroupsPerFile === rowGroups.size)
      for (rowGroup <- rowGroups) {
        assert(rowCountPerRowGroup === rowGroup.getRowCount)
      }
    }
  }
}
