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

package org.apache.spark.sql.delta.deletionvectors

import org.apache.spark.sql.delta.RowIndexFilter
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor._
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore._
import org.apache.spark.sql.delta.util.PathWithFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.ByteType
import org.apache.spark.util.Utils

class DeletedRowsMarkingFilterSuite extends QueryTest with SharedSparkSession {

  test("empty deletion vector") {
    val rowIndexFilter = DeletedRowsMarkingFilter.createInstance(
      DeletionVectorDescriptor.EMPTY,
      newHadoopConf,
      tablePath = None)

    assert(eval(rowIndexFilter, start = 0, end = 20) === Seq.empty)
    assert(eval(rowIndexFilter, start = 20, end = 200) === Seq.empty)
    assert(eval(rowIndexFilter, start = 200, end = 2000) === Seq.empty)
  }

  Seq(true, false).foreach { isInline =>
    test(s"deletion vector single row deleted - isInline=$isInline") {
      withTempDir { tableDir =>
        val tablePath = stringToPath(tableDir.toString)
        val dv = createDV(isInline, tablePath, 25)

        val rowIndexFilter =
          DeletedRowsMarkingFilter.createInstance(dv, newHadoopConf, Some(tablePath))

        assert(eval(rowIndexFilter, start = 0, end = 20) === Seq.empty)
        assert(eval(rowIndexFilter, start = 20, end = 35) === Seq(25))
        assert(eval(rowIndexFilter, start = 35, end = 325) === Seq.empty)
      }
    }
  }

  Seq(true, false).foreach { isInline =>
    test(s"deletion vector with multiple rows deleted - isInline=$isInline") {
      withTempDir { tableDir =>
        val tablePath = stringToPath(tableDir.toString)
        val dv = createDV(isInline, tablePath, 0, 25, 35, 2000, 50000)

        val rowIndexFilter =
          DeletedRowsMarkingFilter.createInstance(dv, newHadoopConf, Some(tablePath))

        assert(eval(rowIndexFilter, start = 0, end = 20) === Seq(0))
        assert(eval(rowIndexFilter, start = 20, end = 35) === Seq(25))
        assert(eval(rowIndexFilter, start = 35, end = 325) === Seq(35))
        assert(eval(rowIndexFilter, start = 325, end = 1000) === Seq.empty)
        assert(eval(rowIndexFilter, start = 1000, end = 60000) === Seq(2000, 50000))
        assert(eval(rowIndexFilter, start = 60000, end = 800000) === Seq.empty)
      }
    }
  }

  private def newBatch(capacity: Int): WritableColumnVector =
    new OnHeapColumnVector(capacity, ByteType)

  protected def newHadoopConf: Configuration = {
    // scalastyle:off deltahadoopconfiguration
    spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
  }

  /**
   * Helper method that creates DV with the given deleted row ids and returns
   * a [[DeletionVectorDescriptor]]. DV created can be an in-line or on disk
   */
  protected def createDV(
      isInline: Boolean, tablePath: Path, deletedRows: Long*): DeletionVectorDescriptor = {
    val bitmap = RoaringBitmapArray(deletedRows: _*)
    val serializedBitmap = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    val cardinality = deletedRows.size
    if (isInline) {
      inlineInLog(serializedBitmap, cardinality)
    } else {
      val tableWithFS = PathWithFileSystem.withConf(tablePath, newHadoopConf).makeQualified()
      val dvPath = dvStore.generateUniqueNameInTable(tableWithFS)
      val dvRange = Utils.tryWithResource(dvStore.createWriter(dvPath)) { writer =>
        writer.write(serializedBitmap)
      }
      onDiskWithAbsolutePath(
        pathToString(dvPath.path), dvRange.length, cardinality, Some(dvRange.offset))
    }
  }

  /** Evaluate the given row index filter instance and return sequence of dropped row indexes */
  protected def eval(rowIndexFilter: RowIndexFilter, start: Long, end: Long): Seq[Long] = {
    val batchSize = (end - start + 1).toInt
    val batch = newBatch(batchSize)
    rowIndexFilter.materializeIntoVector(start, end, batch)
    batch.getBytes(0, batchSize).toSeq
      .zip(Seq.range(start, end))
      .filter(_._1 == RowIndexFilter.DROP_ROW_VALUE) // filter out dropped rows
      .map(_._2) // select only the row id
      .toSeq
  }

  lazy val dvStore: DeletionVectorStore = DeletionVectorStore.createInstance(newHadoopConf)
}
