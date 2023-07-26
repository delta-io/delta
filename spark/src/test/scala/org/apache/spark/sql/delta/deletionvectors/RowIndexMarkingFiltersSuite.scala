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
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
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

class RowIndexMarkingFiltersSuite extends QueryTest with SharedSparkSession {

  test("empty deletion vector (drop filter)") {
    val rowIndexFilter = DropMarkedRowsFilter.createInstance(
      DeletionVectorDescriptor.EMPTY,
      newHadoopConf,
      tablePath = None)

    assert(getMarked(rowIndexFilter, start = 0, end = 20) === Seq.empty)
    assert(getMarked(rowIndexFilter, start = 20, end = 200) === Seq.empty)
    assert(getMarked(rowIndexFilter, start = 200, end = 2000) === Seq.empty)
  }

  test("empty deletion vector (keep filter)") {
    val rowIndexFilter = KeepMarkedRowsFilter.createInstance(
      DeletionVectorDescriptor.EMPTY,
      newHadoopConf,
      tablePath = None)

    assert(getMarked(rowIndexFilter, start = 0, end = 20) === 0.until(20))
    assert(getMarked(rowIndexFilter, start = 20, end = 200) === 20.until(200))
    assert(getMarked(rowIndexFilter, start = 200, end = 2000) === 200.until(2000))
  }

  private val filtersToBeTested =
    Seq((DropMarkedRowsFilter, "drop"), (KeepMarkedRowsFilter, "keep"))

  for {
    (filterType, filterName) <- filtersToBeTested
    isInline <- BOOLEAN_DOMAIN
  } {
    test(s"deletion vector single row marked (isInline=$isInline) ($filterName filter)") {
      withTempDir { tableDir =>
        val tablePath = stringToPath(tableDir.toString)
        val dv = createDV(isInline, tablePath, 25)

        val rowIndexFilter = filterType.createInstance(dv, newHadoopConf, Some(tablePath))

        def correctValues(range: Seq[Long]): Seq[Long] = filterName match {
          case "drop" => range.filter(_ == 25)
          case "keep" => range.filterNot(_ == 25)
          case _ => throw new RuntimeException("unreachable code reached")
        }

        for ((start, end) <- Seq((0, 20), (20, 35), (35, 325))) {
          val actual = getMarked(rowIndexFilter, start, end)
          val correct = correctValues(start.toLong.until(end))
          assert(actual === correct)
        }
      }
    }
  }

  for {
    (filterType, filterName) <- filtersToBeTested
    isInline <- BOOLEAN_DOMAIN
  } {
    test(s"deletion vector with multiple rows marked (isInline=$isInline) ($filterName filter)") {
      withTempDir { tableDir =>
        val tablePath = stringToPath(tableDir.toString)
        val markedRows = Seq[Long](0, 25, 35, 2000, 50000)
        val dv = createDV(isInline, tablePath, markedRows: _*)

        val rowIndexFilter = filterType.createInstance(dv, newHadoopConf, Some(tablePath))

        def correctValues(range: Seq[Long]): Seq[Long] = filterName match {
          case "drop" => range.filter(markedRows.contains(_))
          case "keep" => range.filterNot(markedRows.contains(_))
          case _ => throw new RuntimeException("unreachable code reached")
        }

        for ((start, end) <- Seq(
          (0, 20), (20, 35), (35, 325), (325, 1000), (1000, 60000), (60000, 800000))) {
          val actual = getMarked(rowIndexFilter, start, end)
          val correct = correctValues(start.toLong.until(end))
          assert(actual === correct)
        }
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
      isInline: Boolean, tablePath: Path, markedRows: Long*): DeletionVectorDescriptor = {
    val bitmap = RoaringBitmapArray(markedRows: _*)
    val serializedBitmap = bitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable)
    val cardinality = markedRows.size
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

  /** Evaluate the given row index filter instance and return sequence of marked rows indexes */
  protected def getMarked(rowIndexFilter: RowIndexFilter, start: Long, end: Long): Seq[Long] = {
    val batchSize = (end - start + 1).toInt
    val batch = newBatch(batchSize)
    rowIndexFilter.materializeIntoVector(start, end, batch)
    batch.getBytes(0, batchSize).toSeq
      .zip(Seq.range(start, end))
      .filter(_._1 == RowIndexFilter.DROP_ROW_VALUE) // filter out marked rows
      .map(_._2) // select only the row id
      .toSeq
  }

  lazy val dvStore: DeletionVectorStore = DeletionVectorStore.createInstance(newHadoopConf)
}
