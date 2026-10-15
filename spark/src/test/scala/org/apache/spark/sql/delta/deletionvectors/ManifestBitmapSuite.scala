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
import org.apache.spark.sql.delta.deletionvectors.mumbling.MumblingBitmapWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{ByteType, LongType}

class ManifestBitmapSuite extends SparkFunSuite {

  test("mutating a copy does not change the original bitmap") {
    val original = ManifestBitmap.fromPositions(Seq(1, 3))
    val mutable = original.toMutable

    mutable.add(2)

    assertPositions(original, Seq(1, 3))
    assertPositions(mutable.toManifestBitmap, Seq(1, 2, 3))
  }

  test("immutable snapshots do not change after further mutations") {
    val mutable = ManifestBitmap.fromPositions(Seq(1)).toMutable
    mutable.add(2)
    val firstSnapshot = mutable.toManifestBitmap

    mutable.add(3)
    val secondSnapshot = mutable.toManifestBitmap

    assertPositions(firstSnapshot, Seq(1, 2))
    assertPositions(secondSnapshot, Seq(1, 2, 3))
  }

  test("deserialized bitmaps own their input bytes") {
    val bytes = ManifestBitmap.fromPositions(Seq(1, 3)).serializeAsByteArray()
    val bitmap = ManifestBitmap.fromSerializedByteArray(bytes)

    java.util.Arrays.fill(bytes, 0.toByte)

    assertPositions(bitmap, Seq(1, 3))
  }

  test("bitmaps created from positions own their input sequence") {
    val positions = collection.mutable.ArrayBuffer(1, 3)
    val bitmap = ManifestBitmap.fromPositions(positions)

    positions += 2

    assertPositions(bitmap, Seq(1, 3))
  }

  test("ManifestBitmap uses the Mumbling format") {
    val positions = Seq(1, 2, 300)
    val bitmap = ManifestBitmap.fromPositions(positions)
    val bytes = bitmap.serializeAsByteArray()

    assert(bitmap.isInstanceOf[MumblingManifestBitmap])
    assert(bytes(0) == 1)
    assertPositions(ManifestBitmap.fromSerializedByteArray(bytes), positions)
  }

  test("ManifestBitmap writes a mutated bitmap in the Mumbling format") {
    val mutable = ManifestBitmap.fromPositions(Seq(1, 300)).toMutable
    mutable.add(512)
    val serialized = mutable.toManifestBitmap.serializeAsByteArray()

    assert(serialized(0) == 1)
    assertPositions(ManifestBitmap.fromSerializedByteArray(serialized), Seq(1, 300, 512))
  }

  private def assertPositions(bitmap: ManifestBitmap, expected: Seq[Int]): Unit = {
    assert(bitmap.cardinality == expected.distinct.size)
    expected.foreach(position => assert(bitmap.contains(position)))
  }


  test("MumblingRowIndexFilter materializes every row-index filter entry point") {
    val provider = ManifestBitmap.fromPositions(Seq(1, 3, 257))
      .toRowIndexFilterProvider(new Path("file:/tmp/table"))
    val filter = provider.retrieve(new Configuration())

    withByteVector(5) { batch =>
      filter.materializeIntoVector(0, 5, batch)
      assert(batch.getBytes(0, 5).toSeq === Seq(0, 1, 0, 1, 0))
    }

    withByteVector(6) { batch =>
      val rowIndexes = new OnHeapColumnVector(6, LongType)
      try {
        Seq(-1L, 0L, 1L, 3L, 257L, 258L).zipWithIndex.foreach {
          case (rowIndex, rowId) => rowIndexes.putLong(rowId, rowIndex)
        }
        filter.materializeIntoVectorWithRowIndex(6, rowIndexes, batch)
        assert(batch.getBytes(0, 6).toSeq === Seq(0, 0, 1, 1, 1, 0))
      } finally {
        rowIndexes.close()
      }
    }

    assert(singleRowValue(filter, rowIndex = 3L) == RowIndexFilter.DROP_ROW_VALUE)
    assert(singleRowValue(filter, rowIndex = 4L) == RowIndexFilter.KEEP_ROW_VALUE)
    intercept[IllegalArgumentException] {
      singleRowValue(filter, rowIndex = MumblingBitmapWriter.MAX_POSITION_EXCLUSIVE.toLong)
    }
  }

  private def withByteVector(size: Int)(f: OnHeapColumnVector => Unit): Unit = {
    val vector = new OnHeapColumnVector(size, ByteType)
    try {
      f(vector)
    } finally {
      vector.close()
    }
  }

  private def singleRowValue(filter: RowIndexFilter, rowIndex: Long): Byte = {
    val vector = new OnHeapColumnVector(1, ByteType)
    try {
      filter.materializeSingleRowWithRowIndex(rowIndex, vector)
      vector.getByte(0)
    } finally {
      vector.close()
    }
  }
}
