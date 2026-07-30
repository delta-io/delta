/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.v2

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.delta.actions.{DeletionVectorDescriptor => SparkDvDescriptor}
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore

/** Unit tests for [[CDCDeletionVectorHelper.computeDVDiff]]. All tests use inline DVs. */
class CDCDeletionVectorHelperSuite extends SparkFunSuite {

  private val conf = new Configuration()
  private val dummyPath = "/tmp/dummy-dv-table"

  private def makeInlineDv(rowIds: Long*): String = {
    val bitmap = new RoaringBitmapArray()
    rowIds.foreach(bitmap.add)
    val bytes = DeletionVectorUtils.serialize(bitmap, RoaringBitmapArrayFormat.Portable, None)
    SparkDvDescriptor.inlineInLog(bytes, bitmap.cardinality).serializeToBase64
  }

  private def readBitmap(dvBase64: String): RoaringBitmapArray = {
    val dv = SparkDvDescriptor.deserializeFromBase64(dvBase64)
    DeletionVectorStore.createInstance(conf).read(dv, new Path(dummyPath))
  }

  test("noneAndSome: Add has DV, Remove has none — returns single delete via inline shortcut") {
    val addDvBase64 = makeInlineDv(0L, 1L)
    val result = CDCDeletionVectorHelper.computeDVDiff(addDvBase64, null, conf, dummyPath)

    assert(result.length === 1)
    assert(result(0)._1 === CDCReader.CDC_TYPE_DELETE_STRING)
    // Inline shortcut: the same base64 value is returned without re-serialization.
    assert(result(0)._2 === addDvBase64)
  }

  test("someAndNone: Remove has DV, Add has none — returns single insert via inline shortcut") {
    val removeDvBase64 = makeInlineDv(2L, 3L)
    val result = CDCDeletionVectorHelper.computeDVDiff(null, removeDvBase64, conf, dummyPath)

    assert(result.length === 1)
    assert(result(0)._1 === CDCReader.CDC_TYPE_INSERT)
    assert(result(0)._2 === removeDvBase64)
  }

  test("bothSome: Add DV is superset of Remove DV — only newly deleted rows become delete") {
    // Remove marked row 0; Add marks rows 0 and 1. Row 1 is the new deletion.
    val removeDvBase64 = makeInlineDv(0L)
    val addDvBase64 = makeInlineDv(0L, 1L)
    val result = CDCDeletionVectorHelper.computeDVDiff(addDvBase64, removeDvBase64, conf, dummyPath)

    assert(result.length === 1)
    assert(result(0)._1 === CDCReader.CDC_TYPE_DELETE_STRING)
    val bitmap = readBitmap(result(0)._2)
    assert(bitmap.cardinality === 1L)
    assert(bitmap.contains(1L), "only the newly deleted row 1 should appear")
  }

  test("bothSome: Remove DV is superset of Add DV — only restored rows become insert") {
    // Remove marked rows 0 and 1; Add marks only row 0. Row 1 is restored.
    val removeDvBase64 = makeInlineDv(0L, 1L)
    val addDvBase64 = makeInlineDv(0L)
    val result = CDCDeletionVectorHelper.computeDVDiff(addDvBase64, removeDvBase64, conf, dummyPath)

    assert(result.length === 1)
    assert(result(0)._1 === CDCReader.CDC_TYPE_INSERT)
    val bitmap = readBitmap(result(0)._2)
    assert(bitmap.cardinality === 1L)
    assert(bitmap.contains(1L), "only the restored row 1 should appear")
  }

  test("bothSome: disjoint DVs — produces both delete and insert") {
    // Remove had row 0 deleted (now restored); Add has row 1 newly deleted.
    val removeDvBase64 = makeInlineDv(0L)
    val addDvBase64 = makeInlineDv(1L)
    val result = CDCDeletionVectorHelper.computeDVDiff(addDvBase64, removeDvBase64, conf, dummyPath)

    assert(result.length === 2)
    val types = result.map(_._1).toSet
    assert(types === Set(CDCReader.CDC_TYPE_DELETE_STRING, CDCReader.CDC_TYPE_INSERT))
  }

  test("bothNull: throws IllegalArgumentException") {
    intercept[IllegalArgumentException] {
      CDCDeletionVectorHelper.computeDVDiff(null, null, conf, dummyPath)
    }
  }
}
