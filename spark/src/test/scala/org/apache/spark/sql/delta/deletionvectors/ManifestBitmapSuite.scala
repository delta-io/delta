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

import org.apache.spark.SparkFunSuite

class ManifestBitmapSuite extends SparkFunSuite {

  test("mutating a copy does not change the original bitmap") {
    val original = ManifestBitmap.fromPositions(Seq(1, 3))
    val mutable = original.toMutable

    mutable.add(2)

    assert(original.toArrayForTesting.sameElements(Array(1L, 3L)))
    assert(mutable.toManifestBitmap.toArrayForTesting.sameElements(Array(1L, 2L, 3L)))
  }

  test("immutable snapshots do not change after further mutations") {
    val mutable = ManifestBitmap.fromPositions(Seq(1)).toMutable
    mutable.add(2)
    val firstSnapshot = mutable.toManifestBitmap

    mutable.add(3)
    val secondSnapshot = mutable.toManifestBitmap

    assert(firstSnapshot.toArrayForTesting.sameElements(Array(1L, 2L)))
    assert(secondSnapshot.toArrayForTesting.sameElements(Array(1L, 2L, 3L)))
  }

  test("deserialized bitmaps own their input bytes") {
    val bytes = ManifestBitmap.fromPositions(Seq(1, 3)).serializeAsByteArray()
    val bitmap = ManifestBitmap.fromSerializedByteArray(bytes)

    java.util.Arrays.fill(bytes, 0.toByte)

    assert(bitmap.toArrayForTesting.sameElements(Array(1L, 3L)))
  }

  test("bitmaps created from positions own their input sequence") {
    val positions = collection.mutable.ArrayBuffer(1, 3)
    val bitmap = ManifestBitmap.fromPositions(positions)

    positions += 2

    assert(bitmap.toArrayForTesting.sameElements(Array(1L, 3L)))
  }
}
