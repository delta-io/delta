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

package io.delta.kernel.deletionvectors

import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray
import org.scalatest.funsuite.AnyFunSuite

class RoaringBitmapArraySuite extends AnyFunSuite {


  test("RoaringBitmapArray create empty map") {
    val bitmap = RoaringBitmapArray.create()
    assert(bitmap.toArray.isEmpty)
  }

  test("RoaringBitmapArray create map with values only in first bitmap") {
    // Values <= max unsigned integer (4,294,967,295) will be in the first bitmap
    val bitmap = RoaringBitmapArray.create(1L, 100L)

    assert(bitmap.contains(1L))
    assert(bitmap.contains(100L))

    assert(!bitmap.contains(2L))
    assert(!bitmap.contains(99L))

    assert(bitmap.toArray sameElements Array(1L, 100L))
  }

  test("RoaringBitmapArray create map with values only in second bitmap") {
    // Values > max unsigned integer (4,294,967,295) will be in the second bitmap
    val bitmap = RoaringBitmapArray.create(5000000000L, 5000000100L)

    assert(bitmap.contains(5000000000L))
    assert(bitmap.contains(5000000100L))

    assert(!bitmap.contains(5000000001L))
    assert(!bitmap.contains(5000000099L))

    assert(bitmap.toArray sameElements Array(5000000000L, 5000000100L))
  }

  test("RoaringBitmapArray create map with values in first and second bitmap") {
    val bitmap = RoaringBitmapArray.create(100L, 5000000000L)

    assert(bitmap.contains(100L))
    assert(bitmap.contains(5000000000L))

    assert(!bitmap.contains(101L))
    assert(!bitmap.contains(5000000001L))

    assert(bitmap.toArray sameElements Array(100L, 5000000000L))
  }

  // TODO need to implement serialize to copy over tests

  /**
  final val BITMAP2_NUMBER = Int.MaxValue.toLong * 3L

  for (serializationFormat <- RoaringBitmapArrayFormat.values) {
    test(s"serialization - $serializationFormat") {
      checkSerializeDeserialize(RoaringBitmapArray.create(), serializationFormat)
      checkSerializeDeserialize(RoaringBitmapArray.create(1L), serializationFormat)
      checkSerializeDeserialize(RoaringBitmapArray.create(BITMAP2_NUMBER), serializationFormat)
      checkSerializeDeserialize(RoaringBitmapArray.create(1L, BITMAP2_NUMBER), serializationFormat)
      // checkSerializeDeserialize(allContainerTypesBitmap, serializationFormat)
    }
  }

  private def checkSerializeDeserialize(
    input: RoaringBitmapArray,
    format: RoaringBitmapArrayFormat.Value): Unit = {
    val serializedSize = Ints.checkedCast(input.serializedSizeInBytes(format))
    val buffer = ByteBuffer.allocate(serializedSize).order(ByteOrder.LITTLE_ENDIAN)
    input.serialize(buffer, format)
    val output = RoaringBitmapArray()
    buffer.flip()
    output.deserialize(buffer)
    assert(input === output)
  }
  */
}
