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

import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray

class RoaringBitmapArraySuite extends AnyFunSuite {

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
