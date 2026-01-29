/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.actions

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.{Base64, Optional}

import io.delta.kernel.internal.actions.DeletionVectorDescriptor

import org.scalatest.funsuite.AnyFunSuite

class DeletionVectorDescriptorSuite extends AnyFunSuite {

  // Test cases: (storageType, pathOrInlineDv, offset, sizeInBytes, cardinality)
  private val testCases = Seq(
    ("u", "ab^-aqEH.-t@S}K{vb[*k^", Optional.of[Integer](4), 40, 2L),
    ("p", "path/to/dv.bin", Optional.of[Integer](100), 1024, 50L),
    ("i", "inline_data_here", Optional.empty[Integer](), 16, 3L))

  testCases.foreach { case (storageType, pathOrInlineDv, offset, sizeInBytes, cardinality) =>
    test(s"serializeToBase64 - $storageType storage type") {
      val dv = new DeletionVectorDescriptor(
        storageType,
        pathOrInlineDv,
        offset,
        sizeInBytes,
        cardinality)

      val encoded = dv.serializeToBase64()
      val decoded = Base64.getDecoder.decode(encoded)

      val dis = new DataInputStream(new ByteArrayInputStream(decoded))
      assert(dis.readLong() === cardinality)
      assert(dis.readInt() === sizeInBytes)
      assert(dis.readByte() === storageType.charAt(0).toByte)
      if (offset.isPresent) {
        assert(dis.readInt() === offset.get())
      }
      assert(dis.readUTF() === pathOrInlineDv)
    }
  }
}
