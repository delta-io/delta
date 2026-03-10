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
package io.delta.kernel.internal.actions

import java.io.{ByteArrayInputStream, DataInputStream}
import java.util.{Base64, Optional}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Tests for DeletionVectorDescriptor.serializeToBase64().
 */
class DeletionVectorDescriptorSuite extends AnyFunSuite {

  // Test cases: (storageType, pathOrInlineDv, offset, sizeInBytes, cardinality)
  private val testCases = Seq(
    ("u", "ab^-aqEH.-t@S}K{vb[*k^", Some(4), 40, 2L),
    ("p", "path/to/dv.bin", Some(100), 1024, 50L),
    ("i", "inline_data_here", None, 16, 3L))

  testCases.foreach { case (storageType, pathOrInlineDv, offset, sizeInBytes, cardinality) =>
    test(s"serializeToBase64 - $storageType storage type") {
      val dv = new DeletionVectorDescriptor(
        storageType,
        pathOrInlineDv,
        offset.map(Integer.valueOf).map(Optional.of[Integer]).getOrElse(Optional.empty[Integer]()),
        sizeInBytes,
        cardinality)

      val base64Result = dv.serializeToBase64()

      // Decode and verify the serialization format
      val bytes = Base64.getDecoder.decode(base64Result)
      val dis = new DataInputStream(new ByteArrayInputStream(bytes))

      assert(dis.readLong() === cardinality)
      assert(dis.readInt() === sizeInBytes)
      assert(dis.readByte().toChar.toString === storageType)

      if (storageType != "i") {
        assert(dis.readInt() === offset.get)
      }

      assert(dis.readUTF() === pathOrInlineDv)
      dis.close()
    }
  }

  test("serializeToBase64 throws for non-inline DV without offset") {
    val ex = intercept[IllegalArgumentException] {
      val dv = new DeletionVectorDescriptor(
        "u",
        "ab^-aqEH.-t@S}K{vb[*k^",
        Optional.empty[Integer](),
        40,
        2L)
      dv.serializeToBase64()
    }
    assert(ex.getMessage.contains("Non-inline DV must have offset"))
  }
}
