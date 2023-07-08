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

package org.apache.spark.sql.delta.actions

import java.util.UUID

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor._
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
// scalastyle:on import.ordering.noEmptyLine

/**
 * Test: DV descriptor creation, created DV descriptor properties and utility methods are
 * working as expected.
 */
class DeletionVectorDescriptorSuite extends SparkFunSuite {
  test("Inline DV") {
    val dv = inlineInLog(testDVData, cardinality = 3)

    // Make sure the metadata (type, size etc.) in the DV is as expected
    assert(!dv.isOnDisk && dv.isInline, s"Incorrect DV storage type: $dv")
    assertCardinality(dv, 3)

    val encodedDVData = "0rJua"
    assert(dv.pathOrInlineDv === encodedDVData)
    assert(dv.sizeInBytes === testDVData.size)
    assert(dv.inlineData === testDVData)
    assert(dv.estimatedSerializedSize === 18)

    assert(dv.offset.isEmpty) // There shouldn't be an offset for inline DV

    // Unique id to identify the DV
    assert(dv.uniqueId === s"i$encodedDVData")
    assert(dv.uniqueFileId === s"i$encodedDVData")

    // There is no on-disk file name for an inline DV
    intercept[IllegalArgumentException] { dv.absolutePath(testTablePath) }

    // Copy as on-disk DV with absolute path and relative path -
    // expect the returned DV is same as input, since this is inline
    // so paths are irrelevant.
    assert(dv.copyWithAbsolutePath(testTablePath) === dv)
    assert(dv.copyWithNewRelativePath(UUID.randomUUID(), "predix2") === dv)
  }

  for (offset <- Seq(None, Some(25))) {
    test(s"On disk DV with absolute path with offset=$offset") {
      val dv = onDiskWithAbsolutePath(testDVAbsPath, sizeInBytes = 15, cardinality = 10, offset)

      // Make sure the metadata (type, size etc.) in the DV is as expected
      assert(dv.isOnDisk && !dv.isInline, s"Incorrect DV storage type: $dv")
      assertCardinality(dv, 10)

      assert(dv.pathOrInlineDv === testDVAbsPath)
      assert(dv.sizeInBytes === 15)
      intercept[Exception] { dv.inlineData }
      assert(dv.estimatedSerializedSize === (if (offset.isDefined) 4 else 0) + 37)
      assert(dv.offset === offset)

      // Unique id to identify the DV
      val offsetSuffix = offset.map(o => s"@$o").getOrElse("")
      assert(dv.uniqueId === s"p$testDVAbsPath$offsetSuffix")
      assert(dv.uniqueFileId === s"p$testDVAbsPath")

      // Given the input already has an absolute path, it should return the path in DV
      assert(dv.absolutePath(testTablePath) === new Path(testDVAbsPath))

      // Given the input already has an absolute path, expect the output to be same as input
      assert(dv.copyWithAbsolutePath(testTablePath) === dv)

      // Copy DV as a relative path DV
      val uuid = UUID.randomUUID()
      val dvCopyWithRelativePath = dv.copyWithNewRelativePath(uuid, "prefix")
      assert(dvCopyWithRelativePath.isRelative)
      assert(dvCopyWithRelativePath.isOnDisk)
      assert(dvCopyWithRelativePath.pathOrInlineDv === encodeUUID(uuid, "prefix"))
    }
  }

  for (offset <- Seq(None, Some(25))) {
    test(s"On-disk DV with relative path with offset=$offset") {
      val uuid = UUID.randomUUID()
      val dv = onDiskWithRelativePath(
        uuid, randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25, offset)

      // Make sure the metadata (type, size etc.) in the DV is as expected
      assert(dv.isOnDisk && !dv.isInline, s"Incorrect DV storage type: $dv")
      assertCardinality(dv, 25)

      assert(dv.pathOrInlineDv === encodeUUID(uuid, "prefix"))
      assert(dv.sizeInBytes === 15)
      intercept[Exception] { dv.inlineData }
      assert(dv.estimatedSerializedSize === (if (offset.isDefined) 4 else 0) + 39)
      assert(dv.offset === offset)

      // Unique id to identify the DV
      val offsetSuffix = offset.map(o => s"@$o").getOrElse("")
      val encodedUUID = encodeUUID(uuid, "prefix")
      assert(dv.uniqueId === s"u$encodedUUID$offsetSuffix")
      assert(dv.uniqueFileId === s"u$encodedUUID")

      // Expect the DV final path to be under the given table path
      assert(dv.absolutePath(testTablePath) ===
        new Path(s"$testTablePath/prefix/deletion_vector_$uuid.bin"))

      // Copy DV with an absolute path location
      val dvCopyWithAbsPath = dv.copyWithAbsolutePath(testTablePath)
      assert(dvCopyWithAbsPath.isAbsolute)
      assert(dvCopyWithAbsPath.isOnDisk)
      assert(
        dvCopyWithAbsPath.pathOrInlineDv === s"$testTablePath/prefix/deletion_vector_$uuid.bin")

      // Copy DV as a relative path DV - expect to return the same DV as the current
      // DV already contains relative path.
      assert(dv.copyWithNewRelativePath(UUID.randomUUID(), "predix2") === dv)
    }
  }

  private def assertCardinality(dv: DeletionVectorDescriptor, expSize: Int): Unit = {
    if (expSize == 0) {
      assert(dv.isEmpty, s"Expected DV to be empty: $dv")
    } else {
      assert(!dv.isEmpty && dv.cardinality == expSize, s"Invalid size expected: $expSize, $dv")
    }
  }

  private val testTablePath = new Path("s3a://table/test")
  private val testDVAbsPath = "s3a://table/test/dv1.bin"
  private val testDVData: Array[Byte] = Array(1, 2, 3, 4)
}
