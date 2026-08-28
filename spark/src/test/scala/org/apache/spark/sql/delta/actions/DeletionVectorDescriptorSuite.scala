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
import org.apache.spark.paths.SparkPath
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
    assert(dv.legacyUniqueId === s"i$encodedDVData")
    assert(dv.uniqueFileId === s"i$encodedDVData")

    // There is no on-disk file name for an inline DV
    intercept[IllegalArgumentException] { dv.absolutePath(testTablePath) }

    // Copy as on-disk DV with absolute path and relative path -
    // expect the returned DV is same as input, since this is inline
    // so paths are irrelevant.
    assert(dv.copyWithAbsolutePath(testTablePath) === dv)
    assert(dv.copyWithNewUuidRelativePath(UUID.randomUUID(), "predix2") === dv)
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
      assert(dv.legacyUniqueId === s"p$testDVAbsPath$offsetSuffix")
      assert(dv.uniqueFileId === s"p$testDVAbsPath")

      // Given the input already has an absolute path, it should return the path in DV
      assert(dv.absolutePath(testTablePath) === new Path(testDVAbsPath))

      // Given the input already has an absolute path, expect the output to be same as input
      assert(dv.copyWithAbsolutePath(testTablePath) === dv)

      // Copy DV as a relative path DV
      val uuid = UUID.randomUUID()
      val dvCopyWithUuidRelativePath = dv.copyWithNewUuidRelativePath(uuid, "prefix")
      assert(dvCopyWithUuidRelativePath.isUuidRelative)
      assert(dvCopyWithUuidRelativePath.isOnDisk)
      assert(dvCopyWithUuidRelativePath.pathOrInlineDv === encodeUUID(uuid, "prefix"))
    }
  }

  for (offset <- Seq(None, Some(25))) {
    test(s"On-disk DV with relative path with offset=$offset") {
      val uuid = UUID.randomUUID()
      val dv = onDiskWithUuidRelativePath(
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
      assert(dv.legacyUniqueId === s"u$encodedUUID$offsetSuffix")
      assert(dv.uniqueFileId === s"u$encodedUUID")

      // Expect the DV final path to be under the given table path
      assert(dv.absolutePath(testTablePath) ===
        new Path(s"$testTablePath/prefix/${DELETION_VECTOR_FILE_NAME_CORE}_$uuid.bin"))

      // Copy DV with an absolute path location
      val dvCopyWithAbsPath = dv.copyWithAbsolutePath(testTablePath)
      assert(dvCopyWithAbsPath.isAbsolute)
      assert(dvCopyWithAbsPath.isOnDisk)
      // pathOrInlineDV is URL-encoded.
      assert(
        SparkPath.fromUrlString(dvCopyWithAbsPath.pathOrInlineDv).toPath.toString ===
        s"$testTablePath/prefix/${DELETION_VECTOR_FILE_NAME_CORE}_$uuid.bin")

      // Copy DV as a relative path DV - expect to return the same DV as the current
      // DV already contains relative path.
      assert(dv.copyWithNewUuidRelativePath(UUID.randomUUID(), "predix2") === dv)
    }
  }

  for (offset <- Seq(None, Some(25))) {
    test(s"On-disk DV with un-encoded relative path with offset=$offset") {
      val dv = createRelativePathDVDescriptor(
        testDVRelPath, sizeInBytes = 15, cardinality = 7, offset)

      // Make sure the metadata (type, size etc.) in the DV is as expected
      assert(dv.isOnDisk && !dv.isInline, s"Incorrect DV storage type: $dv")
      assertCardinality(dv, 7)

      assert(dv.isUnencodedRelative)
      assert(!dv.isUuidRelative)
      assert(!dv.isAbsolute)
      // It carries no UUID, so there is no prefix/UUID pair to recover.
      assert(dv.getRandomPrefixAndUuid.isEmpty)

      assert(dv.pathOrInlineDv === testDVRelPath)
      assert(dv.sizeInBytes === 15)
      intercept[Exception] { dv.inlineData }
      assert(dv.offset === offset)

      // Unique id to identify the DV
      val offsetSuffix = offset.map(o => s"@$o").getOrElse("")
      assert(dv.legacyUniqueId === s"r$testDVRelPath$offsetSuffix")
      assert(dv.uniqueFileId === s"r$testDVRelPath")

      // Expect the DV path to resolve under the given table path.
      assert(dv.absolutePath(testTablePath) === new Path(s"$testTablePath/$testDVRelPath"))
      // And to relativize back to exactly what we started with.
      assert(dv.urlEncodedRelativePathIfExists(testTablePath).contains(testDVRelPath))

      val dvCopyWithAbsPath = dv.copyWithAbsolutePath(testTablePath)
      assert(dvCopyWithAbsPath.isAbsolute)
      assert(dvCopyWithAbsPath.isOnDisk)
      assert(dvCopyWithAbsPath.pathOrInlineDv ===
        SparkPath.fromPath(dv.absolutePath(testTablePath)).urlEncoded)
      assert(dvCopyWithAbsPath.absolutePath(testTablePath) === dv.absolutePath(testTablePath))
      assert(dvCopyWithAbsPath.sizeInBytes === dv.sizeInBytes)
      assert(dvCopyWithAbsPath.cardinality === dv.cardinality)
      assert(dvCopyWithAbsPath.offset === dv.offset)
    }
  }

  test("absolutePath() resolves p, u, r DVs with space (%20) to the same unencoded path") {
    // Following paths are all equivalent and follow Delta's path encoding rules.
    val uuid = UUID.fromString("f92b8939-98dc-41c0-ae52-ffb7df72a37f")
    val fileName = assembleDeletionVectorFileName(uuid)
    val relativePath = s"dv dir/$fileName"
    val absolutePath = s"s3a://table/test/dv%20dir/${fileName.replace("%", "%25")}"
    val expectedPath = s"s3a://table/test/dv dir/$fileName"

    val testCases = Seq(
      (PATH_DV_MARKER,
        onDiskWithAbsolutePath(absolutePath, sizeInBytes = 15, cardinality = 10)),
      (UUID_DV_MARKER,
        onDiskWithUuidRelativePath(
          uuid, randomPrefix = "dv dir", sizeInBytes = 15, cardinality = 10)),
      (RELATIVE_DV_MARKER,
        createRelativePathDVDescriptor(relativePath, sizeInBytes = 15, cardinality = 10)))

    for ((storageType, dv) <- testCases) {
      withClue(s"$storageType DV: ") {
        assert(dv.absolutePath(testTablePath).toString === expectedPath)
      }
    }
  }

  test("r DV copyWithAbsolutePath emits URL-encoded path") {
    val path = "data/test%dv%prefix-deletes file.puffin"
    val dv = createRelativePathDVDescriptor(path, sizeInBytes = 15, cardinality = 7)
    val dvCopyWithAbsPath = dv.copyWithAbsolutePath(testTablePath)

    assert(dvCopyWithAbsPath.isAbsolute)
    assert(dvCopyWithAbsPath.pathOrInlineDv.contains("test%25dv%25prefix-deletes%20file"))
    assert(dvCopyWithAbsPath.absolutePath(testTablePath) === dv.absolutePath(testTablePath))
  }

  test("r DV rejects an absolute path") {
    intercept[IllegalArgumentException] {
      createRelativePathDVDescriptor(
        "s3a://other/dv.bin", sizeInBytes = 15, cardinality = 7)
    }
  }

  test("base64 round-trip for inline DV") {
    val dv = inlineInLog(testDVData, cardinality = 3)
    val encoded = dv.serializeToBase64()
    val decoded = DeletionVectorDescriptor.deserializeFromBase64(encoded)
    assert(decoded === dv)
  }

  for {
    offset <- Seq(None, Some(0), Some(25))
    label <- Seq("uuid relative path", "absolute path", "unencoded relative path")
  } {
    test(s"base64 round-trip for $label DV with offset=$offset") {
      val dv = label match {
        case "uuid relative path" => onDiskWithUuidRelativePath(
          UUID.randomUUID(), randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25, offset)
        case "absolute path" => onDiskWithAbsolutePath(
          testDVAbsPath, sizeInBytes = 15, cardinality = 10, offset)
        case "unencoded relative path" => createRelativePathDVDescriptor(
          testDVRelPath, sizeInBytes = 15, cardinality = 7, offset)
      }
      val encoded = dv.serializeToBase64()
      val decoded = DeletionVectorDescriptor.deserializeFromBase64(encoded)
      assert(decoded === dv)
    }
  }

  /** The `r` form of the `u` DV that `uuid`/`prefix` describe. */
  private def rFormOf(uuid: UUID, prefix: String, offset: Option[Int]) = {
    val path = if (prefix.isEmpty) {
      s"${DELETION_VECTOR_FILE_NAME_CORE}_$uuid.bin"
    } else {
      s"$prefix/${DELETION_VECTOR_FILE_NAME_CORE}_$uuid.bin"
    }
    createRelativePathDVDescriptor(path, sizeInBytes = 15, cardinality = 25, offset = offset)
  }

  test("config-aware uniqueId uses descriptor identity when object identity is disabled") {
    val uuid = UUID.randomUUID()
    val uDv = onDiskWithUuidRelativePath(
      uuid, randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25, offset = Some(8))
    val pDv = uDv.copyWithAbsolutePath(testTablePathWithSpace)
    val rDv = rFormOf(uuid, "prefix", offset = Some(8))

    assert(pDv.pathOrInlineDv.contains("test%20path"))
    assert(Set(uDv.legacyUniqueId, pDv.legacyUniqueId, rDv.legacyUniqueId).size === 3)
    assert(uDv.uniqueId(testTablePathWithSpace, useObjectIdentity = false) === uDv.legacyUniqueId)
    assert(pDv.uniqueId(testTablePathWithSpace, useObjectIdentity = false) === pDv.legacyUniqueId)
    assert(rDv.uniqueId(testTablePathWithSpace, useObjectIdentity = false) === rDv.legacyUniqueId)
  }

  test("config-aware uniqueId collapses u, r, and in-root p with object identity") {
    val uuid = UUID.randomUUID()
    val uDv = onDiskWithUuidRelativePath(
      uuid, randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25, offset = Some(8))
    val pDv = uDv.copyWithAbsolutePath(testTablePathWithSpace)
    val rDv = rFormOf(uuid, "prefix", offset = Some(8))

    assert(pDv.pathOrInlineDv.contains("test%20path"))
    assert(uDv.uniqueId(testTablePathWithSpace, useObjectIdentity = true) ===
      rDv.uniqueId(testTablePathWithSpace, useObjectIdentity = true))
    assert(pDv.uniqueId(testTablePathWithSpace, useObjectIdentity = true) ===
      rDv.uniqueId(testTablePathWithSpace, useObjectIdentity = true))
  }

  test("object identity keeps out-of-root p distinct") {
    val uuid = UUID.randomUUID()
    val uDv = onDiskWithUuidRelativePath(
      uuid, randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25, offset = Some(8))
    val pElsewhere = onDiskWithAbsolutePath(
      SparkPath.fromPath(
        new Path(s"s3a://other-bucket/tbl/prefix/${DELETION_VECTOR_FILE_NAME_CORE}_$uuid.bin"))
        .urlEncoded,
      sizeInBytes = 15,
      cardinality = 25,
      offset = Some(8))

    assert(uDv.uniqueId(testTablePath, useObjectIdentity = true) !==
      pElsewhere.uniqueId(testTablePath, useObjectIdentity = true))
  }

  test("object identity decodes absolute p path before relativizing") {
    val pDv = onDiskWithAbsolutePath(
      "file:///tmp/test%20path/temp_dv.bin", sizeInBytes = 15, cardinality = 25, offset = Some(8))

    assert(pDv.normalizedTableRelativeObjectId(new Path("file:///tmp")) ===
      // "$storageType$pathOrInlineDv@$offsetValue"
      "rtest path/temp_dv.bin@8")
  }

  test("object identity strips table root from decoded absolute p path") {
    val pDv = onDiskWithAbsolutePath(
      "file:///tmp/test%20path/temp_dv.bin", sizeInBytes = 15, cardinality = 25, offset = Some(8))

    assert(pDv.normalizedTableRelativeObjectId(new Path("file:///tmp/test path")) ===
      // "$storageType$pathOrInlineDv@$offsetValue"
      "rtemp_dv.bin@8")
  }

  test("object identity includes offset") {
    val base = createRelativePathDVDescriptor(
      testDVRelPath, sizeInBytes = 15, cardinality = 25, offset = Some(8))
    assert(base.uniqueId(testTablePath, useObjectIdentity = true) !==
      base.copy(offset = Some(9)).uniqueId(testTablePath, useObjectIdentity = true))
  }

  test("log replay keeps descriptor identity when object identity is disabled") {
    val uuid = UUID.randomUUID()
    val uDv = onDiskWithUuidRelativePath(
      uuid, randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25)
    val rDv = rFormOf(uuid, "prefix", offset = None)
    val replay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None,
      minSetTransactionRetentionTimestamp = None,
      tableRoot = testTablePath,
      useDeletionVectorObjectIdentity = false)

    replay.append(0, Iterator(addFileWithDv(uDv)))
    replay.append(1, Iterator(removeFileWithDv(rDv)))

    assert(replay.allFiles.map(_.deletionVector) === Seq(uDv))
  }

  test("log replay uses object identity when enabled") {
    val uuid = UUID.randomUUID()
    val uDv = onDiskWithUuidRelativePath(
      uuid, randomPrefix = "prefix", sizeInBytes = 15, cardinality = 25)
    val rDv = rFormOf(uuid, "prefix", offset = None)
    val replay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None,
      minSetTransactionRetentionTimestamp = None,
      tableRoot = testTablePath,
      useDeletionVectorObjectIdentity = true)

    replay.append(0, Iterator(addFileWithDv(uDv)))
    replay.append(1, Iterator(removeFileWithDv(rDv)))

    assert(replay.allFiles.isEmpty)
  }

  private def addFileWithDv(dv: DeletionVectorDescriptor): AddFile = {
    AddFile(
      path = "part-000.parquet",
      partitionValues = Map.empty,
      size = 1,
      modificationTime = 1,
      dataChange = true,
      deletionVector = dv)
  }

  private def removeFileWithDv(dv: DeletionVectorDescriptor): RemoveFile = {
    addFileWithDv(dv).removeWithTimestamp(timestamp = 2L)
  }

  private def assertCardinality(dv: DeletionVectorDescriptor, expSize: Int): Unit = {
    if (expSize == 0) {
      assert(dv.isEmpty, s"Expected DV to be empty: $dv")
    } else {
      assert(!dv.isEmpty && dv.cardinality == expSize, s"Invalid size expected: $expSize, $dv")
    }
  }

  private val testTablePath = new Path("s3a://table/test")
  private val testTablePathWithSpace = new Path("s3a://table/test path")
  private val testDVAbsPath = "s3a://table/test/dv1.bin"
  private val testDVRelPath = "data/deletes-abc.puffin"
  private val testDVData: Array[Byte] = Array(1, 2, 3, 4)
}
