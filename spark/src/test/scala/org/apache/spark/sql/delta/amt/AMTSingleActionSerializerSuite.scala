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

package org.apache.spark.sql.delta.amt

import java.util.UUID

import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, InMemoryLogReplay}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Shape, builder, invariant, and parquet round-trip tests for the AMT
 * [[AMTSingleAction]] row record and its sub-structs. The schema is dictated by
 * the Iceberg V4 / Adaptive Metadata Tree proposal; these tests pin the
 * declared column list, the closed constant sets, the per-kind builders, and
 * the constructor invariants enforced by [[AMTSingleAction.validate]].
 */
class AMTSingleActionSerializerSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  /** A stand-in table root for DV path resolution; sampleAddFile carries no DV, so it is unused. */
  private val tableRoot = new Path("file:/tmp/amt-test-table")

  /** A minimal leaf tracking envelope (ADDED status). */
  private def addedTracking: Tracking = Tracking(
    status = Tracking.Status.Added,
    snapshot_id = None,
    dv_snapshot_id = None,
    sequence_number = None,
    file_sequence_number = None,
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  /** A root tracking envelope with matching sequence numbers (valid for pointers). */
  private def rootTracking: Tracking = Tracking(
    status = Tracking.Status.Existing,
    snapshot_id = Some(7L),
    dv_snapshot_id = None,
    sequence_number = Some(3L),
    file_sequence_number = Some(3L),
    first_row_id = None,
    deleted_positions = None,
    replaced_positions = None)

  private def sampleAddFile: AddFile = AddFile(
    path = "part-00000.parquet",
    partitionValues = Map("p" -> "1"),
    size = 1024L,
    modificationTime = 100L,
    dataChange = true,
    stats = """{"numRecords":42}""")

  private def sampleManifestInfo: ManifestInfo = ManifestInfo(
    added_files_count = 2,
    existing_files_count = 0,
    deleted_files_count = 0,
    replaced_files_count = 0,
    modified_files_count = 5,
    added_rows_count = 42L,
    existing_rows_count = 0L,
    deleted_rows_count = 0L,
    replaced_rows_count = 0L,
    modified_rows_count = 7L,
    min_sequence_number = 3L,
    dv = None,
    dv_cardinality = None)

  test("encoder schema has the expected V4 column names in order") {
    assert(spark.emptyDataset[AMTSingleAction].schema.fieldNames.toSeq == Seq(
      "content_type", "format_version", "location", "file_format", "tracking",
      "deletion_vector", "spec_id", "partition", "sort_order_id", "record_count",
      "file_size_in_bytes", "content_stats", "manifest_info", "key_metadata",
      "split_offsets"))
  }

  test("closed constant sets match Iceberg V4 integer codes") {
    assert(AMTSingleAction.ContentType.all == Set(0, 3))
    assert(AMTSingleAction.ContentType.Type.Data == 0)
    assert(AMTSingleAction.ContentType.Type.DataManifest == 3)
    assert(Tracking.Status.all == Set(0, 1, 2, 3, 4))
    assert(AMTSingleAction.FormatVersionV4 == 4)
    assert(AMTSingleAction.FileFormatParquet == "parquet")
  }

  test("fromAddFile builder produces a DATA entry from an AddFile") {
    val add = sampleAddFile
    val entry = AMTSingleAction.fromAddFile(add, addedTracking, tableRoot)
    assert(entry.content_type == AMTSingleAction.ContentType.Type.Data)
    assert(entry.location == add.path)
    assert(entry.record_count == add.numPhysicalRecords.get)
    assert(entry.file_size_in_bytes == add.size)
    assert(entry.manifest_info.isEmpty)
    assert(entry.tracking == addedTracking)
  }

  test("DataManifestEntry wraps to a DATA_MANIFEST root entry") {
    val info = sampleManifestInfo
    val entry = DataManifestEntry(
      location = "metadata/leaf-0.parquet",
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = rootTracking,
      record_count = 42L,
      file_size_in_bytes = 2048L,
      manifest_info = info).wrap
    assert(entry.content_type == AMTSingleAction.ContentType.Type.DataManifest)
    assert(entry.location == "metadata/leaf-0.parquet")
    assert(entry.file_size_in_bytes == 2048L)
    assert(entry.record_count == 42L)
    assert(entry.manifest_info.contains(info))
    assert(entry.deletion_vector.isEmpty)
    assert(entry.sort_order_id.isEmpty)
  }

  /** Builds a DATA entry, overriding individual fields to probe invariants. */
  private def mkEntry(
      content_type: Int = AMTSingleAction.ContentType.Type.Data,
      deletion_vector: Option[DeletionVector] = None,
      sort_order_id: Option[Int] = None,
      manifest_info: Option[ManifestInfo] = None,
      tracking: Tracking = addedTracking): AMTSingleAction = AMTSingleAction(
    content_type = content_type,
    format_version = AMTSingleAction.FormatVersionV4,
    location = "f.parquet",
    file_format = AMTSingleAction.FileFormatParquet,
    tracking = tracking,
    deletion_vector = deletion_vector,
    spec_id = None,
    partition = None,
    sort_order_id = sort_order_id,
    record_count = 1L,
    file_size_in_bytes = 1L,
    content_stats = None,
    manifest_info = manifest_info,
    key_metadata = None,
    split_offsets = None)

  private def assertRejected(substring: String)(build: => AMTSingleAction): Unit = {
    val ex = intercept[IllegalArgumentException](build)
    assert(ex.getMessage.contains(substring),
      s"expected message to contain '$substring', got '${ex.getMessage}'.")
  }

  test("validate rejects an unknown content_type") {
    assertRejected("Unsupported content_type")(mkEntry(content_type = 5))
  }

  test("validate rejects a non-parquet file_format") {
    assertRejected("file_format must be")(mkEntry().copy(file_format = "orc"))
  }

  test("validate rejects a manifest pointer without manifest_info") {
    assertRejected("manifest_info must be set")(
      mkEntry(content_type = AMTSingleAction.ContentType.Type.DataManifest, manifest_info = None))
  }

  test("validate rejects a non-pointer entry with manifest_info set") {
    assertRejected("manifest_info must be set")(
      mkEntry(content_type = AMTSingleAction.ContentType.Type.Data,
        manifest_info = Some(sampleManifestInfo)))
  }

  test("validate rejects deletion_vector when content_type != 0") {
    assertRejected("deletion_vector must be null")(
      mkEntry(
        content_type = AMTSingleAction.ContentType.Type.DataManifest,
        manifest_info = Some(sampleManifestInfo),
        deletion_vector = Some(DeletionVector("dv", 0L, 1L, 1L))))
  }

  test("validate rejects sort_order_id when content_type != 0") {
    assertRejected("sort_order_id must be null")(
      mkEntry(
        content_type = AMTSingleAction.ContentType.Type.DataManifest,
        manifest_info = Some(sampleManifestInfo),
        sort_order_id = Some(1)))
  }

  test("validate rejects mismatched sequence numbers on a root entry") {
    val badTracking = rootTracking.copy(
      sequence_number = Some(3L), file_sequence_number = Some(4L))
    assertRejected("must equal")(
      mkEntry(
        content_type = AMTSingleAction.ContentType.Type.DataManifest,
        manifest_info = Some(sampleManifestInfo),
        tracking = badTracking))
  }

  test("Tracking rejects an unknown status") {
    assertRejected("Unsupported tracking status")(
      // Reuse the same interceptor; build a Tracking inside a throwaway entry.
      mkEntry(tracking = addedTracking.copy(status = 5)))
  }

  test("parquet round-trip preserves entries of every content_type") {
    withTempDir { dir =>
      val entries = sampleKinds.map(_.wrap) :+
        AMTSingleAction.fromAddFile(sampleAddFile, addedTracking, tableRoot)
      // Write under a fresh subpath: `withTempDir` pre-creates `dir`, and the default
      // parquet save mode errors if the target path already exists.
      val path = new java.io.File(dir, "all-kinds").getCanonicalPath
      spark.createDataset(entries).write.parquet(path)
      val read = spark.read.parquet(path).as[AMTSingleAction].collect()
      assert(read.toSet == entries.toSet)
    }
  }

  test("parquet round-trip preserves every binary field") {
    // Binary columns are `Array[Byte]`, whose case-class `==` is reference equality, so this
    // asserts the bytes structurally. Populate all five Option[Array[Byte]] fields across the
    // row and its sub-structs on a single DATA_MANIFEST entry (the only kind that reaches
    // `manifest_info.dv`) so none is silently dropped by wrap/unwrap or the encoder.
    withTempDir { dir =>
      val keyMeta = Array[Byte](1, 2, 3, 4)
      val deletedPos = Array[Byte](5, 6)
      val replacedPos = Array[Byte](7, 8, 9)
      val manifestDv = Array[Byte](10, 11)
      val rawStats = Array[Byte](12, 13, 14)
      val entry = DataManifestEntry(
        location = "dm.parquet",
        file_format = AMTSingleAction.FileFormatParquet,
        tracking = rootTracking.copy(
          deleted_positions = Some(deletedPos), replaced_positions = Some(replacedPos)),
        record_count = 1L,
        file_size_in_bytes = 1L,
        manifest_info = sampleManifestInfo.copy(dv = Some(manifestDv), dv_cardinality = Some(2L)),
        content_stats = Some(ContentStats(Some(rawStats))),
        key_metadata = Some(keyMeta)).wrap
      val path = new java.io.File(dir, "binary").getCanonicalPath
      spark.createDataset(Seq(entry)).write.parquet(path)
      val read = spark.read.parquet(path).as[AMTSingleAction].collect()
      assert(read.length == 1)
      val r = read.head
      assert(r.key_metadata.exists(_.sameElements(keyMeta)), "key_metadata did not round-trip.")
      assert(r.tracking.deleted_positions.exists(_.sameElements(deletedPos)),
        "tracking.deleted_positions did not round-trip.")
      assert(r.tracking.replaced_positions.exists(_.sameElements(replacedPos)),
        "tracking.replaced_positions did not round-trip.")
      assert(r.manifest_info.flatMap(_.dv).exists(_.sameElements(manifestDv)),
        "manifest_info.dv did not round-trip.")
      assert(r.content_stats.flatMap(_.raw_stats).exists(_.sameElements(rawStats)),
        "content_stats.raw_stats did not round-trip.")
    }
  }

  // One sample of each kind, exercising the kind-specific fields.
  private def sampleKinds: Seq[AMTAction] = Seq(
    DataEntry(
      location = "data.parquet",
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = addedTracking,
      record_count = 10L,
      file_size_in_bytes = 100L,
      deletion_vector = Some(DeletionVector("dv", 0L, 8L, 3L)),
      sort_order_id = Some(2)),
    DataManifestEntry(
      location = "dm.parquet",
      file_format = AMTSingleAction.FileFormatParquet,
      tracking = rootTracking,
      record_count = 42L,
      file_size_in_bytes = 2048L,
      manifest_info = sampleManifestInfo))

  test("unwrap returns the matching kind for each content_type") {
    assert(DataEntry("d", "parquet", addedTracking, 1L, 1L).wrap.unwrap.isInstanceOf[DataEntry])
    assert(DataManifestEntry("m", "parquet", rootTracking, 1L, 1L, sampleManifestInfo)
      .wrap.unwrap.isInstanceOf[DataManifestEntry])
  }

  test("wrap then unwrap round-trips every kind") {
    sampleKinds.foreach { k =>
      assert(k.wrap.unwrap == k, s"kind round-trip failed for $k.")
    }
  }

  test("unwrap then wrap round-trips the flat row for every kind") {
    sampleKinds.foreach { k =>
      val e = k.wrap
      assert(e.unwrap.wrap == e, s"flat round-trip failed for content_type=${e.content_type}.")
    }
  }

  test("fromAddFile unwraps to a DataEntry") {
    assert(AMTSingleAction.fromAddFile(sampleAddFile, addedTracking, tableRoot)
      .unwrap.isInstanceOf[DataEntry])
  }

  // Framing bytes that DeletionVectorStore adds around the raw bitmap on disk (length + checksum).
  private val dvFraming = DeletionVectorStore.getTotalSizeOfDVFieldsInFile(0)

  test("u DV round trip") {
    val id = UUID.randomUUID()
    val dv = DeletionVectorDescriptor.onDiskWithUuidRelativePath(
      id = id,
      randomPrefix = "test%dv%prefix-",
      sizeInBytes = 20,
      cardinality = 3L,
      offset = Some(8))
    val amtDv = DeletionVector.fromDescriptor(dv, tableRoot)
    assert(amtDv.location == dv.absolutePath(tableRoot).toString)
    assert(amtDv.location.contains("test%dv%prefix-"))
    assert(amtDv.offset == 8L)
    assert(amtDv.cardinality == 3L)
    // size_in_bytes is the total on-disk size: raw bitmap plus framing.
    assert(amtDv.size_in_bytes == 20L + dvFraming)

    val roundTripped = DeletionVector.toDescriptor(amtDv, tableRoot)
    assert(roundTripped.storageType == DeletionVectorDescriptor.RELATIVE_DV_MARKER)
    assert(roundTripped.pathOrInlineDv.contains("test%dv%prefix-"))
    assert(roundTripped.absolutePath(tableRoot) == dv.absolutePath(tableRoot))
  }

  test("DeletionVector round-trips an absolute-path DV outside the table root") {
    // An absolute DV whose path is outside the table root stays `p`.
    val dv = DeletionVectorDescriptor.onDiskWithAbsolutePath(
      path = "s3://other-bucket/dvs/custom.bin",
      sizeInBytes = 12, cardinality = 1L, offset = Some(4))
    val roundTripped = DeletionVector.toDescriptor(
      DeletionVector.fromDescriptor(dv, tableRoot), tableRoot)
    assert(roundTripped.storageType == DeletionVectorDescriptor.PATH_DV_MARKER)
    assert(roundTripped.legacyUniqueId == dv.legacyUniqueId)
    assert(roundTripped.sizeInBytes == dv.sizeInBytes)
    assert(roundTripped.offset == dv.offset)
  }

  test("DeletionVector.fromDescriptor rejects an inline DV") {
    val inline = DeletionVectorDescriptor.inlineInLog(Array[Byte](1, 2, 3), cardinality = 1L)
    val ex = intercept[IllegalArgumentException](
      DeletionVector.fromDescriptor(inline, tableRoot))
    assert(ex.getMessage.contains("on-disk deletion vectors"))
  }

  test("DeletionVector.fromDescriptor rejects an on-disk DV with no offset") {
    val dv = DeletionVectorDescriptor.onDiskWithUuidRelativePath(
      id = UUID.randomUUID(), sizeInBytes = 10, cardinality = 1L, offset = None)
    val ex = intercept[IllegalArgumentException](DeletionVector.fromDescriptor(dv, tableRoot))
    assert(ex.getMessage.contains("missing an offset"))
  }

  /** A DATA entry carrying the AMT-native `spec_id` that has no Delta equivalent. */
  private def entryWithPassthrough: DataEntry = DataEntry(
    location = "f.parquet",
    file_format = AMTSingleAction.FileFormatParquet,
    tracking = addedTracking,
    record_count = 10L,
    file_size_in_bytes = 100L,
    spec_id = Some(7))

  test("toAddFile carries no passthrough for the default parquet/v4 entry") {
    val add = DataEntry(
      location = "f.parquet", file_format = AMTSingleAction.FileFormatParquet,
      tracking = addedTracking, record_count = 10L, file_size_in_bytes = 100L).toAddFile(tableRoot)
    assert(add.amtPassthrough.isEmpty, "a default entry must add no per-file passthrough")
  }

  test("DataEntry round-trips toAddFile -> fromAddFile") {
    val add = entryWithPassthrough.toAddFile(tableRoot)
    assert(add.amtPassthrough.isDefined, "a genuine passthrough must be carried")
    val restored = DataEntry.fromAddFile(add, addedTracking, tableRoot)
    assert(restored.spec_id.contains(7))
    // file_format / format_version are not carried; they are reconstructed at the M1 defaults.
    assert(restored.file_format == AMTSingleAction.FileFormatParquet)
    assert(restored.format_version == AMTSingleAction.FormatVersionV4)
  }

  test("fromAddFile falls back to defaults with no passthrough") {
    val restored = DataEntry.fromAddFile(sampleAddFile, addedTracking, tableRoot)
    assert(restored.file_format == AMTSingleAction.FileFormatParquet)
    assert(restored.spec_id.isEmpty)
    assert(restored.format_version == AMTSingleAction.FormatVersionV4)
  }

  test("amtPassthrough round-trips through the commit JSON") {
    val add = entryWithPassthrough.toAddFile(tableRoot)
    assert(add.amtPassthrough.isDefined)
    val json = add.json
    assert(json.contains("amtPassthrough"), s"amtPassthrough must be serialized; got $json")
    assert(json.contains("spec_id"), s"spec_id must be serialized; got $json")

    val roundTripped = Action.fromJson(json) match {
      case a: AddFile => a
      case other => fail(s"expected an AddFile, got $other")
    }
    assert(roundTripped.amtPassthrough == add.amtPassthrough)
    assert(roundTripped.amtPassthrough.flatMap(_.spec_id).contains(7))
  }

  test("an AddFile with no amtPassthrough keeps it out of the commit JSON") {
    // Include.NON_ABSENT: non-AMT tables must not gain a new key in their commit JSON.
    assert(sampleAddFile.amtPassthrough.isEmpty)
    assert(!sampleAddFile.json.contains("amtPassthrough"))
  }

  test("amtPassthrough survives copy and InMemoryLogReplay state reconstruction") {
    val add = entryWithPassthrough.toAddFile(tableRoot).copy(path = "f.parquet")
    val passthrough = add.amtPassthrough
    assert(passthrough.isDefined)
    // Below simulates the real write path in [[writeIncrementalMaterialization]]
    assert(add.copy(dataChange = false).amtPassthrough == passthrough)
    val replay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None,
      minSetTransactionRetentionTimestamp = None,
      tableRoot = tableRoot,
      useDeletionVectorObjectIdentity = true)
    replay.append(0, Iterator(add))
    val reconstructed = replay.allFiles
    assert(reconstructed.length == 1)
    // The passthrough survives replay and is still usable end-to-end.
    assert(reconstructed.head.amtPassthrough == passthrough)
    assert(DataEntry.fromAddFile(reconstructed.head, addedTracking, tableRoot).spec_id.contains(7))
  }

  test("amtPassthrough participates in AddFile equality") {
    val base = sampleAddFile
    val withA = base.copy(amtPassthrough = Some(AMTPassthrough(spec_id = Some(7))))
    val withB = base.copy(amtPassthrough = Some(AMTPassthrough(spec_id = Some(7))))
    assert(withA == withB && withA.hashCode == withB.hashCode,
      "equal-content passthrough must compare equal")
    val withC = base.copy(amtPassthrough = Some(AMTPassthrough(spec_id = Some(9))))
    assert(withA != withC, "different passthrough content must compare unequal")
    // Present vs absent -> unequal.
    assert(withA != base && base != withA)
  }

  test("RowIndices.resolve returns None when the schema does not project amtPassthrough") {
    val schema = StructType(Seq(StructField("path", StringType)))
    assert(AMTPassthrough.RowIndices.resolve(schema).isEmpty)
  }

  test("RowIndices.resolve and fromRow read the passthrough positionally") {
    val addSchema = Action.addFileSchema
    val indices = AMTPassthrough.RowIndices.resolve(addSchema)
      .getOrElse(fail("the AddFile schema must project amtPassthrough"))
    assert(addSchema.fieldNames(indices.structIndex) == AMTPassthrough.FIELD_NAME)
    assert(indices.numFields == AMTPassthrough.STRUCT_TYPE.fields.length)

    // A row carrying spec_id -> read back; a row with a null struct -> None.
    val withPassthrough = new GenericInternalRow(addSchema.fields.length)
    withPassthrough.update(indices.structIndex, new GenericInternalRow(Array[Any](7)))
    assert(AMTPassthrough.fromRow(withPassthrough, indices)
      .contains(AMTPassthrough(spec_id = Some(7))))

    val withoutPassthrough = new GenericInternalRow(addSchema.fields.length)
    assert(AMTPassthrough.fromRow(withoutPassthrough, indices).isEmpty)

    // A present struct whose own field is null -> present, but with no spec_id.
    val nullSpecId = new GenericInternalRow(addSchema.fields.length)
    nullSpecId.update(indices.structIndex, new GenericInternalRow(Array[Any](null)))
    assert(AMTPassthrough.fromRow(nullSpecId, indices).contains(AMTPassthrough(spec_id = None)))
  }
}
