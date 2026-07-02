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

import org.apache.spark.sql.delta.actions.AddFile

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Shape, builder, invariant, and parquet round-trip tests for the AMT
 * [[AmtSingleAction]] row record and its sub-structs. The schema is dictated by
 * the Iceberg V4 / Adaptive Metadata Tree proposal; these tests pin the
 * declared column list, the closed constant sets, the per-kind builders, and
 * the constructor invariants enforced by [[AmtSingleAction.validate]].
 */
class AmtSingleActionSerializerSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

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
    added_rows_count = 42L,
    existing_rows_count = 0L,
    deleted_rows_count = 0L,
    replaced_rows_count = 0L,
    min_sequence_number = 3L,
    dv = None,
    dv_cardinality = None)

  test("encoder schema has the expected V4 column names in order") {
    assert(spark.emptyDataset[AmtSingleAction].schema.fieldNames.toSeq == Seq(
      "content_type", "format_version", "location", "file_format", "tracking",
      "deletion_vector", "spec_id", "partition", "sort_order_id", "record_count",
      "file_size_in_bytes", "content_stats", "manifest_info", "key_metadata",
      "split_offsets", "column_files"))
  }

  test("closed constant sets match Iceberg V4 integer codes") {
    assert(AmtSingleAction.ContentType.all == Set(0, 3))
    assert(AmtSingleAction.ContentType.Type.Data == 0)
    assert(AmtSingleAction.ContentType.Type.DataManifest == 3)
    assert(Tracking.Status.all == Set(0, 1, 2, 3, 4))
    assert(AmtSingleAction.FormatVersionV4 == 4)
    assert(AmtSingleAction.FileFormatParquet == "parquet")
  }

  test("fromAddFile builder produces a DATA entry from an AddFile") {
    val add = sampleAddFile
    val entry = AmtSingleAction.fromAddFile(add, addedTracking)
    assert(entry.content_type == AmtSingleAction.ContentType.Type.Data)
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
      file_format = AmtSingleAction.FileFormatParquet,
      tracking = rootTracking,
      record_count = 42L,
      file_size_in_bytes = 2048L,
      manifest_info = info).wrap
    assert(entry.content_type == AmtSingleAction.ContentType.Type.DataManifest)
    assert(entry.location == "metadata/leaf-0.parquet")
    assert(entry.file_size_in_bytes == 2048L)
    assert(entry.record_count == 42L)
    assert(entry.manifest_info.contains(info))
    assert(entry.deletion_vector.isEmpty)
    assert(entry.sort_order_id.isEmpty)
    assert(entry.column_files.isEmpty)
  }

  /** Builds a DATA entry, overriding individual fields to probe invariants. */
  private def mkEntry(
      content_type: Int = AmtSingleAction.ContentType.Type.Data,
      deletion_vector: Option[DeletionVector] = None,
      sort_order_id: Option[Int] = None,
      manifest_info: Option[ManifestInfo] = None,
      tracking: Tracking = addedTracking): AmtSingleAction = AmtSingleAction(
    content_type = content_type,
    format_version = AmtSingleAction.FormatVersionV4,
    location = "f.parquet",
    file_format = AmtSingleAction.FileFormatParquet,
    tracking = tracking,
    deletion_vector = deletion_vector,
    spec_id = None,
    partition = Partition(),
    sort_order_id = sort_order_id,
    record_count = 1L,
    file_size_in_bytes = 1L,
    content_stats = None,
    manifest_info = manifest_info,
    key_metadata = None,
    split_offsets = None,
    column_files = None)

  private def assertRejected(substring: String)(build: => AmtSingleAction): Unit = {
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
      mkEntry(content_type = AmtSingleAction.ContentType.Type.DataManifest, manifest_info = None))
  }

  test("validate rejects a non-pointer entry with manifest_info set") {
    assertRejected("manifest_info must be set")(
      mkEntry(content_type = AmtSingleAction.ContentType.Type.Data,
        manifest_info = Some(sampleManifestInfo)))
  }

  test("validate rejects deletion_vector when content_type != 0") {
    assertRejected("deletion_vector must be null")(
      mkEntry(
        content_type = AmtSingleAction.ContentType.Type.DataManifest,
        manifest_info = Some(sampleManifestInfo),
        deletion_vector = Some(DeletionVector("dv", 0L, 1L, 1L))))
  }

  test("validate rejects sort_order_id when content_type != 0") {
    assertRejected("sort_order_id must be null")(
      mkEntry(
        content_type = AmtSingleAction.ContentType.Type.DataManifest,
        manifest_info = Some(sampleManifestInfo),
        sort_order_id = Some(1)))
  }

  test("validate rejects mismatched sequence numbers on a root entry") {
    val badTracking = rootTracking.copy(
      sequence_number = Some(3L), file_sequence_number = Some(4L))
    assertRejected("must equal")(
      mkEntry(
        content_type = AmtSingleAction.ContentType.Type.DataManifest,
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
        AmtSingleAction.fromAddFile(sampleAddFile, addedTracking)
      // Write under a fresh subpath: `withTempDir` pre-creates `dir`, and the default
      // parquet save mode errors if the target path already exists.
      val path = new java.io.File(dir, "all-kinds").getCanonicalPath
      spark.createDataset(entries).write.parquet(path)
      val read = spark.read.parquet(path).as[AmtSingleAction].collect()
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
        file_format = AmtSingleAction.FileFormatParquet,
        tracking = rootTracking.copy(
          deleted_positions = Some(deletedPos), replaced_positions = Some(replacedPos)),
        record_count = 1L,
        file_size_in_bytes = 1L,
        manifest_info = sampleManifestInfo.copy(dv = Some(manifestDv), dv_cardinality = Some(2L)),
        content_stats = Some(ContentStats(Some(rawStats))),
        key_metadata = Some(keyMeta)).wrap
      val path = new java.io.File(dir, "binary").getCanonicalPath
      spark.createDataset(Seq(entry)).write.parquet(path)
      val read = spark.read.parquet(path).as[AmtSingleAction].collect()
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
  private def sampleKinds: Seq[AmtAction] = Seq(
    DataEntry(
      location = "data.parquet",
      file_format = AmtSingleAction.FileFormatParquet,
      tracking = addedTracking,
      record_count = 10L,
      file_size_in_bytes = 100L,
      deletion_vector = Some(DeletionVector("dv", 0L, 8L, 3L)),
      sort_order_id = Some(2),
      column_files = Some(Seq(ColumnFile(Some("c0.parquet"))))),
    DataManifestEntry(
      location = "dm.parquet",
      file_format = AmtSingleAction.FileFormatParquet,
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
    assert(AmtSingleAction.fromAddFile(sampleAddFile, addedTracking).unwrap.isInstanceOf[DataEntry])
  }
}
