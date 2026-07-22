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

import scala.util.Try

import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor}
import org.apache.spark.sql.delta.stats.DeltaStatistics
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.hadoop.fs.Path

/**
 * One entry in any AMT manifest file (leaf or root).
 * Same shape for leaves and roots; `content_type` discriminates the kind
 * of entry (data file or data-manifest pointer).
 *
 * @param content_type Entry-kind discriminator (0=DATA, 3=DATA_MANIFEST).
 * @param format_version Iceberg writer format version; 4 for V4.
 * @param location Path of the file relative to the table root, or an absolute URI.
 * @param file_format Physical format of `location` (currently always "parquet").
 * @param tracking Lineage envelope: status plus snapshot/sequence numbers and DV positions.
 * @param deletion_vector Pointer to a deletion-vector blob; only on DATA entries.
 * @param spec_id Id of the partition spec the file was written under.
 * @param partition Partition values of the file.
 * @param sort_order_id Id of the sort order the file was written with; only on DATA entries.
 * @param record_count Rows in the file, or rows the manifest summarizes (for pointers).
 * @param file_size_in_bytes On-disk size of `location` in bytes.
 * @param content_stats Column-level statistics for the file.
 * @param manifest_info Manifest stats plus inline DV; set iff content_type is DATA_MANIFEST.
 * @param key_metadata Encryption key metadata for the file, if encrypted.
 * @param split_offsets Row-group split offsets (Iceberg field 132); Delta does not generate or
 *                      consume these, but the field is kept in the schema to carry it forward
 *                      for tables written by both Delta and Iceberg.
 * @param column_files Per-column file entries; only content_type in {0,3}.
 */
case class AMTSingleAction(
    content_type: Int,                          // ID: 134, required.
    format_version: Int,                        // ID: 157, required.
    location: String,                           // ID: 100, required.
    file_format: String,                        // ID: 101, required ("parquet" for now).
    tracking: Tracking,                         // ID: 147, required.
    deletion_vector: Option[DeletionVector], // ID: 148, optional (only when content_type=0).
    spec_id: Option[Int],                       // ID: 141, optional.
    partition: Partition,                       // ID: 102, required.
    sort_order_id: Option[Int],                 // ID: 140, optional (only when content_type=0).
    record_count: Long,                         // ID: 103, required.
    file_size_in_bytes: Long,                   // ID: 104, required.
    content_stats: Option[ContentStats],        // ID: 146, optional.
    manifest_info: Option[ManifestInfo],        // ID: 150, required for DATA_MANIFEST.
    key_metadata: Option[Array[Byte]],          // ID: 131, optional.
    split_offsets: Option[Seq[Long]],           // ID: 132, optional.
    column_files: Option[Seq[ColumnFile]] // ID: TBD (content_type in {0,3}).
) {
  AMTSingleAction.validate(this)

  /**
   * Returns the strongly-typed [[AMTAction]] view of this row. The `.get` calls are
   * safe because [[AMTSingleAction.validate]] (run at construction) guarantees the required
   * fields are present for each kind.
   */
  def unwrap: AMTAction = content_type match {
    case AMTSingleAction.ContentType.Type.Data =>
      DataEntry(
        location = location,
        file_format = file_format,
        tracking = tracking,
        record_count = record_count,
        file_size_in_bytes = file_size_in_bytes,
        partition = partition,
        deletion_vector = deletion_vector,
        spec_id = spec_id,
        sort_order_id = sort_order_id,
        content_stats = content_stats,
        key_metadata = key_metadata,
        split_offsets = split_offsets,
        column_files = column_files,
        format_version = format_version)
    case AMTSingleAction.ContentType.Type.DataManifest =>
      DataManifestEntry(
        location = location,
        file_format = file_format,
        tracking = tracking,
        record_count = record_count,
        file_size_in_bytes = file_size_in_bytes,
        manifest_info = manifest_info.get,
        partition = partition,
        spec_id = spec_id,
        content_stats = content_stats,
        key_metadata = key_metadata,
        split_offsets = split_offsets,
        column_files = column_files,
        format_version = format_version)
    case other =>
      throw new IllegalStateException(s"Unsupported content_type: $other.")
  }
}

object AMTSingleAction {

  /**
   * Content-type metadata. The integer codes live in the nested [[ContentType.Type]]
   * object; matching Iceberg V4 (0 = DATA, 3 = DATA_MANIFEST). Codes 2 (EQUALITY_DELETES)
   * and 4 (DELETE_MANIFEST) are intentionally omitted: Delta writers emit neither. These
   * are expected only when an existing Iceberg table is upgraded from v3 to v4; support
   * will be added later if needed.
   */
  object ContentType {
    object Type {
      val Data: Int = 0
      val DataManifest: Int = 3
    }
    val all: Set[Int] = Set(Type.Data, Type.DataManifest)
    /** True iff this content type may only appear in a root manifest. */
    def isRootOnly(t: Int): Boolean = t == Type.DataManifest
  }

  /** `format_version` value that V4 writers must emit. */
  val FormatVersionV4: Int = 4
  /** File format for AMT data and manifest files. */
  val FileFormatParquet: String = "parquet"

  /**
   * Validates spec invariants. Throws `IllegalArgumentException` on any
   * violation. Called from the case-class constructor; rejecting bad
   * entries at construction.
   *
   *   - content_type in {0, 3}.
   *   - file_format missing or "parquet".
   *   - manifest_info MUST be set iff content_type == DATA_MANIFEST (3).
   *   - deletion_vector MUST be null when content_type != 0.
   *   - sort_order_id MUST be null when content_type != 0.
   *   - tracking.sequence_number == tracking.file_sequence_number when
   *     content_type == DATA_MANIFEST (3) and both are set.
   */
  def validate(action: AMTSingleAction): Unit = {
    require(ContentType.all.contains(action.content_type),
      s"Unsupported content_type: ${action.content_type}.")
    require(Option(action.file_format).forall(_ == FileFormatParquet),
      s"file_format must be missing or '$FileFormatParquet'; got ${action.file_format}.")
    require(
      ContentType.isRootOnly(action.content_type) == action.manifest_info.isDefined,
      s"manifest_info must be set iff content_type is a manifest pointer; " +
        s"got content_type=${action.content_type}, " +
        s"manifest_info.isDefined=${action.manifest_info.isDefined}.")
    require(action.content_type == ContentType.Type.Data || action.deletion_vector.isEmpty,
      s"deletion_vector must be null when content_type != ${ContentType.Type.Data}; " +
        s"got content_type=${action.content_type}.")
    require(action.content_type == ContentType.Type.Data || action.sort_order_id.isEmpty,
      s"sort_order_id must be null when content_type != ${ContentType.Type.Data}; " +
        s"got content_type=${action.content_type}.")
    if (ContentType.isRootOnly(action.content_type)) {
      (action.tracking.sequence_number, action.tracking.file_sequence_number) match {
        case (Some(a), Some(b)) => require(a == b,
          s"For root entries, tracking.sequence_number must equal " +
            s"tracking.file_sequence_number; got $a vs $b.")
        case _ => ()
      }
    }
  }

  /** Creates [[AMTSingleAction]] from AddFile. */
  def fromAddFile(add: AddFile, tracking: Tracking, tableRoot: Path): AMTSingleAction =
    DataEntry.fromAddFile(add, tracking, tableRoot).wrap
}

/**
 * Strongly-typed, in-memory view of an [[AMTSingleAction]], one case class per
 * `content_type` kind. Only [[AMTSingleAction]] is persisted; [[wrap]] flattens a
 * kind back to it and [[AMTSingleAction.unwrap]] recovers it.
 */
sealed trait AMTAction {
  /** The `content_type` discriminator this kind maps to. */
  protected def content_type: Int

  require(AMTSingleAction.ContentType.all.contains(content_type),
    s"Unsupported content_type: $content_type.")

  /** Flatten this typed view back into the on-disk [[AMTSingleAction]] row. */
  def wrap: AMTSingleAction
}

/**
 * A data-file entry (`content_type = 0`), used by leaves.
 *
 * @param location Path of the data file relative to the table root, or an absolute URI.
 * @param file_format Physical format of `location` (currently always "parquet").
 * @param tracking Lineage envelope: status plus snapshot/sequence numbers and DV positions.
 * @param record_count Number of records in the data file.
 * @param file_size_in_bytes On-disk size of `location` in bytes.
 * @param partition Partition values of the file.
 * @param deletion_vector Pointer to a deletion-vector blob for this data file.
 * @param spec_id Id of the partition spec the file was written under.
 * @param sort_order_id Id of the sort order the file was written with.
 * @param content_stats Column-level statistics for the file.
 * @param key_metadata Encryption key metadata for the file, if encrypted.
 * @param split_offsets Row-group split offsets (Iceberg field 132); Delta does not generate or
 *                      consume these, but the field is kept in the schema to carry it forward
 *                      for tables written by both Delta and Iceberg.
 * @param column_files Per-column file entries.
 * @param format_version Iceberg writer format version; 4 for V4.
 */
case class DataEntry(
    location: String,
    file_format: String,
    tracking: Tracking,
    record_count: Long,
    file_size_in_bytes: Long,
    partition: Partition = Partition(),
    deletion_vector: Option[DeletionVector] = None,
    spec_id: Option[Int] = None,
    sort_order_id: Option[Int] = None,
    content_stats: Option[ContentStats] = None,
    key_metadata: Option[Array[Byte]] = None,
    split_offsets: Option[Seq[Long]] = None,
    column_files: Option[Seq[ColumnFile]] = None,
    format_version: Int = AMTSingleAction.FormatVersionV4)
  extends AMTAction {

  override protected def content_type: Int = AMTSingleAction.ContentType.Type.Data

  override def wrap: AMTSingleAction = AMTSingleAction(
    content_type = content_type,
    format_version = format_version,
    location = location,
    file_format = file_format,
    tracking = tracking,
    deletion_vector = deletion_vector,
    spec_id = spec_id,
    partition = partition,
    sort_order_id = sort_order_id,
    record_count = record_count,
    file_size_in_bytes = file_size_in_bytes,
    content_stats = content_stats,
    manifest_info = None,
    key_metadata = key_metadata,
    split_offsets = split_offsets,
    column_files = column_files)

  def toAddFile(tableRoot: Path): AddFile = {
    val dv = deletion_vector.map(DeletionVector.toDescriptor(_, tableRoot)).orNull
    // `record_count` (Iceberg field 103) and the Delta `numRecords` statistic are both the physical
    // row count (total records in the file, including DV-deleted rows), so store it directly.
    val stats = s"""{"${DeltaStatistics.NUM_RECORDS}":$record_count}"""
    AddFile(
      path = location,
      partitionValues = partition.values.getOrElse(Map.empty),
      size = file_size_in_bytes,
      modificationTime = 0L,
      dataChange = false,
      stats = stats,
      deletionVector = dv,
      baseRowId = tracking.first_row_id,
      defaultRowCommitVersion = tracking.sequence_number)
  }
}

object DataEntry {
  /** Creates [[DataEntry]] from AddFile. */
  def fromAddFile(add: AddFile, tracking: Tracking, tableRoot: Path): DataEntry =
    DataEntry(
      location = add.path,
      file_format = AMTSingleAction.FileFormatParquet,
      // Round-trip the AddFile's row-tracking fields through the Iceberg tracking envelope so a
      // rowTracking-enabled table can reconstruct them on read.
      tracking = tracking.copy(
        first_row_id = add.baseRowId,
        sequence_number = add.defaultRowCommitVersion),
      // Iceberg field 103 is the physical record count of the file, not the live/logical
      // count after deletes; throw rather than guess when the AddFile carries no stats.
      record_count = add.numPhysicalRecords.getOrElse(
        throw new IllegalArgumentException(
          s"Cannot build AMT entry: AddFile has no record count (missing stats): ${add.path}.")),
      file_size_in_bytes = add.size,
      partition = Partition(Option(add.partitionValues).filter(_.nonEmpty)),
      deletion_vector =
        Option(add.deletionVector).map(DeletionVector.fromDescriptor(_, tableRoot)),
      content_stats = None)
}

/**
 * A pointer from the root to a leaf data manifest (`content_type = 3`).
 *
 * @param location Path of the leaf data manifest relative to the table root, or an absolute URI.
 * @param file_format Physical format of `location` (currently always "parquet").
 * @param tracking Lineage envelope: status plus snapshot/sequence numbers and DV positions.
 * @param record_count Rows summarized across the referenced manifest.
 * @param file_size_in_bytes On-disk size of the manifest file in bytes.
 * @param manifest_info Stats for the referenced manifest, plus its inline DV.
 * @param partition Partition values.
 * @param spec_id Id of the partition spec the manifest was written under.
 * @param content_stats Column-level statistics.
 * @param key_metadata Encryption key metadata for the manifest, if encrypted.
 * @param split_offsets Row-group split offsets (Iceberg field 132); Delta does not generate or
 *                      consume these, but the field is kept in the schema to carry it forward
 *                      for tables written by both Delta and Iceberg.
 * @param column_files Per-column file entries.
 * @param format_version Iceberg writer format version; 4 for V4.
 */
case class DataManifestEntry(
    location: String,
    file_format: String,
    tracking: Tracking,
    record_count: Long,
    file_size_in_bytes: Long,
    manifest_info: ManifestInfo,
    partition: Partition = Partition(),
    spec_id: Option[Int] = None,
    content_stats: Option[ContentStats] = None,
    key_metadata: Option[Array[Byte]] = None,
    split_offsets: Option[Seq[Long]] = None,
    column_files: Option[Seq[ColumnFile]] = None,
    format_version: Int = AMTSingleAction.FormatVersionV4)
  extends AMTAction {

  override protected def content_type: Int = AMTSingleAction.ContentType.Type.DataManifest

  override def wrap: AMTSingleAction = AMTSingleAction(
    content_type = content_type,
    format_version = format_version,
    location = location,
    file_format = file_format,
    tracking = tracking,
    deletion_vector = None,
    spec_id = spec_id,
    partition = partition,
    sort_order_id = None,
    record_count = record_count,
    file_size_in_bytes = file_size_in_bytes,
    content_stats = content_stats,
    manifest_info = Some(manifest_info),
    key_metadata = key_metadata,
    split_offsets = split_offsets,
    column_files = column_files)
}

/**
 * Inheritance / lineage envelope on every [[AMTSingleAction]]. Matches the
 * `tracking` struct in the V4 spec (field IDs in comments).
 *
 * @param status Entry status (0=existing, 1=added, 2=deleted, 3=replaced, 4=modified).
 * @param snapshot_id Snapshot the file was added/deleted/replaced in; inherited from root.
 * @param dv_snapshot_id Snapshot the DV was added in; null when the entry has no DV.
 * @param sequence_number Data sequence number of the file.
 * @param file_sequence_number File sequence number (when the file was added).
 * @param first_row_id Id of the first row in the data file.
 * @param deleted_positions Bitmap of positions deleted in this snapshot.
 * @param replaced_positions Bitmap of positions replaced in this snapshot.
 */
case class Tracking(
    status: Int,                            // ID: 0, required.
    snapshot_id: Option[Long],              // ID: 1, optional.
    dv_snapshot_id: Option[Long],           // ID: 5, optional.
    sequence_number: Option[Long],          // ID: 3, optional.
    file_sequence_number: Option[Long],     // ID: 4, optional.
    first_row_id: Option[Long],             // ID: 142, optional.
    deleted_positions: Option[Array[Byte]], // ID: 6, optional (bitmap).
    replaced_positions: Option[Array[Byte]] // ID: 7, optional (bitmap).
) {
  require(Tracking.Status.all.contains(status),
    s"Unsupported tracking status: $status.")
}

object Tracking {
  /**
   * Closed set of `status` values, matching Iceberg V4 integer codes.
   *   0 = EXISTING, 1 = ADDED, 2 = DELETED, 3 = REPLACED, 4 = MODIFIED.
   *
   * REPLACED / MODIFIED come in pairs: a `REPLACED` row marks the prior
   * tracking-snapshot-id state; a `MODIFIED` row carries the current state
   * with updated DV info or column updates. They need not be co-located
   * in the same manifest.
   */
  object Status {
    val Existing: Int = 0
    val Added: Int = 1
    val Deleted: Int = 2
    val Replaced: Int = 3
    val Modified: Int = 4
    val all: Set[Int] = Set(Existing, Added, Deleted, Replaced, Modified)
  }
}

/**
 * Partition-values carrier for [[AMTSingleAction]] (Iceberg V4 `partition`, field 102).
 *
 * Iceberg models `partition` as a per-table dynamic struct (one typed field per
 * partition column); that shape cannot be expressed statically here, so this PR carries
 * Delta's raw `AddFile.partitionValues`. The struct is present at field 102 (the spec
 * marks it required); binary interop with a strict V4 reader that expects the typed
 * per-column struct is deferred.
 *
 *
 * @param values Raw Delta partition values (column name to value); None when unpartitioned.
 */
case class Partition(values: Option[Map[String, String]] = None)

/**
 * Pointer to a deletion-vector blob, mirroring the Iceberg V4 `deletion_vector` struct.
 *
 * @param location Absolute path of the file holding the DV blob.
 * @param offset Byte offset where the DV content starts within that file.
 * @param size_in_bytes Total on-disk DV size = raw bitmap + length + checksum framing.
 * @param cardinality Number of positions the DV marks deleted.
 */
case class DeletionVector(
    location: String,         // ID: 155, required (DV blob file path).
    offset: Long,             // ID: 144, required.
    size_in_bytes: Long,      // ID: 145, required.
    cardinality: Long)        // ID: 156, required.

object DeletionVector {

  /** Maps a Delta on-disk [[DeletionVectorDescriptor]] onto the AMT sub-struct; rejects inline. */
  def fromDescriptor(dv: DeletionVectorDescriptor, tableRoot: Path): DeletionVector = {
    require(dv.isOnDisk,
      s"AMT tables only support on-disk deletion vectors; got storageType=${dv.storageType}.")
    val offset = dv.offset.getOrElse(
      throw new IllegalArgumentException(
        s"On-disk deletion vector is missing an offset: ${dv.pathOrInlineDv}."))
    DeletionVector(
      location = dv.absolutePath(tableRoot).toString,
      offset = offset.toLong,
      size_in_bytes = DeletionVectorStore.getTotalSizeOfDVFieldsInFile(dv.sizeInBytes).toLong,
      cardinality = dv.cardinality)
  }

  /** Rebuilds the Delta [[DeletionVectorDescriptor]] from the AMT sub-struct. */
  def toDescriptor(dv: DeletionVector, tableRoot: Path): DeletionVectorDescriptor = {
    val rawSize = dv.size_in_bytes.toInt -
      (DeletionVectorStore.getTotalSizeOfDVFieldsInFile(0))
    recoverRelativeUuid(dv.location, tableRoot) match {
      case Some((id, randomPrefix)) =>
        DeletionVectorDescriptor.onDiskWithRelativePath(
          id = id,
          randomPrefix = randomPrefix,
          sizeInBytes = rawSize,
          cardinality = dv.cardinality,
          offset = Some(dv.offset.toInt))
      case None =>
        DeletionVectorDescriptor.onDiskWithAbsolutePath(
          path = dv.location,
          sizeInBytes = rawSize,
          cardinality = dv.cardinality,
          offset = Some(dv.offset.toInt))
    }
  }

  /**
   * If `location` is a Delta-written DV blob under `tableRoot` (a `deletion_vector_<uuid>.bin` file
   * directly under it or under one random-prefix directory), returns its UUID and prefix so the `u`
   * descriptor can be rebuilt; None otherwise (treated as an absolute `p`).
   */
  private def recoverRelativeUuid(location: String, tableRoot: Path): Option[(UUID, String)] = {
    val path = new Path(location)
    val parent = path.getParent
    val uuid =
      Try(DeletionVectorDescriptor.getUUIDFromDeletionVectorFileName(path.getName)).toOption
    val rootStr = tableRoot.toString
    uuid.flatMap { id =>
      if (parent.toString == rootStr) {
        // <tableRoot>/deletion_vector_<uuid>.bin -- no random prefix.
        Some((id, ""))
      } else if (parent.getParent.toString == rootStr) {
        // <tableRoot>/<prefix>/deletion_vector_<uuid>.bin -- one random-prefix directory.
        Some((id, parent.getName))
      } else {
        None
      }
    }
  }
}

/**
 * Statistics + inline manifest DV for a `content_type == DATA_MANIFEST (3)` root entry.
 * Field IDs and required/optional match the V4 `manifest_info` struct verbatim.
 *
 * `dv` carries the inline manifest deletion-vector bitmap over leaf row
 * positions; `dv_cardinality` is its count. Manifest DVs live INSIDE
 * `manifest_info`, not as separate root rows -- a deliberate choice in
 * the Combined Data + DV Entry model.
 *
 * @param added_files_count Count of ADDED file entries in the referenced manifest.
 * @param existing_files_count Count of EXISTING file entries.
 * @param deleted_files_count Count of DELETED file entries.
 * @param replaced_files_count Count of REPLACED file entries.
 * @param added_rows_count Rows across ADDED files.
 * @param existing_rows_count Rows across EXISTING files.
 * @param deleted_rows_count Rows across DELETED files.
 * @param replaced_rows_count Rows across REPLACED files.
 * @param min_sequence_number Minimum data sequence number across the manifest's entries.
 * @param dv Inline manifest deletion-vector bitmap over leaf row positions.
 * @param dv_cardinality Number of positions the inline manifest DV marks.
 */
case class ManifestInfo(
    added_files_count: Int,           // ID: 504, required.
    existing_files_count: Int,        // ID: 505, required.
    deleted_files_count: Int,         // ID: 506, required.
    replaced_files_count: Int,        // ID: 520, required.
    added_rows_count: Long,           // ID: 512, required.
    existing_rows_count: Long,        // ID: 513, required.
    deleted_rows_count: Long,         // ID: 514, required.
    replaced_rows_count: Long,        // ID: 521, required.
    min_sequence_number: Long,        // ID: 516, required.
    dv: Option[Array[Byte]],          // ID: 522, optional (inline manifest DV bitmap).
    dv_cardinality: Option[Long])     // ID: 523, optional.

/**
 * Column-level statistics carrier. Iceberg V4 leaves the inner shape
 * up to the caller (it depends on the table schema). A future PR introduces
 * the Delta-stats mapping.
 *
 * The single nullable `raw_stats` field is deliberate: an empty case class encodes to an
 * empty Parquet group, which the Parquet data source rejects
 * (`EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE`). One nullable column keeps
 * `AMTSingleAction` Parquet-writable without committing to the final stats shape; M1
 * writers leave it `None`.
 *
 *
 * @param raw_stats Column-stats payload; M1 writers leave it None.
 */
case class ContentStats(raw_stats: Option[Array[Byte]] = None)

/**
 * Per-column file entries; shape TBD per the spec.
 *
 * The single nullable `location` field is deliberate: an empty case class encodes to an
 * empty Parquet group, which the Parquet data source rejects
 * (`EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE`). One nullable column keeps
 * `AMTSingleAction` Parquet-writable without committing to the final per-column-file
 * shape; M1 writers leave `column_files` `None`.
 *
 *
 * @param location Per-column file path; M1 writers leave it None.
 */
case class ColumnFile(location: Option[String] = None)
