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

import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

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
 * @param tags Delta [[AddFile]] tags. They have no Iceberg V4 slot, so the field is a Delta-private
 *             extension carried on DATA entries only, so a file's tags survive a manifest round
 *             trip.
 */
case class AMTSingleAction(
    content_type: Int,                          // ID: 134, required.
    format_version: Int,                        // ID: 157, required.
    location: String,                           // ID: 100, required.
    file_format: String,                        // ID: 101, required ("parquet" for now).
    tracking: Tracking,                         // ID: 147, required.
    deletion_vector: Option[DeletionVector],    // ID: 148, optional (only when content_type=0).
    spec_id: Option[Int],                       // ID: 141, optional.
    partition: Option[Map[String, String]],     // ID: 102, optional.
    sort_order_id: Option[Int],                 // ID: 140, optional (only when content_type=0).
    record_count: Long,                         // ID: 103, required.
    file_size_in_bytes: Long,                   // ID: 104, required.
    content_stats: Option[String],              // ID: 146, optional.
    manifest_info: Option[ManifestInfo],        // ID: 150, required for DATA_MANIFEST.
    key_metadata: Option[Array[Byte]],          // ID: 131, optional.
    split_offsets: Option[Seq[Long]],           // ID: 132, optional.
    tags: Option[Map[String, String]]           // Delta-private id (only when content_type=0).
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
        tags = tags,
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
    require(action.content_type == ContentType.Type.Data || action.tags.isEmpty,
      s"tags must be null when content_type != ${ContentType.Type.Data}; " +
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

  /**
   * The Iceberg V4 spec metadata for one field of an AMT manifest struct: its field id and whether
   * the spec marks it required. Construct via the [[required]] / [[optional]] factories.
   */
  case class AMTFieldSpec private (name: String, id: Long, required: Boolean)

  /** Declares a spec-required field (Parquet `REQUIRED`). */
  private def required(id: Long, name: String): AMTFieldSpec =
    AMTFieldSpec(name, id, required = true)

  /** Declares an optional field (Parquet `OPTIONAL`). */
  private def optional(id: Long, name: String): AMTFieldSpec =
    AMTFieldSpec(name, id, required = false)

  /**
   * Field ids for the Delta-private `tags` map. Delta's [[AddFile]] tags have no Iceberg V4 slot,
   * so they are stamped from the top of the id space, just below Iceberg's reserved band. The V4
   * spec forbids field ids greater than `Int.MaxValue - 200` (2147483447), reserving that range for
   * metadata columns (`_file`, `_row_id`, ...); anchoring `tags` at `Int.MaxValue - 300` keeps it a
   * legal, non-reserved id that does not collide with any other id this schema assigns in practice.
   * The one theoretical exception is `content_stats` (ids grow as 10000 + 200 * columnId): a table
   * would need more than ~10.7M stats-collected columns for those ids to reach this band, which is
   * not realistic.
   */
  private[amt] val TagsFieldId: Long = Int.MaxValue.toLong - 300L
  private val TAGS_KEY_FIELD_ID: Long = TagsFieldId + 1L
  private val TAGS_VALUE_FIELD_ID: Long = TagsFieldId + 2L

  /** Iceberg V4 field specs for the top-level [[AMTSingleAction]] fields. */
  private val topLevelFields: Seq[AMTFieldSpec] = Seq(
    required(134L, "content_type"),
    required(157L, "format_version"),
    required(100L, "location"),
    required(101L, "file_format"),
    required(147L, "tracking"),
    optional(148L, "deletion_vector"),
    optional(141L, "spec_id"),
    optional(102L, "partition"),
    optional(140L, "sort_order_id"),
    required(103L, "record_count"),
    required(104L, "file_size_in_bytes"),
    optional(146L, "content_stats"),
    optional(150L, "manifest_info"),
    optional(131L, "key_metadata"),
    optional(132L, "split_offsets"),
    optional(TagsFieldId, "tags"))

  /** Iceberg V4 field specs for the scalar fields of the nested [[Tracking]] struct. */
  private val trackingFields: Seq[AMTFieldSpec] = Seq(
    required(0L, "status"),
    optional(1L, "snapshot_id"),
    optional(3L, "sequence_number"),
    optional(4L, "file_sequence_number"),
    optional(5L, "dv_snapshot_id"),
    optional(142L, "first_row_id"),
    optional(6L, "deleted_positions"),
    optional(7L, "replaced_positions"))

  /** Iceberg V4 field specs for the scalar fields of the nested [[DeletionVector]] struct. */
  private val deletionVectorFields: Seq[AMTFieldSpec] = Seq(
    required(155L, "location"),
    required(144L, "offset"),
    required(145L, "size_in_bytes"),
    required(156L, "cardinality"))

  /** Iceberg V4 field specs for the scalar fields of the nested [[ManifestInfo]] struct. */
  private val manifestInfoFields: Seq[AMTFieldSpec] = Seq(
    required(504L, "added_files_count"),
    required(505L, "existing_files_count"),
    required(506L, "deleted_files_count"),
    required(520L, "replaced_files_count"),
    required(524L, "modified_files_count"),
    required(512L, "added_rows_count"),
    required(513L, "existing_rows_count"),
    required(514L, "deleted_rows_count"),
    required(521L, "replaced_rows_count"),
    required(525L, "modified_rows_count"),
    required(516L, "min_sequence_number"),
    optional(522L, "dv"),
    optional(523L, "dv_cardinality"))

  /** Nested-struct field specs, keyed by the top-level field name that carries the struct. */
  private val nestedStructFields: Map[String, Seq[AMTFieldSpec]] = Map(
    "tracking" -> trackingFields,
    "deletion_vector" -> deletionVectorFields,
    "manifest_info" -> manifestInfoFields)

  /** Iceberg V4 field id for the `split_offsets` list element. */
  private val SPLIT_OFFSETS_ELEMENT_FIELD_ID: Long = 133L

  /** Top-level specs by name, for lookup during stamping. */
  private val topLevelFieldByName: Map[String, AMTFieldSpec] =
    topLevelFields.map(f => f.name -> f).toMap

  /**
   * Test-only Parquet field-id map for an AMT checkpoint schema, keyed by dotted path. Keys are the
   * top-level field name (`content_type`), a nested-struct scalar (`tracking.status`), or a list
   * element (`split_offsets.element`).
   */
  private[amt] val allFieldIdByName: Map[String, Int] = {
    val topLevel = topLevelFields.filterNot(_.name == "partition").map(f => f.name -> f.id.toInt)
    val nested = nestedStructFields.flatMap { case (parent, children) =>
      children.map(f => s"$parent.${f.name}" -> f.id.toInt)
    }
    val nestedContainers = Map(
      "split_offsets.element" -> SPLIT_OFFSETS_ELEMENT_FIELD_ID.toInt,
      "tags.key" -> TAGS_KEY_FIELD_ID.toInt,
      "tags.value" -> TAGS_VALUE_FIELD_ID.toInt)
    (topLevel ++ nested ++ nestedContainers).toMap
  }

  private lazy val staticStampedSchema: StructType = {
    import org.apache.spark.sql.delta.implicits._
    StructType(amtSingleActionEncoder.schema.map(stampFieldSpec))
  }

  /**
   * Applies the Iceberg [[AMTFieldSpec]] to a single top-level [[AMTSingleAction]] field:
   * stamps the field id (and any nested element/map ids) and sets nullability from
   * `spec.required`.
   *
   * `partition` keeps the encoder's data type here; [[persistedSchema]] substitutes the table's
   * typed partition struct, or drops the field, once the partition schema is known.
   */
  private def stampFieldSpec(field: StructField): StructField = {
    val spec = topLevelFieldByName.getOrElse(field.name,
      throw new IllegalStateException(
        s"No Iceberg field spec defined for top-level AMTSingleAction field '${field.name}'."))
    val builder = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, spec.id)
    // Attach nested (list-element / map key-value) ids for container-typed fields.
    // DeltaParquetWriteSupport reads these off the field itself, keyed by the relative path
    // (`<field>.element` for a list, `<field>.key` / `<field>.value` for a map).
    val nestedIds = nestedFieldIds(field.name)
    if (nestedIds.nonEmpty) {
      val nestedBuilder = new MetadataBuilder()
      nestedIds.foreach { case (relativePath, id) => nestedBuilder.putLong(relativePath, id) }
      builder.putMetadata(
        DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY, nestedBuilder.build())
    }
    // Rewrite the field's data type by applying nested-struct field specs (id + nullability).
    val stampedDataType = field.dataType match {
      case struct: StructType =>
        nestedStructFields.get(field.name) match {
          case Some(specs) => stampNestedStructFieldSpecs(field.name, struct, specs)
          case None =>
            assert(field.name == "content_stats",
              s"Unexpected struct-typed AMTSingleAction field '${field.name}': expected a " +
                "nested-id struct or content_stats.")
            struct
        }
      case other =>
        assert(!nestedStructFields.contains(field.name),
          s"Field '${field.name}' has a nested-id spec but is not struct-typed " +
            s"(${other.typeName}).")
        other
    }
    // Stamp the field id and set nullability from the spec (even when it matches the encoder),
    // so every id-stamped field's required/optional is driven by the spec.
    field.copy(
      dataType = stampedDataType,
      nullable = !spec.required,
      metadata = builder.build())
  }

  /**
   * Nested (list-element / map key-value) field ids for a top-level container field.
   */
  private def nestedFieldIds(fieldName: String): Seq[(String, Long)] = fieldName match {
    case "split_offsets" =>
      Seq(
        s"$fieldName.${DeltaColumnMapping.PARQUET_LIST_ELEMENT_FIELD_NAME}" ->
          SPLIT_OFFSETS_ELEMENT_FIELD_ID)
    case "tags" =>
      Seq(
        s"$fieldName.${DeltaColumnMapping.PARQUET_MAP_KEY_FIELD_NAME}" -> TAGS_KEY_FIELD_ID,
        s"$fieldName.${DeltaColumnMapping.PARQUET_MAP_VALUE_FIELD_NAME}" -> TAGS_VALUE_FIELD_ID)
    case _ => Seq.empty
  }

  /**
   * Applies each [[AMTFieldSpec]] to the matching direct child of `struct` by name: stamps
   * `parquet.field.id` and sets nullability from `spec.required`. Non-recursive. Fails if the
   * encoder exposes a child with no matching spec (same contract as top-level [[stampFieldSpec]]).
   */
  private def stampNestedStructFieldSpecs(
      parentName: String, struct: StructType, specs: Seq[AMTFieldSpec]): StructType = {
    val specByName = specs.map(f => f.name -> f).toMap
    StructType(struct.map { f =>
      val spec = specByName.getOrElse(f.name,
        throw new IllegalStateException(
          s"No Iceberg field spec defined for nested AMT field '$parentName.${f.name}'."))
      // Stamp the field id and set nullability from the spec (even when it matches the
      // encoder), so every id-stamped field's required/optional is driven by the spec.
      f.copy(
        nullable = !spec.required,
        metadata = new MetadataBuilder()
          .withMetadata(f.metadata)
          .putLong(ParquetUtils.FIELD_ID_METADATA_KEY, spec.id)
          .build())
    })
  }

  /**
   * The [[AMTSingleAction]] schema as written to disk, which differs from the encoder's in-memory
   * schema: every mapped field carries its Iceberg field id, required/optional is driven by the
   * field spec rather than the encoder, and the two per-table fields take their table-dependent
   * shape -- `partition` holds the typed partition struct (absent entirely for an unpartitioned
   * table), and `content_stats` holds the typed per-column statistics struct.
   */
  def persistedSchema(metadata: Metadata, protocol: Protocol): StructType =
    StructType(staticStampedSchema.flatMap {
      case field if field.name == "partition" =>
        if (metadata.partitionSchema.isEmpty) None
        else Some(field.copy(
          dataType = AMTPartitionValues.persistedSchema(metadata.partitionSchema)))
      case field if field.name == "content_stats" =>
        val contentStatsSchema = AMTContentStats.persistedSchema(metadata, protocol)
        if (contentStatsSchema.isEmpty) None
        else Some(field.copy(dataType = contentStatsSchema))
      case field => Some(field)
    })
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
 * @param tags Delta [[AddFile]] tags, preserved verbatim across the manifest round trip.
 * @param format_version Iceberg writer format version; 4 for V4.
 */
case class DataEntry(
    location: String,
    file_format: String,
    tracking: Tracking,
    record_count: Long,
    file_size_in_bytes: Long,
    partition: Option[Map[String, String]] = None,
    deletion_vector: Option[DeletionVector] = None,
    spec_id: Option[Int] = None,
    sort_order_id: Option[Int] = None,
    content_stats: Option[String] = None,
    key_metadata: Option[Array[Byte]] = None,
    split_offsets: Option[Seq[Long]] = None,
    tags: Option[Map[String, String]] = None,
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
    tags = tags)

  def toAddFile(tableRoot: Path): AddFile = {
    val dv = deletion_vector.map(DeletionVector.toDescriptor(_, tableRoot)).orNull
    val stats = AMTContentStats.toStatsJson(content_stats, record_count)
    AddFile(
      path = location,
      partitionValues = partition.getOrElse(Map.empty),
      size = file_size_in_bytes,
      modificationTime = 0L,
      dataChange = false,
      stats = stats,
      deletionVector = dv,
      baseRowId = tracking.first_row_id,
      defaultRowCommitVersion = tracking.sequence_number,
      tags = tags.orNull,
      amtPassthrough = AMTPassthrough.fromDataEntry(this))
  }
}

object DataEntry {
  /** Creates [[DataEntry]] from AddFile. */
  def fromAddFile(add: AddFile, tracking: Tracking, tableRoot: Path): DataEntry = {
    val passthrough = add.amtPassthrough
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
      partition = Option(add.partitionValues).filter(_.nonEmpty),
      deletion_vector =
        Option(add.deletionVector).map(DeletionVector.fromDescriptor(_, tableRoot)),
      spec_id = passthrough.flatMap(_.spec_id),
      sort_order_id = passthrough.flatMap(_.sort_order_id),
      key_metadata = passthrough.flatMap(_.key_metadata),
      split_offsets = passthrough.flatMap(_.split_offsets),
      tags = Option(add.tags).map(_.toMap).filter(_.nonEmpty),
      content_stats = AMTContentStats.fromStatsJson(add.stats))
  }
}

/**
 * Deserializes split_offsets from commit-log JSON as a Seq[Long].
 */
private[amt] class AMTSplitOffsetsDeserializer extends JsonDeserializer[Seq[Long]] {
  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Seq[Long] =
    p.readValueAs(classOf[Array[Long]]).toSeq
}

/**
 * The AMT-native fields of a [[DataEntry]] that should be carried by [[AddFile]].
 */
case class AMTPassthrough(
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    spec_id: Option[Int] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    sort_order_id: Option[Int] = None,
    key_metadata: Option[Array[Byte]] = None,
    @JsonDeserialize(contentUsing = classOf[AMTSplitOffsetsDeserializer])
    split_offsets: Option[Seq[Long]] = None) {

  // override equals and hashCode to compare by content for some fields.
  override def equals(obj: Any): Boolean = obj match {
    case that: AMTPassthrough =>
      spec_id == that.spec_id &&
        sort_order_id == that.sort_order_id &&
        split_offsets == that.split_offsets &&
        ((key_metadata, that.key_metadata) match {
          case (Some(a), Some(b)) => java.util.Arrays.equals(a, b)
          case (None, None) => true
          case _ => false
        })
    case _ => false
  }

  override def hashCode(): Int = {
    val keyMetadataHash = key_metadata.map(b => java.util.Arrays.hashCode(b)).getOrElse(0)
    (spec_id, sort_order_id, keyMetadataHash, split_offsets).hashCode()
  }
}

object AMTPassthrough {
  /** Name of the `amtPassthrough` field on the AddFile schema. */
  final val FIELD_NAME: String = "amtPassthrough"

  /** The struct type of the `amtPassthrough` field on the AddFile schema. */
  final lazy val STRUCT_TYPE: StructType =
    Action.addFileSchema(FIELD_NAME).dataType.asInstanceOf[StructType]

  /**
   * Positions of the `amtPassthrough` struct within an [[InternalRow]].
   */
  case class RowIndices(
      structIndex: Int,
      numFields: Int,
      specId: Int,
      sortOrderId: Int,
      keyMetadata: Int,
      splitOffsets: Int)

  object RowIndices {
    /**
     * Resolve the positions against `schema`, the schema of an `add`-shaped row, or `None` when
     * `schema` does not project `amtPassthrough` at all.
     */
    def resolve(schema: StructType): Option[RowIndices] =
      schema.getFieldIndex(FIELD_NAME).map { structIndex =>
        val passthroughSchema = schema(FIELD_NAME).dataType.asInstanceOf[StructType]
        RowIndices(
          structIndex = structIndex,
          numFields = passthroughSchema.fields.length,
          specId = passthroughSchema.fieldIndex("spec_id"),
          sortOrderId = passthroughSchema.fieldIndex("sort_order_id"),
          keyMetadata = passthroughSchema.fieldIndex("key_metadata"),
          splitOffsets = passthroughSchema.fieldIndex("split_offsets"))
      }
  }

  /**
   * Read the [[AMTPassthrough]] out of `row` at the pre-resolved `indices`, or `None` when the row
   * carries no AMT-native fields.
   */
  def fromRow(row: InternalRow, indices: RowIndices): Option[AMTPassthrough] = {
    if (row.isNullAt(indices.structIndex)) {
      None
    } else {
      val struct = row.getStruct(indices.structIndex, indices.numFields)
      Some(AMTPassthrough(
        spec_id =
          if (struct.isNullAt(indices.specId)) None else Some(struct.getInt(indices.specId)),
        sort_order_id =
          if (struct.isNullAt(indices.sortOrderId)) None
          else Some(struct.getInt(indices.sortOrderId)),
        key_metadata =
          if (struct.isNullAt(indices.keyMetadata)) None
          else Some(struct.getBinary(indices.keyMetadata)),
        split_offsets =
          if (struct.isNullAt(indices.splitOffsets)) None
          else Some(struct.getArray(indices.splitOffsets).toLongArray().toSeq)))
    }
  }

  /**
   * Build the [[AMTPassthrough]] carried on a Delta [[AddFile]] from `entry`, or `None` when the
   * entry has no AMT-native fields to carry.
   */
  def fromDataEntry(entry: DataEntry): Option[AMTPassthrough] = {
    require(
      entry.file_format == AMTSingleAction.FileFormatParquet &&
        entry.format_version == AMTSingleAction.FormatVersionV4,
      s"amtPassthrough only supports parquet/v4. got " +
        s"${entry.file_format}/${entry.format_version}.")
    val passthrough = AMTPassthrough(
      spec_id = entry.spec_id,
      sort_order_id = entry.sort_order_id,
      key_metadata = entry.key_metadata,
      split_offsets = entry.split_offsets)
    if (passthrough == AMTPassthrough()) None else Some(passthrough)
  }
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
 * @param format_version Iceberg writer format version; 4 for V4.
 */
case class DataManifestEntry(
    location: String,
    file_format: String,
    tracking: Tracking,
    record_count: Long,
    file_size_in_bytes: Long,
    manifest_info: ManifestInfo,
    partition: Option[Map[String, String]] = None,
    spec_id: Option[Int] = None,
    content_stats: Option[String] = None,
    key_metadata: Option[Array[Byte]] = None,
    split_offsets: Option[Seq[Long]] = None,
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
    tags = None)

  /** Absolute [[Path]] to the referenced leaf manifest, resolving `location` against the root. */
  @JsonIgnore
  def getAbsolutePath(tableRoot: Path): Path =
    AMTUtils.absolutePathForManifestFile(tableRoot, location)

  /** The leaf manifest as a Hadoop [[FileStatus]] carrying its path and size. */
  @JsonIgnore
  def toFileStatus(tableRoot: Path): FileStatus = {
    new FileStatus(
      /* length = */ file_size_in_bytes,
      /* isdir = */ false,
      /* block_replication = */ 0,
      /* blocksize = */ 1L,
      // modificationTime is not tracked on the manifest entry, so report 0.
      /* modification_time = */ 0L,
      getAbsolutePath(tableRoot))
  }

  /**
   * The inline manifest deletion vector on this leaf, if any, as (bitmap bytes, cardinality).
   * Per the V4 spec, `dv` and `dv_cardinality` must both be set or both unset; a partially
   * populated pair is malformed and rejected.
   */
  @JsonIgnore
  def manifestDV: Option[(Array[Byte], Long)] =
    (manifest_info.dv, manifest_info.dv_cardinality) match {
      case (Some(dvBytes), Some(cardinality)) => Some((dvBytes, cardinality))
      case (None, None) => None
      case _ =>
        throw new IllegalStateException(
          s"Malformed manifest DV on leaf $location: dv and dv_cardinality must both be set or " +
            s"both unset (dv.isDefined=${manifest_info.dv.isDefined}, " +
            s"dv_cardinality=${manifest_info.dv_cardinality}).")
    }
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

  /**
   * Rebuilds the Delta [[DeletionVectorDescriptor]] from the AMT sub-struct.
   */
  def toDescriptor(dv: DeletionVector, tableRoot: Path): DeletionVectorDescriptor = {
    val rawSize = dv.size_in_bytes.toInt -
      DeletionVectorStore.getTotalSizeOfDVFieldsInFile(0)
    // AMT stored paths are unencoded.
    val absolutePath = DeletionVectorStore.unescapedStringToPath(dv.location)
    require(absolutePath.isAbsolute)
    val relativePath = AMTUtils.relativizeLocation(tableRoot.toString, absolutePath.toString)
    if (AMTUtils.isAbsoluteLocation(relativePath)) {
      DeletionVectorDescriptor.onDiskWithAbsolutePath(
        path = DeletionVectorStore.pathToEscapedString(absolutePath),
        sizeInBytes = rawSize,
        cardinality = dv.cardinality,
        offset = Some(dv.offset.toInt))
    } else {
      DeletionVectorDescriptor.createRelativePathDVDescriptor(
        relativePath = relativePath,
        sizeInBytes = rawSize,
        cardinality = dv.cardinality,
        offset = Some(dv.offset.toInt))
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
 * @param modified_files_count Count of MODIFIED file entries.
 * @param added_rows_count Rows across ADDED files.
 * @param existing_rows_count Rows across EXISTING files.
 * @param deleted_rows_count Rows across DELETED files.
 * @param replaced_rows_count Rows across REPLACED files.
 * @param modified_rows_count Rows across MODIFIED files.
 * @param min_sequence_number Minimum data sequence number across the manifest's entries.
 * @param dv Inline manifest deletion-vector bitmap over leaf row positions.
 * @param dv_cardinality Number of positions the inline manifest DV marks.
 */
case class ManifestInfo(
    added_files_count: Int,           // ID: 504, required.
    existing_files_count: Int,        // ID: 505, required.
    deleted_files_count: Int,         // ID: 506, required.
    replaced_files_count: Int,        // ID: 520, required.
    modified_files_count: Int,        // ID: 524, required.
    added_rows_count: Long,           // ID: 512, required.
    existing_rows_count: Long,        // ID: 513, required.
    deleted_rows_count: Long,         // ID: 514, required.
    replaced_rows_count: Long,        // ID: 521, required.
    modified_rows_count: Long,        // ID: 525, required.
    min_sequence_number: Long,        // ID: 516, required.
    dv: Option[Array[Byte]],          // ID: 522, optional (inline manifest DV bitmap).
    dv_cardinality: Option[Long]) {   // ID: 523, optional.

  /** Live (non-tombstone) file entries: ADDED, EXISTING and MODIFIED are all live. */
  def liveFilesCount: Int = added_files_count + existing_files_count + modified_files_count

  /** Tombstone file entries: DELETED and REPLACED. */
  def tombstoneFilesCount: Int = deleted_files_count + replaced_files_count
}
