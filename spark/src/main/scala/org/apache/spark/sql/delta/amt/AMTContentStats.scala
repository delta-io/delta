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

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaColumnMappingMode}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.stats.{DeltaStatistics, SkippingEligibleDataType, StatisticsCollection, StatsCollectionUtils}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.functions.{coalesce, col, from_json, lit, struct, to_json, when}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType}

/**
 * Converts between Delta's stats JSON string and Iceberg V4's typed `content_stats` struct
 * (field 146).
 *
 * Iceberg models `content_stats` as a per-table struct-of-structs: one sub-struct per table field,
 * named by that field's logical name (informational), holding the individual statistics as typed
 * values. The sub-struct's Parquet field id is derived from the table field id, and each statistic
 * sits at a fixed offset from it, so a reader resolves stats by id.
 *
 * A table column's Delta column-mapping id is its Iceberg field id, so no id translation is needed:
 * in both `id` and `name` column-mapping mode Delta assigns every column a stable id and stamps it
 * as the Parquet `field_id`. We use that Iceberg table column field id to compute the stats
 * sub-struct's field ids.
 *
 * Only the statistics Delta actually collects get a slot: `null_value_count`, `lower_bound`,
 * `upper_bound` and `tight_bounds`. The remaining spec-optional statistics (`value_count`,
 * `nan_value_count`, `avg_value_size`, `max_value_size`) are omitted.
 */
private[amt] object AMTContentStats {

  /** Name of the [[AMTSingleAction]] field this object shapes. */
  private val CONTENT_STATS_FIELD = "content_stats"

  /**
   * Start of the Iceberg stats field-id space, and the number of ids reserved per table field.
   * A table field's stats sub-struct is stamped `STATS_ID_START + STATS_IDS_PER_FIELD * fieldId`,
   * and each statistic within it sits at a fixed offset from that base. See
   * https://iceberg.apache.org/spec/#content-stats.
   */
  private val STATS_ID_START: Long = 10000L
  private val STATS_IDS_PER_FIELD: Long = 200L

  /**
   * Per-statistic offsets from a sub-struct's base id, as defined by the Iceberg V4 spec
   * (https://iceberg.apache.org/spec/#content-stats).
   */
  private val LOWER_BOUND_ID_OFFSET: Long = 1L
  private val UPPER_BOUND_ID_OFFSET: Long = 2L
  private val TIGHT_BOUNDS_ID_OFFSET: Long = 3L
  private val NULL_VALUE_COUNT_ID_OFFSET: Long = 5L

  /** Names of the per-column statistics, as defined by the Iceberg V4 spec. */
  private val LOWER_BOUND = "lower_bound"
  private val UPPER_BOUND = "upper_bound"
  private val TIGHT_BOUNDS = "tight_bounds"
  private val NULL_VALUE_COUNT = "null_value_count"

  /**
   * One statistics-collected leaf column:
   *  - `fieldId`: the id Delta column mapping assigned to the column. Delta stamps it as the
   *    Parquet `field_id` and Iceberg resolves fields by id, so this one value is both the Delta
   *    column-mapping id and the column's Iceberg field id.
   *  - `name`: the logical column name (the underscore-joined path). Informational only, and not
   *    unique -- a top-level `a_b` and a nested `a`.`b` both flatten to `a_b` -- so it is not used
   *    to resolve the sub-struct; see `fieldName`.
   *  - `path`: the physical-name path to the column inside a parsed Delta stats struct (nested).
   *  - `boundType`: the column's Iceberg bound type, or None.
   *
   * `boundType` is None for a column Delta counts nulls for but collects no min/max on (an array
   * or map, say, or a type with no Iceberg bound representation). Such a leaf still gets a
   * `null_value_count`; its bound fields are simply omitted.
   */
  private case class StatsLeaf(
      fieldId: Long,
      name: String,
      path: Seq[String],
      boundType: Option[DataType]) {
    def hasBounds: Boolean = boundType.isDefined

    /**
     * The sub-struct's field name in the persisted schema: the logical name with the (unique)
     * field id attached. Iceberg resolves content_stats entries by field id and treats the name as
     * informational. Attaching the field id -- unique per column -- keeps every sub-struct name
     * distinct, so name-based resolution stays unambiguous when two logical names collide.
     */
    def fieldName: String = s"${name}_$fieldId"
  }

  /**
   * A [[StatisticsCollection]] describing the stats this table collects.
   */
  private def statisticsCollection(
      sparkSession: SparkSession,
      tableMetadata: Metadata,
      tableProtocol: Protocol): StatisticsCollection = {
    new StatisticsCollection {
      override protected def spark: SparkSession = sparkSession
      // We consume only `statCollectionPhysicalSchema`, which restricts to the configured indexed
      // columns (statsColumnSpec's numIndexedCols / explicit stats columns) applied to these
      // schemas. There is no separate stats or attribute projection to reproduce from just
      // (Metadata, Protocol), so all three are the full data schema; the indexed-column subsetting
      // still happens inside `getIndexedColumns`.
      override def tableSchema: StructType = tableMetadata.dataSchema
      override def outputTableStatsSchema: StructType = tableMetadata.dataSchema
      override def outputAttributeSchema: StructType = tableMetadata.dataSchema
      override val statsColumnSpec =
        StatisticsCollection.configuredDeltaStatsColumnSpec(tableMetadata)
      override def columnMappingMode: DeltaColumnMappingMode = tableMetadata.columnMappingMode
      override protected def protocol: Protocol = tableProtocol
      override protected def getDataSkippingStringPrefixLength: Int =
        StatsCollectionUtils.getDataSkippingStringPrefixLength(sparkSession, tableMetadata)
    }
  }

  /**
   * The statistics-collected leaf columns that each get a `content_stats` entry, taken from
   * `statCollectionPhysicalSchema` and ordered by field id so the persisted schema is stable across
   * writes. That schema is Delta's set of columns it collects per-file statistics for.
   * This method recurses into structs (a struct column contributes its scalar leaves, not itself)
   * and sets `boundType` only for types with an Iceberg bound; a boundless leaf (an array, say)
   * gets a `null_value_count` only.
   *
   * Example `statCollectionPhysicalSchema`:
   *   name     type                          {id, physical name}
   *   d_long   long                          {1, col-1}
   *   d_str    string                        {2, col-2}
   *   d_nested struct<inner: int {4, col-4}> {3, col-3}
   *   d_arr    array<string>                 {5, col-5}
   *
   * The physical names form the path into a parsed Delta stats struct; the logical names name the
   * sub-structs. This yields (d_nested contributes only its scalar leaf `inner`, not itself):
   *   StatsLeaf(1, "d_long",         ["col-1"],         Some(LongType))
   *   StatsLeaf(2, "d_str",          ["col-2"],         Some(StringType))
   *   StatsLeaf(4, "d_nested_inner", ["col-3","col-4"], Some(IntegerType))  // struct leaf
   *   StatsLeaf(5, "d_arr",          ["col-5"],         None)               // array: null count
   *
   * `name` is the field's full logical name. It names the sub-struct (informational, per the spec)
   * while the field id is what a reader resolves by; using the full path rather than the bare leaf
   * name reduces name clashes when two structs share a leaf name.
   */
  private def statsLeafColumns(statCollectionPhysicalSchema: StructType): Seq[StatsLeaf] = {
    def collect(schema: StructType, prefix: Seq[String], names: Seq[String]): Seq[StatsLeaf] =
      schema.fields.toSeq.flatMap { field =>
        val path = prefix :+ DeltaColumnMapping.getPhysicalName(field)
        val namePath = names :+ field.name
        field.dataType match {
          case nested: StructType => collect(nested, path, namePath)
          case dt =>
            val boundType = if (isBoundTypeSupported(dt)) Some(dt) else None
            Some(StatsLeaf(
              fieldId = DeltaColumnMapping.getColumnId(field).toLong,
              name = namePath.mkString("_"),
              path = path,
              boundType = boundType))
        }
      }
    collect(statCollectionPhysicalSchema, Nil, Nil).sortBy(_.fieldId)
  }

  /**
   * Whether a min/max bound of this type can be represented as a typed Iceberg value.
   *
   * Mirrors `IcebergStatsConverter.isMinMaxStatTypeSupported`, the authority for Delta-to-Iceberg
   * bound conversion. It is restated rather than called because that object lives in the
   * `scala-bazel` source tree and pulls in the shaded Iceberg library, neither of which this
   * package can depend on. Types outside this set are left out of `content_stats`, matching that
   * converter's behavior of ignoring unsupported stats.
   */
  private def isBoundTypeSupported(dataType: DataType): Boolean =
    SkippingEligibleDataType(dataType) && (dataType match {
      case _: StringType | _: IntegerType | _: FloatType | _: DoubleType | _: DecimalType |
          _: BooleanType | _: DateType | _: TimestampType | _: TimestampNTZType | _: LongType |
          _: ByteType | _: ShortType => true
      case _ => false
    })

  /**
   * Whether the type's stored bound is truncated. Delta prefix-truncates string bounds and rounds
   * timestamp bounds down to milliseconds, so for those types the stored bound is a valid superset
   * endpoint but not the exact min/max. Iceberg's `tight_bounds` (bounds equal the min/max) must
   * therefore be false for them even when Delta's file-level `tightBounds` is true. That flag
   * describes only the non-truncated columns (everything except String/Timestamp/TimestampNTZ);
   * the truncated columns are never tight regardless of it.
   */
  private def isTruncatedType(dataType: DataType): Boolean = dataType match {
    case _: StringType | _: TimestampType | _: TimestampNTZType => true
    case _ => false
  }

  /**
   * The persisted `content_stats` struct for a table: one sub-struct per statistics-collected leaf
   * column, named by that column's logical name. Empty when the table collects no per-column stats,
   * in which case `AMTSingleAction.persistedSchema` drops the whole `content_stats` field (a struct
   * with no fields is not Parquet-writable) -- the same way it drops `partition` when the table is
   * unpartitioned.
   */
  def persistedSchema(metadata: Metadata, protocol: Protocol): StructType = {
    val physicalSchema =
      statisticsCollection(SparkSession.active, metadata, protocol).statCollectionPhysicalSchema
    val perColumn = statsLeafColumns(physicalSchema).map { leaf =>
      val baseId = STATS_ID_START + STATS_IDS_PER_FIELD * leaf.fieldId
      // A leaf with no bounds gets only its null count; omitting the bound fields keeps them out
      // of the Parquet schema rather than persisting columns that are always null. Fields are
      // ordered by ascending id (bounds 1-3, then null count 5).
      val boundFields = leaf.boundType.toSeq.flatMap { boundType =>
        Seq(
          statField(LOWER_BOUND, boundType, baseId + LOWER_BOUND_ID_OFFSET),
          statField(UPPER_BOUND, boundType, baseId + UPPER_BOUND_ID_OFFSET),
          statField(TIGHT_BOUNDS, BooleanType, baseId + TIGHT_BOUNDS_ID_OFFSET))
      }
      val nullCountField =
        statField(NULL_VALUE_COUNT, LongType, baseId + NULL_VALUE_COUNT_ID_OFFSET)
      // The sub-struct is named by the column's logical name with its field id attached (see
      // StatsLeaf.fieldName) and stamped with the base id. Per the spec, stats are resolved by id;
      // the name is informational, but the id suffix keeps it unique for Spark's name-based access.
      statField(leaf.fieldName, StructType(boundFields :+ nullCountField), baseId)
    }
    StructType(perColumn)
  }

  /**
   * Builds one id-stamped field of a per-column stats sub-struct. Every field is nullable: a
   * statistic can be absent for a given file (a column with no min/max, or a null bound), and the
   * Iceberg V4 spec marks these per-column statistics optional.
   */
  private def statField(name: String, dataType: DataType, id: Long): StructField =
    StructField(
      name,
      dataType,
      nullable = true,
      metadata = new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build())

  /**
   * Delta's stats JSON string -> the typed `content_stats` struct.
   *
   * `df` is a DataFrame of [[AMTSingleAction]] rows whose `content_stats` column still holds the
   * encoder's shape (the raw Delta stats JSON). An entry with no statistics gets a null struct.
   */
  def forWrite(df: DataFrame, metadata: Metadata, protocol: Protocol): DataFrame = {
    val stats = statisticsCollection(df.sparkSession, metadata, protocol)
    val statsSchema = stats.statsSchema // Delta's stats_parsed schema
    val leaves = statsLeafColumns(stats.statCollectionPhysicalSchema)
    // No per-column stats -> `content_stats` is dropped from the persisted schema (an empty struct
    // is not Parquet-writable), so drop the column here too. Mirrors `AMTPartitionValues.forWrite`
    // for an unpartitioned table.
    if (leaves.isEmpty) return df.drop(CONTENT_STATS_FIELD)
    // `content_stats` holds the raw Delta stats JSON string (the `Option[String]` encoder shape).
    val rawJson = col(CONTENT_STATS_FIELD)
    val statsParsedExpression = from_json(rawJson, statsSchema)

    // Delta's `tightBounds` is a per-file flag; Iceberg's `tight_bounds` is per column and asserts
    // the bounds equal the min/max. The Delta spec treats an absent `tightBounds` as tight, so
    // the default is true.
    val fileTightBounds =
      coalesce(statsParsedExpression.getField(DeltaStatistics.TIGHT_BOUNDS), lit(true))

    val perColumn = leaves.map { leaf =>
      // Ordered by ascending id (bounds, then null count) to match the persisted schema.
      val boundColumns = leaf.boundType.toSeq.flatMap { boundType =>
        // A column's bounds are exact only when the file is tight AND the type is not truncated, so
        // a truncated column (string/timestamp) is never `tight_bounds = true`.
        val columnTightBounds =
          if (isTruncatedType(boundType)) lit(false) else fileTightBounds
        Seq(
          statPath(statsParsedExpression, DeltaStatistics.MIN, leaf.path).as(LOWER_BOUND),
          statPath(statsParsedExpression, DeltaStatistics.MAX, leaf.path).as(UPPER_BOUND),
          columnTightBounds.as(TIGHT_BOUNDS))
      }
      val nullValueCount = statPath(statsParsedExpression, DeltaStatistics.NULL_COUNT, leaf.path)
        .cast(LongType).as(NULL_VALUE_COUNT)
      struct(boundColumns :+ nullValueCount: _*).as(leaf.fieldName)
    }

    val contentStatsSchema = AMTContentStats.persistedSchema(metadata, protocol)
    val contentStats = when(rawJson.isNull, lit(null).cast(contentStatsSchema))
      .otherwise(struct(perColumn: _*))
    replaceContentStats(df, contentStats)
  }

  /**
   * The typed `content_stats` struct -> the raw Delta stats JSON string the `Option[String]` field
   * holds.
   *
   * `numRecords` is not persisted in `content_stats` (the entry carries it as Iceberg field 103
   * `record_count`); for an entry that has per-column stats it is folded back in here. An entry
   * with no per-column stats (a table that collects none, a data file written without stats, or
   * a manifest pointer) reconstructs to `null`; [[toStatsJson]] supplies its record count.
   */
  def forRead(df: DataFrame, metadata: Metadata, protocol: Protocol): DataFrame = {
    val stats = statisticsCollection(df.sparkSession, metadata, protocol)
    val leaves = statsLeafColumns(stats.statCollectionPhysicalSchema)
    val numRecords = col("record_count").cast(LongType).as(DeltaStatistics.NUM_RECORDS)
    val jsonStats = if (leaves.isEmpty) {
      // No per-column stats were persisted (`content_stats` was dropped from the schema); the
      // encoder decodes `None` and `toStatsJson` supplies the record count.
      lit(null).cast(StringType)
    } else {
      val typedStats = col(CONTENT_STATS_FIELD)

      // Rebuild the nested minValues/maxValues/nullCount shape from each leaf's recorded path, then
      // serialize it back to the JSON string Delta expects. minValues/maxValues cover only the
      // leaves that carry bounds, matching the sub-schemas Delta itself builds.
      val boundLeaves = leaves.filter(_.hasBounds)
      // Recover Delta's file-level `tightBounds`. On write each untruncated column carries
      // `tight_bounds = fileTightBounds`; truncated columns are forced to `false` for Iceberg's
      // exact-bounds semantics. So AND only the untruncated columns; if a file has none, fall back
      // to `false` conservatively.
      val tightBoundLeaves = boundLeaves.filter(_.boundType.exists(bt => !isTruncatedType(bt)))
      val recoveredTightBounds =
        if (tightBoundLeaves.nonEmpty) {
          tightBoundLeaves
            .map(leaf => coalesce(statOf(typedStats, leaf, TIGHT_BOUNDS), lit(false)))
            .reduce(_ && _)
        } else {
          lit(false)
        }
      val statsFields =
        boundLeaves.headOption.toSeq.flatMap { _ =>
          Seq(
            nestByPath(boundLeaves, statOf(typedStats, _, LOWER_BOUND)).as(DeltaStatistics.MIN),
            nestByPath(boundLeaves, statOf(typedStats, _, UPPER_BOUND)).as(DeltaStatistics.MAX))
        } ++ leaves.headOption.toSeq.map { _ =>
          nestByPath(leaves, statOf(typedStats, _, NULL_VALUE_COUNT)).as(DeltaStatistics.NULL_COUNT)
        } :+ recoveredTightBounds.as(DeltaStatistics.TIGHT_BOUNDS)
      val statsJson = to_json(struct(numRecords +: statsFields: _*))
      when(typedStats.isNull, lit(null).cast(StringType)).otherwise(statsJson)
    }
    if (df.columns.contains(CONTENT_STATS_FIELD)) {
      replaceContentStats(df, jsonStats)
    } else {
      df.withColumn(CONTENT_STATS_FIELD, jsonStats)
    }
  }

  /**
   * Projects one statistic of one leaf column out of a parsed Delta stats struct (used on write),
   * following the leaf's nested physical-name path.
   *
   * Example: statPath(parsed, "minValues", ["col-3","col-4"]) => parsed.minValues.col-3.col-4.
   */
  private def statPath(parsed: Column, statName: String, path: Seq[String]): Column =
    path.foldLeft(parsed.getField(statName))((column, name) => column.getField(name))

  /**
   * Reads one statistic out of a leaf's typed, flat `content_stats` sub-struct (used on read) --
   * the inverse shape of [[statPath]]. The sub-struct is keyed by the leaf's unique `fieldName`.
   *
   * Example: statOf(typed, leaf(fieldName="d_nested_inner_4"), "lower_bound") reads the
   * `lower_bound` of the sub-struct field named "d_nested_inner_4".
   */
  private def statOf(typed: Column, leaf: StatsLeaf, statName: String): Column =
    typed.getField(leaf.fieldName).getField(statName)

  /**
   * Rebuilds Delta's nested stats shape from the flat leaves, grouping by each leaf's recorded path
   * so a struct column's per-field statistics land back under that struct.
   *
   * Example -- rebuilding `minValues` from the three bound leaves below, where `value` reads
   * each leaf's `lower_bound` out of the typed `content_stats` sub-struct (e.g. value of leaf id 1
   * = typed.`1`.lower_bound):
   *
   *   StatsLeaf(1, ["col-1"])
   *   StatsLeaf(2, ["col-2"])
   *   StatsLeaf(4, ["col-3","col-4"])
   *
   * Group by the first path segment -> col-1, col-2, col-3. col-1 and col-2 have a length-1 path so
   * they take their value directly; col-3 has no length-1 entry, so it recurses on the remaining
   * path ["col-4"] and builds a sub-struct. The result is:
   *
   *   struct(
   *     "col-1" -> typed.`1`.lower_bound,
   *     "col-2" -> typed.`2`.lower_bound,
   *     "col-3" -> struct("col-4" -> typed.`4`.lower_bound))
   *
   * which `to_json`s to {"col-1":.., "col-2":.., "col-3":{"col-4":..}} -- the physical-keyed,
   * nested shape Delta writes minValues/maxValues/nullCount in, with col-4 back under col-3.
   */
  private def nestByPath(leaves: Seq[StatsLeaf], value: StatsLeaf => Column): Column = {
    def build(entries: Seq[(Seq[String], StatsLeaf)]): Column = {
      val fields = entries
        .groupBy(_._1.head)
        .toSeq
        .sortBy { case (_, group) => group.map(_._2.fieldId).min }
        .map { case (name, group) =>
          group.find(_._1.length == 1) match {
            case Some((_, leaf)) => value(leaf).as(name)
            case None => build(group.map { case (path, leaf) => (path.tail, leaf) }).as(name)
          }
        }
      struct(fields: _*)
    }
    build(leaves.map(leaf => (leaf.path, leaf)))
  }

  private def replaceContentStats(df: DataFrame, contentStats: Column): DataFrame =
    df.select(df.columns.map { name =>
      if (name == CONTENT_STATS_FIELD) contentStats.as(name) else col(name)
    }.toIndexedSeq: _*)

  /**
   * The value stored in the `content_stats` field: the `AddFile.stats` JSON string verbatim, or
   * None when the file has none.
   */
  def fromStatsJson(statsJson: String): Option[String] =
    Option(statsJson).filter(_.nonEmpty)

  /**
   * The `AddFile.stats` JSON for a data entry. [[forRead]] rebuilds `content_stats` as the complete
   * stats JSON (including `numRecords`), so it is returned as-is; the fallback covers an entry with
   * no `content_stats` by emitting the record count alone. The inverse of [[fromStatsJson]].
   */
  def toStatsJson(contentStats: Option[String], recordCount: Long): String =
    contentStats.getOrElse(s"""{"${DeltaStatistics.NUM_RECORDS}":$recordCount}""")
}
