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
import org.apache.spark.sql.delta.metering.DeltaLogging
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
 * Only the statistics Delta actually collects get a slot: `null_value_count`, `lower_bound` and
 * `upper_bound`. The remaining spec-optional statistics (`value_count`, `nan_value_count`,
 * `total_bytes`) are omitted.
 *
 * Stats tightness is file-level. Iceberg's `tight_stats` (field 151, a sibling of `content_stats`
 * on the entry) holds Delta's `tightStats`, and is null if the file's stats lack it. An entry
 * without stats, such as a manifest pointer, keeps the `tight_stats` it was built with, `false`.
 * Delta's legacy `tightBounds` has no Iceberg counterpart and is dropped, so a file without
 * `tightStats` reads back with `tightBounds = false`. That is never wrong, since an absent
 * `tightBounds` would mean tight.
 * TODO(v4amt): keep `tightBounds` in a Delta-private field of the entry, so that a file without
 * `tightStats` keeps its tightness through AMT.
 */
private[amt] object AMTContentStats extends DeltaLogging {

  /** Names of the [[AMTSingleAction]] fields this object shapes. */
  private val CONTENT_STATS_FIELD = "content_stats"
  private val TIGHT_STATS_FIELD = "tight_stats"

  /** Temporary column [[forWrite]] parses an entry's stats JSON into; dropped before it returns. */
  private val PARSED_STATS_COLUMN = "__amt_parsed_stats"

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
  private val NULL_VALUE_COUNT_ID_OFFSET: Long = 4L

  /** Names of the per-column statistics, as defined by the Iceberg V4 spec. */
  private val LOWER_BOUND = "lower_bound"
  private val UPPER_BOUND = "upper_bound"
  private val NULL_VALUE_COUNT = "null_value_count"

  /**
   * One column Delta collects statistics for:
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
   * or map, say, or a type with no Iceberg bound representation). Such a column still gets a
   * `null_value_count`; its bound fields are simply omitted.
   */
  private case class StatsColumn(
      fieldId: Long,
      name: String,
      path: Seq[String],
      boundType: Option[DataType]) {
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
      override def outputUsesPhysicalNames: Boolean = false
      override val statsColumnSpec =
        StatisticsCollection.configuredDeltaStatsColumnSpec(tableMetadata)
      override def columnMappingMode: DeltaColumnMappingMode = tableMetadata.columnMappingMode
      override protected def protocol: Protocol = tableProtocol
      override protected def getDataSkippingStringPrefixLength: Int =
        StatsCollectionUtils.getDataSkippingStringPrefixLength(sparkSession, tableMetadata)
    }
  }

  /**
   * The columns that each get a `content_stats` entry, taken from `statCollectionPhysicalSchema`
   * and ordered by field id so the persisted schema is stable across writes. That schema is Delta's
   * set of columns it collects per-file statistics for.
   * This method recurses into structs (a struct column contributes its non-struct fields, not
   * itself) and sets `boundType` only for types with an Iceberg bound; a column without bounds (an
   * array, say) gets a `null_value_count` only.
   *
   * Example `statCollectionPhysicalSchema`:
   *   name     type                          {id, physical name}
   *   d_long   long                          {1, col-1}
   *   d_str    string                        {2, col-2}
   *   d_nested struct<inner: int {4, col-4}> {3, col-3}
   *   d_arr    array<string>                 {5, col-5}
   *
   * The physical names form the path into a parsed Delta stats struct; the logical names name the
   * sub-structs. This yields (d_nested contributes only its non-struct field `inner`, not itself):
   *   StatsColumn(1, "d_long",         ["col-1"],         Some(LongType))
   *   StatsColumn(2, "d_str",          ["col-2"],         Some(StringType))
   *   StatsColumn(4, "d_nested_inner", ["col-3","col-4"], Some(IntegerType))  // nested field
   *   StatsColumn(5, "d_arr",          ["col-5"],         None)               // array: null count
   *
   * `name` is the field's full logical name. It names the sub-struct (informational, per the spec)
   * while the field id is what a reader resolves by; using the full path rather than the bare field
   * name reduces name clashes when two structs share a field name.
   */
  private def statsColumns(statCollectionPhysicalSchema: StructType): Seq[StatsColumn] = {
    def collect(schema: StructType, prefix: Seq[String], names: Seq[String]): Seq[StatsColumn] =
      schema.fields.toSeq.flatMap { field =>
        val path = prefix :+ DeltaColumnMapping.getPhysicalName(field)
        val namePath = names :+ field.name
        field.dataType match {
          case nested: StructType => collect(nested, path, namePath)
          case dt =>
            val boundType = if (isSupportedBoundType(dt)) Some(dt) else None
            Some(StatsColumn(
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
   *
   * Timestamps are deliberately excluded, unlike in that converter. Delta serializes timestamp
   * stats at millisecond precision, so its max can sit below the file's true max (Delta's data
   * skipping compensates by widening the max by 1ms on read), and persisting it as-is would give
   * Iceberg an `upper_bound` smaller than values in the file. Until the bound is converted
   * soundly, a timestamp column carries only its null count.
   */
  private def isSupportedBoundType(dataType: DataType): Boolean =
    SkippingEligibleDataType(dataType) && (dataType match {
      case _: StringType | _: IntegerType | _: FloatType | _: DoubleType | _: DecimalType |
          _: BooleanType | _: DateType | _: LongType | _: ByteType | _: ShortType => true
      case _: TimestampType | _: TimestampNTZType => false
      case _ => false
    })

  /**
   * The persisted `content_stats` struct for a table: one sub-struct per statistics-collected
   * column, named by that column's logical name. Empty when the table collects no per-column stats,
   * in which case `AMTSingleAction.persistedSchema` drops the whole `content_stats` field (a struct
   * with no fields is not Parquet-writable) -- the same way it drops `partition` when the table is
   * unpartitioned.
   */
  def persistedSchema(metadata: Metadata, protocol: Protocol): StructType = {
    val physicalSchema =
      statisticsCollection(SparkSession.active, metadata, protocol).statCollectionPhysicalSchema
    val perColumn = statsColumns(physicalSchema).map { column =>
      val baseId = STATS_ID_START + STATS_IDS_PER_FIELD * column.fieldId
      // A column with no bounds gets only its null count; omitting the bound fields keeps them out
      // of the Parquet schema rather than persisting columns that are always null. Fields are
      // ordered by ascending id (bounds 1-2, then null count 4).
      val boundFields = column.boundType.toSeq.flatMap { boundType =>
        Seq(
          statField(LOWER_BOUND, boundType, baseId + LOWER_BOUND_ID_OFFSET),
          statField(UPPER_BOUND, boundType, baseId + UPPER_BOUND_ID_OFFSET))
      }
      val nullCountField =
        statField(NULL_VALUE_COUNT, LongType, baseId + NULL_VALUE_COUNT_ID_OFFSET)
      // The sub-struct is named by the column's logical name with its field id attached (see
      // StatsColumn.fieldName) and stamped with the base id. Per the spec, stats are resolved by
      // id; the name is informational, but the id suffix keeps it unique for Spark's name-based
      // access.
      statField(column.fieldName, StructType(boundFields :+ nullCountField), baseId)
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
   * Delta's stats JSON string -> the typed `content_stats` struct, and the file's `tightStats` ->
   * the entry's `tight_stats`. The file's `tightBounds` is dropped; see [[AMTContentStats]]. On a
   * table that collects no per-column stats, the entry keeps the `tight_stats` it was built with,
   * `false`: with no bounds or null counts, there is nothing for the flag to describe.
   *
   * `df` is a DataFrame of [[AMTSingleAction]] rows whose `content_stats` column still holds the
   * encoder's shape (the raw Delta stats JSON). An entry with no statistics gets a null struct.
   */
  def forWrite(df: DataFrame, metadata: Metadata, protocol: Protocol): DataFrame = {
    val stats = statisticsCollection(df.sparkSession, metadata, protocol)
    val statsSchema = stats.statsSchema // Delta's stats_parsed schema
    val columns = statsColumns(stats.statCollectionPhysicalSchema)
    // `content_stats` holds the raw Delta stats JSON string (the `Option[String]` encoder shape).
    val rawJson = col(CONTENT_STATS_FIELD)
    // No per-column stats -> `content_stats` is dropped from the persisted schema (an empty struct
    // is not Parquet-writable), so drop the column here too. Mirrors `AMTPartitionValues.forWrite`
    // for an unpartitioned table.
    if (columns.isEmpty) return df.drop(CONTENT_STATS_FIELD)
    // Parse stats once in a separate projection so all statistics reuse the result.
    val withParsedStats = df.withColumn(PARSED_STATS_COLUMN, from_json(rawJson, statsSchema))
    val statsParsed = col(PARSED_STATS_COLUMN)

    val perColumn = columns.map { column =>
      // Ordered by ascending id (bounds, then null count) to match the persisted schema.
      val boundColumns = column.boundType.toSeq.flatMap { _ =>
        Seq(
          statPath(statsParsed, DeltaStatistics.MIN, column.path).as(LOWER_BOUND),
          statPath(statsParsed, DeltaStatistics.MAX, column.path).as(UPPER_BOUND))
      }
      val nullValueCount = statPath(statsParsed, DeltaStatistics.NULL_COUNT, column.path)
        .cast(LongType).as(NULL_VALUE_COUNT)
      struct(boundColumns :+ nullValueCount: _*).as(column.fieldName)
    }

    val contentStatsSchema = AMTContentStats.persistedSchema(metadata, protocol)
    val contentStats = when(rawJson.isNull, lit(null).cast(contentStatsSchema))
      .otherwise(struct(perColumn: _*))
    val fileTightStats =
      if (statsSchema.fieldNames.contains(DeltaStatistics.TIGHT_STATS)) {
        statsParsed.getField(DeltaStatistics.TIGHT_STATS)
      } else {
        // Stats without `tightStats`, such as those of preview AMT tables, keep the `tight_stats`
        // the entry was built with, as before `tightStats`.
        col(TIGHT_STATS_FIELD)
      }
    val tightStats = when(rawJson.isNull, col(TIGHT_STATS_FIELD)).otherwise(fileTightStats)
    // `tightStats` reads the raw stats JSON in `content_stats`, so set it before replacing them.
    val withTightStats = withParsedStats.withColumn(TIGHT_STATS_FIELD, tightStats)
    replaceContentStats(withTightStats, contentStats).drop(PARSED_STATS_COLUMN)
  }

  /**
   * The typed `content_stats` struct -> the raw Delta stats JSON string the `Option[String]` field
   * holds.
   *
   * `numRecords` is not persisted in `content_stats` (the entry carries it as Iceberg field 103
   * `record_count`); for an entry that has per-column stats it is folded back in here, and so is
   * the file's `tightStats` (see [[AMTContentStats]]). An entry with no per-column stats (a table
   * that collects none, a data file written without stats, or a manifest pointer) reconstructs to
   * `null`; [[toStatsJson]] supplies its record count and `tightBounds`.
   */
  def forRead(df: DataFrame, metadata: Metadata, protocol: Protocol): DataFrame = {
    val jsonStats = statsJson(df.sparkSession, metadata, protocol)
    if (df.columns.contains(CONTENT_STATS_FIELD)) {
      replaceContentStats(df, jsonStats)
    } else {
      df.withColumn(CONTENT_STATS_FIELD, jsonStats)
    }
  }

  /** The `AddFile.stats` JSON of a persisted data entry row, as [[toStatsJson]] derives it. */
  def addFileStatsJson(spark: SparkSession, metadata: Metadata, protocol: Protocol): Column =
    coalesce(
      statsJson(spark, metadata, protocol),
      to_json(struct(numRecords, tightBounds)))

  private def numRecords: Column =
    col("record_count").cast(LongType).as(DeltaStatistics.NUM_RECORDS)

  /** The persisted `content_stats` of an entry row as the JSON that [[forRead]] decodes it to. */
  private def statsJson(spark: SparkSession, metadata: Metadata, protocol: Protocol): Column = {
    val stats = statisticsCollection(spark, metadata, protocol)
    if (statsColumns(stats.statCollectionPhysicalSchema).isEmpty) {
      // No per-column stats were persisted (`content_stats` was dropped from the schema); the
      // encoder decodes `None` and `toStatsJson` supplies the record count and `tightBounds`.
      lit(null).cast(StringType)
    } else {
      // Delta's stats schema of the manifests' own metadata, so that every persisted statistic is
      // matched; `to_json` leaves out the null ones, as Delta writes no stats it does not have.
      val encoded = to_json(parsedStats(stats, stats.statsSchema, omitMissing = true))
      when(col(CONTENT_STATS_FIELD).isNull, lit(null).cast(StringType)).otherwise(encoded)
    }
  }

  /**
   * The persisted stats of an entry row as Delta's parsed stats struct of type `statsSchema`, built
   * out of plain field accesses into the typed `content_stats` struct.
   *
   * Statistics are matched by their physical path, so `statsSchema` may come from newer metadata
   * than `stats` describes: statistics `content_stats` lacks, or that AMT does not persist (e.g.
   * collation statistics), are null. A matched statistic of another type is cast: with column
   * mapping in `id` mode, a column keeps its physical path only through a type widening, and Delta
   * only allows widenings that up-cast. `numRecords` is the entry's `record_count` and
   * `tightBounds` and `tightStats` come from `tight_stats` as the comment in the method describes.
   * `numRecords` is set even when the entry has no `content_stats`.
   *
   * @param stats       the stats the manifests were written with
   * @param omitMissing whether a struct of statistics none of which `content_stats` persists, e.g.
   *                    the `minValues` of columns AMT keeps no bounds for, is null rather than a
   *                    struct of nulls, so that `to_json` leaves it out
   */
  private def parsedStats(
      stats: StatisticsCollection,
      statsSchema: StructType,
      omitMissing: Boolean): Column = {
    val columns = statsColumns(stats.statCollectionPhysicalSchema)
    // With no per-column stats, `content_stats` is dropped from the persisted schema.
    val typedStatsOpt = if (columns.isEmpty) None else Some(col(CONTENT_STATS_FIELD))
    val columnByPath = columns.map(column => column.path -> column).toMap
    // AMT drops `tightBounds` (see [[AMTContentStats]]), so on a table whose stats have
    // `tightStats`, a file without `tightStats` gets `tightBounds = false`, and a file with it gets
    // none. An entry without `content_stats` reads back with `tightBounds` alone, as
    // [[toStatsJson]] and [[addFileStatsJson]] spell it: it has no bounds for either flag to
    // describe. On other tables, such as preview AMT tables, `tightBounds` is `tight_stats`.
    val hasContentStats = typedStatsOpt.map(_.isNotNull).getOrElse(lit(false))
    val fileTightBounds = if (statsSchema.fieldNames.contains(DeltaStatistics.TIGHT_STATS)) {
      when(hasContentStats, when(col(TIGHT_STATS_FIELD).isNull, lit(false)))
        .otherwise(tightStatsAsTightBounds)
    } else {
      tightStatsAsTightBounds
    }
    val fileTightStats = when(hasContentStats, col(TIGHT_STATS_FIELD))

    // The persisted `statName` of the column at `path` as a `dataType`, or None if AMT keeps none
    // for it. Each column's stats are a sub-struct of `content_stats` keyed by its `fieldName`.
    def persistedStat(statName: String, path: Seq[String], dataType: DataType): Option[Column] =
      for {
        typedStats <- typedStatsOpt
        column <- columnByPath.get(path)
        statType <- if (statName == NULL_VALUE_COUNT) Some(LongType) else column.boundType
      } yield {
        val stat = typedStats.getField(column.fieldName).getField(statName)
        if (statType == dataType) stat else stat.cast(dataType)
      }

    // The `statName` of every column of `schema`, nested like `schema`, or None if `omitMissing`
    // and no column has a persisted value.
    def perColumnStats(statName: String, schema: StructType, prefix: Seq[String])
        : Option[Column] = {
      val values = schema.fields.toIndexedSeq.map { field =>
        val path = prefix :+ field.name
        field -> (field.dataType match {
          case nested: StructType if !columnByPath.contains(path) =>
            perColumnStats(statName, nested, path)
          case dataType => persistedStat(statName, path, dataType)
        })
      }
      if (omitMissing && values.forall { case (_, value) => value.isEmpty }) {
        None
      } else {
        Some(struct(values.map { case (field, value) =>
          value.getOrElse(lit(null).cast(field.dataType)).as(field.name)
        }: _*))
      }
    }

    struct(statsSchema.fields.toIndexedSeq.map { field =>
      val value = (field.name, field.dataType) match {
        case (DeltaStatistics.NUM_RECORDS, dataType) => col("record_count").cast(dataType)
        case (DeltaStatistics.MIN, schema: StructType) =>
          perColumnStats(LOWER_BOUND, schema, Nil).getOrElse(lit(null).cast(schema))
        case (DeltaStatistics.MAX, schema: StructType) =>
          perColumnStats(UPPER_BOUND, schema, Nil).getOrElse(lit(null).cast(schema))
        case (DeltaStatistics.NULL_COUNT, schema: StructType) =>
          perColumnStats(NULL_VALUE_COUNT, schema, Nil).getOrElse(lit(null).cast(schema))
        case (DeltaStatistics.TIGHT_BOUNDS, dataType) => fileTightBounds.cast(dataType)
        case (DeltaStatistics.TIGHT_STATS, dataType) => fileTightStats.cast(dataType)
        case (name, dataType) =>
          // Fails tests, so that a statistic Delta adds gets mapped here; null in production.
          deltaAssert(
            check = false,
            name = "amt.unmappedStatistic",
            msg = "The Delta statistic " + name + " is not mapped to AMT content stats; map it " +
              "in AMTContentStats.parsedStats.")
          lit(null).cast(dataType)
      }
      value.as(field.name)
    }: _*)
  }

  /**
   * Delta's `tightBounds` is the entry's `tight_stats` as-is. The Delta spec treats an absent
   * `tightBounds` as tight, whereas Iceberg treats an absent `tight_stats` as not tight, so an
   * absent flag reads back as `false`.
   */
  private def tightBounds: Column = tightStatsAsTightBounds.as(DeltaStatistics.TIGHT_BOUNDS)

  /** [[tightBounds]] without its column name, to use inside an expression. */
  private def tightStatsAsTightBounds: Column = coalesce(col(TIGHT_STATS_FIELD), lit(false))

  /**
   * Projects one statistic of one column out of a parsed Delta stats struct (used on write),
   * following the column's nested physical-name path.
   *
   * Example: statPath(parsed, "minValues", ["col-3","col-4"]) => parsed.minValues.col-3.col-4.
   */
  private def statPath(parsed: Column, statName: String, path: Seq[String]): Column =
    path.foldLeft(parsed.getField(statName))((column, name) => column.getField(name))

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
   * stats JSON (including `numRecords` and `tightBounds`), so it is returned as-is; the fallback
   * covers an entry with no `content_stats` by emitting the record count and `tightBounds` alone.
   * `tightBounds` is spelled out from `tightStats` as [[forRead]] does -- an absent flag as `false`
   * -- since Delta would read an absent `tightBounds` as tight. The inverse of [[fromStatsJson]].
   */
  def toStatsJson(
      contentStats: Option[String],
      recordCount: Long,
      tightStats: Option[Boolean]): String =
    contentStats.getOrElse(
      s"""{"${DeltaStatistics.NUM_RECORDS}":$recordCount,""" +
        s""""${DeltaStatistics.TIGHT_BOUNDS}":${tightStats.getOrElse(false)}}""")
}
