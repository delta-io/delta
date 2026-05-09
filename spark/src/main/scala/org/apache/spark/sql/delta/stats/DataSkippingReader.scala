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

package org.apache.spark.sql.delta.stats

// scalastyle:off import.ordering.noEmptyLine
import java.io.Closeable

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaLog, DeltaTableUtils}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.expressions.DecodeNestedZ85EncodedVariant
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaDataSkippingType.DeltaDataSkippingType
import org.apache.spark.sql.delta.stats.DeltaStatistics._
import org.apache.spark.sql.delta.util.StateCache
import org.apache.spark.sql.util.ScalaExtensions._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.execution.datasources.VariantMetadata
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AtomicType, BooleanType, CalendarIntervalType, DataType, DateType, LongType, NumericType, StringType, StructField, StructType, TimestampNTZType, TimestampType, VariantType}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Used to hold the list of files and scan stats after pruning files using the limit.
 */
case class ScanAfterLimit(
    files: Seq[AddFile],
    byteSize: Option[Long],
    numPhysicalRecords: Option[Long],
    numLogicalRecords: Option[Long])

/**
 * Used in deduplicateAndFilterRemovedLocally/getFilesAndNumRecords iterator for grouping
 * physical and logical number of records.
 *
 * @param numPhysicalRecords The number of records physically present in the file.
 * @param numLogicalRecords The physical number of records minus the Deletion Vector cardinality.
 */
case class NumRecords(numPhysicalRecords: java.lang.Long, numLogicalRecords: java.lang.Long)

/**
 * Represents a stats column (MIN, MAX, etc) for a given (nested) user table column name. Used to
 * keep track of which stats columns a data skipping query depends on.
 *
 * The `pathToStatType` is path to a stats type accepted by `getStatsColumnOpt()`
 *  (see object `DeltaStatistics`);
 * `pathToColumn` is the nested name of the user column whose stats are to be accessed.
 * `columnDataType` is the data type of the column.
 */
private[stats] case class StatsColumn private(
    pathToStatType: Seq[String],
    pathToColumn: Seq[String])

object StatsColumn {
  def apply(statType: String, pathToColumn: Seq[String], columnDataType: DataType): StatsColumn = {
    StatsColumn(Seq(statType), pathToColumn)
  }
}

/**
 * A data skipping predicate, which includes the expression itself, plus the set of stats columns
 * that expression depends on. The latter is required to correctly handle missing stats, which would
 * make the predicate unreliable; for details, see `DataSkippingReader.verifyStatsForFilter`.
 *
 * NOTE: It would be more accurate to call these "file keeping" predicates, because they specify the
 * set of files a query must examine, not the set of rows a query can safely skip.
 */
private [sql] case class DataSkippingPredicate(
    expr: Column,
    referencedStats: Set[StatsColumn]
)

/**
 * Overloads the constructor for `DataSkippingPredicate`, allowing callers to pass referenced stats
 * as individual arguments, rather than wrapped up as a Set.
 *
 * For example, instead of this:
 *
 *   DataSkippingPredicate(pred, Set(stat1, stat2))
 *
 * We can just do:
 *
 *   DataSkippingPredicate(pred, stat1, stat2)
 */
private [sql] object DataSkippingPredicate {
  def apply(filters: Column, referencedStats: StatsColumn*): DataSkippingPredicate = {
    DataSkippingPredicate(filters, referencedStats.toSet)
  }
}

/**
 * An extractor that matches on access of a skipping-eligible column. We only collect stats for leaf
 * columns, so internal columns of nested types are ineligible for skipping.
 *
 * NOTE: This check is sufficient for safe use of NULL_COUNT stats, but safe use of MIN and MAX
 * stats requires additional restrictions on column data type (see SkippingEligibleLiteral).
 *
 * @return The path to the column and the column's data type if it exists and is eligible.
 *         Otherwise, return None.
 */
object SkippingEligibleColumn {
  def unapply(arg: Expression): Option[(Seq[String], DataType)] = {
    // Only atomic types are eligible for skipping, and args should always be resolved by now.
    // When `pushVariantIntoScan` is true, Variants in the read schema are transformed into Structs
    // to facilitate shredded reads. Therefore, filters like `v is not null` where `v` is a variant
    // column look like the filters on struct data. `VariantMetadata.isVariantStruct` helps in
    // distinguishing between "true structs" and "variant structs".
    val eligible = arg.resolved && (arg.dataType.isInstanceOf[AtomicType] ||
      VariantMetadata.isVariantStruct(arg.dataType))
    if (eligible) searchChain(arg).map(_ -> arg.dataType) else None
  }

  private def searchChain(arg: Expression): Option[Seq[String]] = arg match {
    case a: Attribute => Some(a.name :: Nil)
    case GetStructField(child, _, Some(name)) =>
      searchChain(child).map(name +: _)
    case g @ GetStructField(child, ord, None) if g.resolved =>
      searchChain(child).map(g.childSchema(ord).name +: _)
    case _ =>
      None
  }
}

/**
 * An extractor that matches on access of a skipping-eligible Literal. Delta tables track min/max
 * stats for a limited set of data types, and only Literals of those types are skipping-eligible.
 *
 * @return The Literal, if it is eligible. Otherwise, return None.
 */
object SkippingEligibleLiteral {
  def unapply(arg: Literal): Option[Column] = {
    if (SkippingEligibleDataType(arg.dataType)) Some(Column(arg)) else None
  }
}

object SkippingEligibleDataType {
  // Call this directly, e.g. `SkippingEligibleDataType(dataType)`
  def apply(dataType: DataType): Boolean = dataType match {
    case _: NumericType | DateType | TimestampType | TimestampNTZType | StringType => true
    case _: VariantType =>
      SQLConf.get.getConf(DeltaSQLConf.COLLECT_VARIANT_DATA_SKIPPING_STATS)
    case _ => false
  }

  // Use these in `match` statements
  def unapply(dataType: DataType): Option[DataType] = {
    if (SkippingEligibleDataType(dataType)) Some(dataType) else None
  }

  def unapply(f: StructField): Option[DataType] = unapply(f.dataType)
}

/**
 * An extractor that matches expressions that are eligible for data skipping predicates.
 *
 * @return A tuple of 1) column name referenced in the expression, 2) date type for the
 *         expression, 3) [[DataSkippingPredicateBuilder]] that builds the data skipping
 *         predicate for the expression, if the given expression is eligible.
 *         Otherwise, return None.
 */
abstract class GenericSkippingEligibleExpression() {

  def unapply(arg: Expression): Option[(Seq[String], DataType, DataSkippingPredicateBuilder)] = {
    arg match {
      case SkippingEligibleColumn(c, dt) =>
        Some((c, dt, DataSkippingPredicateBuilder.ColumnBuilder))
      case _ => None
    }
  }
}

/**
 * This object is used to avoid referencing DataSkippingReader in DetlaConfig.
 * Otherwise, it might cause the cyclic import through SQLConf -> SparkSession -> DetlaConfig.
 */
private[delta] object DataSkippingReaderConf {

  /**
   * Default number of cols for which we should collect stats
   */
  val DATA_SKIPPING_NUM_INDEXED_COLS_DEFAULT_VALUE = 32
}

private[delta] object DataSkippingReader {

  private[this] def col(e: Expression): Column = Column(e)
  def fold(e: Expression): Column = col(new Literal(e.eval(), e.dataType))

  // Literals often used in the data skipping reader expressions.
  val trueLiteral: Column = col(TrueLiteral)
  val falseLiteral: Column = col(FalseLiteral)
  val nullStringLiteral: Column = col(new Literal(null, StringType))
  val nullBooleanLiteral: Column = col(new Literal(null, BooleanType))
  val oneMillisecondLiteralExpr: Literal = {
    val oneMillisecond = new CalendarInterval(0, 0, 1000 /* micros */)
    new Literal(oneMillisecond, CalendarIntervalType)
  }

  lazy val sizeCollectorInputEncoders: Seq[Option[ExpressionEncoder[_]]] = Seq(
    Option(ExpressionEncoder[Boolean]()),
    Option(ExpressionEncoder[java.lang.Long]()),
    Option(ExpressionEncoder[java.lang.Long]()),
    Option(ExpressionEncoder[java.lang.Long]()))

  /**
   * For timestamps, JSON serialization will truncate to milliseconds. This means
   * that we must adjust 1 millisecond upwards for max stats, or we will incorrectly skip
   * records that differ only in microsecond precision. (For example, a file containing only
   * 01:02:03.456789 will be written with min == max == 01:02:03.456, so we must consider it
   * to contain the range from 01:02:03.456 to 01:02:03.457.)
   *
   * To avoid overflow when the timestamp is near Long.MAX_VALUE, we check if adding 1
   * millisecond would overflow. If so, we saturate to Long.MAX_VALUE to ensure the max stat
   * is >= all actual values in the file while avoiding arithmetic overflow.
   */
  def getAdjustedTimestamp(col: Column, tsType: DataType): Column = {
    val maxTimestampLiteral = Literal(Long.MaxValue, tsType)
    val overflowThresholdLiteral = Literal(Long.MaxValue - 1000, tsType)
    val adjustedExpr = If(
      GreaterThan(col.expr, overflowThresholdLiteral),
      maxTimestampLiteral,
      TimestampAdd("MILLISECOND", Literal(1L, LongType), col.expr))
    Column(Cast(adjustedExpr, tsType))
  }
}

/**
 * Adds the ability to use statistics to filter the set of files based on predicates
 * to a [[org.apache.spark.sql.delta.Snapshot]] of a given Delta table.
 */
trait DataSkippingReaderBase
  extends DeltaScanGenerator
  with StatisticsCollection
  with ReadsMetadataFields
  with StateCache
  with DeltaLogging {

  import DataSkippingReader._

  def allFiles: Dataset[AddFile]
  def path: Path
  def version: Long
  def metadata: Metadata
  private[delta] def sizeInBytesIfKnown: Option[Long]
  def deltaLog: DeltaLog
  def schema: StructType
  private[delta] def numOfFilesIfKnown: Option[Long]
  def redactedPath: String

  private def useStats = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)

  private lazy val limitPartitionLikeFiltersToClusteringColumns = spark.sessionState.conf.getConf(
    DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_CLUSTERING_COLUMNS_ONLY)
  private lazy val additionalPartitionLikeFilterSupportedExpressions =
    spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ADDITIONAL_SUPPORTED_EXPRESSIONS)
      .toSet.flatMap((exprs: String) => exprs.split(","))

  /** Returns a DataFrame expression to obtain a list of files with parsed statistics. */
  private def withStatsInternal0: DataFrame = {
    val parsedStats = from_json(col("stats"), statsSchema)
    // Only use DecodeNestedZ85EncodedVariant if the schema contains VariantType.
    // This avoids performance overhead for tables without variant columns.
    // `DecodeNestedZ85EncodedVariant` is a temporary workaround since the Spark 4.1 from_json
    // expression has no way to decode a VariantVal from an encoded Z85 string.
    // TODO: Add Z85 decoding to Variant in Spark 4.2 and use that from_json option here.
    val decodedStats = if (SchemaUtils.checkForVariantTypeColumnsRecursively(statsSchema)) {
      Column(DecodeNestedZ85EncodedVariant(parsedStats.expr))
    } else {
      parsedStats
    }
    allFiles.withColumn("stats", decodedStats)
  }

  private lazy val withStatsCache =
    cacheDS(withStatsInternal0, s"Delta Table State with Stats #$version - $redactedPath")

  protected def withStatsInternal: DataFrame = withStatsCache.getDS

  /** All files with the statistics column dropped completely. */
  def withNoStats: DataFrame = allFiles.drop("stats")

  /**
   * Returns a parsed and cached representation of files with statistics.
   *
   *
   * @return [[DataFrame]]
   */
  final def withStats: DataFrame = {
    withStatsInternal
  }


  def withStatsDeduplicated: DataFrame = withStats


  /**
   * Returns an expression to access the given statistics for a specific column, or None if that
   * stats column does not exist.
   *
   * @param pathToStatType Path components of one of the fields declared by the `DeltaStatistics`
   *                       object. For statistics of collated strings, this path contains the
   *                       versioned collation identifier. In all other cases the path only has one
   *                       element. The path is in reverse order.
   * @param pathToColumn The components of the nested column name to get stats for. The components
   *                     are in reverse order.
   */
  final protected def getStatsColumnOpt(
      pathToStatType: Seq[String], pathToColumn: Seq[String]): Option[Column] = {

    require(pathToStatType.nonEmpty, "No path to stats type provided.")

    // First validate that pathToStatType is a valid path in the statsSchema. We start at the root
    // of the stats schema and then follow the path. Note that the path is stored in reverse order.
    // If one of the path components does not exist, the foldRight operation returns None.
    val (initialColumn, initialFieldType) = pathToStatType
      .foldRight(Option((getBaseStatsColumn, statsSchema.asInstanceOf[DataType]))) {
        case (statTypePathComponent: String, Some((column: Column, struct: StructType))) =>
          // Find the field matching the current path component name or return None otherwise.
          struct.fields.collectFirst {
            case StructField(name, dataType: DataType, _, _) if name == statTypePathComponent =>
              (column.getField(statTypePathComponent), dataType)
          }
        case _ => None
      }
      // If the requested stats type doesn't even exist, just return None right away. This can
      // legitimately happen if we have no stats at all, or if column stats are disabled (in which
      // case only the NUM_RECORDS stat type is available).
      .getOrElse { return None }

    // Given a set of path segments in reverse order, e.g. column a.b.c is Seq("c", "b", "a"), we
    // use a foldRight operation to build up the requested stats column, by successively applying
    // each new path step against both the table schema and the stats schema. We can't use the stats
    // schema alone, because the caller-provided path segments use logical column names, while the
    // stats schema requires physical column names. Instead, we must step into the table schema to
    // extract that field's physical column name, and use the result to step into the stats schema.
    //
    // We use a three-tuple to track state. The traversal starts with the base column for the
    // requested stat type, the stats schema for the requested stat type, and the table schema. Each
    // step of the traversal emits the updated column, along with the stats schema and table schema
    // elements corresponding to that column.
    val initialState: Option[(Column, DataType, DataType)] =
      Some((initialColumn, initialFieldType, metadata.schema))
    pathToColumn
      .foldRight(initialState) {
        // NOTE: Only match on StructType, because we cannot traverse through other DataTypes.
        case (fieldName, Some((statCol, statsSchema: StructType, tableSchema: StructType))) =>
          // First try to step into the table schema
          val tableFieldOpt = tableSchema.findNestedFieldIgnoreCase(Seq(fieldName))

          // If that worked, try to step into the stats schema, using its its physical name
          val statsFieldOpt = tableFieldOpt
            .map(DeltaColumnMapping.getPhysicalName)
            .filter(physicalFieldName => statsSchema.exists(_.name == physicalFieldName))
            .map(statsSchema(_))

          // If all that succeeds, return the new stats column and the corresponding data types.
          statsFieldOpt.map(statsField =>
            (statCol.getField(statsField.name), statsField.dataType, tableFieldOpt.get.dataType))

        // Propagate failure if the above match failed (or if already None)
        case _ => None
      }
      // Filter out non-leaf columns -- they lack stats so skipping predicates can't use them.
      .filterNot(_._2.isInstanceOf[StructType])
      .map {
        case (statCol, TimestampType, _) if pathToStatType.head == MAX =>
          getAdjustedTimestamp(statCol, TimestampType)
        case (statCol, TimestampNTZType, _) if pathToStatType.head == MAX =>
          getAdjustedTimestamp(statCol, TimestampNTZType)
        case (statCol, _, _) =>
          statCol
      }
  }

  /** Convenience overload for single element stat type paths. */
  final protected def getStatsColumnOpt(
      statType: String, pathToColumn: Seq[String] = Nil): Option[Column] =
    getStatsColumnOpt(Seq(statType), pathToColumn)

  /**
   * Returns an expression to access the given statistics for a specific column, or a NULL
   * literal expression if that column does not exist.
   */
  final protected[delta] def getStatsColumnOrNullLiteral(
      statType: String,
      pathToColumn: Seq[String] = Nil) : Column =
    getStatsColumnOpt(Seq(statType), pathToColumn).getOrElse(lit(null))

  /** Overload for convenience working with StatsColumn helpers */
  final protected def getStatsColumnOpt(stat: StatsColumn): Option[Column] =
    getStatsColumnOpt(stat.pathToStatType, stat.pathToColumn)

  /** Overload for convenience working with StatsColumn helpers */
  final protected[delta] def getStatsColumnOrNullLiteral(stat: StatsColumn): Column =
    getStatsColumnOpt(stat.pathToStatType, stat.pathToColumn).getOrElse(lit(null))

  /** Overload for delta table property override */
  override protected def getDataSkippingStringPrefixLength: Int =
    StatsCollectionUtils.getDataSkippingStringPrefixLength(spark, metadata)

  /**
   * Returns an expression that can be used to check that the required statistics are present for a
   * given file. If any required statistics are missing we must include the corresponding file.
   *
   * NOTE: We intentionally choose to disable skipping for any file if any required stat is missing,
   * because doing it that way allows us to check each stat only once (rather than once per
   * use). Checking per-use would anyway only help for tables where the number of indexed columns
   * has changed over time, producing add.stats_parsed records with differing schemas. That should
   * be a rare enough case to not worry about optimizing for, given that the fix requires more
   * complex skipping predicates that would penalize the common case.
   */
  protected def verifyStatsForFilter(referencedStats: Set[StatsColumn]): Column = {
    recordFrameProfile("Delta", "DataSkippingReader.verifyStatsForFilter") {
      // The NULL checks for MIN and MAX stats depend on NULL_COUNT and NUM_RECORDS. Derive those
      // implied dependencies first, so the main pass can treat them like any other column.
      //
      // NOTE: We must include explicit NULL checks on all stats columns we access here, because our
      // caller will negate the expression we return. In case a stats column is NULL, `NOT(expr)`
      // must return `TRUE`, and without these NULL checks it would instead return
      // `NOT(NULL)` => `NULL`.
      referencedStats.flatMap { stat => stat match {
        case StatsColumn(MIN +: _, _) | StatsColumn(MAX +: _, _) =>
          Seq(stat, StatsColumn(NULL_COUNT, stat.pathToColumn, LongType),
            StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType))
        case _ =>
          Seq(stat)
      }}.map{stat => stat match {
        // A usable MIN or MAX stat must be non-NULL, unless the column is provably all-NULL
        //
        // NOTE: We don't care about NULL/missing NULL_COUNT and NUM_RECORDS here, because the
        // separate NULL checks we emit for those columns will force the overall validation
        // predicate conjunction to FALSE in that case -- AND(FALSE, <anything>) is FALSE.
        case StatsColumn(MIN +: _, _) | StatsColumn(MAX +: _, _) =>
          getStatsColumnOrNullLiteral(stat).isNotNull ||
            (getStatsColumnOrNullLiteral(NULL_COUNT, stat.pathToColumn) ===
              getStatsColumnOrNullLiteral(NUM_RECORDS))
        case _ =>
          // Other stats, such as NULL_COUNT and NUM_RECORDS stat, merely need to be non-NULL
          getStatsColumnOrNullLiteral(stat).isNotNull
      }}
        .reduceLeftOption(_.and(_))
        .getOrElse(trueLiteral)
    }
  }

  private def buildSizeCollectorFilter(): (ArrayAccumulator, Column => Column) = {
    val bytesCompressed = col("size")
    val rows = getStatsColumnOrNullLiteral(NUM_RECORDS)
    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0L))
    val logicalRows = (rows - dvCardinality).as("logicalRows")

    val accumulator = new ArrayAccumulator(4)

    spark.sparkContext.register(accumulator)

    // The arguments (order and datatype) must match the encoders defined in the
    // `sizeCollectorInputEncoders` value.
    val collector = (include: Boolean,
                     bytesCompressed: java.lang.Long,
                     logicalRows: java.lang.Long,
                     rows: java.lang.Long) => {
      if (include) {
        accumulator.add((0, bytesCompressed)) /* count bytes of AddFiles */
        accumulator.add((1, Option(rows).map(_.toLong).getOrElse(-1L))) /* count rows in AddFiles */
        accumulator.add((2, 1)) /* count number of AddFiles */
        accumulator.add((3, Option(logicalRows)
          .map(_.toLong).getOrElse(-1L))) /* count logical rows in AddFiles */
      }
      include
    }
    val collectorUdf = SparkUserDefinedFunction(
      f = collector,
      dataType = BooleanType,
      inputEncoders = sizeCollectorInputEncoders,
      deterministic = false)

    (accumulator, collectorUdf(_: Column, bytesCompressed, logicalRows, rows))
  }

  override def filesWithStatsForScan(partitionFilters: Seq[Expression]): DataFrame = {
    DeltaLog.filterFileList(metadata.partitionSchema, withStats, partitionFilters)
  }

  /**
   * Get all the files in this table.
   *
   * @param keepNumRecords Also select `stats.numRecords` in the query.
   *                       This may slow down the query as it has to parse json.
   */
  protected def getAllFiles(keepNumRecords: Boolean): Seq[AddFile] = recordFrameProfile(
      "Delta", "DataSkippingReader.getAllFiles") {
    val ds = if (keepNumRecords) {
      withStats // use withStats instead of allFiles so the `stats` column is already parsed
        // keep only the numRecords field as a Json string in the stats field
        .withColumn("stats", to_json(struct(col("stats.numRecords") as "numRecords")))
    } else {
      allFiles.withColumn("stats", nullStringLiteral)
    }
    convertDataFrameToAddFiles(ds.toDF())
  }

  /**
   * Given the partition filters on the data, rewrite these filters by pointing to the metadata
   * columns.
   */
  protected def constructPartitionFilters(filters: Seq[Expression]): Column = {
    recordFrameProfile("Delta", "DataSkippingReader.constructPartitionFilters") {
      val rewritten = DeltaLog.rewritePartitionFilters(
        metadata.partitionSchema, spark.sessionState.conf.resolver, filters)
      rewritten.reduceOption(And).map { expr => Column(expr) }.getOrElse(trueLiteral)
    }
  }

  /**
   * Get all the files in this table given the partition filter and the corresponding size of
   * the scan.
   *
   * @param keepNumRecords Also select `stats.numRecords` in the query.
   *                       This may slow down the query as it has to parse json.
   */
  protected def filterOnPartitions(
      partitionFilters: Seq[Expression],
      keepNumRecords: Boolean): (Seq[AddFile], DataSize) = recordFrameProfile(
      "Delta", "DataSkippingReader.filterOnPartitions") {
    val forceCollectRowCount =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALWAYS_COLLECT_STATS)
    val shouldCollectStats = keepNumRecords || forceCollectRowCount
    val df = if (shouldCollectStats) {
      // use withStats instead of allFiles so the `stats` column is already parsed
      val filteredFiles =
        DeltaLog.filterFileList(metadata.partitionSchema, withStats, partitionFilters)
      filteredFiles
        // keep only the numRecords field as a Json string in the stats field
        .withColumn("stats", to_json(struct(col("stats.numRecords") as "numRecords")))
    } else {
      val filteredFiles =
        DeltaLog.filterFileList(metadata.partitionSchema, allFiles.toDF(), partitionFilters)
      filteredFiles
        .withColumn("stats", nullStringLiteral)
    }
    val files = convertDataFrameToAddFiles(df)
    val sizeInBytesByPartitionFilters = files.map(_.size).sum
    // Compute row count if we have stats available and forceCollectRowCount is enabled
    val (rowCount, logicalRowCount) = if (forceCollectRowCount) {
      sumRowCounts(files)
    } else {
      (None, None)
    }
    files.toSeq -> DataSize(Some(sizeInBytesByPartitionFilters), rowCount, Some(files.size),
      logicalRowCount)
  }

  /**
   * Sums up the numPhysicalRecords and numLogicalRecords from the given AddFile objects.
   * Returns (None, None) if any file is missing physical record stats.
   * Returns (Some(physical), None) if any file is missing logical record stats.
   */
  private def sumRowCounts(files: Seq[AddFile]): (Option[Long], Option[Long]) = {
    var physicalRows = 0L
    var logicalRows = 0L
    var physicalMissing = false
    var logicalMissing = false
    files.foreach { file =>
      physicalMissing = physicalMissing || file.numPhysicalRecords.isEmpty
      logicalMissing = logicalMissing || file.numLogicalRecords.isEmpty
      physicalRows += file.numPhysicalRecords.getOrElse(0L)
      logicalRows += file.numLogicalRecords.getOrElse(0L)
    }
    (
      if (physicalMissing) None else Some(physicalRows),
      if (logicalMissing) None else Some(logicalRows)
    )
  }

  /**
   * Given the partition and data filters, leverage data skipping statistics to find the set of
   * files that need to be queried. Returns a tuple of the files and optionally the size of the
   * scan that's generated if there were no filters, if there were only partition filters, and
   * combined effect of partition and data filters respectively.
   */
  protected def getDataSkippedFiles(
      partitionFilters: Column,
      dataFilters: DataSkippingPredicate,
      keepNumRecords: Boolean): (Seq[AddFile], Seq[DataSize]) = recordFrameProfile(
      "Delta", "DataSkippingReader.getDataSkippedFiles") {
    val (totalSize, totalFilter) = buildSizeCollectorFilter()
    val (partitionSize, partitionFilter) = buildSizeCollectorFilter()
    val (scanSize, scanFilter) = buildSizeCollectorFilter()

    // NOTE: If any stats are missing, the value of `dataFilters` is untrustworthy -- it could be
    // NULL or even just plain incorrect. We rely on `verifyStatsForFilter` to be FALSE in that
    // case, forcing the overall OR to evaluate as TRUE no matter what value `dataFilters` takes.
    val filteredFiles = withStats.where(
        totalFilter(trueLiteral) &&
          partitionFilter(partitionFilters) &&
          scanFilter(dataFilters.expr || !verifyStatsForFilter(dataFilters.referencedStats))
      )

    val statsColumn = if (keepNumRecords) {
      // keep only the numRecords field as a Json string in the stats field
      to_json(struct(col("stats.numRecords") as "numRecords"))
    } else nullStringLiteral

    val files =
      recordFrameProfile(
        "Delta", "DataSkippingReader.getDataSkippedFiles.collectFiles") {
      val df = filteredFiles.withColumn("stats", statsColumn)
      convertDataFrameToAddFiles(df)
    }
    files.toSeq -> Seq(DataSize(totalSize), DataSize(partitionSize), DataSize(scanSize))
  }

  private def getCorrectDataSkippingType(
      dataSkippingType: DeltaDataSkippingType): DeltaDataSkippingType = {
    dataSkippingType
  }

  /**
   * Gathers files that should be included in a scan based on the given predicates.
   * Statistics about the amount of data that will be read are gathered and returned.
   * Note, the statistics column that is added when keepNumRecords = true should NOT
   * take into account DVs. Consumers of this method might commit the file. The semantics
   * of the statistics need to be consistent across all files.
   */
  override def filesForScan(filters: Seq[Expression], keepNumRecords: Boolean): DeltaScan = {
    val startTime = System.currentTimeMillis()
    if (filters == Seq(TrueLiteral) || filters.isEmpty || schema.isEmpty) {
      recordDeltaOperation(snapshotToScan, "delta.skipping.none") {
        // When there are no filters we can just return allFiles with no extra processing
        val forceCollectRowCount =
          spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALWAYS_COLLECT_STATS)
        val shouldCollectStats = keepNumRecords || forceCollectRowCount
        lazy val files = getAllFiles(shouldCollectStats)
        // Compute row count if forceCollectRowCount is enabled
        val (rowCount, logicalRowCount) = if (forceCollectRowCount) {
          sumRowCounts(files)
        } else {
          (None, None)
        }
        val dataSize = DataSize(
          bytesCompressed = sizeInBytesIfKnown,
          rows = rowCount,
          files = numOfFilesIfKnown,
          logicalRows = logicalRowCount)
        return DeltaScan(
          version = version,
          files = files,
          total = dataSize,
          partition = dataSize,
          scanned = dataSize)(
          scannedSnapshot = snapshotToScan,
          partitionFilters = ExpressionSet(Nil),
          dataFilters = ExpressionSet(Nil),
          partitionLikeDataFilters = ExpressionSet(Nil),
          rewrittenPartitionLikeDataFilters = Set.empty,
          unusedFilters = ExpressionSet(Nil),
          scanDurationMs = System.currentTimeMillis() - startTime,
          dataSkippingType = getCorrectDataSkippingType(DeltaDataSkippingType.noSkippingV1)
        )
      }
    }

    import DeltaTableUtils._
    val partitionColumns = metadata.partitionColumns

    // For data skipping, avoid using the filters that either:
    // 1. involve subqueries.
    // 2. are non-deterministic.
    // 3. involve file metadata struct fields
    var (ineligibleFilters, eligibleFilters) = filters.partition {
      case f => containsSubquery(f) || !f.deterministic || f.exists {
        case MetadataAttribute(_) => true
        case _ => false
      }
    }


    val (partitionFilters, dataFilters) = eligibleFilters
      .partition(isPredicatePartitionColumnsOnly(_, partitionColumns, spark))

    if (dataFilters.isEmpty) recordDeltaOperation(deltaLog, "delta.skipping.partition") {
      // When there are only partition filters we can scan allFiles
      // rather than withStats and thus we skip data skipping information.
      val (files, scanSize) = filterOnPartitions(partitionFilters, keepNumRecords)
      DeltaScan(
        version = version,
        files = files,
        total = DataSize(sizeInBytesIfKnown, None, numOfFilesIfKnown),
        partition = scanSize,
        scanned = scanSize)(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(partitionFilters),
        dataFilters = ExpressionSet(Nil),
        partitionLikeDataFilters = ExpressionSet(Nil),
        rewrittenPartitionLikeDataFilters = Set.empty,
        unusedFilters = ExpressionSet(ineligibleFilters),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType =
          getCorrectDataSkippingType(DeltaDataSkippingType.partitionFilteringOnlyV1)
      )
    } else recordDeltaOperation(snapshotToScan, "delta.skipping.data") {
      val finalPartitionFilters = constructPartitionFilters(partitionFilters)

      val dataSkippingType = if (partitionFilters.isEmpty) {
        DeltaDataSkippingType.dataSkippingOnlyV1
      } else {
        DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1
      }

      var (skippingFilters, unusedFilters) = if (useStats) {
        val constructDataFilters = new DataFiltersBuilder(
          spark = spark,
          dataSkippingType = dataSkippingType,
          getStatsColumnOpt = (s: StatsColumn) => getStatsColumnOpt(s),
          additionalPartitionLikeFilterSupportedExpressions =
            additionalPartitionLikeFilterSupportedExpressions,
          limitPartitionLikeFiltersToClusteringColumns =
            limitPartitionLikeFiltersToClusteringColumns)
        dataFilters.map(f => (f, constructDataFilters(f))).partition(f => f._2.isDefined)
      } else {
        (Nil, dataFilters.map(f => (f, None)))
      }

      // If enabled, rewrite unused data filters to use partition-like data skipping for clustered
      // tables. Only rewrite filters if the table is expected to benefit from partition-like
      // data skipping:
      // 1. The table should be have a large portion of files with the same min-max values on the
      //    referenced columns - as a rough heuristic, require the table to be a clustered table, as
      //    many files often have the same min-max on the clustering columns.
      // 2. The table should be large enough to benefit from partition-like data skipping - as a
      //    rough heuristic, require the table to no longer be considered a "small delta table."
      // 3. At least 1 data filter was not already used for data skipping.
      val shouldRewriteDataFiltersAsPartitionLike =
        spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ENABLED) &&
          ClusteredTableUtils.isSupported(snapshotToScan.protocol) &&
          snapshotToScan.numOfFilesIfKnown.exists(_ >=
            spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_THRESHOLD)) &&
          unusedFilters.nonEmpty
      val partitionLikeFilters = if (shouldRewriteDataFiltersAsPartitionLike) {
        val clusteringColumns = ClusteringColumnInfo.extractLogicalNames(snapshotToScan)
        val (rewrittenUsedFilters, rewrittenUnusedFilters) = {
          val constructDataFilters = new DataFiltersBuilder(
            spark = spark,
            dataSkippingType = dataSkippingType,
            getStatsColumnOpt = (s: StatsColumn) => getStatsColumnOpt(s),
            additionalPartitionLikeFilterSupportedExpressions =
              additionalPartitionLikeFilterSupportedExpressions,
            limitPartitionLikeFiltersToClusteringColumns =
              limitPartitionLikeFiltersToClusteringColumns)
          unusedFilters
            .map { case (expr, _) =>
              val rewrittenExprOpt = constructDataFilters.rewriteDataFiltersAsPartitionLike(
                clusteringColumns, expr)
              (expr, rewrittenExprOpt)
            }
            .partition(_._2.isDefined)
        }
        skippingFilters = skippingFilters ++ rewrittenUsedFilters
        unusedFilters = rewrittenUnusedFilters
        rewrittenUsedFilters.map { case (orig, rewrittenOpt) => (orig, rewrittenOpt.get) }
      } else {
        Nil
      }

      val finalSkippingFilters = skippingFilters
        .map(_._2.get)
        .reduceOption((skip1, skip2) => DataSkippingPredicate(
          // Fold the filters into a conjunction, while unioning their referencedStats.
          skip1.expr && skip2.expr, skip1.referencedStats ++ skip2.referencedStats))
        .getOrElse(DataSkippingPredicate(trueLiteral))

      val (files, sizes) = {
        getDataSkippedFiles(finalPartitionFilters, finalSkippingFilters, keepNumRecords)
      }

      DeltaScan(
        version = version,
        files = files,
        total = sizes(0),
        partition = sizes(1),
        scanned = sizes(2))(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(partitionFilters),
        dataFilters = ExpressionSet(skippingFilters.map(_._1)),
        partitionLikeDataFilters = ExpressionSet(partitionLikeFilters.map(_._1)),
        rewrittenPartitionLikeDataFilters = partitionLikeFilters.map(_._2.expr.expr).toSet,
        unusedFilters = ExpressionSet(unusedFilters.map(_._1) ++ ineligibleFilters),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType = getCorrectDataSkippingType(dataSkippingType)
      )
    }
  }

  /**
   * Gathers files that should be included in a scan based on the given predicates and limit.
   * This will be called only when all predicates are on partitioning columns.
   * Statistics about the amount of data that will be read are gathered and returned.
   */
  override def filesForScan(limit: Long, partitionFilters: Seq[Expression]): DeltaScan =
    recordDeltaOperation(snapshotToScan, "delta.skipping.filteredLimit") {
      val startTime = System.currentTimeMillis()
      val finalPartitionFilters = constructPartitionFilters(partitionFilters)

      val scan = {
        pruneFilesByLimit(withStats.where(finalPartitionFilters), limit)
      }

      val totalDataSize = new DataSize(
        sizeInBytesIfKnown,
        None,
        numOfFilesIfKnown,
        None
      )

      val scannedDataSize = new DataSize(
        scan.byteSize,
        scan.numPhysicalRecords,
        Some(scan.files.size),
        scan.numLogicalRecords
      )

      DeltaScan(
        version = version,
        files = scan.files,
        total = totalDataSize,
        partition = null,
        scanned = scannedDataSize)(
        scannedSnapshot = snapshotToScan,
        partitionFilters = ExpressionSet(partitionFilters),
        dataFilters = ExpressionSet(Nil),
        partitionLikeDataFilters = ExpressionSet(Nil),
        rewrittenPartitionLikeDataFilters = Set.empty,
        unusedFilters = ExpressionSet(Nil),
        scanDurationMs = System.currentTimeMillis() - startTime,
        dataSkippingType = DeltaDataSkippingType.filteredLimit
      )
    }

  /**
   * Get AddFile (with stats) actions corresponding to given set of paths in the Snapshot.
   * If a path doesn't exist in snapshot, it will be ignored and no [[AddFile]] will be returned
   * for it.
   * @param paths Sequence of paths for which we want to get [[AddFile]] action
   * @return a sequence of addFiles for the given `paths`
   */
  def getSpecificFilesWithStats(paths: Seq[String]): Seq[AddFile] = {
    recordFrameProfile(
        "Delta", "DataSkippingReader.getSpecificFilesWithStats") {
      val right = paths.toDF(spark, "path")
      val df = allFiles.join(right, Seq("path"), "leftsemi")
      convertDataFrameToAddFiles(df)
    }
  }

  /** Get the files and number of records within each file, to perform limit pushdown. */
  def getFilesAndNumRecords(
      df: DataFrame): Iterator[(AddFile, NumRecords)] with Closeable = recordFrameProfile(
    "Delta", "DataSkippingReaderEdge.getFilesAndNumRecords") {
    import org.apache.spark.sql.delta.implicits._

    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0L))
    val numLogicalRecords = col("stats.numRecords") - dvCardinality

    val result = df.withColumn("numPhysicalRecords", col("stats.numRecords")) // Physical
      .withColumn("numLogicalRecords", numLogicalRecords) // Logical
      .withColumn("stats", nullStringLiteral)
      .select(struct(col("*")).as[AddFile],
        col("numPhysicalRecords").as[java.lang.Long], col("numLogicalRecords").as[java.lang.Long])
      .collectAsList()

    new Iterator[(AddFile, NumRecords)] with Closeable {
      private val underlying = result.iterator
      override def hasNext: Boolean = underlying.hasNext
      override def next(): (AddFile, NumRecords) = {
        val next = underlying.next()
        (next._1, NumRecords(numPhysicalRecords = next._2, numLogicalRecords = next._3))
      }

      override def close(): Unit = {
      }

    }
  }

  protected def convertDataFrameToAddFiles(df: DataFrame): Array[AddFile] = {
    df.as[AddFile].collect()
  }

  protected[delta] def pruneFilesByLimit(df: DataFrame, limit: Long): ScanAfterLimit = {
    val withNumRecords = {
      getFilesAndNumRecords(df)
    }
    pruneFilesWithIterator(withNumRecords, limit)
  }

  /**
   * Accepts an iterator of files with record counts and prunes them based on the limit.
   */
  protected def pruneFilesWithIterator(
      withNumRecords: Iterator[(AddFile, NumRecords)] with Closeable,
      limit: Long): ScanAfterLimit = {

    var logicalRowsToScan = 0L
    var physicalRowsToScan = 0L
    var bytesToScan = 0L
    var bytesToIgnore = 0L
    var rowsUnknown = false

    val filesAfterLimit = try {
      val iter = withNumRecords
      val filesToScan = ArrayBuffer[AddFile]()
      val filesToIgnore = ArrayBuffer[AddFile]()
      while (iter.hasNext && logicalRowsToScan < limit) {
        val file = iter.next()
        if (file._2.numPhysicalRecords == null || file._2.numLogicalRecords == null) {
          // this file has no stats, ignore for now
          bytesToIgnore += file._1.size
          filesToIgnore += file._1
        } else {
          physicalRowsToScan += file._2.numPhysicalRecords.toLong
          logicalRowsToScan += file._2.numLogicalRecords.toLong
          bytesToScan += file._1.size
          filesToScan += file._1
        }
      }

      // If the files that have stats do not contain enough rows, fall back to reading all files
      if (logicalRowsToScan < limit && filesToIgnore.nonEmpty) {
        filesToScan ++= filesToIgnore
        bytesToScan += bytesToIgnore
        rowsUnknown = true
      }
      filesToScan.toSeq
    } finally {
      withNumRecords.close()
    }

    if (rowsUnknown) {
      ScanAfterLimit(filesAfterLimit, Some(bytesToScan), None, None)
    } else {
      ScanAfterLimit(filesAfterLimit, Some(bytesToScan),
        Some(physicalRowsToScan), Some(logicalRowsToScan))
    }
  }
}

trait DataSkippingReader extends DataSkippingReaderBase
