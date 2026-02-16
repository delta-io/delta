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
  private[delta] def stateProvider: DeltaStateProvider

  /** Lazily constructed stats column resolver shared by planner, executor, and reader. */
  private[delta] lazy val statsColumnResolver: StatsColumnResolver = new StatsColumnResolver(
    baseStatsColumn = getBaseStatsColumn,
    statsSchema = statsSchema,
    tableSchema = metadata.schema)

  private def useStats = spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)

  private lazy val limitPartitionLikeFiltersToClusteringColumns = spark.sessionState.conf.getConf(
    DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_CLUSTERING_COLUMNS_ONLY)
  private lazy val additionalPartitionLikeFilterSupportedExpressions =
    spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_DATASKIPPING_PARTITION_LIKE_FILTERS_ADDITIONAL_SUPPORTED_EXPRESSIONS)
      .toSet.flatMap((exprs: String) => exprs.split(","))

  /** All files with the statistics column dropped completely. */
  def withNoStats: DataFrame = allFiles.drop("stats")

  /**
   * Returns a parsed and cached representation of files with statistics.
   *
   *
   * @return [[DataFrame]]
   */
  final def withStats: DataFrame = stateProvider.allAddFilesWithParsedStats

  /**
   * Constructs a [[DataSkippingPredicate]] for isNotNull predicates.
   */
   protected def constructNotNullFilter(
      statsProvider: StatsProvider,
      pathToColumn: Seq[String]): Option[DataSkippingPredicate] = {
    val nullCountCol = StatsColumn(NULL_COUNT, pathToColumn, LongType)
    val numRecordsCol = StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)
    statsProvider.getPredicateWithStatsColumnsIfExists(nullCountCol, numRecordsCol) {
      (nullCount, numRecords) => nullCount < numRecords
    }
  }

  def withStatsDeduplicated: DataFrame = withStats

  /**
   * Creates a [[DataFiltersBuilder]] wired with this reader's stats context.
   */
  protected def createDataFiltersBuilder(
      dataSkippingType: DeltaDataSkippingType): DataFiltersBuilder = {
    new DataFiltersBuilder(
      spark = spark,
      dataSkippingType = dataSkippingType,
      getStatsColumnOpt = (stat: StatsColumn) => getStatsColumnOpt(stat),
      constructNotNullFilter = constructNotNullFilter,
      limitPartitionLikeFiltersToClusteringColumns =
        limitPartitionLikeFiltersToClusteringColumns,
      additionalPartitionLikeFilterSupportedExpressions =
        additionalPartitionLikeFilterSupportedExpressions)
  }

  // --- Stats column resolution delegated to StatsColumnResolver ---

  final protected def getStatsColumnOpt(
      pathToStatType: Seq[String], pathToColumn: Seq[String]): Option[Column] =
    statsColumnResolver.getStatsColumnOpt(pathToStatType, pathToColumn)

  final protected def getStatsColumnOpt(
      statType: String, pathToColumn: Seq[String] = Nil): Option[Column] =
    statsColumnResolver.getStatsColumnOpt(statType, pathToColumn)

  final protected[delta] def getStatsColumnOrNullLiteral(
      statType: String,
      pathToColumn: Seq[String] = Nil): Column =
    statsColumnResolver.getStatsColumnOrNullLiteral(statType, pathToColumn)

  final protected def getStatsColumnOpt(stat: StatsColumn): Option[Column] =
    statsColumnResolver.getStatsColumnOpt(stat)

  final protected[delta] def getStatsColumnOrNullLiteral(stat: StatsColumn): Column =
    statsColumnResolver.getStatsColumnOrNullLiteral(stat)

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
    val df = if (keepNumRecords) {
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
    files.toSeq -> DataSize(Some(sizeInBytesByPartitionFilters), None, Some(files.size))
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
      recordFrameProfile("Delta", "DataSkippingReader.getDataSkippedFiles.collectFiles") {
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
      recordDeltaOperation(deltaLog, "delta.skipping.none") {
        // When there are no filters we can just return allFiles with no extra processing
        val dataSize = DataSize(
          bytesCompressed = sizeInBytesIfKnown,
          rows = None,
          files = numOfFilesIfKnown)
        return DeltaScan(
          version = version,
          files = getAllFiles(keepNumRecords),
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

    val dataSkippingType = DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1
    val builder = createDataFiltersBuilder(dataSkippingType)
    val filterPlanner = new DefaultDataSkippingFilterPlanner(
      spark = spark,
      builder = builder,
      partitionColumns = metadata.partitionColumns,
      useStats = useStats,
      clusteringColumns = ClusteringColumnInfo.extractLogicalNames(snapshotToScan),
      protocol = Some(snapshotToScan.protocol),
      numOfFilesIfKnown = snapshotToScan.numOfFilesIfKnown)
    val filterPlan = filterPlanner.plan(filters)

    val ineligibleFilters = filterPlan.ineligibleFilters
    val partitionFilters = filterPlan.partitionFilters
    val dataFilters = filterPlan.dataFilters

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
    } else recordDeltaOperation(deltaLog, "delta.skipping.data") {
      val finalPartitionFilters = constructPartitionFilters(partitionFilters)

      val finalDataSkippingType = if (partitionFilters.isEmpty) {
        DeltaDataSkippingType.dataSkippingOnlyV1
      } else {
        DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1
      }

      val skippingFilters = filterPlan.skippingFilters
      val unusedFilters = filterPlan.unusedFilters
      val partitionLikeFilters = filterPlan.partitionLikeFilters
      val finalSkippingFilters = filterPlan.finalSkippingFilter

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
        dataSkippingType = getCorrectDataSkippingType(finalDataSkippingType)
      )
    }
  }

  /**
   * Gathers files that should be included in a scan based on the given predicates and limit.
   * This will be called only when all predicates are on partitioning columns.
   * Statistics about the amount of data that will be read are gathered and returned.
   */
  override def filesForScan(limit: Long, partitionFilters: Seq[Expression]): DeltaScan =
    recordDeltaOperation(deltaLog, "delta.skipping.filteredLimit") {
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
    recordFrameProfile("Delta", "DataSkippingReader.getSpecificFilesWithStats") {
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
