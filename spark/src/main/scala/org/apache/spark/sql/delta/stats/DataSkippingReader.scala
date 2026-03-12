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

  /** Lazily constructed scan executor that owns the three execution paths. */
  private[delta] lazy val scanExecutor: DefaultDeltaScanExecutor = new DefaultDeltaScanExecutor(
    spark = spark,
    withStats = withStats,
    allFiles = allFiles,
    getStatsColumnOpt = (stat: StatsColumn) => getStatsColumnOpt(stat),
    partitionSchema = metadata.partitionSchema)

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

  // verifyStatsForFilter, constructPartitionFilters -> DataSkippingFilterPlanner
  // buildSizeCollectorFilter, getAllFiles, filterOnPartitions, getDataSkippedFiles
  // -> DeltaScanExecutor

  override def filesWithStatsForScan(partitionFilters: Seq[Expression]): DataFrame = {
    DeltaLog.filterFileList(metadata.partitionSchema, withStats, partitionFilters)
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

    // Early exit: no filters or empty schema -- return all files, no skipping.
    if (filters == Seq(TrueLiteral) || filters.isEmpty || schema.isEmpty) {
      return recordDeltaOperation(deltaLog, "delta.skipping.none") {
        val dataSize = DataSize(
          bytesCompressed = sizeInBytesIfKnown, rows = None, files = numOfFilesIfKnown)
        DeltaScan(
          version = version,
          files = scanExecutor.getAllFiles(keepNumRecords),
          total = dataSize, partition = dataSize, scanned = dataSize)(
          scannedSnapshot = snapshotToScan,
          partitionFilters = ExpressionSet(Nil),
          dataFilters = ExpressionSet(Nil),
          partitionLikeDataFilters = ExpressionSet(Nil),
          rewrittenPartitionLikeDataFilters = Set.empty,
          unusedFilters = ExpressionSet(Nil),
          scanDurationMs = System.currentTimeMillis() - startTime,
          dataSkippingType = getCorrectDataSkippingType(DeltaDataSkippingType.noSkippingV1))
      }
    }

    // 1. PLAN: classify and rewrite filters
    val filterPlanner = new DefaultDataSkippingFilterPlanner(
      spark = spark,
      dataSkippingType = DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1,
      getStatsColumnOpt = (stat: StatsColumn) => getStatsColumnOpt(stat),
      partitionColumns = metadata.partitionColumns,
      partitionSchema = metadata.partitionSchema,
      useStats = useStats,
      clusteringColumns = ClusteringColumnInfo.extractLogicalNames(snapshotToScan),
      protocol = Some(snapshotToScan.protocol),
      numOfFilesIfKnown = snapshotToScan.numOfFilesIfKnown)
    val plan = filterPlanner.plan(filters)

    // 2. EXECUTE: partition-only fast path or full data-skipping path
    if (plan.dataFilters.isEmpty) {
      recordDeltaOperation(deltaLog, "delta.skipping.partition") {
        val (files, scanSize) =
          scanExecutor.filterOnPartitions(plan.partitionFilters, keepNumRecords)
        DeltaScan(
          version = version,
          files = files,
          total = DataSize(sizeInBytesIfKnown, None, numOfFilesIfKnown),
          partition = scanSize, scanned = scanSize)(
          scannedSnapshot = snapshotToScan,
          partitionFilters = ExpressionSet(plan.partitionFilters),
          dataFilters = ExpressionSet(Nil),
          partitionLikeDataFilters = ExpressionSet(Nil),
          rewrittenPartitionLikeDataFilters = Set.empty,
          unusedFilters = ExpressionSet(plan.ineligibleFilters),
          scanDurationMs = System.currentTimeMillis() - startTime,
          dataSkippingType =
            getCorrectDataSkippingType(DeltaDataSkippingType.partitionFilteringOnlyV1))
      }
    } else {
      recordDeltaOperation(deltaLog, "delta.skipping.data") {
        val finalPartitionFilters =
          filterPlanner.constructPartitionFilters(plan.partitionFilters)
        val (files, sizes) = scanExecutor.getDataSkippedFiles(
          finalPartitionFilters, plan.verifiedSkippingExpr, keepNumRecords)

        val dataSkippingType = if (plan.partitionFilters.isEmpty) {
          DeltaDataSkippingType.dataSkippingOnlyV1
        } else {
          DeltaDataSkippingType.dataSkippingAndPartitionFilteringV1
        }

        // 3. ASSEMBLE: build DeltaScan from plan metadata + execution results
        DeltaScan(
          version = version,
          files = files,
          total = sizes(0), partition = sizes(1), scanned = sizes(2))(
          scannedSnapshot = snapshotToScan,
          partitionFilters = ExpressionSet(plan.partitionFilters),
          dataFilters = ExpressionSet(plan.skippingFilters.map(_._1)),
          partitionLikeDataFilters = ExpressionSet(plan.partitionLikeFilters.map(_._1)),
          rewrittenPartitionLikeDataFilters =
            plan.partitionLikeFilters.map(_._2.expr.expr).toSet,
          unusedFilters =
            ExpressionSet(plan.unusedFilters.map(_._1) ++ plan.ineligibleFilters),
          scanDurationMs = System.currentTimeMillis() - startTime,
          dataSkippingType = getCorrectDataSkippingType(dataSkippingType))
      }
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
      val rewritten = PartitionFilterUtils.rewritePartitionFilters(
        metadata.partitionSchema, spark.sessionState.conf.resolver, partitionFilters)
      val finalPartitionFilters = rewritten.reduceOption(And)
        .map { expr => Column(expr) }.getOrElse(trueLiteral)

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
