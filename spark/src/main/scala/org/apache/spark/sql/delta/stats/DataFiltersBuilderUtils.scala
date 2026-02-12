/*
 * Copyright (2025) The Delta Lake Project Authors.
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
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, from_json, lit, struct, substring, to_json}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.DeltaTableUtils.isPredicatePartitionColumnsOnly
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MIN, MAX, NULL_COUNT, NUM_RECORDS}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
// scalastyle:on import.ordering.noEmptyLine

/**
 * Shared data skipping logic used by both V1 and V2 connectors.
 *
 * Contains:
 *  - Predicate builders (equalTo, lessThan, etc.) - from V1's ColumnPredicateBuilder
 *  - Stats schema construction - from V1's StatisticsCollection.statsSchema
 *  - Stats verification - from V1's DataSkippingReader.verifyStatsForFilter
 *  - Full data skipping pipeline (applyDataSkipping) - from V1's getDataSkippedFiles
 *
 * The core constructDataFilters logic is in [[SharedDataFiltersBuilder]], which
 * is directly used by both V1 (via DataSkippingReader.DataFiltersBuilder extends)
 * and V2 (via DataFiltersBuilderV2).
 */
object DataFiltersBuilderUtils {

  // ===== Shared Methods (using StatsProvider) =====
  // These methods are extracted from V1's ColumnPredicateBuilder and used by both V1 and V2.

  /**
   * EqualTo predicate builder.
   * Extracted from V1's ColumnPredicateBuilder.equalTo
   */
  def equalTo(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypesIfExists(colPath, value.expr.dataType, MIN, MAX) {
      (min, max) => min <= value && value <= max
    }
  }

  /**
   * NotEqualTo predicate builder.
   * Extracted from V1's ColumnPredicateBuilder.notEqualTo
   */
  def notEqualTo(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypesIfExists(colPath, value.expr.dataType, MIN, MAX) {
      (min, max) => min < value || value < max
    }
  }

  /**
   * LessThan predicate builder.
   * Extracted from V1's ColumnPredicateBuilder.lessThan
   */
  def lessThan(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MIN)(_ < value)
  }

  /**
   * LessThanOrEqual predicate builder.
   * Extracted from V1's ColumnPredicateBuilder.lessThanOrEqual
   */
  def lessThanOrEqual(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MIN)(_ <= value)
  }

  /**
   * GreaterThan predicate builder.
   * Extracted from V1's ColumnPredicateBuilder.greaterThan
   */
  def greaterThan(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MAX)(_ > value)
  }

  /**
   * GreaterThanOrEqual predicate builder.
   * Extracted from V1's ColumnPredicateBuilder.greaterThanOrEqual
   */
  def greaterThanOrEqual(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MAX)(_ >= value)
  }

  /**
   * IsNull predicate builder: match any file whose null count is > 0.
   * Extracted from V1's DataSkippingReader.constructDataFilters IsNull case.
   */
  def constructIsNullFilter(
      statsProvider: StatsProvider,
      pathToColumn: Seq[String]): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(pathToColumn, LongType, NULL_COUNT) {
      nullCount => nullCount > lit(0L)
    }
  }

  /**
   * IsNotNull predicate builder: match any file whose null count < numRecords.
   * Extracted from V1's DataSkippingReader.constructNotNullFilter.
   */
  def constructNotNullFilter(
      statsProvider: StatsProvider,
      pathToColumn: Seq[String]): Option[DataSkippingPredicate] = {
    val nullCountCol = StatsColumn(NULL_COUNT, pathToColumn, LongType)
    val numRecordsCol = StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)
    statsProvider.getPredicateWithStatsColumnsIfExists(nullCountCol, numRecordsCol) {
      (nullCount, numRecords) => nullCount < numRecords
    }
  }

  // ==================== Stats Schema (from V1 StatisticsCollection.statsSchema) ==================

  /**
   * Build stats schema for parsing JSON stats string into struct.
   * Used by BOTH V1 (StatisticsCollection.statsSchema) and V2.
   *
   * Schema: {numRecords: Long, minValues: {...}, maxValues: {...}, nullCount: {...}}
   *
   * For minValues/maxValues: keeps only SkippingEligibleDataType columns.
   * For nullCount: uses LongType for ALL columns.
   *
   * @param tableSchema          The table's data schema (V1: statCollectionPhysicalSchema)
   * @param getFieldName         Maps StructField to output field name.
   *                             V1 passes DeltaColumnMapping.getPhysicalName;
   *                             V2 passes _.name (no column mapping).
   * @param tightBoundsSupported Whether to include tightBounds field (V1: deletionVectorsSupported)
   * @return StructType for from_json parsing of the stats column
   */
  def buildStatsSchema(
      tableSchema: StructType,
      getFieldName: StructField => String = _.name,
      tightBoundsSupported: Boolean = false): StructType = {
    val minMaxSchemaOpt = getMinMaxStatsSchema(tableSchema, getFieldName)
    val nullCountSchemaOpt = getNullCountSchema(tableSchema, getFieldName)
    val tightBoundsFieldOpt =
      if (tightBoundsSupported) {
        Some(DeltaStatistics.TIGHT_BOUNDS -> org.apache.spark.sql.types.BooleanType)
      } else None

    val fields: Array[(String, DataType)] =
      Array(NUM_RECORDS -> LongType) ++
      minMaxSchemaOpt.map(MIN -> _) ++
      minMaxSchemaOpt.map(MAX -> _) ++
      nullCountSchemaOpt.map(NULL_COUNT -> _) ++
      tightBoundsFieldOpt

    StructType(fields.map { case (name, dataType) => StructField(name, dataType) })
  }

  /** Recursively builds min/max schema. Uses SkippingEligibleDataType from V1. */
  private[stats] def getMinMaxStatsSchema(
      schema: StructType,
      getFieldName: StructField => String): Option[StructType] = {
    val fields = schema.fields.flatMap {
      case f @ StructField(_, dataType: StructType, _, _) =>
        getMinMaxStatsSchema(dataType, getFieldName).map(dt =>
          StructField(getFieldName(f), dt))
      case f @ StructField(_, SkippingEligibleDataType(dataType), _, _) =>
        Some(StructField(getFieldName(f), dataType))
      case _ => None
    }
    if (fields.nonEmpty) Some(StructType(fields)) else None
  }

  /** Recursively builds null count schema with LongType. */
  private[stats] def getNullCountSchema(
      schema: StructType,
      getFieldName: StructField => String): Option[StructType] = {
    val fields = schema.fields.flatMap {
      case f @ StructField(_, _: StructType, _, _) =>
        getNullCountSchema(f.dataType.asInstanceOf[StructType], getFieldName).map(dt =>
          StructField(getFieldName(f), dt))
      case f: StructField =>
        Some(StructField(getFieldName(f), LongType))
    }
    if (fields.nonEmpty) Some(StructType(fields)) else None
  }

  // ==================== verifyStatsForFilter (from V1 DataSkippingReader) ========================

  /**
   * Check that all required stats columns are present for a given file.
   *
   * Ported from V1's DataSkippingReader.verifyStatsForFilter. If any required stat is
   * missing (NULL), we must INCLUDE the file (disable skipping) - this is a safety mechanism.
   *
   * @param referencedStats Set of stats columns the filter depends on
   * @param getStatColumn   Function to resolve a StatsColumn to a Column expression, or None
   *                        if the column doesn't exist. Same signature as V1's getStatsColumnOpt.
   * @return Column expression that is TRUE when all required stats are present
   */
  def verifyStatsForFilter(
      referencedStats: Set[StatsColumn],
      getStatColumn: StatsColumn => Option[Column]): Column = {
    def getColumnOrNull(stat: StatsColumn): Column =
      getStatColumn(stat).getOrElse(lit(null))

    // Derive implied dependencies: MIN/MAX stats depend on NULL_COUNT and NUM_RECORDS
    val allStats = referencedStats.flatMap { stat =>
      stat match {
        case StatsColumn(MIN +: _, _) | StatsColumn(MAX +: _, _) =>
          Seq(stat,
            StatsColumn(NULL_COUNT, stat.pathToColumn, LongType),
            StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType))
      case _ =>
          Seq(stat)
      }
    }

    allStats.map { stat =>
      stat match {
        // A usable MIN or MAX stat must be non-NULL, unless the column is provably all-NULL
        case StatsColumn(MIN +: _, _) | StatsColumn(MAX +: _, _) =>
          getColumnOrNull(stat).isNotNull ||
            (getColumnOrNull(StatsColumn(NULL_COUNT, stat.pathToColumn, LongType)) ===
              getColumnOrNull(StatsColumn(NUM_RECORDS, pathToColumn = Nil, LongType)))
        case _ =>
          // Other stats (NULL_COUNT, NUM_RECORDS) merely need to be non-NULL
          getColumnOrNull(stat).isNotNull
      }
    }.reduceLeftOption(_.and(_))
     .getOrElse(lit(true))
  }

  // ==================== Composable Pipeline Steps ================================================
  // These steps are used by BOTH V1 and V2. V1 composes them with extra partition filters
  // and size collectors; V2 uses the convenience applyDataSkipping method.

  /**
   * Step 1: Parse JSON stats column into a struct.
   * V1 equivalent: DataSkippingReader.withStatsInternal0
   *
   * @param df          DataFrame with a "stats" column containing JSON strings
   * @param statsSchema Schema for parsing (from buildStatsSchema)
   * @return DataFrame with "stats" column replaced by parsed struct
   */
  def withParsedStats(df: DataFrame, statsSchema: StructType): DataFrame =
    df.withColumn("stats", from_json(col("stats"), statsSchema))

  /**
   * Step 2: Build safe data skipping filter expression.
   * V1 equivalent: the filter expression in DataSkippingReader.getDataSkippedFiles
   *
   * Pattern: dataFilters.expr OR NOT verifyStatsForFilter(referencedStats)
   * When stats are missing (verifyStats = FALSE), the OR forces TRUE - file is kept.
   *
   * @param predicate     DataSkippingPredicate from constructDataFilters
   * @param getStatColumn Function to resolve StatsColumn to Column
   * @return Column expression that is safe to use in .where()
   */
  def buildSafeSkippingFilter(
      predicate: DataSkippingPredicate,
      getStatColumn: StatsColumn => Option[Column]): Column = {
    predicate.expr || !verifyStatsForFilter(predicate.referencedStats, getStatColumn)
  }

  /**
   * Step 3: Convert parsed stats struct back to JSON string.
   *
   * @param df DataFrame with a parsed "stats" struct column
   * @return DataFrame with "stats" column as JSON string
   */
  def withSerializedStats(df: DataFrame): DataFrame =
    df.withColumn("stats", to_json(col("stats")))

  // ==================== Convenience: Full Pipeline ===============================================

  /**
   * Apply data skipping to a flat AddFile DataFrame (Steps 1+2+3 combined).
   *
   * This is a convenience method for the simple case (V2). V1 composes the
   * individual steps (withParsedStats, buildSafeSkippingFilter) directly because
   * V1 interleaves partition filters and size collectors.
   *
   * @param addFilesDF     Flat AddFile DataFrame with {path, stats (string), ...}
   * @param predicate      DataSkippingPredicate from constructDataFilters
   * @param statsSchema    Schema for parsing JSON stats (from buildStatsSchema)
   * @param getStatColumn  Function to resolve StatsColumn to Column (for verifyStatsForFilter)
   * @return Filtered DataFrame with same schema as input
   */
  def applyDataSkipping(
      addFilesDF: DataFrame,
      predicate: DataSkippingPredicate,
      statsSchema: StructType,
      getStatColumn: StatsColumn => Option[Column]): DataFrame = {
    val withStats = withParsedStats(addFilesDF, statsSchema)
    val filtered = withStats.where(buildSafeSkippingFilter(predicate, getStatColumn))
    withSerializedStats(filtered)
  }

  // ==================== Partition Pruning ========================================

  /**
   * Filters the given [[DataFrame]] by the given `partitionFilters`,
   * returning those that match.
   *
   * This is the canonical implementation, shared by V1 and V2.
   * `DeltaLog.filterFileList` delegates here.
   *
   * @param partitionSchema The partition schema
   * @param files The active files, with partition value information
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes Path to `partitionValues`, if nested
   * @param shouldRewritePartitionFilters Whether to rewrite filters
   *        to be over the AddFile schema
   * @return Filtered DataFrame
   */
  def filterFileList(
      partitionSchema: StructType,
      files: DataFrame,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil,
      shouldRewritePartitionFilters: Boolean = true
  ): DataFrame = {
    val rewrittenFilters = if (shouldRewritePartitionFilters) {
      rewritePartitionFilters(
        partitionSchema,
        files.sparkSession.sessionState.conf.resolver,
        partitionFilters,
        partitionColumnPrefixes)
    } else {
      partitionFilters
    }
    val expr = rewrittenFilters
      .reduceLeftOption(And)
      .getOrElse(Literal.TrueLiteral)
    files.filter(Column(expr))
  }

  /**
   * Rewrite the given `partitionFilters` to be used for filtering
   * partition values.
   *
   * Partition columns are stored as keys of a Map type instead of
   * attributes in the AddFile schema, so we need to explicitly
   * resolve them here.
   *
   * This is the canonical implementation, shared by V1 and V2.
   * `DeltaLog.rewritePartitionFilters` delegates here.
   *
   * @param partitionFilters Filters on the partition columns
   * @param partitionColumnPrefixes Path to `partitionValues` column
   */
  def rewritePartitionFilters(
      partitionSchema: StructType,
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil
  ): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // Strip backticks for special column names like `a.a`
        val unquoted = a.name
          .stripPrefix("`").stripSuffix("`")
        val partCol = partitionSchema
          .find(f => resolver(f.name, unquoted))
        partCol match {
          case Some(f: StructField) =>
            val name = DeltaColumnMapping.getPhysicalName(f)
            Cast(
              UnresolvedAttribute(
                partitionColumnPrefixes ++
                  Seq("partitionValues", name)),
              f.dataType)
          case None =>
            UnresolvedAttribute(
              partitionColumnPrefixes ++
                Seq("partitionValues", a.name))
        }
    })
  }

  /**
   * Apply partition pruning to a flat AddFile DataFrame.
   *
   * Convenience wrapper over [[filterFileList]].
   *
   * @param addFilesDF           Flat AddFile DataFrame
   * @param partitionSchema      Partition schema
   * @param partitionFilterExprs Catalyst expressions for partitions
   * @return Filtered DataFrame
   */
  def applyPartitionPruning(
      addFilesDF: DataFrame,
      partitionSchema: StructType,
      partitionFilterExprs: Seq[Expression]): DataFrame = {
    if (partitionFilterExprs.isEmpty) addFilesDF
    else filterFileList(
      partitionSchema, addFilesDF, partitionFilterExprs)
  }

  // ==================== Full Pipeline: Partition + Data Skipping =================

  /**
   * Apply both partition pruning and data skipping to a flat
   * AddFile DataFrame. Combines:
   *  1. Partition pruning via [[filterFileList]]
   *  2. Data skipping via stats-based filtering
   *
   * Shared between V1 and V2.
   *
   * @param addFilesDF              Flat AddFile DataFrame
   * @param partitionFilterExprs    Catalyst expressions for partition columns
   * @param dataSkippingPredicate   DataSkippingPredicate for data columns (None = no data skipping)
   * @param partitionSchema         Partition schema for partition filter rewriting
   * @param statsSchema             Schema for parsing JSON stats
   * @param getStatColumn           Function to resolve StatsColumn to Column
   * @return Filtered DataFrame with same schema as input
   */
  def applyAllFilters(
      addFilesDF: DataFrame,
      partitionFilterExprs: Seq[Expression],
      dataSkippingPredicate: Option[DataSkippingPredicate],
      partitionSchema: StructType,
      statsSchema: StructType,
      getStatColumn: StatsColumn => Option[Column]): DataFrame = {
    // Step 1: Partition pruning
    var result = applyPartitionPruning(addFilesDF, partitionSchema, partitionFilterExprs)

    // Step 2: Data skipping
    dataSkippingPredicate.foreach { predicate =>
      result = applyDataSkipping(result, predicate, statsSchema, getStatColumn)
    }

    result
  }

  /**
   * Split filter expressions into partition-only and data filters.
   *
   * Partition-only: all referenced columns are partition columns.
   * Data filters: at least one referenced column is NOT a partition column.
   *
   * Shared between V1 and V2.
   */
  def splitFilters(
      filterExprs: Seq[Expression],
      partitionColumns: Seq[String],
      spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    filterExprs.partition(isPredicatePartitionColumnsOnly(_, partitionColumns, spark))
  }

  // ==================== Size Collector (shared V1/V2) ==============

  /**
   * Input encoders for the size collector UDF.
   * Moved from DataSkippingReader companion object to be shared.
   */
  lazy val sizeCollectorInputEncoders
    : Seq[Option[ExpressionEncoder[_]]] = Seq(
    Option(ExpressionEncoder[Boolean]()),
    Option(ExpressionEncoder[java.lang.Long]()),
    Option(ExpressionEncoder[java.lang.Long]()),
    Option(ExpressionEncoder[java.lang.Long]()))

  /**
   * Build a UDF-based filter that counts bytes/rows/files via an
   * [[ArrayAccumulator]]. The UDF is a pass-through boolean filter
   * with the side-effect of accumulating size statistics.
   *
   * Moved from V1 DataSkippingReader.buildSizeCollectorFilter to be
   * shared between V1 and V2.
   *
   * Accumulator slots:
   *  0 - bytes compressed (from `size` column)
   *  1 - physical row count (from numRecords stat, -1 if unknown)
   *  2 - file count (1 per file)
   *  3 - logical row count (physical - DV cardinality, -1 if unknown)
   *
   * @param spark          SparkSession (for accumulator registration)
   * @param numRecordsCol  Column for stats.numRecords (V1 may use
   *                       column-mapping; V2 uses col("stats.numRecords"))
   * @return (accumulator, filterFn) where filterFn wraps a boolean
   *         Column with the size-collecting UDF
   */
  def buildSizeCollectorFilter(
      spark: SparkSession,
      numRecordsCol: Column
  ): (ArrayAccumulator, Column => Column) = {
    val bytesCompressed = col("size")
    val dvCardinality =
      coalesce(col("deletionVector.cardinality"), lit(0L))
    val logicalRows =
      (numRecordsCol - dvCardinality).as("logicalRows")

    val accumulator = new ArrayAccumulator(4)
    spark.sparkContext.register(accumulator)

    val collector = (
        include: Boolean,
        bytesCompressed: java.lang.Long,
        logicalRows: java.lang.Long,
        rows: java.lang.Long) => {
      if (include) {
        accumulator.add((0, bytesCompressed))
        accumulator.add(
          (1, Option(rows).map(_.toLong).getOrElse(-1L)))
        accumulator.add((2, 1))
        accumulator.add(
          (3, Option(logicalRows)
            .map(_.toLong).getOrElse(-1L)))
      }
      include
    }
    val collectorUdf = SparkUserDefinedFunction(
      f = collector,
      dataType = BooleanType,
      inputEncoders = sizeCollectorInputEncoders,
      deterministic = false)

    (accumulator,
      collectorUdf(
        _: Column, bytesCompressed, logicalRows, numRecordsCol))
  }

  // ==================== Scan Pipeline ==============================

  /**
   * Result of the shared scan pipeline.
   *
   * Accumulators are populated as a side-effect when
   * [[filteredDF]] is materialized (collected / iterated).
   * Call [[totalSize]] / [[partitionSize]] / [[scanSize]]
   * ONLY after the DataFrame has been fully consumed.
   */
  case class ScanPipelineResult(
      filteredDF: DataFrame,
      totalAccumulator: ArrayAccumulator,
      partitionAccumulator: ArrayAccumulator,
      scanAccumulator: ArrayAccumulator) {
    def totalSize: DataSize = DataSize(totalAccumulator)
    def partitionSize: DataSize = DataSize(partitionAccumulator)
    def scanSize: DataSize = DataSize(scanAccumulator)
  }

  /**
   * Execute the shared scan pipeline: partition pruning + data
   * skipping + size collection in ONE DataFrame pass.
   *
   * Both V1 and V2 delegate here. Three accumulator-instrumented
   * UDF filters are applied together:
   *  1. totalFilter(TRUE) - counts ALL files
   *  2. partFilter(partitionFilters) - counts partition-matched
   *  3. scanFilter(safeDataFilter) - counts data-skipped
   *
   * @param withStatsDF      DataFrame with parsed stats column
   * @param partitionFilters Partition filter as a Column expression
   *                         (already rewritten to partitionValues.*)
   * @param dataSkippingPred Data skipping predicate
   * @param getStatColumn    Resolves StatsColumn to Column
   * @param numRecordsCol    Column for stats.numRecords
   * @param spark            SparkSession
   * @return ScanPipelineResult (DF + accumulators)
   */
  def executeScanPipeline(
      withStatsDF: DataFrame,
      partitionFilters: Column,
      dataSkippingPred: DataSkippingPredicate,
      getStatColumn: StatsColumn => Option[Column],
      numRecordsCol: Column,
      spark: SparkSession): ScanPipelineResult = {
    val trueLit: Column = Column(TrueLiteral)

    val (totalAcc, totalFilter) =
      buildSizeCollectorFilter(spark, numRecordsCol)
    val (partAcc, partFilter) =
      buildSizeCollectorFilter(spark, numRecordsCol)
    val (scanAcc, scanFilter) =
      buildSizeCollectorFilter(spark, numRecordsCol)

    val safeFilter =
      buildSafeSkippingFilter(dataSkippingPred, getStatColumn)

    val filteredDF = withStatsDF.where(
      totalFilter(trueLit) &&
        partFilter(partitionFilters) &&
        scanFilter(safeFilter))

    ScanPipelineResult(filteredDF, totalAcc, partAcc, scanAcc)
  }

  /**
   * Build a partition filter Column from rewritten partition
   * expressions. Convenience for callers that have already split
   * and rewritten their partition filters.
   *
   * @return Column that is TRUE when no partition filters exist
   */
  def buildPartitionFilterColumn(
      partitionFilterExprs: Seq[Expression]): Column = {
    partitionFilterExprs
      .reduceLeftOption(And)
      .map(e => Column(e))
      .getOrElse(Column(TrueLiteral))
  }
}

/**
 * Shared data filters builder containing the full V1 constructDataFilters logic.
 *
 * This class is extracted from V1's DataSkippingReaderBase.DataFiltersBuilder to allow
 * both V1 and V2 to share the same comprehensive expression-to-predicate conversion.
 *
 * V1 usage: DataSkippingReader.DataFiltersBuilder extends this class, adding partition-like
 * filter rewriting methods that depend on V1-specific context (getStatsColumnOpt, etc.).
 *
 * V2 usage: DataFiltersBuilderV2 creates an instance directly to convert Spark filters.
 *
 * Supported expression types (matching V1 exactly):
 *  - And, Or, Not (with DeMorgan rewrites)
 *  - EqualTo, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual
 *  - EqualNullSafe (rewritten to EqualTo + IsNotNull)
 *  - IsNull, IsNotNull (with null-intolerant pushdown)
 *  - StartsWith
 *  - In, InSet, InSubqueryExec
 *
 * @param spark         SparkSession (for DeltaSQLConf access)
 * @param statsProvider StatsProvider for resolving stat columns
 */
private[stats] class SharedDataFiltersBuilder(
    protected val spark: SparkSession,
    protected val statsProvider: StatsProvider) {

  protected object SkippingEligibleExpression extends GenericSkippingEligibleExpression()

  // Convenience literals
  protected val trueLiteral: Column = Column(TrueLiteral)
  protected val falseLiteral: Column = Column(FalseLiteral)

  /** Main entry point: convert a Catalyst Expression to DataSkippingPredicate. */
  def apply(dataFilter: Expression): Option[DataSkippingPredicate] =
    constructDataFilters(dataFilter, isNullExpansionDepth = 0)

  /**
   * Helper to construct a [[DataSkippingPredicate]] for an IsNull predicate on
   * null-intolerant expressions. This method is only valid if the passed-in expression
   * returns null for any null children.
   */
  protected def constructIsNullFilterForNullIntolerant(
      expr: Expression,
      isNullExpansionDepth: Int): Option[DataSkippingPredicate] = {
    val filters = expr.children.map {
      case l: Literal =>
        if (l.value == null) {
          Some(DataSkippingPredicate(trueLiteral))
        } else {
          Some(DataSkippingPredicate(falseLiteral))
        }
      case c => constructDataFilters(IsNull(c), isNullExpansionDepth)
    }
    filters.reduceOption { (a, b) =>
      (a, b) match {
        case (Some(a), Some(b)) =>
          Some(DataSkippingPredicate(a.expr || b.expr, a.referencedStats ++ b.referencedStats))
        case _ => None
      }
    }.flatten
  }

  /**
   * Helper for expression types that represent an IN-list of literal values.
   * For excessively long IN-lists, we test whether the file's min/max range overlaps
   * the range spanned by the list's smallest and largest elements.
   */
  private def constructLiteralInListDataFilters(
      a: Expression,
      possiblyNullValues: Seq[Any],
      isNullExpansionDepth: Int): Option[DataSkippingPredicate] = {
    val values = possiblyNullValues.filter(_ != null)
    if (values.isEmpty) {
      return Some(DataSkippingPredicate(falseLiteral))
    }

    val (_, dt, _) = SkippingEligibleExpression.unapply(a).getOrElse {
      return None
    }

    lazy val ordering = TypeUtils.getInterpretedOrdering(dt)
    if (!SkippingEligibleDataType(dt)) {
      None
    } else {
      val min = Literal(values.min(ordering), dt)
      val max = Literal(values.max(ordering), dt)
      constructDataFilters(
        And(GreaterThanOrEqual(max, a), LessThanOrEqual(min, a)), isNullExpansionDepth)
    }
  }

  // scalastyle:off line.size.limit
  /**
   * Returns a file skipping predicate expression, derived from the user query, which uses column
   * statistics to prune away files that provably contain no rows the query cares about.
   *
   * This is the full V1 logic, extracted from DataSkippingReaderBase.DataFiltersBuilder to
   * be shared between V1 and V2.
   *
   * Key rules:
   * 1. constructDataFilters(e) must return TRUE unless we can prove e won't return TRUE
   *    for any row the file might contain.
   * 2. Unsafe to skip on ops that return NULL or error for non-NULL inputs.
   * 3. NOT is dangerous: Not(constructDataFilters(e)) != constructDataFilters(Not(e)) in general.
   */
  private[stats] def constructDataFilters(
      dataFilter: Expression,
      isNullExpansionDepth: Integer): Option[DataSkippingPredicate] = dataFilter match {
    // Expressions that contain only literals are not eligible for skipping.
    case cmp: Expression if cmp.children.forall(areAllLeavesLiteral) => None

    // AND: both legs contribute independently; one-leg success is still useful.
    case And(e1, e2) =>
      val e1Filter = constructDataFilters(e1, isNullExpansionDepth)
      val e2Filter = constructDataFilters(e2, isNullExpansionDepth)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(DataSkippingPredicate(
          e1Filter.get.expr && e2Filter.get.expr,
          e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
      } else if (e1Filter.isDefined) {
        e1Filter
      } else {
        e2Filter // possibly None
      }

    // DeMorgan: NOT(AND(a, b)) => OR(NOT(a), NOT(b))
    case Not(And(e1, e2)) =>
      constructDataFilters(Or(Not(e1), Not(e2)), isNullExpansionDepth)

    // OR: both legs must succeed; if either fails, no filtering power.
    case Or(e1, e2) =>
      val e1Filter = constructDataFilters(e1, isNullExpansionDepth)
      val e2Filter = constructDataFilters(e2, isNullExpansionDepth)
      if (e1Filter.isDefined && e2Filter.isDefined) {
        Some(DataSkippingPredicate(
          e1Filter.get.expr || e2Filter.get.expr,
          e1Filter.get.referencedStats ++ e2Filter.get.referencedStats))
      } else {
        None
      }

    // DeMorgan: NOT(OR(a, b)) => AND(NOT(a), NOT(b))
    case Not(Or(e1, e2)) =>
      constructDataFilters(And(Not(e1), Not(e2)), isNullExpansionDepth)

    // IsNull: match any file whose null count > 0
    case IsNull(SkippingEligibleColumn(a, _)) =>
      DataFiltersBuilderUtils.constructIsNullFilter(statsProvider, a)

    // IsNull pushdown through null-intolerant expressions
    case IsNull(e @ (_: GreaterThan | _: GreaterThanOrEqual | _: LessThan | _: LessThanOrEqual |
        _: EqualTo | _: Not | _: StartsWith)) if spark.conf.get(
          DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) =>
      constructIsNullFilterForNullIntolerant(e, isNullExpansionDepth)

    // IsNull(And): custom pushdown with depth tracking
    case IsNull(And(left, right)) if spark.conf.get(
        DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) && (isNullExpansionDepth <=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_MAX_DEPTH)) =>
      constructDataFilters(
        And(
          Or(IsNull(left), IsNull(right)),
          Not(
            Or(
              EqualNullSafe(left, FalseLiteral),
              EqualNullSafe(right, FalseLiteral)
            )
          )
        ),
        isNullExpansionDepth = isNullExpansionDepth + 1
      )

    // IsNull(Or): custom pushdown with depth tracking
    case IsNull(Or(left, right)) if spark.conf.get(
        DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_ENABLED) && (isNullExpansionDepth <=
          spark.conf.get(DeltaSQLConf.DELTA_DATASKIPPING_ISNULL_PUSHDOWN_EXPRS_MAX_DEPTH)) =>
      constructDataFilters(
        And(
          Or(IsNull(left), IsNull(right)),
          Not(
            Or(
              EqualNullSafe(left, TrueLiteral),
              EqualNullSafe(right, TrueLiteral)
            )
          )
        ),
        isNullExpansionDepth = isNullExpansionDepth + 1
      )

    case Not(IsNull(e)) =>
      constructDataFilters(IsNotNull(e), isNullExpansionDepth)

    // IsNotNull: match any file whose null count < row count
    case IsNotNull(SkippingEligibleColumn(a, _)) =>
      DataFiltersBuilderUtils.constructNotNullFilter(statsProvider, a)

    case Not(IsNotNull(e)) =>
      constructDataFilters(IsNull(e), isNullExpansionDepth)

    // EqualTo: min <= v AND v <= max
    case EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.equalTo(statsProvider, c, v)
    case EqualTo(v: Literal, a) =>
      constructDataFilters(EqualTo(a, v), isNullExpansionDepth)

    // NotEqual: min < v OR v < max
    case Not(EqualTo(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v))) =>
      builder.notEqualTo(statsProvider, c, v)
    case Not(EqualTo(v: Literal, a)) =>
      constructDataFilters(Not(EqualTo(a, v)), isNullExpansionDepth)

    // EqualNullSafe: rewrite to EqualTo + IsNotNull / IsNull
    case EqualNullSafe(a, v: Literal) =>
      val rewrittenExpr = if (v.value != null) And(IsNotNull(a), EqualTo(a, v)) else IsNull(a)
      constructDataFilters(rewrittenExpr, isNullExpansionDepth)
    case EqualNullSafe(v: Literal, a) =>
      constructDataFilters(EqualNullSafe(a, v), isNullExpansionDepth)
    case Not(EqualNullSafe(a, v: Literal)) =>
      val rewrittenExpr = if (v.value != null) And(IsNotNull(a), EqualTo(a, v)) else IsNull(a)
      constructDataFilters(Not(rewrittenExpr), isNullExpansionDepth)
    case Not(EqualNullSafe(v: Literal, a)) =>
      constructDataFilters(Not(EqualNullSafe(a, v)), isNullExpansionDepth)

    // LessThan: min < v
    case LessThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.lessThan(statsProvider, c, v)
    case LessThan(v: Literal, a) =>
      constructDataFilters(GreaterThan(a, v), isNullExpansionDepth)
    case Not(LessThan(a, b)) =>
      constructDataFilters(GreaterThanOrEqual(a, b), isNullExpansionDepth)

    // LessThanOrEqual: min <= v
    case LessThanOrEqual(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.lessThanOrEqual(statsProvider, c, v)
    case LessThanOrEqual(v: Literal, a) =>
      constructDataFilters(GreaterThanOrEqual(a, v), isNullExpansionDepth)
    case Not(LessThanOrEqual(a, b)) =>
      constructDataFilters(GreaterThan(a, b), isNullExpansionDepth)

    // GreaterThan: max > v
    case GreaterThan(SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.greaterThan(statsProvider, c, v)
    case GreaterThan(v: Literal, a) =>
      constructDataFilters(LessThan(a, v), isNullExpansionDepth)
    case Not(GreaterThan(a, b)) =>
      constructDataFilters(LessThanOrEqual(a, b), isNullExpansionDepth)

    // GreaterThanOrEqual: max >= v
    case GreaterThanOrEqual(
    SkippingEligibleExpression(c, _, builder), SkippingEligibleLiteral(v)) =>
      builder.greaterThanOrEqual(statsProvider, c, v)
    case GreaterThanOrEqual(v: Literal, a) =>
      constructDataFilters(LessThanOrEqual(a, v), isNullExpansionDepth)
    case Not(GreaterThanOrEqual(a, b)) =>
      constructDataFilters(LessThan(a, b), isNullExpansionDepth)

    // StartsWith: prefix match on min/max
    case StartsWith(SkippingEligibleColumn(a, _), v @ Literal(s: UTF8String, _: StringType)) =>
      statsProvider.getPredicateWithStatTypesIfExists(a, StringType, MIN, MAX) { (min, max) =>
        val sLen = s.numChars()
        substring(min, 0, sLen) <= v && substring(max, 0, sLen) >= v
      }

    // In-list: range test covering entire list
    case in @ In(a, values) if in.inSetConvertible =>
      constructLiteralInListDataFilters(
        a, values.map(_.asInstanceOf[Literal].value), isNullExpansionDepth)

    // InSet (optimizer converts long IN-lists to InSet)
    case InSet(a, values) =>
      constructLiteralInListDataFilters(a, values.toSeq, isNullExpansionDepth)

    // InSubqueryExec: materialized subquery result
    case in: InSubqueryExec =>
      in.values().flatMap(v =>
        constructLiteralInListDataFilters(in.child, v.toSeq, isNullExpansionDepth))

    // Remove redundant pairs of NOT
    case Not(Not(e)) =>
      constructDataFilters(e, isNullExpansionDepth)

    // NOT is dangerous in general; must special-case each Not(e) we support.
    case Not(_) => None

    // Unknown expression type - can't use for data skipping.
    case _ => None
  }
  // scalastyle:on line.size.limit

  /** Check if all leaves of an expression are literals (iterative to avoid stack overflow). */
  private[stats] def areAllLeavesLiteral(e: Expression): Boolean = {
    val stack = scala.collection.mutable.Stack[Expression]()
    def pushIfNonLiteral(e: Expression): Unit = e match {
      case _: Literal =>
      case _ => stack.push(e)
    }
    pushIfNonLiteral(e)
    while (stack.nonEmpty) {
      val children = stack.pop().children
      if (children.isEmpty) {
        return false
      }
      children.foreach(pushIfNonLiteral)
    }
    true
  }
}
