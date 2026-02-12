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

import io.delta.kernel.Snapshot
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MIN, MAX, NULL_COUNT, NUM_RECORDS}
import org.apache.spark.sql.types.StructType

/**
 * Internal Scala bridge for V2 data skipping.
 *
 * This object bridges V2-specific concerns to the shared
 * [[DefaultScanPredicateBuilder]] and [[DefaultScanPlanner]].
 *
 * V2-specific logic:
 *  1. [[createGetStatColumnFn]] - maps stat paths using simple column paths
 *     (no column mapping).
 *  2. [[resolveFilterExpressions]] - resolves UnresolvedAttribute to
 *     AttributeReference.
 *  3. Kernel Snapshot to Spark schema conversion.
 *  4. Unwrap/re-wrap the `{add: struct}` nesting from distributed log replay.
 *  5. Stats JSON parse / serialize.
 *
 * Everything else (partition pruning, data skipping, constructDataFilters,
 * verifyStatsForFilter) is shared with V1 via [[DefaultScanPredicateBuilder]]
 * and [[DataFiltersBuilderUtils]].
 */
object DataFiltersBuilderV2 {

  // ==================== V2-SPECIFIC: StatsProvider creation ======================================

  /**
   * Creates the V2-specific getStatColumn function.
   * Maps StatsColumn paths to DataFrame column references using simple paths
   * (no V1 column mapping / physical names).
   */
  private[stats] def createGetStatColumnFn(
      snapshot: Snapshot,
      spark: SparkSession): StatsColumn => Option[Column] = {
    (statCol: StatsColumn) => {
      val statType = statCol.pathToStatType.head
      val columnPath = statCol.pathToColumn

      if (columnPath.isEmpty) {
        if (statType == NUM_RECORDS) Some(col(s"stats.$NUM_RECORDS"))
        else None
      } else {
        val columnName = columnPath.reverse.mkString(".")
        statType match {
          case MIN => Some(col(s"stats.$MIN.$columnName"))
          case MAX => Some(col(s"stats.$MAX.$columnName"))
          case NULL_COUNT => Some(col(s"stats.$NULL_COUNT.$columnName"))
          case _ => None
        }
      }
    }
  }

  // ==================== V2-SPECIFIC: Attribute Resolution ========================================

  /**
   * Resolve UnresolvedAttribute references against the table schema.
   * DeltaSourceUtils.translateFilters creates UnresolvedAttribute, but V1's
   * SkippingEligibleColumn requires resolved AttributeReference.
   */
  private def resolveFilterExpressions(
      expr: Expression, tableSchema: StructType): Expression = {
    expr.transformUp {
      case u: UnresolvedAttribute if u.nameParts.size == 1 =>
        val name = u.nameParts.head
        tableSchema.find(_.name.equalsIgnoreCase(name)) match {
          case Some(field) =>
            AttributeReference(field.name, field.dataType, field.nullable)()
          case None => u
        }
    }
  }

  def getSparkTableSchema(snapshot: Snapshot): StructType = {
    io.delta.spark.internal.v2.utils.SchemaUtils
      .convertKernelSchemaToSparkSchema(snapshot.getSchema())
  }

  // ==================== Factory: PredicateBuilder + Planner ======================================

  /**
   * Creates a shared [[DefaultScanPredicateBuilder]] for V2.
   *
   * This bridges the V2-specific context (Kernel Snapshot, simple stat column paths)
   * to the shared [[DefaultScanPredicateBuilder]] interface. The returned builder
   * accepts flat AddFile DataFrames with parsed stats and resolved Expressions.
   *
   * @param snapshot        Kernel snapshot (provides table schema)
   * @param spark           SparkSession
   * @param partitionSchema Partition schema for partition filter rewriting
   * @return A [[DefaultScanPredicateBuilder]] backed by shared pipeline logic
   */
  def createPredicateBuilder(
      snapshot: Snapshot,
      spark: SparkSession,
      partitionSchema: StructType): DefaultScanPredicateBuilder = {
    val getStatColumn = createGetStatColumnFn(snapshot, spark)
    val numRecordsCol = col(s"stats.$NUM_RECORDS")
    val statsProvider = new StatsProvider(getStatColumn)
    val builder = new SharedDataFiltersBuilder(spark, statsProvider)

    new DefaultScanPredicateBuilder(
      spark = spark,
      getStatColumn = getStatColumn,
      numRecordsCol = numRecordsCol,
      partitionSchema = partitionSchema,
      buildDataFilters = builder(_)
      // V2 uses defaults: useStats=true, no partition-like (for now)
    )
  }

  /**
   * Creates a [[DefaultScanPlanner]] for V2.
   *
   * The planner composes a V2-specific data source with the shared
   * [[DefaultScanPredicateBuilder]].
   *
   * @param dataSource       Supplier for the AddFile DataFrame
   *                         (with parsed stats, from log replay)
   * @param predicateBuilder The predicate builder created by
   *                         [[createPredicateBuilder]]
   * @return A [[DefaultScanPlanner]] ready for V2 use
   */
  def createPlanner(
      dataSource: () => DataFrame,
      predicateBuilder: DefaultScanPredicateBuilder): DefaultScanPlanner = {
    new DefaultScanPlanner(
      dataSource = dataSource,
      predicateBuilder = predicateBuilder
    )
  }

  // ==================== V2 Helpers (called from Java) ============================================

  /**
   * Creates a V2 data source function: distributed log replay -> unwrap
   * `{add: struct}` -> parse stats JSON to struct.
   *
   * The returned function is suitable as the `dataSource` parameter for
   * [[DefaultScanPlanner]].
   *
   * @param wrappedDFSupplier Supplier for the wrapped DataFrame
   *                          (e.g., distributed log replay)
   * @param snapshot          Kernel snapshot (provides table schema for stats)
   * @return Function that returns flat AddFile DF with parsed stats
   */
  def prepareV2DataSource(
      wrappedDFSupplier: () => DataFrame,
      snapshot: Snapshot): () => DataFrame = {
    val tableSchema = getSparkTableSchema(snapshot)
    val statsSchema = DataFiltersBuilderUtils.buildStatsSchema(tableSchema)
    () => {
      val wrappedDF = wrappedDFSupplier()
      val addFiles = wrappedDF.selectExpr("add.*")
      DataFiltersBuilderUtils.withParsedStats(addFiles, statsSchema)
    }
  }

  /**
   * V2-specific post-processing: serialize stats back to JSON and
   * re-wrap as `{add: struct}`.
   *
   * @param flatFilteredDF Flat AddFile DF with parsed stats (from pipeline)
   * @return DataFrame with schema `{add: {path, partitionValues, stats, ...}}`
   */
  def postProcessV2(flatFilteredDF: DataFrame): DataFrame = {
    DataFiltersBuilderUtils.withSerializedStats(flatFilteredDF)
      .select(struct(col("*")).as("add"))
  }

  /**
   * V2-specific helper: convert Spark [[Filter]]s to resolved Catalyst
   * Expressions.
   *
   * Steps:
   *  1. [[DeltaSourceUtils.translateFilters]] to get Catalyst expressions
   *  2. [[resolveFilterExpressions]] to resolve UnresolvedAttribute ->
   *     AttributeReference
   *
   * @param filters     Spark Filter array
   * @param tableSchema Table schema for attribute resolution
   * @return Resolved Catalyst Expressions
   */
  def filtersToResolvedExpressions(
      filters: Array[Filter],
      tableSchema: StructType): Seq[Expression] = {
    filters.map { f =>
      val expr = DeltaSourceUtils.translateFilters(Array(f))
      resolveFilterExpressions(expr, tableSchema)
    }.toSeq
  }
}
