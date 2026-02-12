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
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.delta.sources.DeltaSourceUtils
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MIN, MAX, NULL_COUNT, NUM_RECORDS}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

/**
 * Thin V2 adapter that creates a Kernel-Snapshot-based StatsProvider, then delegates
 * ALL data skipping logic to the shared [[SharedDataFiltersBuilder]].
 *
 * The only V2-specific code here is:
 *  1. [[createGetStatColumnFn]] - maps StatsColumn paths to DataFrame column references
 *     using simple column paths (no V1 column mapping / physical names).
 *  2. [[resolveFilterExpressions]] - resolves UnresolvedAttribute from translateFilters
 *     to AttributeReference using the table schema, so V1's extractors can match them.
 *
 * Everything else - constructDataFilters, predicate building, stats schema,
 * verifyStatsForFilter, applyDataSkipping - is shared with V1 via
 * [[SharedDataFiltersBuilder]] and [[DataFiltersBuilderUtils]].
 */
object DataFiltersBuilderV2 {

  /**
   * Apply data skipping to a flat AddFile DataFrame (the main entry point).
   *
   * Delegates to [[DataFiltersBuilderUtils.applyDataSkipping]] with predicates
   * built by [[SharedDataFiltersBuilder]] (same constructDataFilters as V1).
   *
   * @param addFilesDF  Flat AddFile DataFrame with {path, stats (string), ...}
   * @param filters     Spark V2 filters for data skipping
   * @param snapshot    Kernel snapshot (provides schema)
   * @param spark       SparkSession
   * @return Filtered DataFrame with same schema as input
   */
  def applyDataSkipping(
      addFilesDF: DataFrame,
      filters: Array[Filter],
      snapshot: Snapshot,
      spark: SparkSession): DataFrame = {
    if (filters.isEmpty) return addFilesDF

    val tableSchema = getSparkTableSchema(snapshot)
    val getStatColumn = createGetStatColumnFn(snapshot, spark)
    val statsProvider = new StatsProvider(getStatColumn)

    // Convert filters to DataSkippingPredicate using shared V1 constructDataFilters
    val predicateOpt = convertFiltersInternal(filters, statsProvider, tableSchema, spark)
    predicateOpt match {
      case None => addFilesDF
      case Some(predicate) =>
        val statsSchema = DataFiltersBuilderUtils.buildStatsSchema(tableSchema)
        DataFiltersBuilderUtils.applyDataSkipping(
          addFilesDF, predicate, statsSchema, getStatColumn)
    }
  }

  /**
   * Converts Spark V2 Filters to a DataSkippingPredicate.
   */
  def convertFiltersToDataSkippingPredicate(
      filters: Array[Filter],
      snapshot: Snapshot,
      spark: SparkSession): Option[DataSkippingPredicate] = {
    if (filters.isEmpty) return None
    val tableSchema = getSparkTableSchema(snapshot)
    val statsProvider = new StatsProvider(createGetStatColumnFn(snapshot, spark))
    convertFiltersInternal(filters, statsProvider, tableSchema, spark)
  }

  /**
   * Converts Spark V2 Filters to a Column expression (just the expr part).
   */
  def convertFiltersToColumn(
      filters: Array[Filter],
      snapshot: Snapshot,
      spark: SparkSession): Option[Column] = {
    convertFiltersToDataSkippingPredicate(filters, snapshot, spark).map(_.expr)
  }

  // ==================== V2-SPECIFIC: StatsProvider creation ======================================

  /**
   * Creates the V2-specific getStatColumn function.
   *
   * This is the ONLY V2-specific code: it maps StatsColumn paths to DataFrame column
   * references using simple "stats.{statType}.{columnName}" paths.
   *
   * V1 equivalent: DataSkippingReader.getStatsColumnOpt (which handles column mapping).
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

  // ==================== INTERNAL ================================================================

  /**
   * Convert Spark V2 filters using the shared [[SharedDataFiltersBuilder]] (same as V1).
   *
   * Key step: we resolve UnresolvedAttribute to AttributeReference against the table
   * schema so that V1's SkippingEligibleColumn/SkippingEligibleExpression extractors
   * can match them. Without resolution, translateFilters produces UnresolvedAttribute
   * which V1's extractors skip (they check arg.resolved).
   */
  private def convertFiltersInternal(
      filters: Array[Filter],
      statsProvider: StatsProvider,
      tableSchema: StructType,
      spark: SparkSession): Option[DataSkippingPredicate] = {
    // Step 1: Convert Spark Filters to Catalyst Expressions
    val expressions = filters.map(f => DeltaSourceUtils.translateFilters(Array(f)))

    // Step 2: Resolve UnresolvedAttribute to AttributeReference using table schema
    val resolved = expressions.map(resolveFilterExpressions(_, tableSchema))

    // Step 3: Use SharedDataFiltersBuilder (identical to V1's constructDataFilters)
    val builder = new SharedDataFiltersBuilder(spark, statsProvider)
    val predicates = resolved.flatMap(builder(_))

    if (predicates.isEmpty) None
    else Some(predicates.reduce { (a, b) =>
      DataSkippingPredicate(a.expr && b.expr, a.referencedStats ++ b.referencedStats)
    })
  }

  /**
   * Resolve UnresolvedAttribute references in a Catalyst expression against the table schema.
   *
   * DeltaSourceUtils.translateFilters creates UnresolvedAttribute for column references,
   * but V1's SkippingEligibleColumn extractor requires resolved AttributeReference
   * (it checks arg.resolved). This method resolves flat column references by looking up
   * the column name in the table schema.
   */
  private def resolveFilterExpressions(
      expr: Expression, tableSchema: StructType): Expression = {
    expr.transformUp {
      case u: UnresolvedAttribute if u.nameParts.size == 1 =>
        val name = u.nameParts.head
        tableSchema.find(_.name.equalsIgnoreCase(name)) match {
          case Some(field) =>
            AttributeReference(field.name, field.dataType, field.nullable)()
          case None => u // leave unresolved if not found
        }
    }
  }

  private def getSparkTableSchema(snapshot: Snapshot): StructType = {
    io.delta.spark.internal.v2.utils.SchemaUtils
      .convertKernelSchemaToSparkSchema(snapshot.getSchema())
  }
}
