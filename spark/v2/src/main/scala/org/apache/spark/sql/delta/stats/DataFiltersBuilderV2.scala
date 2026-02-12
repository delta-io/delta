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
 * Internal Scala bridge for [[io.delta.spark.internal.v2.read.DeltaScanPredicateBuilder]]
 * (Component 2: PredicateBuilder).
 *
 * This object exists because the predicate rewriting pipeline requires Scala
 * features (pattern matching, `transformUp`, Catalyst `Expression` manipulation)
 * that are impractical to express in pure Java. The public Java component
 * [[io.delta.spark.internal.v2.read.DeltaScanPredicateBuilder]] delegates here.
 *
 * V2-specific logic:
 *  1. [[createGetStatColumnFn]] - maps stat paths using simple column paths
 *     (no column mapping).
 *  2. [[resolveFilterExpressions]] - resolves UnresolvedAttribute to
 *     AttributeReference.
 *  3. Kernel Snapshot to Spark schema conversion.
 *  4. Unwrap/re-wrap the `{add: struct}` nesting from distributed log replay.
 *
 * Everything else (partition pruning, data skipping, constructDataFilters,
 * verifyStatsForFilter) is shared with V1 via [[DataFiltersBuilderUtils]].
 */
object DataFiltersBuilderV2 {

  /**
   * Apply both partition pruning and data skipping to a wrapped AddFile DataFrame.
   *
   * V2-specific steps: unwrap {add: struct}, convert Filter[] to resolved Catalyst
   * expressions, then delegate to shared [[DataFiltersBuilderUtils.applyAllFilters]],
   * then re-wrap.
   *
   * Called from ScanPredicateBuilder (Java).
   *
   * @param wrappedDF        DataFrame with {add: {path, partitionValues, stats, ...}}
   * @param allFilters       All pushable Spark filters (partition + data)
   * @param partitionSchema  Partition schema for partition filter rewriting
   * @param snapshot         Kernel snapshot (provides table schema)
   * @param spark            SparkSession
   * @return Filtered DataFrame with same wrapped schema {add: {...}}
   */
  def applyAllFilters(
      wrappedDF: DataFrame,
      allFilters: Array[Filter],
      partitionSchema: StructType,
      snapshot: Snapshot,
      spark: SparkSession): DataFrame = {
    if (allFilters.isEmpty) return wrappedDF

    // V2-specific: unwrap the "add" struct
    val addFiles = wrappedDF.selectExpr("add.*")

    val tableSchema = getSparkTableSchema(snapshot)
    val getStatColumn = createGetStatColumnFn(snapshot, spark)
    val statsProvider = new StatsProvider(getStatColumn)
    val partitionColumns = partitionSchema.fieldNames.toSeq

    // V2-specific: convert Filter[] to Catalyst and resolve attributes
    val filterExprs = allFilters.map(f => DeltaSourceUtils.translateFilters(Array(f)))
    val resolvedExprs = filterExprs.map(resolveFilterExpressions(_, tableSchema))

    // Shared: split filters
    val (partitionFilterExprs, dataFilterExprs) =
      DataFiltersBuilderUtils.splitFilters(resolvedExprs.toSeq, partitionColumns, spark)

    // Shared: build data skipping predicate using V1's constructDataFilters
    val dataSkippingPredicate = {
      val builder = new SharedDataFiltersBuilder(spark, statsProvider)
      val predicates = dataFilterExprs.flatMap(builder(_))
      if (predicates.isEmpty) None
      else Some(predicates.reduce { (a, b) =>
        DataSkippingPredicate(a.expr && b.expr, a.referencedStats ++ b.referencedStats)
      })
    }

    val statsSchema = DataFiltersBuilderUtils.buildStatsSchema(tableSchema)

    // Shared: partition pruning + data skipping pipeline
    val filtered = DataFiltersBuilderUtils.applyAllFilters(
      addFiles, partitionFilterExprs, dataSkippingPredicate,
      partitionSchema, statsSchema, getStatColumn)

    // V2-specific: re-wrap as "add" struct
    filtered.select(struct(col("*")).as("add"))
  }

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

  private def getSparkTableSchema(snapshot: Snapshot): StructType = {
    io.delta.spark.internal.v2.utils.SchemaUtils
      .convertKernelSchemaToSparkSchema(snapshot.getSchema())
  }
}
