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

import org.apache.spark.sql.delta.{DataFrameUtils, DeltaColumnMapping, NoMapping}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.expressions.EncodeNestedVariantAsZ85String

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.StructType

/**
 * Shared helpers for preparing Delta write statistics. These helpers intentionally stop before
 * Spark's FileFormatWriter orchestration so alternative writer paths can reuse the same stats
 * expression setup.
 */
object DeltaWriteStatsUtils {

  /**
   * Return a tuple of (outputStatsCollectionSchema, statsCollectionSchema).
   * outputStatsCollectionSchema is the data source schema from DataFrame used for stats collection.
   * It contains the columns in the DataFrame output, excluding the partition columns.
   * tableStatsCollectionSchema is the schema to collect stats for. It contains the columns in the
   * table schema, excluding the partition columns.
   * Note: We only collect NULL_COUNT stats (as the number of rows) for the columns in
   * statsCollectionSchema but missing in outputStatsCollectionSchema.
   */
  def getStatsSchema(
      dataFrameOutput: Seq[Attribute],
      partitionSchema: StructType,
      metadata: Metadata): (Seq[Attribute], Seq[Attribute]) = {
    val partitionColNames = partitionSchema.map(_.name).toSet

    val outputStatsCollectionSchema = dataFrameOutput
      .filterNot(c => partitionColNames.contains(c.name))

    val statsTableSchema = toAttributes(metadata.schema)
    val mappedStatsTableSchema = if (metadata.columnMappingMode == NoMapping) {
      statsTableSchema
    } else {
      DeltaColumnMapping.createPhysicalAttributes(
        statsTableSchema, metadata.schema, metadata.columnMappingMode)
    }

    val tableStatsCollectionSchema = mappedStatsTableSchema
      .filterNot(c => partitionColNames.contains(c.name))

    (outputStatsCollectionSchema, tableStatsCollectionSchema)
  }

  /**
   * Returns a resolved `statsCollection.statsCollector` expression with `statsDataSchema`
   * attributes re-resolved to be used for writing Delta file stats.
   */
  def getStatsColExpr(
      spark: SparkSession,
      statsDataSchema: Seq[Attribute],
      statsCollection: StatisticsCollection): (Expression, Seq[Attribute]) = {
    val resolvedPlan = DataFrameUtils.ofRows(spark, LocalRelation(statsDataSchema))
      .select(to_json(Column(
        EncodeNestedVariantAsZ85String(statsCollection.statsCollector.expr))))
      .queryExecution.analyzed

    // We have to use the new attributes with regenerated attribute IDs, because the Analyzer
    // doesn't guarantee that attributes IDs will stay the same.
    val newStatsDataSchema = resolvedPlan.children.head.output

    resolvedPlan.expressions.head -> newStatsDataSchema
  }
}
