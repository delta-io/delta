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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaColumnMapping
import org.apache.spark.sql.delta.stats.DeltaStatistics.{MAX, MIN, NULL_COUNT, NUM_RECORDS, TIGHT_BOUNDS}
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructField, StructType}

/**
 * Interface for data-filter and stats-schema construction behavior.
 *
 * The default implementation is [[DefaultDataFilterSupport]].
 */
private[delta] trait DataFilterSupport {

  def equalTo(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate]

  def notEqualTo(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate]

  def lessThan(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate]

  def lessThanOrEqual(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate]

  def greaterThan(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate]

  def greaterThanOrEqual(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate]

  def filterFileList(
      partitionSchema: StructType,
      files: DataFrame,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil,
      shouldRewritePartitionFilters: Boolean = true,
      onMissingPartitionColumn: String => Unit = _ => ()): DataFrame

  def rewritePartitionFilters(
      partitionSchema: StructType,
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil,
      onMissingPartitionColumn: String => Unit = _ => ()): Seq[Expression]

  def buildStatsSchema(
      tableSchema: StructType,
      getFieldName: StructField => String = _.name,
      tightBoundsSupported: Boolean = false): StructType
}

/**
 * Default implementation of [[DataFilterSupport]].
 */
private[delta] object DefaultDataFilterSupport extends DataFilterSupport {

  override def equalTo(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypesIfExists(colPath, value.expr.dataType, MIN, MAX) {
      (min, max) => min <= value && value <= max
    }
  }

  override def notEqualTo(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypesIfExists(colPath, value.expr.dataType, MIN, MAX) {
      (min, max) => min < value || value < max
    }
  }

  override def lessThan(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MIN)(_ < value)
  }

  override def lessThanOrEqual(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MIN)(_ <= value)
  }

  override def greaterThan(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MAX)(_ > value)
  }

  override def greaterThanOrEqual(
      statsProvider: StatsProvider,
      colPath: Seq[String],
      value: Column): Option[DataSkippingPredicate] = {
    statsProvider.getPredicateWithStatTypeIfExists(colPath, value.expr.dataType, MAX)(_ >= value)
  }

  override def filterFileList(
      partitionSchema: StructType,
      files: DataFrame,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil,
      shouldRewritePartitionFilters: Boolean = true,
      onMissingPartitionColumn: String => Unit = _ => ()): DataFrame = {
    val rewrittenFilters = if (shouldRewritePartitionFilters) {
      rewritePartitionFilters(
        partitionSchema,
        files.sparkSession.sessionState.conf.resolver,
        partitionFilters,
        partitionColumnPrefixes,
        onMissingPartitionColumn)
    } else {
      partitionFilters
    }
    val expr = rewrittenFilters.reduceLeftOption(And).getOrElse(Literal.TrueLiteral)
    files.filter(Column(expr))
  }

  override def rewritePartitionFilters(
      partitionSchema: StructType,
      resolver: Resolver,
      partitionFilters: Seq[Expression],
      partitionColumnPrefixes: Seq[String] = Nil,
      onMissingPartitionColumn: String => Unit = _ => ()): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(f: StructField) =>
            val name = DeltaColumnMapping.getPhysicalName(f)
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", name)),
              f.dataType)
          case None =>
            onMissingPartitionColumn(a.name)
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("partitionValues", a.name))
        }
    })
  }

  override def buildStatsSchema(
      tableSchema: StructType,
      getFieldName: StructField => String = _.name,
      tightBoundsSupported: Boolean = false): StructType = {
    // Delegate to the canonical implementation in StatsColumnResolver.
    // The getFieldName parameter is unused here because callers always pass
    // schemas that already have physical names (statCollectionPhysicalSchema).
    StatsColumnResolver.buildStatsSchema(tableSchema, tightBoundsSupported)
  }
}

