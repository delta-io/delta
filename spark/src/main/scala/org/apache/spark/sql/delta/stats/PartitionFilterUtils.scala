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
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Pure-function utilities for rewriting partition filters against the
 * [[org.apache.spark.sql.delta.actions.AddFile]] schema.
 *
 * These are intentionally separated from [[org.apache.spark.sql.delta.DeltaLog]] because
 * they have no dependency on a log instance and are reusable by both V1 and V2 connectors.
 */
private[delta] object PartitionFilterUtils {

  /**
   * Filters the given [[DataFrame]] by the given `partitionFilters`, returning those that match.
   *
   * @param partitionSchema The schema of the partition columns.
   * @param files The active files, which contain partition value information.
   * @param partitionFilters Filters on the partition columns.
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested.
   * @param shouldRewritePartitionFilters Whether to rewrite `partitionFilters` to be over the
   *                                      [[org.apache.spark.sql.delta.actions.AddFile]] schema.
   * @param onMissingPartitionColumn Callback invoked when a filter references a column not in
   *                                 the partition schema.
   */
  def filterFileList(
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

  /**
   * Rewrites the given `partitionFilters` so that partition column references point to the
   * nested `partitionValues` map inside the [[org.apache.spark.sql.delta.actions.AddFile]] schema.
   *
   * @param partitionSchema The schema of the partition columns.
   * @param resolver The session-specific column name resolver.
   * @param partitionFilters Filters on the partition columns.
   * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested.
   * @param onMissingPartitionColumn Callback invoked when a filter references a column not in
   *                                 the partition schema.
   */
  def rewritePartitionFilters(
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
}
