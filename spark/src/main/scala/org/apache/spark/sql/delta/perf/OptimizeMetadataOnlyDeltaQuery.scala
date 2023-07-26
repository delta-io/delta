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

package org.apache.spark.sql.delta.perf

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.DeltaTable
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.DeltaScanGenerator
import org.apache.spark.sql.functions.{coalesce, col, count, lit, sum, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

trait OptimizeMetadataOnlyDeltaQuery {
  def optimizeQueryWithMetadata(plan: LogicalPlan): LogicalPlan = {
    plan.transformUpWithSubqueries {
      case agg@CountStarDeltaTable(countValue) =>
        LocalRelation(agg.output, Seq(InternalRow(countValue)))
      case agg@ShowCountStarDeltaTable(countValue) =>
        LocalRelation(agg.output, Seq(InternalRow(countValue)))
    }
  }

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator

  /** Return the number of rows in the table or `None` if we cannot calculate it from stats */
  private def extractGlobalCount(tahoeLogFileIndex: TahoeLogFileIndex): Option[Long] = {
    // account for deleted rows according to deletion vectors
    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0))
    val numLogicalRecords = (col("stats.numRecords") - dvCardinality).as("numLogicalRecords")

    val row = getDeltaScanGenerator(tahoeLogFileIndex).filesWithStatsForScan(Nil)
      .agg(
        sum(numLogicalRecords),
        // Calculate the number of files missing `numRecords`
        count(when(col("stats.numRecords").isNull, 1)))
      .first

    // The count agg is never null. A non-zero value means we have incomplete stats; otherwise,
    // the sum agg is either null (for an empty table) or gives an accurate record count.
    if (row.getLong(1) > 0) return None
    val numRecords = if (row.isNullAt(0)) 0 else row.getLong(0)
    Some(numRecords)
  }

  /** e.g. sql("SELECT COUNT(*) FROM <deta-table>").collect() */
  object CountStarDeltaTable {
    def unapply(plan: Aggregate): Option[Long] = plan match {
      case Aggregate(
          Nil,
          Seq(Alias(AggregateExpression(Count(Seq(Literal(1, _))), Complete, false, None, _), _)),
          PhysicalOperation(_, Nil, DeltaTable(i: TahoeLogFileIndex))) if i.partitionFilters.isEmpty
        => extractGlobalCount(i)
      case _ => None
    }
  }

  /** e.g. sql("SELECT COUNT(*) FROM <delta-table>").show() */
  object ShowCountStarDeltaTable {
    def unapply(plan: Aggregate): Option[UTF8String] = plan match {
      case Aggregate(
        Nil,
        Seq(
          Alias(
            Cast(
              AggregateExpression(Count(Seq(Literal(1, _))), Complete, false, None, _),
              StringType, _, _),
          _)
        ),
        PhysicalOperation(_, Nil, DeltaTable(i: TahoeLogFileIndex))
      ) if i.partitionFilters.isEmpty =>
        extractGlobalCount(i).map { count =>
          // The following code is following how Spark casts a long type to a string type.
          // See org.apache.spark.sql.catalyst.expressions.Cast#castToString.
          UTF8String.fromString(count.toString)
        }
      case _ => None
    }
  }
}
