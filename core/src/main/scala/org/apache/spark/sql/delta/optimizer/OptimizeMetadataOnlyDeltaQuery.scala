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

package org.apache.spark.sql.delta.optimizer

import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.delta.DeltaTable
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.stats.DeltaScanGenerator
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.types.LongType

trait OptimizeMetadataOnlyDeltaQuery {
  def optimizeQueryWithMetadata(plan: LogicalPlan): LogicalPlan = {
    def transform(plan: LogicalPlan): LogicalPlan =
      plan.transformUpWithSubqueries {
        case CountStarDeltaTable(alias, countValue) =>
          createLocalRelationPlan(alias, countValue)
      }

    transform(plan)
  }

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator

  private def createLocalRelationPlan(
    alias: Alias,
    rowCount: Long): LogicalPlan = {
    val relation = LocalRelation.fromExternalRows(
      output = Seq(AttributeReference(alias.name, LongType)(alias.exprId, alias.qualifier)),
      data = Seq(Row(rowCount)))

    relation
  }

  object CountStarDeltaTable {

    def unapply(plan: Aggregate): Option[(Alias, Long)] = {
      plan match {
        case Aggregate(
        Nil,
        Seq(oldAlias@Alias
          (AggregateExpression(Count(Seq(Literal(1, _))), Complete, false, None, _), _)),
        PhysicalOperation(_, Nil, DeltaTable(tahoeLogFileIndex: TahoeLogFileIndex))) =>
          extractGlobalCount(tahoeLogFileIndex).map(rowCount =>
            (oldAlias, rowCount))
        case _ => None
      }
    }

    private def extractGlobalCount(tahoeLogFileIndex: TahoeLogFileIndex): Option[Long] = {
      val row = getDeltaScanGenerator(tahoeLogFileIndex).filesWithStatsForScan(Nil)
        .agg(
          sum("stats.numRecords"),
          count(new Column("*")),
          count(new Column("stats.numRecords")))
        .first

      val numOfFiles = row.getLong(1)
      val numOfFilesWithStats = row.getLong(2)

      if (numOfFiles == numOfFilesWithStats) {
        val numRecords = if (row.isNullAt(0)) {
          0 // It is Null if deltaLog.snapshot.allFiles is empty
        } else { row.getLong(0) }

        Some(numRecords)
      } else {
        // If COUNT(*) is greater than COUNT(numRecords) means not every AddFile records has stats
        None
      }
    }
  }
}
