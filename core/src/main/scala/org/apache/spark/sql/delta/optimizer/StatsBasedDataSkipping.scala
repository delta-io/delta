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

import org.apache.spark.sql.{Column, Row, SparkSession, functions}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.{DeltaTable, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.LongType

class StatsBasedDataSkipping(protected val spark: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_RETURN_VALUE)) {
      plan
    } else {
      plan transform {
        case CountStarDeltaTable(aliasName, countValue) =>
          createLocalRelationPlan(aliasName, countValue)
      }
    }
  }

  private def createLocalRelationPlan(aliasName: String, rowCount: Long): LogicalPlan = {
    val relation = LocalRelation.fromExternalRows(
      output = Seq(AttributeReference(aliasName, LongType)()),
      data = Seq(Row(rowCount)))

    relation
  }

  /**
   * This is an extractor object. See https://docs.scala-lang.org/tour/extractor-objects.html.
   */
  object CountStarDeltaTable {

    /**
     * This is an extractor method (basically, the opposite of a constructor) which takes in an
     * object `plan` and tries to give back the arguments as a [[CountStarDeltaTable]].
     */
    def unapply(plan: Aggregate): Option[(String, Long)] = {
      plan match {
        case Aggregate(Nil,
        Seq(Alias(AggregateExpression(Count(Seq(Literal(1, _))), _, false, None, _), aliasName)),
        Project(_, DeltaTable(tahoeLogFileIndex: TahoeLogFileIndex))) =>
          extractGlobalCount(tahoeLogFileIndex).map(rowCount => (aliasName, rowCount))
        case _ => None
      }
    }
  }

  def extractGlobalCount(tahoeFileIndex: TahoeLogFileIndex): Option[Long] = {
    val row = getSnapshot(tahoeFileIndex).withStats
      .agg(
        functions.sum("stats.numRecords"),
        count(new Column("stats.*")),
        count(new Column("stats.numRecords")))
      .first

    if (row.isNullAt(0) // It is Null if deltaLog.snapshot.allFiles is empty
      // If COUNT(*) is greater than COUNT(numRecords) means not every AddFile records has stats
      || row.getLong(1) != row.getLong(2)
    ) {
      None
    } else {
      if (!tahoeFileIndex.isTimeTravelQuery) {
        val transaction = OptimisticTransaction.getActive()
        if (transaction.isDefined) {
          // When a transaction is committed it checks if any of the files read in that transaction
          // has been changed and throws a DeltaConcurrentModificationException if it did.
          // Mark the whole table as read to simulate the behavior of getting the row count.
          transaction.get.readWholeTable()
        }
      }
      Some(row.getLong(0))
    }
  }

  private def getSnapshot(fileIndex: TahoeLogFileIndex): Snapshot = {
    if (fileIndex.isTimeTravelQuery) {
      fileIndex.snapshotAtAnalysis
    } else {
      val transaction = OptimisticTransaction.getActive()
      if (transaction.isDefined) {
        // Inside a transaction we use the transaction snapshot instead of the most recent.
        transaction.get.getDeltaScanGenerator(fileIndex).snapshotToScan
      }
      else {
        fileIndex.getSnapshot
      }
    }
  }
}
