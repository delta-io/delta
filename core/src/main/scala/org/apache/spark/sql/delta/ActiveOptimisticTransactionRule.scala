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

package org.apache.spark.sql.delta

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Subquery, SupportsSubquery, V2WriteCommand}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.{PinnedTahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * This rule:
 * - Tries to find if we attempt to read the same table when there is an active transaction. If so,
 *   reading the same table should use the same snapshot of the transaction, and the transaction
 *   should also record the filters that are used to read the table.
 * - Generates and applies partition filters from the data filters for generated columns if
 *   generated column partition filter optimization is enabled
 *
 */
class ActiveOptimisticTransactionRule(spark: SparkSession) extends Rule[LogicalPlan] {

  /**
   * Gets the [[Snapshot]] for the given log.
   *
   * Internally, it will use the snapshot of the file index or the snapshot of the active
   * transaction (if any).
   */
  private def getSnapshot(index: TahoeLogFileIndex): Snapshot = {
    import ActiveOptimisticTransactionRule._
    // The first case means that we've fixed the table snapshot for time travel
    if (index.isTimeTravelQuery) return index.getSnapshot

    val snapshot = OptimisticTransaction.getActive()
      .filter(_.deltaLog.isSameLogAs(index.deltaLog))
      .map(_.snapshot)
      .getOrElse(index.getSnapshot)

    if (onGetSnapshotCallback != null) onGetSnapshotCallback(snapshot)
    snapshot
  }

  private def optimizeGeneratedColumns(
      snapshot: Snapshot,
      plan: LogicalPlan,
      filters: Seq[Expression],
      delta: LogicalRelation): (LogicalPlan, Seq[Expression]) = {
    val generatedPartitionFilters =
      GeneratedColumn.generatePartitionFilters(spark, snapshot, filters, delta)
    val planWithFilters = if (generatedPartitionFilters.nonEmpty) {
      plan transformUp {
        case delta @ DeltaTable(_: TahoeLogFileIndex) =>
          Filter(generatedPartitionFilters.reduceLeft(And), delta)
      }
    } else {
      plan
    }
    planWithFilters ->
      (filters ++ generatedPartitionFilters)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {

    // Should not be applied to subqueries to avoid duplicate delta jobs.
    val isSubquery = plan.isInstanceOf[Subquery] || plan.isInstanceOf[SupportsSubquery]
    // Skip this rule for V2 write commands - it will be evaluated later when the V1 fallback
    // is planned. We don't want to pin the file index yet because we're not in the transaction.
    val isDataSourceV2 = plan.isInstanceOf[V2WriteCommand]
    if (isSubquery || isDataSourceV2) {
      return plan
    }

    // We need to first prepare the scans in the subqueries of a node. Otherwise, because of the
    // short-circuiting nature of the pattern matching in the transform method, if a
    // PhysicalOperation node is matched, its subqueries that may contain other PhysicalOperation
    // nodes will be skipped.
    def transformSubqueries(plan: LogicalPlan): LogicalPlan =
      plan transformAllExpressions {
        case subquery: SubqueryExpression =>
          subquery.withNewPlan(transform(subquery.plan))
      }

    def transform(plan: LogicalPlan): LogicalPlan =
      transformSubqueries(plan) transform {
        case p @ PhysicalOperation(_, filters, delta @ DeltaTable(fileIndex: TahoeLogFileIndex)) =>

          assert(fileIndex.partitionFilters.isEmpty,
            "Partition filters should have been extracted by DeltaAnalysis.")
          val snapshot = getSnapshot(fileIndex)
          val pinnedIndex = PinnedTahoeFileIndex(
            spark,
            fileIndex.deltaLog,
            fileIndex.path,
            snapshot
          )

          val (planWithFilters, optimizedFilters) =
            if (GeneratedColumn.partitionFilterOptimizationEnabled(spark)) {
              optimizeGeneratedColumns(snapshot, p, filters, delta)
            } else {
              p -> filters
            }

          // The active transaction should record the filters that are used to read the table so
          // that we can detect the conflicts correctly.
          OptimisticTransaction.getActive()
            .filter(_ => !fileIndex.isTimeTravelQuery)
            .filter(txn => fileIndex.deltaLog.isSameLogAs(txn.deltaLog))
            .foreach(txn => txn.filterFiles(optimizedFilters))

          DeltaTableUtils.replaceFileIndex(planWithFilters, pinnedIndex)
      }
    transform(plan)
  }
}

object ActiveOptimisticTransactionRule {
  /**
   * Optional callback function that is called after `getSnapshot` is called
   * by the ActiveOptimisticTransactionRule. This is primarily used for testing purposes.
   */
  @volatile private var onGetSnapshotCallback: Snapshot => Unit = _

  /**
   * Run a thunk of code with the given callback function injected into the
   * ActiveOptimisticTransactionRule.
   * The callback function is called after `getSnapshot` is called
   * by the ActiveOptimisticTransactionRule. This is primarily used for testing purposes.
   */
  private[delta] def withCallbackOnGetSnapshot[T](
      callback: Snapshot => Unit)(thunk: => T): T = {
    try {
      onGetSnapshotCallback = callback
      thunk
    } finally {
      onGetSnapshotCallback = null
    }
  }
}
