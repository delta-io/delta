/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.files.{PinnedTahoeFileIndex, TahoeLogFileIndex}

/**
 * This rule tries to find if we attempt to read the same table when there is an active transaction.
 * If so, reading the same table should use the same snapshot of the transaction, and the
 * transaction should also record the filters that are used to read the table.
 *
 */
class ActiveOptimisticTransactionRule(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
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
        case p @ PhysicalOperation(_, filters, DeltaTable(fileIndex: TahoeLogFileIndex))
            if !fileIndex.isTimeTravelQuery && OptimisticTransaction.getActive().nonEmpty =>
          // When there is an active transaction, reading the same table should use the same
          // snapshot of the transaction.
          val txn = OptimisticTransaction.getActive().get
          if (fileIndex.deltaLog.isSameLogAs(txn.deltaLog)) {
            assert(fileIndex.partitionFilters.isEmpty,
              "Partition filters should have been extracted by DeltaAnalysis.")
            // The active transaction should record the filters that are used to read the table so
            // that we can detect the conflicts correctly.
            txn.filterFiles(filters)
            val pinnedIndex = PinnedTahoeFileIndex(
              spark,
              fileIndex.deltaLog,
              fileIndex.path,
              txn.snapshot)
            DeltaTableUtils.replaceFileIndex(p, pinnedIndex)
          } else {
            p
          }
      }

    transform(plan)
  }
}
