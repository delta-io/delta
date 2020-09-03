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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, Metadata}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

trait DeltaTestUtilsBase {

  /**
   * Helper class for to ensure initial commits contain a Metadata action.
   */
  implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {
    def commitManually(actions: Action*): Long = {
      if (txn.readVersion == -1 && !actions.exists(_.isInstanceOf[Metadata])) {
        txn.commit(Metadata() +: actions, ManualUpdate)
      } else {
        txn.commit(actions, ManualUpdate)
      }
    }
  }

  class LogicalPlanCapturingListener(optimized: Boolean) extends QueryExecutionListener {
    val plans = new ArrayBuffer[LogicalPlan]
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      if (optimized) plans.append(qe.optimizedPlan) else plans.append(qe.analyzed)
    }

    override def onFailure(
      funcName: String, qe: QueryExecution, error: Exception): Unit = {}
  }

  /**
   * Run a thunk with physical plans for all queries captured and passed into a provided buffer.
   */
  def withLogicalPlansCaptured[T](
      spark: SparkSession,
      optimizedPlan: Boolean)(
      thunk: => Unit): ArrayBuffer[LogicalPlan] = {
    val planCapturingListener = new LogicalPlanCapturingListener(optimizedPlan)

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(planCapturingListener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      planCapturingListener.plans
    } finally {
      spark.listenerManager.unregister(planCapturingListener)
    }
  }
}

object DeltaTestUtils extends DeltaTestUtilsBase
