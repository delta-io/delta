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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{Action, Metadata}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.test.SharedSparkSession
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

  def countSparkJobs(sc: SparkContext, f: => Unit): Int = {
    val jobCount = new AtomicInteger(0)
    val listener = new SparkListener {

      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        // Spark will always log a job start/end event even when the job does not launch any task.
        if (jobStart.stageInfos.exists(_.numTasks > 0)) {
          jobCount.incrementAndGet()
        }
      }
    }
    sc.addSparkListener(listener)
    try {
      sc.listenerBus.waitUntilEmpty(15000)
      f
      sc.listenerBus.waitUntilEmpty(15000)
    } finally {
      sc.removeSparkListener(listener)
    }
    jobCount.get()
  }
}

object DeltaTestUtils extends DeltaTestUtilsBase
trait DeltaTestUtilsForTempViews
  extends SharedSparkSession
{
  def testWithTempView(testName: String)(testFun: Boolean => Any): Unit = {
    Seq(true, false).foreach { isSQLTempView =>
      val tempViewUsed = if (isSQLTempView) "SQL TempView" else "Dataset TempView"
      test(s"$testName - $tempViewUsed") {
        withTempView("v") {
          testFun(isSQLTempView)
        }
      }
    }
  }

  def testOssOnlyWithTempView(testName: String)(testFun: Boolean => Any): Unit = {
    Seq(true, false).foreach { isSQLTempView =>
      val tempViewUsed = if (isSQLTempView) "SQL TempView" else "Dataset TempView"
      test(s"$testName - $tempViewUsed") {
        withTempView("v") {
          testFun(isSQLTempView)
        }
      }
    }
  }

  def testQuietlyWithTempView(testName: String)(testFun: Boolean => Any): Unit = {
    Seq(true, false).foreach { isSQLTempView =>
      val tempViewUsed = if (isSQLTempView) "SQL TempView" else "Dataset TempView"
      testQuietly(s"$testName - $tempViewUsed") {
        withTempView("v") {
          testFun(isSQLTempView)
        }
      }
    }
  }

  def createTempViewFromTable(
      tableName: String,
      isSQLTempView: Boolean,
      format: Option[String] = None): Unit = {
    if (isSQLTempView) {
      sql(s"CREATE OR REPLACE TEMP VIEW v AS SELECT * from $tableName")
    } else {
      spark.read.format(format.getOrElse("delta")).table(tableName).createOrReplaceTempView("v")
    }
  }

  def createTempViewFromSelect(text: String, isSQLTempView: Boolean): Unit = {
    if (isSQLTempView) {
      sql(s"CREATE OR REPLACE TEMP VIEW v AS $text")
    } else {
      sql(text).createOrReplaceTempView("v")
    }
  }

  def testErrorMessageAndClass(
      isSQLTempView: Boolean,
      ex: AnalysisException,
      expectedErrorMsgForSQLTempView: String = null,
      expectedErrorMsgForDataSetTempView: String = null,
      expectedErrorClassForSQLTempView: String = null,
      expectedErrorClassForDataSetTempView: String = null): Unit = {
    if (isSQLTempView) {
      if (expectedErrorMsgForSQLTempView != null) {
        assert(ex.getMessage.contains(expectedErrorMsgForSQLTempView))
      }
      if (expectedErrorClassForSQLTempView != null) {
        assert(ex.getErrorClass == expectedErrorClassForSQLTempView)
      }
    } else {
      if (expectedErrorMsgForDataSetTempView != null) {
        assert(ex.getMessage.contains(expectedErrorMsgForDataSetTempView))
      }
      if (expectedErrorClassForDataSetTempView != null) {
        assert(ex.getErrorClass == expectedErrorClassForDataSetTempView, ex.getMessage)
      }
    }
  }
}
