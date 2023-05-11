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

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import scala.util.matching.Regex

import org.apache.spark.sql.delta.DeltaTestUtils.Plans
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, RDDScanExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.QueryExecutionListener

trait DeltaTestUtilsBase {
  import DeltaTestUtils.TableIdentifierOrPath

  final val BOOLEAN_DOMAIN: Seq[Boolean] = Seq(true, false)

  class PlanCapturingListener() extends QueryExecutionListener {

    private[this] var capturedPlans = List.empty[Plans]

    def plans: Seq[Plans] = capturedPlans.reverse

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      capturedPlans ::= Plans(
          qe.analyzed,
          qe.optimizedPlan,
          qe.sparkPlan,
          qe.executedPlan)
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
      thunk: => Unit): Seq[LogicalPlan] = {
    val planCapturingListener = new PlanCapturingListener

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(planCapturingListener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      planCapturingListener.plans.map { plans =>
        if (optimizedPlan) plans.optimized else plans.analyzed
      }
    } finally {
      spark.listenerManager.unregister(planCapturingListener)
    }
  }

  /**
   * Run a thunk with physical plans for all queries captured and passed into a provided buffer.
   */
  def withPhysicalPlansCaptured[T](
      spark: SparkSession)(
      thunk: => Unit): Seq[SparkPlan] = {
    val planCapturingListener = new PlanCapturingListener

    spark.sparkContext.listenerBus.waitUntilEmpty(15000)
    spark.listenerManager.register(planCapturingListener)
    try {
      thunk
      spark.sparkContext.listenerBus.waitUntilEmpty(15000)
      planCapturingListener.plans.map(_.sparkPlan)
    } finally {
      spark.listenerManager.unregister(planCapturingListener)
    }
  }

  /**
   * Run a thunk with logical and physical plans for all queries captured and passed
   * into a provided buffer.
   */
  def withAllPlansCaptured[T](
      spark: SparkSession)(
      thunk: => Unit): Seq[Plans] = {
    val planCapturingListener = new PlanCapturingListener

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

  protected def getfindTouchedFilesJobPlans(plans: Seq[Plans]): SparkPlan = {
    // The expected plan for touched file computation is of the format below.
    // The data column should be pruned from both leaves.
    // HashAggregate(output=[count#3463L])
    // +- HashAggregate(output=[count#3466L])
    //   +- Project
    //      +- Filter (isnotnull(count#3454L) AND (count#3454L > 1))
    //         +- HashAggregate(output=[count#3454L])
    //            +- HashAggregate(output=[_row_id_#3418L, sum#3468L])
    //               +- Project [_row_id_#3418L, UDF(_file_name_#3422) AS one#3448]
    //                  +- BroadcastHashJoin [id#3342L], [id#3412L], Inner, BuildLeft
    //                     :- Project [id#3342L]
    //                     :  +- Filter isnotnull(id#3342L)
    //                     :     +- FileScan parquet [id#3342L,part#3343L]
    //                     +- Filter isnotnull(id#3412L)
    //                        +- Project [...]
    //                           +- Project [...]
    //                             +- FileScan parquet [id#3412L,part#3413L]
    // Note: It can be RDDScanExec instead of FileScan if the source was materialized.
    // We pick the first plan starting from FileScan and ending in HashAggregate as a
    // stable heuristic for the one we want.
    plans.map(_.executedPlan)
      .filter {
        case WholeStageCodegenExec(hash: HashAggregateExec) =>
          hash.collectLeaves().size == 2 &&
            hash.collectLeaves()
              .forall { s =>
                s.isInstanceOf[FileSourceScanExec] ||
                  s.isInstanceOf[RDDScanExec]
              }
        case _ => false
      }.head
  }

  /**
   * Separate name- from path-based SQL table identifiers.
   */
  def getTableIdentifierOrPath(sqlIdentifier: String): TableIdentifierOrPath = {
    // Match: delta.`path`[ alias] or tahoe.`path`[ alias]
    val pathMatcher: Regex = raw"(?:delta|tahoe)\.`([^`]+)`(?: (.+))?".r
    // Match: db.table[ alias]
    val qualifiedDbMatcher: Regex = raw"`?([^\.` ]+)`?\.`?([^\.` ]+)`?(?: (.+))?".r
    // Match: table[ alias]
    val unqualifiedNameMatcher: Regex = raw"([^ ]+)(?: (.+))?".r
    sqlIdentifier match {
      case pathMatcher(path, alias) =>
        TableIdentifierOrPath.Path(path, Option(alias))
      case qualifiedDbMatcher(dbName, tableName, alias) =>
        TableIdentifierOrPath.Identifier(TableIdentifier(tableName, Some(dbName)), Option(alias))
      case unqualifiedNameMatcher(tableName, alias) =>
        TableIdentifierOrPath.Identifier(TableIdentifier(tableName), Option(alias))
    }
  }
}

object DeltaTestUtils extends DeltaTestUtilsBase {

  sealed trait TableIdentifierOrPath
  object TableIdentifierOrPath {
    case class Identifier(id: TableIdentifier, alias: Option[String])
      extends TableIdentifierOrPath
    case class Path(path: String, alias: Option[String]) extends TableIdentifierOrPath
  }

  case class Plans(
      analyzed: LogicalPlan,
      optimized: LogicalPlan,
      sparkPlan: SparkPlan,
      executedPlan: SparkPlan)

  /**
   * Extracts the table name and alias (if any) from the given string. Correctly handles whitespaces
   * in table name but doesn't support whitespaces in alias.
   */
  def parseTableAndAlias(table: String): (String, Option[String]) = {
    // Matches 'delta.`path` AS alias' (case insensitive).
    val deltaPathWithAsAlias = raw"(?i)(delta\.`.+`)(?: AS) (\S+)".r
    // Matches 'delta.`path` alias'.
    val deltaPathWithAlias = raw"(delta\.`.+`) (\S+)".r
    // Matches 'delta.`path`'.
    val deltaPath = raw"(delta\.`.+`)".r
    // Matches 'tableName AS alias' (case insensitive).
    val tableNameWithAsAlias = raw"(?i)(.+)(?: AS) (\S+)".r
    // Matches 'tableName alias'.
    val tableNameWithAlias = raw"(.+) (.+)".r

    table match {
      case deltaPathWithAsAlias(tableName, alias) => tableName -> Some(alias)
      case deltaPathWithAlias(tableName, alias) => tableName -> Some(alias)
      case deltaPath(tableName) => tableName -> None
      case tableNameWithAsAlias(tableName, alias) => tableName -> Some(alias)
      case tableNameWithAlias(tableName, alias) => tableName -> Some(alias)
      case tableName => tableName -> None
    }
  }

  /**
   * Implements an ordering where `x < y` iff both reader and writer versions of
   * `x` are strictly less than those of `y`.
   *
   * Can be used to conveniently check that this relationship holds in tests/assertions
   * without having to write out the conjunction of the two subconditions every time.
   */
  case object StrictProtocolOrdering extends PartialOrdering[Protocol] {
    override def tryCompare(x: Protocol, y: Protocol): Option[Int] = {
      if (x.minReaderVersion == y.minReaderVersion &&
        x.minWriterVersion == y.minWriterVersion) {
        Some(0)
      } else if (x.minReaderVersion < y.minReaderVersion &&
        x.minWriterVersion < y.minWriterVersion) {
        Some(-1)
      } else if (x.minReaderVersion > y.minReaderVersion &&
        x.minWriterVersion > y.minWriterVersion) {
        Some(1)
      } else {
        None
      }
    }

    override def lteq(x: Protocol, y: Protocol): Boolean =
      x.minReaderVersion <= y.minReaderVersion && x.minWriterVersion <= y.minWriterVersion

    // Just a more readable version of `lteq`.
    def fulfillsVersionRequirements(actual: Protocol, requirement: Protocol): Boolean =
      lteq(requirement, actual)
  }
}

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

  protected def errorContains(errMsg: String, str: String): Unit = {
    assert(errMsg.toLowerCase(Locale.ROOT).contains(str.toLowerCase(Locale.ROOT)))
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
        errorContains(ex.getMessage, expectedErrorMsgForSQLTempView)
      }
      if (expectedErrorClassForSQLTempView != null) {
        assert(ex.getErrorClass == expectedErrorClassForSQLTempView)
      }
    } else {
      if (expectedErrorMsgForDataSetTempView != null) {
        errorContains(ex.getMessage, expectedErrorMsgForDataSetTempView)
      }
      if (expectedErrorClassForDataSetTempView != null) {
        assert(ex.getErrorClass == expectedErrorClassForDataSetTempView, ex.getMessage)
      }
    }
  }
}
