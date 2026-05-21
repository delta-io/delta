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

package org.apache.spark.sql.delta.concurrency

import java.util.concurrent.ExecutorService

import scala.concurrent.Future

import org.apache.spark.sql.delta.DeltaInsertReplaceOnOrUsingTestUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.commands.InsertReplaceOnOrUsingAPIOrigin
import org.apache.spark.sql.delta.fuzzer.{ExecutionController, InsertAtomicReplaceExecutionController, InsertAtomicReplacePhases, PhaseLockingInsertAtomicReplaceExecutionObserver}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.util.ThreadUtils

/**
 * The [[ExecutionObserverContext]] of an instrumented INSERT REPLACE ON/USING operation.
 */
case class InsertReplaceOnOrUsingExecutionObserverContext(
    protected val controller: InsertAtomicReplaceExecutionController,
    protected val executor: ExecutorService,
    protected val resultFuture: Future[Array[Row]])
  extends ExecutionObserverContext[InsertAtomicReplaceExecutionController]

/**
 * Common test utilities for INSERT REPLACE ON/USING concurrency tests.
 */
trait DeltaInsertReplaceOnOrUsingConcurrencyTestUtils
  extends DeltaInsertReplaceOnOrUsingTestUtils
  with ExecutionObserverTestMixin {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.REPLACE_ON_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")
    .set(DeltaSQLConf.REPLACE_USING_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")

  import testImplicits._

  /**
   * Execute a REPLACE ON operation using the specified API.
   */
  protected def executeReplaceOn(
      api: InsertReplaceOnOrUsingAPIOrigin.Value,
      tableName: String,
      matchingCond: String,
      sourceQuery: String): Unit = {
    api match {
      case InsertReplaceOnOrUsingAPIOrigin.DFv1Save =>
        val targetPath = DeltaLog.forTable(
          spark, TableIdentifier(tableName)).dataPath.toString
        sql(sourceQuery).as("s").write.format("delta")
          .mode("overwrite")
          .option("replaceOn", matchingCond)
          .option("targetAlias", "t")
          .save(targetPath)
      case InsertReplaceOnOrUsingAPIOrigin.DFv1InsertInto =>
        val targetPath = DeltaLog.forTable(
          spark, TableIdentifier(tableName)).dataPath.toString
        sql(sourceQuery).as("s").write.format("delta")
          .mode("overwrite")
          .option("replaceOn", matchingCond)
          .option("targetAlias", "t")
          .insertInto(s"delta.`$targetPath`")
      case InsertReplaceOnOrUsingAPIOrigin.DFv1SaveAsTable =>
        sql(sourceQuery).as("s").write.format("delta")
          .mode("overwrite")
          .option("replaceOn", matchingCond)
          .option("targetAlias", "t")
          .saveAsTable(tableName)
    }
  }

  /**
   * Execute a REPLACE USING operation using the specified API.
   */
  protected def executeReplaceUsing(
      api: InsertReplaceOnOrUsingAPIOrigin.Value,
      tableName: String,
      matchingCols: Seq[String],
      sourceQuery: String): Unit = {
    api match {
      case InsertReplaceOnOrUsingAPIOrigin.DFv1Save =>
        val targetPath = DeltaLog.forTable(
          spark, TableIdentifier(tableName)).dataPath.toString
        sql(sourceQuery).write.format("delta")
          .mode("overwrite")
          .option("replaceUsing", matchingCols.mkString(", "))
          .save(targetPath)
      case InsertReplaceOnOrUsingAPIOrigin.DFv1InsertInto =>
        val targetPath = DeltaLog.forTable(
          spark, TableIdentifier(tableName)).dataPath.toString
        sql(sourceQuery).write.format("delta")
          .mode("overwrite")
          .option("replaceUsing", matchingCols.mkString(", "))
          .insertInto(s"delta.`$targetPath`")
      case InsertReplaceOnOrUsingAPIOrigin.DFv1SaveAsTable =>
        sql(sourceQuery).write.format("delta")
          .mode("overwrite")
          .option("replaceUsing", matchingCols.mkString(", "))
          .saveAsTable(tableName)
    }
  }

  /**
   * Create a [[InsertReplaceOnOrUsingExecutionObserverContext]]
   * with the given execution function.
   */
  protected def createObserverCtx(threadName: String)(
      f: => Unit): InsertReplaceOnOrUsingExecutionObserverContext = {
    val executor = ThreadUtils.newDaemonSingleThreadExecutor(threadName = threadName)
    val observer =
      PhaseLockingInsertAtomicReplaceExecutionObserver(InsertAtomicReplacePhases())
    val controller = new InsertAtomicReplaceExecutionController(observer)
    val future = ExecutionController.runWithController(spark, executor, controller) {
      f
      Array[Row]()
    }
    InsertReplaceOnOrUsingExecutionObserverContext(controller, executor, future)
  }

  protected def createReplaceOnObserverCtx(
      api: InsertReplaceOnOrUsingAPIOrigin.Value,
      tableName: String,
      matchingCond: String,
      sourceQuery: String): InsertReplaceOnOrUsingExecutionObserverContext = {
    createObserverCtx(s"${api}-replace-on-executor") {
      executeReplaceOn(
        api = api,
        tableName = tableName,
        matchingCond = matchingCond,
        sourceQuery = sourceQuery)
    }
  }

  protected def createReplaceUsingObserverCtx(
      api: InsertReplaceOnOrUsingAPIOrigin.Value,
      tableName: String,
      matchingCols: Seq[String],
      sourceQuery: String): InsertReplaceOnOrUsingExecutionObserverContext = {
    createObserverCtx(s"${api}-replace-using-executor") {
      executeReplaceUsing(
        api = api,
        tableName = tableName,
        matchingCols = matchingCols,
        sourceQuery = sourceQuery)
    }
  }
}
