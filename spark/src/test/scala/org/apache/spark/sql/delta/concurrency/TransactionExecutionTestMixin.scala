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

import java.util.concurrent.{ExecutorService, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.{ConcurrencyHelpers, OptimisticTransaction, TransactionExecutionObserver}
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver => TransactionObserver}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

trait TransactionExecutionTestMixin {
  self: PhaseLockingTestMixin with SharedSparkSession with Logging =>

  /**
   * Timeout used when waiting for individual phases of instrumented operations to complete.
   */
  val timeout: FiniteDuration = 120.seconds

  /** Run a given function `fn` inside the given `executor` within a [[TransactionObserver]] */
  private[delta] def runFunctionWithObserver(
      name: String,
      executorService: ExecutorService,
      fn: () => Array[Row]): (TransactionObserver, Future[Array[Row]]) = {
    val observer =
      new TransactionObserver(OptimisticTransactionPhases.forName(s"observer-txn-$name"))
    implicit val ec = ExecutionContext.fromExecutorService(executorService)
    val txn = OptimisticTransaction.getActive()
    val future = Future {
      ConcurrencyHelpers.withOptimisticTransaction(txn) {
        spark.withActive(
          try {
            TransactionExecutionObserver.withObserver(observer) {
              fn()
            }
          } catch {
            case ex: Exception =>
              logError(s"Error on test thread", ex)
              throw ex
          }
        )
      }
    }
    (observer, future)
  }

  /** Run a given `queryString` inside the given `executor` */
  def runQueryWithObserver(name: String, executor: ExecutorService, queryString: String)
    : (TransactionObserver, Future[Array[Row]]) = {
    def fn(): Array[Row] = spark.sql(queryString).collect()
    runFunctionWithObserver(name, executor, fn)
  }

  /**
   * Run `functions` with the ordering defined by `observerOrdering` function.
   * This function returns futures for each of the query results.
   */
  private[delta] def runFunctionsWithOrderingFromObserver
      (functions: Seq[() => Array[Row]])
      (observerOrdering: (Seq[TransactionObserver]) => Unit)
      : Seq[Future[Array[Row]]] = {
    val executors = functions.zipWithIndex.map { case (_, index) =>
      ThreadUtils.newDaemonSingleThreadExecutor(threadName = s"executor-txn-$index")
    }
    try {
      val (observers, futures) = functions.zipWithIndex.map { case (fn, index) =>
        runFunctionWithObserver(name = s"query-$index", executors(index), fn)
      }.unzip

      // Run the observer ordering function.
      observerOrdering(observers)

      // wait for futures to succeed or fail
      for (future <- futures) {
        try {
          ThreadUtils.awaitResult(future, timeout)
        } catch {
          case _: SparkException =>
            // pass
            true
        }
      }

      futures
    } finally {
      for (executor <- executors) {
        executor.shutdownNow()
        executor.awaitTermination(timeout.toMillis, TimeUnit.MILLISECONDS)
      }
    }
  }

  /** Unblocks all phases before the `commitPhase` for [[TransactionObserver]] */
  def unblockUntilPreCommit(observer: TransactionObserver): Unit = {
    observer.phases.initialPhase.entryBarrier.unblock()
    observer.phases.preparePhase.entryBarrier.unblock()
  }

  /**
   * Unblocks the `commitPhase` and `backfillPhase` for [[TransactionObserver]].
   */
  def unblockCommit(observer: TransactionObserver): Unit = {
    observer.phases.commitPhase.entryBarrier.unblock()
    observer.phases.backfillPhase.entryBarrier.unblock()
  }

  /** Unblocks all phases for [[TransactionObserver]] so that corresponding query can finish. */
  def unblockAllPhases(observer: TransactionObserver): Unit = {
    observer.phases.initialPhase.entryBarrier.unblock()
    observer.phases.preparePhase.entryBarrier.unblock()
    observer.phases.commitPhase.entryBarrier.unblock()
    observer.phases.backfillPhase.entryBarrier.unblock()
  }

  def waitForPrecommit(observer: TransactionObserver): Unit =
    busyWaitFor(observer.phases.preparePhase.hasEntered, timeout)

  def waitForCommit(observer: TransactionObserver): Unit = {
    busyWaitFor(observer.phases.commitPhase.hasLeft, timeout)
    busyWaitFor(observer.phases.backfillPhase.hasLeft, timeout)
  }

  /**
   * Prepare and commit the transaction managed by the given observer.
   * If nextObserver is set, we need to manually call backfillPhase.leave() to advance to the
   * nextObserver. Details in [[TransactionObserver.waitForCommitPhaseAndAdvanceToNextObserver]].
   */
  private def prepareAndCommitBase(
      observer: TransactionObserver, hasNextObserver: Boolean): Unit = {
    unblockUntilPreCommit(observer)
    waitForPrecommit(observer)
    unblockCommit(observer)
    if (hasNextObserver) {
      observer.phases.backfillPhase.leave()
    }
    waitForCommit(observer)
  }

  /**
   * Prepare and commit the transaction managed by the given observer.
   */
  def prepareAndCommit(observer: TransactionObserver): Unit = {
    prepareAndCommitBase(observer, hasNextObserver = false)
  }

  /**
   * Prepare and commit the transaction managed by the given observer which has nextObserver set.
   */
  def prepareAndCommitWithNextObserverSet(observer: TransactionObserver): Unit = {
    prepareAndCommitBase(observer, hasNextObserver = true)
  }

  /**
   * Run 2 transactions A, B with following order:
   *
   * t1 -------------------------------------- TxnA starts
   * t2 --------- TxnB starts and commits (no transaction observer)
   * t6 -------------------------------------- TxnA commits
   *
   * This function returns futures for each of the query runs.
   */
  def runTxnsWithOrder__A_Start__B__A_end_without_observer_on_B(
      txnA: () => Array[Row],
      txnB: () => Array[Row]): Future[Array[Row]] = {
    val Seq(futureA) =
      runFunctionsWithOrderingFromObserver(Seq(txnA)) {
        case (observerA :: Nil) =>
          // A starts
          unblockUntilPreCommit(observerA)
          busyWaitFor(observerA.phases.preparePhase.hasEntered, timeout)

          // B starts and finishes
          txnB()

          // A commits
          unblockCommit(observerA)
          waitForCommit(observerA)
      }
    futureA
  }

  /**
   * Run 2 transactions A, B with following order:
   *
   * t1 -------------------------------------- TxnA starts
   * t2 --------- TxnB starts
   * t3 --------- TxnB commits
   * t6 -------------------------------------- TxnA commits
   *
   * This function returns futures for each of the query runs.
   */
  def runTxnsWithOrder__A_Start__B__A_End(txnA: () => Array[Row], txnB: () => Array[Row])
      : (Future[Array[Row]], Future[Array[Row]]) = {
    val Seq(futureA, futureB) =
      runFunctionsWithOrderingFromObserver(Seq(txnA, txnB)) {
        case (observerA :: observerB :: Nil) =>
          // A starts
          unblockUntilPreCommit(observerA)
          busyWaitFor(observerA.phases.preparePhase.hasEntered, timeout)

          // B starts and commits
          unblockAllPhases(observerB)
          busyWaitFor(observerB.phases.backfillPhase.hasLeft, timeout)

          // A commits
          observerA.phases.commitPhase.entryBarrier.unblock()
          busyWaitFor(observerA.phases.commitPhase.hasLeft, timeout)
          observerA.phases.backfillPhase.entryBarrier.unblock()
          busyWaitFor(observerA.phases.backfillPhase.hasLeft, timeout)
      }
    (futureA, futureB)
  }

  /**
   * Run 3 queries A, B, C with following order:
   *
   * t1 -------------------------------------- TxnA starts
   * t2 --------- TxnB starts
   * t3 --------- TxnB commits
   * t4 ----------------- TxnC starts
   * t5 ----------------- TxnC commits
   * t6 -------------------------------------- TxnA commits
   *
   * This function returns futures for each of the query runs.
   */
  def runTxnsWithOrder__A_Start__B__C__A_End(
      txnA: () => Array[Row],
      txnB: () => Array[Row],
      txnC: () => Array[Row])
      : (Future[Array[Row]], Future[Array[Row]], Future[Array[Row]]) = {

    val Seq(futureA, futureB, futureC) =
      runFunctionsWithOrderingFromObserver(Seq(txnA, txnB, txnC)) {
        case (observerA :: observerB :: observerC :: Nil) =>
          // A starts
          unblockUntilPreCommit(observerA)
          busyWaitFor(observerA.phases.preparePhase.hasEntered, timeout)

          // B starts and commits
          unblockAllPhases(observerB)
          busyWaitFor(observerB.phases.backfillPhase.hasLeft, timeout)

          // C starts and commits
          unblockAllPhases(observerC)
          busyWaitFor(observerC.phases.backfillPhase.hasLeft, timeout)

          // A commits
          observerA.phases.commitPhase.entryBarrier.unblock()
          busyWaitFor(observerA.phases.commitPhase.hasLeft, timeout)
          observerA.phases.backfillPhase.entryBarrier.unblock()
          busyWaitFor(observerA.phases.backfillPhase.hasLeft, timeout)
      }
    (futureA, futureB, futureC)
  }
}
