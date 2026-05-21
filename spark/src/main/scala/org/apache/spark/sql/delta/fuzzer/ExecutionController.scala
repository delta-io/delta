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

package org.apache.spark.sql.delta.fuzzer

import java.util.concurrent.ExecutorService

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.sql.delta.{ConcurrencyHelpers, OptimisticTransaction}
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * An execution controller allows controlling the order of executions of the different phases of a
 * [[PhaseLockingExecutionObserver]]. It is meant to be used in **testing only** and offers
 * higher-level primitives to control execution compared to manually unlocking the entry and exit
 * barriers of the observer's [[ExecutionPhaseLock]].
 */
trait ExecutionController {
  /**
   * The execution observer to control.
   */
  def observer: PhaseLockingExecutionObserver

  /**
   * Concrete controllers must provide an implementation of `withObserver` to set the thread-local
   * variable that instruments the operation to be observed.
   */
  def withObserver[T](fn: => T): T

  /**
   * This trait provides a mechanism to forward an exception from the background thread the
   * operation runs on to the test thread that interacts with the controller. This allows writing
   * more expressive tests as the exception is raised by the call that unblocked the failing phase
   * rather than at the end of the test when all futures completed.
   */
  @volatile private var activeException: Option[Throwable] = None

  /**
   * This flag tracks whether the active exception was raised via `checkException`. Any exception
   * that wasn't re-raised by the controller will be surfaced by the background thread, ensuring
   * tests don't ignore exceptions.
   */
  @volatile private var exceptionHandled: Boolean = false

  /**
   * Sets an active exception related to the operation this controller is instrumenting.
   */
  def setException(ex: Throwable): Unit = {
    require(activeException.isEmpty, "An exception is already being handled, can't handle " +
      s"another exception.\nPrevious exception: ${activeException.get.getMessage}\n" +
      s"New exception: ${ex.getMessage}")
    activeException = Some(ex)
    unblockAllPhases()
  }

  /**
   * Raise the active exception if any. `checkException` is called whenever the controller is
   * waiting for a phase to complete.
   */
  def checkException(): Unit = activeException.ifDefined { ex =>
    exceptionHandled = true
    throw ex
  }

  /** Returns whether there's an active exception that has been handled. See `exceptionHandled`. */
  def hasHandledException: Boolean = activeException.isDefined && exceptionHandled

  /** Unblocks all phases for [[TransactionObserver]] so that corresponding query can finish. */
  def unblockAllPhases(): Unit = unblockAllPhases(this.observer)

  /** Unblocks all phases for [[TransactionObserver]] so that corresponding query can finish. */
  def unblockAllPhases(executionObserver: PhaseLockingExecutionObserver): Unit = {
    executionObserver.phaseLocks
      .filterNot { phase =>
        phase.hasEntered || phase.entryBarrier.load() == AtomicBarrier.State.Unblocked
      }
      .foreach { _.entryBarrier.unblock() }
  }

  val DEFAULT_SLEEP_TIME: FiniteDuration = 10.millis

  /**
   * Keep checking if `check` returns `true` until it's the case or `waitTime` expires.
   *
   * Return `true` when the `check` returned `true`, and `false` if `waitTime` expired.
   */
  protected def busyWaitFor(check: => Boolean, timeout: FiniteDuration): Unit = {
    val deadline = timeout.fromNow
    checkException()
    while (!check) {
      if (!deadline.hasTimeLeft()) {
        throw new Exception(s"Exceeded deadline($timeout) for check to become true")
      }
      val sleepTimeMs = DEFAULT_SLEEP_TIME.min(deadline.timeLeft).toMillis
      Thread.sleep(sleepTimeMs)
      checkException()
    }
  }

  /**
   * Ensures the current execution state is consistent with the phase that we want to run.
   * The goal is to make debugging easier when writing tests.
   */
  protected def checkPhaseCanStart(
      phase: ExecutionPhaseLock,
      dependencies: Seq[ExecutionPhaseLock] = Seq.empty,
      incompatiblePhases: Seq[ExecutionPhaseLock] = Seq.empty,
      skipped: Boolean = false): Unit = {
    for (incompatible <- incompatiblePhases) {
      assert(!incompatible.hasEntered,
        s"Can't run ${phase.name} because ${incompatible.name} has already started or completed.")
    }
    for (dependency <- dependencies) {
      assert(dependency.hasLeft,
        s"Can't run ${phase.name} because ${dependency.name} hasn't completed yet.")
    }
    assert(!skipped, s"Can't run ${phase.name} because it was already skipped.")
    assert(!phase.hasEntered && !phase.hasLeft,
      s"Can't run ${phase.name} because it has already started or completed.")
  }
}

object ExecutionController extends Logging {
  /**
   * Runs the given function `fn` on a different thread instrumented with an execution observer.
   */
  def runWithController[T](
      spark: SparkSession,
      executorService: ExecutorService,
      controller: ExecutionController)(fn: => T): Future[T] = {
    implicit val ec = ExecutionContext.fromExecutorService(executorService)
    val txn = OptimisticTransaction.getActive()
    Future {
      ConcurrencyHelpers.withOptimisticTransaction(txn) {
        spark.withActive {
          try {
            controller.withObserver(fn)
          } catch {
            case ex: Exception =>
              logError(s"Error on test thread", ex)
              // Forward the error through the observer so that the controller can check and
              // throw it on the test thread
              controller.setException(ex)
              throw ex
          }
        }
      }
    }
  }
}
