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

import java.util.concurrent.{ExecutionException, ExecutorService, TimeUnit}

import scala.concurrent.Future
import scala.concurrent.duration._

import org.apache.spark.sql.delta.fuzzer.ExecutionController

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.util.ThreadUtils

/**
 * Test mixin for running concurrency tests interleaving multiple - potentially different -
 * instrumented operations.
 *
 * Tests start by creating an [[ExecutionObserverContext]] for each operation to instrument - see
 * e.g. `createTransactionObserver()` - and pass them to `runWithObservers` to get access to the
 * corresponding [[ExecutionController]] that allows controlling the order of execution of the
 * different phases of the instrumented operations.
 *
 * Example usage:
 *
 * val observer1 = createXXXObserver()
 * val observer2 = createXXXObserver()
 *
 * runWithObservers(observer1, observer2) { (operationController1, operationController2) =>
 *   operationController1.runSomePhase()
 *   operationController2.runSomePhase()
 *   operationController1.runSomeOtherPhase()
 *   intercept[Exception] {
 *     operationController2.runPhaseThatFails()
 *   }
 * }
 */
trait ExecutionObserverTestMixin {
  /**
   * Timeout used when waiting for individual phases of instrumented operations to complete.
   */
  val timeout: FiniteDuration = 120.seconds

  /**
   * Runs a single observer, giving access to its corresponding controller used to enforce execution
   * ordering. Returns the result of the instrumented operation.
   */
  def runWithObserver[T <: ExecutionController](
      observerContext1: ExecutionObserverContext[T])(
      fn: T => Unit): Array[Row] =
    ExecutionObserverContext.runWithObservers(observerContext1)(fn)

}

/**
 * An [[ExecutionObserverContext]] encapsulates information required to run an instrumented
 * operation in concurrency tests. Tests must pass a context to `runWithObservers` to access the
 * underlying controller and operation result: `runWithObservers` will ensure that the outcome of
 * the future is checked and the executor is shut down at the end of the operation.
 */
abstract class ExecutionObserverContext[+Controller <: ExecutionController] {
  protected val controller: Controller
  protected val executor: ExecutorService
  protected val resultFuture: Future[Array[Row]]
}

/**
 * Provides helper methods that consume [[ExecutionObserverContext]]s, taking care of managing the
 * executor lifecycle that is used to run the operation, enforcing the requested ordering of phases
 * and surfacing operation failures so that tests can focus on defining the ordering of phases using
 * the corresponding [[ExecutionController]]s without dealing with lower-level concerns.
 */
object ExecutionObserverContext {
  /**
   * Timeout used both for waiting for operations to complete after the ordering function has been
   * run and for executors to shutdown.
   */
  val timeout: FiniteDuration = 120.seconds

  /**
   * Runs the given instrumented operations represented using their observer context while applying
   * the given phase ordering function. This is intended as the only way for tests to get access to
   * the execution controllers that they need to control execution ordering so that we have a single
   * place to manage the futures and executors lifecycle.
   */
  private[delta] def runWithObservers[T <: ExecutionController](
      observer1: ExecutionObserverContext[T])(
      fn: T => Unit): Array[Row] = try {
    // Run the observer ordering function.
    fn(observer1.controller)
    waitForCompletion(observer1)
  } finally {
    shutdownExecutor(observer1.executor)
  }

  /**
   * Wait for the instrumented operation to complete, return its result if successful and raise
   * any exception that was not already surfaced on the test thread otherwise.
   */
  private def waitForCompletion[T <: ExecutionController](
      observer: ExecutionObserverContext[T]): Array[Row] = try {
    ThreadUtils.awaitResult(observer.resultFuture, timeout)
  } catch {
    case _: SparkException if observer.controller.hasHandledException =>
      // The exception was already forwarded to the main test thread via the controller,
      // ignore it.
      Array.empty
    case e: SparkException if e.getCause.isInstanceOf[ExecutionException] =>
      throw e.getCause.getCause
    case e: SparkException =>
      throw e.getCause
  }

  /**
   * Shutdown the executor after the instrumented operation completed.
   */
  private def shutdownExecutor(executor: ExecutorService): Unit = {
    executor.shutdownNow()
    executor.awaitTermination(timeout.toMillis, TimeUnit.MILLISECONDS)
  }
}
