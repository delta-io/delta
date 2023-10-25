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

package org.apache.spark.sql.delta.util

import java.util.concurrent._

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

/** A wrapper for [[ThreadPoolExecutor]] whose tasks run with the caller's [[SparkSession]]. */
private[delta] class DeltaThreadPool(tpe: ThreadPoolExecutor) {
  /** Submits a task for execution and returns a [[Future]] representing that task. */
  def submit[T](spark: SparkSession)(body: => T): Future[T] = {
    tpe.submit { () => spark.withActive(body) }
  }

  /**
   *  Executes `f` on each element of `items` as a task and returns the result.
   *  Throws [[SparkException]] on error.
   */
  def parallelMap[T, R](
      spark: SparkSession,
      items: Iterable[T])(
      f: T => R): Iterable[R] = {
    // Materialize a list of futures, to ensure they all got submitted before we start waiting.
    val futures = items.map(i => submit(spark)(f(i))).toList
    futures.map(f => ThreadUtils.awaitResult(f, Duration.Inf)).toSeq
  }

  def submitNonFateSharing[T](f: SparkSession => T): NonFateSharingFuture[T] =
    new NonFateSharingFuture(this)(f)
}


/** Convenience constructor that creates a [[ThreadPoolExecutor]] with sensible defaults. */
private[delta] object DeltaThreadPool {
  def apply(prefix: String, numThreads: Int): DeltaThreadPool =
    new DeltaThreadPool(ThreadUtils.newDaemonCachedThreadPool(prefix, numThreads))
}

/**
 * A future invocation of `f` which avoids "fate sharing" of errors, in case multiple threads could
 * wait on the future's result.
 *
 * The future is only launched if a [[SparkSession]] is available.
 *
 * If the future succeeds, any thread can consume the result.
 *
 * If the future fails, threads will just invoke `f` directly -- except that fatal errors will
 * propagate (once) if the caller is from the same [[SparkSession]] that created the future.
 */
class NonFateSharingFuture[T](pool: DeltaThreadPool)(f: SparkSession => T)
  extends DeltaLogging {

  // Submit `f` as a future if a spark session is available
  @volatile private var futureOpt = SparkSession.getActiveSession.map { spark =>
    spark -> pool.submit(spark) { f(spark) }
  }

  def get(timeout: Duration): T = {
    // Prefer to get a prefetched result from the future, but never fail because of it.
    val futureResult = futureOpt.flatMap { case (ownerSession, future) =>
      try {
        Some(ThreadUtils.awaitResult(future, timeout))
      } catch {
        // NOTE: ThreadUtils.awaitResult wraps all non-fatal exceptions other than TimeoutException
        // with SparkException. Meanwhile, Java Future.get only throws four exceptions, all
        // non-fatal: CancellationException, ExecutionException, InterruptedException,
        // TimeoutException. Any Throwable from the task itself will surface as ExecutionException,
        // so task failure usually means SparkException(ExecutionException(OriginalException)).
        case e: TimeoutException =>
          logWarning("Timed out waiting for future")
          None
        case outer: SparkException => outer.getCause match {
          case e: InterruptedException =>
            logWarning("Interrupted while waiting for future")
            throw e
          case e: CancellationException =>
            logWarning("Future was cancelled")
            futureOpt = None
            None
          case inner: ExecutionException => inner.getCause match {
            case NonFatal(e) =>
              logWarning("Future threw non-fatal exception", e)
              futureOpt = None
              None
            case e: Throwable =>
              logWarning("Future threw fatal error", e)
              if (ownerSession eq SparkSession.active) {
                futureOpt = None
                throw e
              }
              None
          }
        }
        case NonFatal(e) =>
          logWarning("Unknown failure while waiting for future", e)
          None
      }
    }

    futureResult.getOrElse {
      // Future missing or failed, so fall back to direct execution.
      SparkSession.getActiveSession match {
        case Some(spark) => f(spark)
        case _ => throw DeltaErrors.sparkSessionNotSetException()
      }
    }
  }
}
