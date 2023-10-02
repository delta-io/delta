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

import java.util.concurrent.ThreadPoolExecutor

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils

/** A wrapper for [[ThreadPoolExecutor]] whose tasks run with the caller's [[SparkSession]]. */
private[delta] class DeltaThreadPool(tpe: ThreadPoolExecutor) {
  // WARNING: DO NOT use the EC directly. It exists here only to interface with scala Futures.
  implicit protected val ec = ExecutionContext.fromExecutorService(tpe)

  /** Submits a task for execution and returns a [[Future]] representing that task. */
  def submit[T](spark: SparkSession)(body: => T): Future[T] = Future[T](spark.withActive(body))

  /**
   *  Executes `f` on each element of `items` as a task and returns the result.
   *  Throws a [[SparkException]] if a timeout occurs.
   */
  def parallelMap[T, R](
      spark: SparkSession,
      items: Iterable[T],
      timeout: Duration = Duration.Inf)(
      f: T => R): Iterable[R] = {
    val futures = items.map(i => submit(spark)(f(i)))
    ThreadUtils.awaitResult(Future.sequence(futures), timeout)
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
 * Helper class to run a function `f` immediately in a threadpool and avoid sharing [[SparkSession]]
 * on further retries if the the first attempt of function `f` (in the future) fails due to some
 * reason.
 * Everyone will use the future that prefetches f -- it succeeds -- but if the future fails,
 * everyone will call f themselves.
 */
class NonFateSharingFuture[T](pool: DeltaThreadPool)(f: SparkSession => T)
  extends DeltaLogging {

  // We may not have a spark session yet, but that's ok (the future is best-effort)
  // Submit a future if a spark session is available
  private var futureOpt = SparkSession.getActiveSession.map { spark =>
    pool.submit(spark) { f(spark) }
  }

  def get(timeout: Duration): T = {
    // Prefer to get a prefetched result from the future
    futureOpt.foreach { future =>
      try {
        return ThreadUtils.awaitResult(future, timeout)
      } catch {
        case e: Throwable =>
          logError("Failed to get result from future", e)
          futureOpt = None // avoid excessive log spam
          throw e
      }
    }

    // Future missing or failed, so fall back to direct execution
    SparkSession.getActiveSession match {
      case Some(spark) => f(spark)
      case _ => throw DeltaErrors.sparkSessionNotSetException()
    }
  }
}
