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

package org.apache.spark.sql.delta.util.threads

import java.util.concurrent._

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.ThreadUtils.namedThreadFactory

/** A wrapper for [[ThreadPoolExecutor]] whose tasks run with the caller's [[SparkSession]]. */
private[delta] class DeltaThreadPool(tpe: ThreadPoolExecutor) {
  def getActiveCount: Int = tpe.getActiveCount
  def getMaximumPoolSize: Int = tpe.getMaximumPoolSize

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
    new DeltaThreadPool(newDaemonCachedThreadPool(prefix, numThreads))

  /**
   * Create a cached thread pool whose max number of threads is `maxThreadNumber`. Thread names
   * are formatted as prefix-ID, where ID is a unique, sequentially assigned integer.
   */
  def newDaemonCachedThreadPool(
      prefix: String,
      maxThreadNumber: Int): ThreadPoolExecutor = {
    val keepAliveSeconds = 60
    val queueSize = Integer.MAX_VALUE
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new SparkThreadLocalForwardingThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable](queueSize),
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }
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
        // with SparkException. Meanwhile, Java Future.get only throws four exceptions:
        // ExecutionException (non-fatal, wrapped, and itself wraps any Throwable from the task
        // itself), CancellationException (non-fatal, wrapped), InterruptedException (fatal, not
        // wrapped), and TimeoutException (non-fatal, but not wrapped). Thus, any "normal" failure
        // of the future will surface as SparkException(ExecutionException(OriginalException)).
        case outer: SparkException => outer.getCause match {
          case e: CancellationException =>
            logWarning("Future was cancelled")
            futureOpt = None
            None
          case inner: ExecutionException if inner.getCause != null => inner.getCause match {
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
        case e: TimeoutException =>
          logWarning("Timed out waiting for future")
          None
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
