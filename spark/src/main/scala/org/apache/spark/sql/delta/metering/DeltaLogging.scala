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

package org.apache.spark.sql.delta.metering

import scala.concurrent.duration._
import scala.util.Try
import scala.util.control.NonFatal

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{DatabricksLogging, OpType, TagDefinition}
import com.databricks.spark.util.MetricDefinitions.{EVENT_LOGGING_FAILURE, EVENT_TAHOE}
import com.databricks.spark.util.TagDefinitions.{
  TAG_OP_TYPE,
  TAG_TAHOE_ID,
  TAG_TAHOE_PATH
}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.util.DeltaProgressReporter
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.{LoggingShims, MDC, MessageWithContext}
import org.apache.spark.util.Utils

/**
 * Convenience wrappers for logging that include delta specific options and
 * avoids the need to predeclare all operations. Metrics in Delta should respect the following
 * conventions:
 *  - Tags should identify the context of the event (which shard, user, table, machine, etc).
 *  - All actions initiated by a user should be wrapped in a recordOperation so we can track usage
 *    latency and failures. If there is a significant (more than a few seconds) subaction like
 *    identifying candidate files, consider nested recordOperation.
 *  - Events should be used to return detailed statistics about usage. Generally these should be
 *    defined with a case class to ease analysis later.
 *  - Events can also be used to record that a particular codepath was hit (i.e. a checkpoint
 *    failure, a conflict, or a specific optimization).
 *  - Both events and operations should be named hierarchically to allow for analysis at different
 *    levels. For example, to look at the latency of all DDL operations we could scan for operations
 *    that match "delta.ddl.%".
 *
 *  Underneath these functions use the standard usage log reporting defined in
 *  [[com.databricks.spark.util.DatabricksLogging]].
 */
trait DeltaLogging
  extends DeltaProgressReporter
  with DatabricksLogging {

  /**
   * Used to record the occurrence of a single event or report detailed, operation specific
   * statistics.
   *
   * @param path Used to log the path of the delta table when `deltaLog` is null.
   */
  protected def recordDeltaEvent(
      deltaLog: DeltaLog,
      opType: String,
      tags: Map[TagDefinition, String] = Map.empty,
      data: AnyRef = null,
      path: Option[Path] = None): Unit = {
    try {
      val json = if (data != null) JsonUtils.toJson(data) else ""
      val tableTags = if (deltaLog != null) {
        getCommonTags(deltaLog, Try(deltaLog.unsafeVolatileSnapshot.metadata.id).getOrElse(null))
      } else if (path.isDefined) {
        Map(TAG_TAHOE_PATH -> path.get.toString)
      } else {
        Map.empty[TagDefinition, String]
      }
      recordProductEvent(
        EVENT_TAHOE,
        Map((TAG_OP_TYPE: TagDefinition) -> opType) ++ tableTags ++ tags,
        blob = json)
    } catch {
      case NonFatal(e) =>
        recordEvent(
          EVENT_LOGGING_FAILURE,
          blob = JsonUtils.toJson(
            Map("exception" -> e.getMessage,
              "opType" -> opType,
              "method" -> "recordDeltaEvent"))
        )
    }
  }

  /**
   * Used to report the duration as well as the success or failure of an operation on a `tahoePath`.
   */
  protected def recordDeltaOperationForTablePath[A](
      tablePath: String,
      opType: String,
      tags: Map[TagDefinition, String] = Map.empty)(
      thunk: => A): A = {
    recordDeltaOperationInternal(Map(TAG_TAHOE_PATH -> tablePath), opType, tags)(thunk)
  }

  /**
   * Used to report the duration as well as the success or failure of an operation on a `deltaLog`.
   */
  protected def recordDeltaOperation[A](
      deltaLog: DeltaLog,
      opType: String,
      tags: Map[TagDefinition, String] = Map.empty)(
      thunk: => A): A = {
    val tableTags: Map[TagDefinition, String] = if (deltaLog != null) {
      getCommonTags(deltaLog, Try(deltaLog.unsafeVolatileSnapshot.metadata.id).getOrElse(null))
    } else {
      Map.empty
    }
    recordDeltaOperationInternal(tableTags, opType, tags)(thunk)
  }

  private def recordDeltaOperationInternal[A](
      tableTags: Map[TagDefinition, String],
      opType: String,
      tags: Map[TagDefinition, String])(thunk: => A): A = {
    recordOperation(
      new OpType(opType, ""),
      extraTags = tableTags ++ tags) {
        recordFrameProfile("Delta", opType) {
            thunk
        }
    }
  }

  /**
   * Helper method to check invariants in Delta code. Fails when running in tests, records a delta
   * assertion event and logs a warning otherwise.
   */
  protected def deltaAssert(
      check: => Boolean,
      name: String,
      msg: String,
      deltaLog: DeltaLog = null,
      data: AnyRef = null,
      path: Option[Path] = None)
    : Unit = {
    if (Utils.isTesting) {
      assert(check, msg)
    } else if (!check) {
      recordDeltaEvent(
        deltaLog = deltaLog,
        opType = s"delta.assertions.$name",
        data = data,
        path = path
      )
      logWarning(msg)
    }
  }

  protected def recordFrameProfile[T](group: String, name: String)(thunk: => T): T = {
    // future work to capture runtime information ...
    thunk
  }

  private def withDmqTag[T](thunk: => T): T = {
    thunk
  }

  // Extract common tags from the delta log and snapshot.
  def getCommonTags(deltaLog: DeltaLog, tahoeId: String): Map[TagDefinition, String] = {
    (
      Map(
        TAG_TAHOE_ID -> tahoeId,
        TAG_TAHOE_PATH -> Try(deltaLog.dataPath.toString).getOrElse(null)
      )
    )
  }

  /*
   * Returns error data suitable for logging.
   *
   * It will recursively look for the error class and sql state in the cause of the exception.
   */
  def getErrorData(e: Throwable): Map[String, Any] = {
    var data = Map[String, Any]("exceptionMessage" -> e.getMessage)
    e condDo {
      case sparkEx: SparkThrowable
        if sparkEx.getCondition != null && sparkEx.getCondition.nonEmpty =>
        data ++= Map(
          "errorClass" -> sparkEx.getCondition,
          "sqlState" -> sparkEx.getSqlState
        )
      case NonFatal(e) if e.getCause != null =>
        data = getErrorData(e.getCause)
    }
    data
  }
}

object DeltaLogging {

  // The opType for delta commit stats.
  final val DELTA_COMMIT_STATS_OPTYPE = "delta.commit.stats"
}

/**
 * A thread-safe token bucket-based throttler implementation with nanosecond accuracy.
 *
 * Each instance must be shared across all scopes it should throttle.
 * For global throttling that means either by extending this class in an `object` or
 * by creating the instance as a field of an `object`.
 *
 * @param bucketSize This corresponds to the largest possible burst without throttling,
 *                   in number of executions.
 * @param tokenRecoveryInterval Time between two tokens being added back to the bucket.
 *                              This is reciprocal of the long-term average unthrottled rate.
 *
 * Example: With a bucket size of 100 and a recovery interval of 1s, we could log up to 100 events
 * in under a second without throttling, but at that point the bucket is exhausted and we only
 * regain the ability to log more events at 1 event per second. If we log less than 1 event/s
 * the bucket will slowly refill until it's back at 100.
 * Either way, we can always log at least 1 event/s.
 */
class LogThrottler(
    val bucketSize: Int = 100,
    val tokenRecoveryInterval: FiniteDuration = 1.second,
    val timeSource: NanoTimeTimeSource = SystemNanoTimeSource) extends LoggingShims {

  private var remainingTokens = bucketSize
  private var nextRecovery: DeadlineWithTimeSource =
    DeadlineWithTimeSource.now(timeSource) + tokenRecoveryInterval
  private var numSkipped: Long = 0

  /**
   * Run `thunk` as long as there are tokens remaining in the bucket,
   * otherwise skip and remember number of skips.
   *
   * The argument to `thunk` is how many previous invocations have been skipped since the last time
   * an invocation actually ran.
   *
   * Note: This method is `synchronized`, so it is concurrency safe.
   * However, that also means no heavy-lifting should be done as part of this
   * if the throttler is shared between concurrent threads.
   * This also means that the synchronized block of the `thunk` that *does* execute will still
   * hold up concurrent `thunk`s that will actually get rejected once they hold the lock.
   * This is fine at low concurrency/low recovery rates. But if we need this to be more efficient at
   * some point, we will need to decouple the check from the `thunk` execution.
   */
  def throttled(thunk: Long => Unit): Unit = this.synchronized {
    tryRecoverTokens()
    if (remainingTokens > 0) {
      thunk(numSkipped)
      numSkipped = 0
      remainingTokens -= 1
    } else {
      numSkipped += 1L
    }
  }

  /**
   * Same as [[throttled]] but turns the number of skipped invocations into a logging message
   * that can be appended to item being logged in `thunk`.
   */
  def throttledWithSkippedLogMessage(thunk: MessageWithContext => Unit): Unit = {
    this.throttled { numSkipped =>
      val skippedStr = if (numSkipped != 0L) {
        log" [${MDC(DeltaLogKeys.NUM_SKIPPED, numSkipped)} similar messages were skipped.]"
      } else {
        log""
      }
      thunk(skippedStr)
    }
  }

  /**
   * Try to recover tokens, if the rate allows.
   *
   * Only call from within a `this.synchronized` block!
   */
  private def tryRecoverTokens(): Unit = {
    try {
      // Doing it one-by-one is a bit inefficient for long periods, but it's easy to avoid jumps
      // and rounding errors this way. The inefficiency shouldn't matter as long as the bucketSize
      // isn't huge.
      while (remainingTokens < bucketSize && nextRecovery.isOverdue()) {
        remainingTokens += 1
        nextRecovery += tokenRecoveryInterval
      }
      if (remainingTokens == bucketSize &&
        (DeadlineWithTimeSource.now(timeSource) - nextRecovery) > tokenRecoveryInterval) {
        // Reset the recovery time, so we don't accumulate infinite recovery while nothing is
        // going on.
        nextRecovery = DeadlineWithTimeSource.now(timeSource) + tokenRecoveryInterval
      }
    } catch {
      case _: IllegalArgumentException =>
        // Adding FiniteDuration throws IllegalArgumentException instead of wrapping on overflow.
        // Given that this happens every ~300 years, we can afford some non-linearity here,
        // rather than taking the effort to properly work around that.
        nextRecovery = DeadlineWithTimeSource(Duration(-Long.MaxValue, NANOSECONDS), timeSource)
    }
  }
}

/**
 * This is essentially the same as Scala's [[Deadline]],
 * just with a custom source of nanoTime so it can actually be tested properly.
 */
case class DeadlineWithTimeSource(
    time: FiniteDuration,
    timeSource: NanoTimeTimeSource = SystemNanoTimeSource) {
  // Only implemented the methods LogThrottler actually needs for now.

  /**
   * Return a deadline advanced (i.e., moved into the future) by the given duration.
   */
  def +(other: FiniteDuration): DeadlineWithTimeSource = copy(time = time + other)

  /**
   * Calculate time difference between this and the other deadline, where the result is directed
   * (i.e., may be negative).
   */
  def -(other: DeadlineWithTimeSource): FiniteDuration = time - other.time

  /**
   * Determine whether the deadline lies in the past at the point where this method is called.
   */
  def isOverdue(): Boolean = (time.toNanos - timeSource.nanoTime()) <= 0
}

object DeadlineWithTimeSource {
  /**
   * Construct a deadline due exactly at the point where this method is called. Useful for then
   * advancing it to obtain a future deadline, or for sampling the current time exactly once and
   * then comparing it to multiple deadlines (using subtraction).
   */
  def now(timeSource: NanoTimeTimeSource = SystemNanoTimeSource): DeadlineWithTimeSource =
    DeadlineWithTimeSource(Duration(timeSource.nanoTime(), NANOSECONDS), timeSource)
}

/** Generalisation of [[System.nanoTime()]]. */
private[delta] trait NanoTimeTimeSource {
  def nanoTime(): Long
}
private[delta] object SystemNanoTimeSource extends NanoTimeTimeSource {
  override def nanoTime(): Long = System.nanoTime()
}
