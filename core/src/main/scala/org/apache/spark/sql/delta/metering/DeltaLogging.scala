/*
 * Copyright (2020) The Delta Lake Project Authors.
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

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.util.Utils

// scalastyle:off import.ordering.noEmptyLine

import com.databricks.spark.util.{DatabricksLogging, OpType, TagDefinition}
import com.databricks.spark.util.MetricDefinitions.{EVENT_LOGGING_FAILURE, EVENT_TAHOE}
import com.databricks.spark.util.TagDefinitions.{TAG_OP_TYPE, TAG_TAHOE_ID, TAG_TAHOE_PATH}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaProgressReporter
import org.apache.spark.sql.delta.util.JsonUtils

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
  extends DeltaProgressReporter {

  protected def logConsole(line: String): Unit = LoggerImplementation.activeLogger.logConsole(line)

  /**
   * Used to record the occurrence of a single event or report detailed, operation specific
   * statistics.
   */
  protected def recordDeltaEvent(
      deltaLog: DeltaLog,
      opType: String,
      tags: Map[TagDefinition, String] = Map.empty,
      data: AnyRef = null): Unit = {
    try {
      val json = if (data != null) JsonUtils.toJson(data) else ""
      val tableTags = if (deltaLog != null) {
        Map(
          TAG_TAHOE_PATH -> Try(deltaLog.dataPath.toString).getOrElse(null),
          TAG_TAHOE_ID -> Try(deltaLog.snapshot.metadata.id).getOrElse(null))
      } else {
        Map.empty
      }
      LoggerImplementation.activeLogger.recordEvent(
        EVENT_TAHOE,
        Map(TAG_OP_TYPE -> opType) ++ tableTags ++ tags,
        blob = json)
    } catch {
      case NonFatal(e) =>
        LoggerImplementation.activeLogger.recordEvent(
          EVENT_LOGGING_FAILURE,
          blob = JsonUtils.toJson(
            Map("exception" -> e.getMessage,
              "opType" -> opType,
              "method" -> "recordDeltaEvent"))
        )
    }
  }

  /**
   * Used to report the duration as well as the success or failure of an operation.
   */
  protected def recordDeltaOperation[A](
      deltaLog: DeltaLog,
      opType: String,
      tags: Map[TagDefinition, String] = Map.empty)(
      thunk: => A): A = {
    val tableTags = if (deltaLog != null) {
      Map(
        TAG_TAHOE_PATH -> Try(deltaLog.dataPath.toString).getOrElse(null),
        TAG_TAHOE_ID -> Try(deltaLog.snapshot.metadata.id).getOrElse(null))
    } else {
      Map.empty
    }
    LoggerImplementation.activeLogger.recordOperation(
        OpType(opType, ""),
        extraTags = tableTags ++ tags) {
        thunk
      }
  }
}

/**
 * Private factory to construct the logger implementation based on the active config.
 */
private object LoggerImplementation {

  private val loggers: mutable.Map[String, DatabricksLogging] = new mutable.HashMap()

  def activeLogger: DatabricksLogging =
    SparkSession.active.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_TELEMETRY_LOGGER).map{ name =>
        loggers.getOrElseUpdate(name, {
          Utils.classForName(name).newInstance.asInstanceOf[DatabricksLogging]
        })
    }.getOrElse(new EmptyLogger)

  private class EmptyLogger extends DatabricksLogging
}