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
import org.apache.spark.sql.delta.util.DeltaProgressReporter
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.hadoop.fs.Path


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
}

object DeltaLogging {

  // The opType for delta commit stats.
  final val DELTA_COMMIT_STATS_OPTYPE = "delta.commit.stats"
}
