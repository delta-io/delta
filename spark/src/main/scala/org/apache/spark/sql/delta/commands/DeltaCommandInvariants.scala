/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.commands

import java.util.UUID

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog}
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.internal.{LogEntry, Logging, MDC}
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.execution.metric.SQLMetric

trait DeltaCommandInvariants extends DeltaLogging with SQLConfHelper with Logging {
  /**
   * Evaluate that `invariant` "holds" (evaluates to `true`) or log the violation.
   * @param invariant The invariant to evaluate.
   * @param label  It's suggested to make `label` the same text as `invariant` so it's easy to
   *               see in the logs what was evaluated.
   * @param op Operation name.
   * @param deltaLog Delta log of the table this invariant is evaluated on.
   * @param parameters Parameters that were used to evaluate the invariant.
   * @param additionalInfo Additional info to be included in usage logs.
   */
  protected def checkCommandInvariant(
      invariant: () => Boolean,
      label: String,
      op: Operation,
      deltaLog: DeltaLog,
      parameters: => Map[String, Long],
      additionalInfo: => Map[String, String] = Map.empty): Unit = {
    val id = UUID.randomUUID()
    val invariantResult = try {
      invariant()
    } catch {
      case NonFatal(e) =>
        logWarning(log"Exception thrown while evaluating command invariant." +
          log" Reference: ${MDC(DeltaLogKeys.ERROR_ID, id.toString)}.", e)
        false
    }
    if (!invariantResult) {
      val shouldFail = conf.getConf(DeltaSQLConf.COMMAND_INVARIANT_CHECKS_THROW)
      try {
        val opType =
          "delta.assertions.unreliable.commandInvariantViolated"
        val info = CommandInvariantCheckInfo(
          exceptionThrown = shouldFail,
          id = id,
          invariantExpression = label,
          invariantParameters = parameters,
          operation = op.name,
          operationParameters = op.jsonEncodedValues,
          additionalInfo = additionalInfo
        )
        recordDeltaEvent(
          deltaLog,
          opType = opType,
          data = info)

        // Log this to Spark logs as well, so someone looking through there knows to look for the
        // details in usage logs.
        // FIXME: Needs inline type annotations because otherwise compiler gets confused on
        //        implicit conversions.
        val logEntry: LogEntry = log"Delta Command Invariant violated." +
          log" Reference: ${MDC(DeltaLogKeys.ERROR_ID, id.toString)}." +
          log" Info: ${MDC(DeltaLogKeys.INVARIANT_CHECK_INFO, info)}."
        logWarning(logEntry)
      } catch {
        case NonFatal(e) =>
          logWarning(log"Unexpected error while logging command invariant violation." +
            log" Reference: ${MDC(DeltaLogKeys.ERROR_ID, id.toString)}.", e)
      }

      if (shouldFail) {
        throw DeltaErrors.commandInvariantViolationException(operation = op.name, id = id)
      }
    }
  }
}

// Recorded when a command invariant is violated.
case class CommandInvariantCheckInfo(
    exceptionThrown: Boolean,
    id: UUID,
    invariantExpression: String,
    invariantParameters: Map[String, Long],
    operation: String,
    operationParameters: Map[String, String],
    additionalInfo: Map[String, String])

abstract class CommandInvariantMetricValue extends Logging {

  protected def value: Try[Long]

  def getOrThrow: Long = value.get

  def getOrDummy: Long = value.getOrElse(-1L)
}

case class CommandInvariantMetricValueFromSingle(
    metric: SQLMetric) extends CommandInvariantMetricValue  {
  override protected val value: Try[Long] = Try {
    metric.value
  }
}
