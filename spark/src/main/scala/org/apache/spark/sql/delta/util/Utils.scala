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

import scala.util.Random
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaColumnMappingMode, DeltaConfigs, DeltaErrors, DeltaLog, NoMapping}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf.DeltaBreakingChangeEnum
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{functions, Column, Dataset}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.ElementAt

/**
 * Various utility methods used by Delta.
 */
object Utils extends DeltaLogging {

  /** Measures the time taken by function `f` */
  def timedMs[T](f: => T): (T, Long) = {
    val start = System.currentTimeMillis()
    val res = f
    val duration = System.currentTimeMillis() - start
    (res, duration)
  }

  /** Returns the length of the random prefix to use for the data files of a Delta table. */
  def getRandomPrefixLength(metadata: Metadata): Int = {
    if (DeltaConfigs.RANDOMIZE_FILE_PREFIXES.fromMetaData(metadata)) {
      DeltaConfigs.RANDOM_PREFIX_LENGTH.fromMetaData(metadata)
    } else {
      0
    }
  }

  /**
   * Returns the length of the random prefix directory under which new data files of a Delta
   * table are written, or None to write them without one. Callers use it to decide the
   * directory prefix of the new data files they write.
   *
   * Unlike [[getRandomPrefixLength]], which only honors `delta.randomizeFilePrefixes`, this
   * method also forces a random prefix when column mapping is enabled.
   */
  private[delta] def getRandomPrefixLengthOpt(metadata: Metadata): Option[Int] = {
    getRandomPrefixLengthOpt(
      randomizeFilePrefixes = DeltaConfigs.RANDOMIZE_FILE_PREFIXES.fromMetaData(metadata),
      columnMappingMode = metadata.columnMappingMode,
      randomPrefixLength = DeltaConfigs.RANDOM_PREFIX_LENGTH.fromMetaData(metadata))
  }

  /**
   * Same as `getRandomPrefixLengthOpt(metadata)`, for callers that have already read the table
   * properties. `columnMappingMode` is evaluated only when `randomizeFilePrefixes` is false, and
   * `randomPrefixLength` only when a random prefix is used.
   */
  private[delta] def getRandomPrefixLengthOpt(
      randomizeFilePrefixes: Boolean,
      columnMappingMode: => DeltaColumnMappingMode,
      randomPrefixLength: => Int): Option[Int] = {
    if (randomizeFilePrefixes || columnMappingMode != NoMapping) {
      Some(randomPrefixLength)
    } else {
      None
    }
  }

  /** Generates a string created of `randomPrefixLength` alphanumeric characters. */
  def getRandomPrefix(numChars: Int): String = {
    Random.alphanumeric.take(numChars).mkString
  }

  /**
   * Construct a delta log from either the catalog table or a path.
   *
   * If catalogTableOpt is defined, use it to construct the delta log; otherwise, fall back to use
   * path-based delta log construction.
   */
  def getDeltaLogFromTableOrPath(
      sparkSession: SparkSession,
      catalogTableOpt: Option[CatalogTable],
      path: Path,
      options: Map[String, String] = Map.empty): DeltaLog = {
    catalogTableOpt
      .map(catalogTable => DeltaLog.forTable(sparkSession, catalogTable, options))
      .getOrElse(DeltaLog.forTable(sparkSession, path, options))
  }

  /**
   * Indicates whether Delta is currently running unit tests.
   */
  def isTesting: Boolean = {
    System.getenv("DELTA_TESTING") != null
  }

  /**
   * Returns value for the given key in value if column is a map and the key is present, NULL
   * otherwise.
   */
  def try_element_at(mapColumn: Column, key: Any): Column = {
    functions.try_element_at(mapColumn, functions.lit(key))
  }

  /**
   * Executes a validation function with configurable behavior for rolling out breaking changes.
   *
   * This utility function supports a tri-state config pattern for gradually introducing
   * validations that may break existing workloads:
   *
   * - OFF: Validation is completely skipped, maintains the current behavior.
   * - LOG_ONLY: Validation runs, but failures are only logged.
   * - ASSERT: Throw exceptions when validation fails.
   *
   * This pattern allows safe rollout of breaking changes by:
   * Starting with LOG_ONLY to gather telemetry on how many workloads would be affected,
   * switching to ASSERT after assessing the change's impact.
   *
   * All events are recorded under the opType "delta.breakingChange.recordFailure".
   * *validationConfName* can be used to determine the conf that was being applied.
   *
   * @param confEnum Enum configuration controlling the validation behavior.
   *                 Must extend [[DeltaBreakingChangeEnum]] with OFF, LOG_ONLY, and ASSERT values.
   * @param deltaLog The DeltaLog instance used for recording usage events.
   * @param spark    The SparkSession to read the configuration value from.
   * @param params   Additional parameters to include in the usage event blob.
   * @param fn       The validation function to execute. *MUST* throw an exception on failure.
   */
  def withBreakingChangeRollout(
      confEnum: DeltaBreakingChangeEnum,
      deltaLog: DeltaLog,
      spark: SparkSession,
      params: Map[String, String] = Map.empty)(fn: => Unit): Unit = {
    val mode = confEnum.fromConf(spark.sessionState.conf)
    if (mode == confEnum.OFF) return
    try {
      fn
    } catch {
      case NonFatal(e) =>
        val errorClassName = e match {
          case analysisEx: AnalysisException => analysisEx.getErrorClass
          case _ => e.getClass
        }
        recordDeltaEvent(
          deltaLog,
          "delta.breakingChange.recordFailure",
          data = Map(
            "errorClassName" -> errorClassName,
            "errorMessage" -> e.getMessage,
            "validationConfName" -> confEnum.confName,
            "additionalParams" -> params.toString()
          )
        )
        if (mode == confEnum.ASSERT) {
          throw e
        }
    }
  }
}
