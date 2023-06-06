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

package org.apache.spark.sql.delta

import org.scalatest.Assertions._

/**
 * Various helper methods to for metric tests.
 */
object DeltaMetricsUtils
  {

  /**
   * Get operation metrics of the last operation of a table.
   *
   * @param table The Delta table to query
   * @return The operation metrics of the last command.
   */
  def getLastOperationMetrics(table: io.delta.tables.DeltaTable): Map[String, Long] = {
    table.history().select("operationMetrics").take(1).head.getMap(0)
      .asInstanceOf[Map[String, String]].mapValues(_.toLong).toMap
  }

  def getLastOperationMetrics(tableName: String): Map[String, Long] = {
    getLastOperationMetrics(io.delta.tables.DeltaTable.forName(tableName))
  }

   /**
   * Assert that metrics of a Delta operation have the expected values.
   *
   * @param expectedMetrics The expected metrics the values of which to check.
   * @param operationMetrics The operation metrics that were collected from Delta log.
   */
  def checkOperationMetrics(
      expectedMetrics: Map[String, Long],
      operationMetrics: Map[String, Long]): Unit = {
    val sep = System.lineSeparator() * 2
    val failMessages = expectedMetrics.flatMap { case (metric, expectedValue) =>
      // Check missing metrics.
      var errMsg = if (!operationMetrics.contains(metric)) {
        Some(
          s"""The recorded operation metrics does not contain metric: $metric"
             | ExpectedMetrics = $expectedMetrics
             | ActualMetrics = $operationMetrics
             |""".stripMargin)
      } else {
        None
      }

      // Check negative values.
      errMsg = errMsg.orElse {
        if (operationMetrics(metric) < 0) {
          Some(s"Invalid non-positive value for metric $metric: ${operationMetrics(metric)}")
        } else {
          None
        }
      }

      // Check unexpected values.
      errMsg = errMsg.orElse {
        if (expectedValue != operationMetrics(metric)) {
          Some(
            s"""The recorded metric for $metric does not equal the expected value.
               | Expected = ${expectedMetrics(metric)}
               | Actual = ${operationMetrics(metric)}
               | ExpectedMetrics = $expectedMetrics
               | ActualMetrics = $operationMetrics
               |""".stripMargin)
        } else {
          None
        }
      }
      errMsg
    }.mkString(sep, sep, sep).trim
    assert(failMessages.isEmpty)
  }

  /**
   * Check that time metrics for a Delta operation are valid.
   *
   * @param operationMetrics The collected operation metrics from the Delta log.
   * @param expectedMetrics The keys of the expected time metrics. Set to None to check for
   *                        common time metrics.
   */
  def checkOperationTimeMetrics(
      operationMetrics: Map[String, Long],
      expectedMetrics: Set[String]): Unit = {
    // Validate that all time metrics exist and have a non-negative value.
    for (key <- expectedMetrics) {
      assert(operationMetrics.contains(key), s"Missing operation metric $key")
      val value: Long = operationMetrics(key)
      assert(value >= 0,
        s"Invalid non-positive value for metric $key: $value")
    }

    // Validate that if 'executionTimeMs' exists, is larger than all other time metrics.
    if (expectedMetrics.contains("executionTimeMs")) {
      val executionTimeMs = operationMetrics("executionTimeMs")
      val maxTimeMs = operationMetrics.filterKeys(k => expectedMetrics.contains(k))
        .valuesIterator.max
      assert(executionTimeMs == maxTimeMs)
    }
  }
}
