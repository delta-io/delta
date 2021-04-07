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

package org.apache.spark.sql.delta.files

import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * This trait is used to register SQL metrics for a Delta Operation.
 * Registering will allow the metrics to be instrumented via the CommitInfo and is accessible via
 * DescribeHistory
 */
trait SQLMetricsReporting {

  // Map of SQL Metrics
  private var operationSQLMetrics = Map[String, SQLMetric]()

  /**
   * Register SQL metrics for an operation by appending the supplied metrics map to the
   * operationSQLMetrics map.
   */
  def registerSQLMetrics(spark: SparkSession, metrics: Map[String, SQLMetric]): Unit = {
    if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
      operationSQLMetrics = operationSQLMetrics ++ metrics
    }
  }

  /**
   * Get the metrics for an operation based on collected SQL Metrics and filtering out
   * the ones based on the metric parameters for that operation.
   */
  def getMetricsForOperation(operation: Operation): Map[String, String] = {
    operation.transformMetrics(operationSQLMetrics)
  }

  /** Returns the metric with `name` registered for the given transaction if it exists. */
  def getMetric(name: String): Option[SQLMetric] = {
    operationSQLMetrics.get(name)
  }
}
