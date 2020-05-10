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

package com.databricks.spark.util

/**
 * This file contains stub implementation for logging that exists in Databricks.
 */


class TagDefinition

object TagDefinitions {
  object TAG_TAHOE_PATH extends TagDefinition
  object TAG_TAHOE_ID extends TagDefinition
  object TAG_ASYNC extends TagDefinition
  object TAG_LOG_STORE_CLASS extends TagDefinition
  object TAG_OP_TYPE extends TagDefinition
}

case class OpType(typeName: String, description: String)

class MetricDefinition

object MetricDefinitions {
  object EVENT_LOGGING_FAILURE extends MetricDefinition
  object EVENT_TAHOE extends MetricDefinition
}

trait DatabricksLogging {
  // scalastyle:off println
  def logConsole(line: String): Unit = println(line)
  // scalastyle:on println

  def recordUsage(
      metric: MetricDefinition,
      quantity: Double,
      additionalTags: Map[TagDefinition, String] = Map.empty,
      blob: String = null,
      forceSample: Boolean = false,
      trimBlob: Boolean = true,
      silent: Boolean = false): Unit = {

  }

  def recordEvent(
      metric: MetricDefinition,
      additionalTags: Map[TagDefinition, String] = Map.empty,
      blob: String = null,
      trimBlob: Boolean = true): Unit = {
    recordUsage(metric, 1, additionalTags, blob, trimBlob)
  }

  def recordOperation[S](
      opType: OpType,
      opTarget: String = null,
      extraTags: Map[TagDefinition, String],
      isSynchronous: Boolean = true,
      alwaysRecordStats: Boolean = false,
      allowAuthTags: Boolean = false,
      killJvmIfStuck: Boolean = false,
      outputMetric: MetricDefinition = null,
      silent: Boolean = true)(thunk: => S): S = {
    thunk
  }
}
