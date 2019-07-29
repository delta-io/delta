/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.metering.EventLoggingProvider
import org.apache.spark.sql.delta.util.JsonUtils

/**
 * This file contains stub implementation for logging that exists in Databricks.
 */


class TagDefinition {
  def getSimpleClassName: String = {
    getClass.getSimpleName.replaceAll("\\$", "")
  }
}

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

  val eventLogging = EventLogging()

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
    val event = Map(
      "metric" -> metric,
      "quantity" -> quantity,
      "additionalTags" -> additionalTags.map(kv => (kv._1.getSimpleClassName, kv._2)),
      "blob" -> blob,
      "forceSample" -> forceSample,
      "trimBlob" -> trimBlob
    )
    val jsonString = JsonUtils.toJson(event)
    eventLogging.logEvent(jsonString)

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
    val event = Map(
      "opType" -> opType,
      "opTarget" -> opTarget,
      "extraTags" -> extraTags.map(kv => (kv._1.getSimpleClassName, kv._2)),
      "isSynchronous" -> isSynchronous,
      "alwaysRecordStats" -> alwaysRecordStats,
      "allowAuthTags" -> allowAuthTags,
      "killJvmIfStuck" -> killJvmIfStuck,
      "outputMetric" -> outputMetric,
      "silent" -> silent
    )
    val jsonString = JsonUtils.toJson(event)
    eventLogging.logEvent(jsonString)
    thunk
  }
}

/**
 * event logging that logs delta metrics to custom metrics system.
 * default implementation is [[DefaultConsoleEventLogging]].
 */
trait EventLogging {
  def logEvent(json: String)
}

object EventLogging extends EventLoggingProvider {
  def apply(): EventLogging = createEventLogging(SparkEnv.get.conf)
}

/**
 * Console event logging implementation.
 */
class DefaultConsoleEventLogging(conf: SparkConf) extends EventLogging with Logging {
  def logEvent(json: String): Unit = {
    logInfo(json)
  }
}
