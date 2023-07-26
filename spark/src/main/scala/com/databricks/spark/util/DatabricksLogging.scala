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

package com.databricks.spark.util

import scala.collection.mutable.ArrayBuffer

/**
 * This file contains stub implementation for logging that exists in Databricks.
 */

/** Used to return a recorded usage record for testing. */
case class UsageRecord(
    metric: String,
    quantity: Double,
    blob: String,
    tags: Map[String, String] = Map.empty,
    opType: Option[OpType] = None,
    opTarget: Option[String] = None)

class TagDefinition(val name: String) {
  def this() = this("BACKWARD COMPATIBILITY")
}

object TagDefinitions {
  object TAG_TAHOE_PATH extends TagDefinition("tahoePath")
  object TAG_TAHOE_ID extends TagDefinition("tahoeId")
  object TAG_ASYNC extends TagDefinition("async")
  object TAG_LOG_STORE_CLASS extends TagDefinition("logStore")
  object TAG_OP_TYPE extends TagDefinition("opType")
}

case class OpType(typeName: String, description: String)

class MetricDefinition(val name: String) {
  def this() = this("BACKWARD COMPATIBILITY")
}

object MetricDefinitions {
  object EVENT_LOGGING_FAILURE extends MetricDefinition("loggingFailureEvent")
  object EVENT_TAHOE extends MetricDefinition("tahoeEvent") with CentralizableMetric
  val METRIC_OPERATION_DURATION = new MetricDefinition("sparkOperationDuration")
    with CentralizableMetric
}

object Log4jUsageLogger {
  @volatile var usageTracker: ArrayBuffer[UsageRecord] = null

  /**
   * Records and returns all usage logs that are emitted while running the given function.
   * Intended for testing metrics that we expect to report. Note that this class does not
   * support nested invocations of the tracker.
   */
  def track(f: => Unit): Seq[UsageRecord] = {
    synchronized {
      assert(usageTracker == null, "Usage tracking does not support nested invocation.")
      usageTracker = new ArrayBuffer[UsageRecord]()
    }
    var records: ArrayBuffer[UsageRecord] = null
    try {
      f
    } finally {
      records = usageTracker
      synchronized {
        usageTracker = null
      }
    }
    records.toSeq
  }
}

trait DatabricksLogging {
  import MetricDefinitions._

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
    Log4jUsageLogger.synchronized {
      if (Log4jUsageLogger.usageTracker != null) {
        val record =
          UsageRecord(metric.name, quantity, blob, additionalTags.map(kv => (kv._1.name, kv._2)))
        Log4jUsageLogger.usageTracker.append(record)
      }
    }
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
      outputMetric: MetricDefinition = METRIC_OPERATION_DURATION,
      silent: Boolean = true)(thunk: => S): S = {
    try {
      thunk
    } finally {
      Log4jUsageLogger.synchronized {
        if (Log4jUsageLogger.usageTracker != null) {
          val record = UsageRecord(outputMetric.name, 0, null,
            extraTags.map(kv => (kv._1.name, kv._2)), Some(opType), Some(opTarget))
          Log4jUsageLogger.usageTracker.append(record)
        }
      }
    }
  }

  def recordProductUsage(
      metric: MetricDefinition with CentralizableMetric,
      quantity: Double,
      additionalTags: Map[TagDefinition, String] = Map.empty,
      blob: String = null,
      forceSample: Boolean = false,
      trimBlob: Boolean = true,
      silent: Boolean = false): Unit = {
    Log4jUsageLogger.synchronized {
      if (Log4jUsageLogger.usageTracker != null) {
        val record =
          UsageRecord(metric.name, quantity, blob, additionalTags.map(kv => (kv._1.name, kv._2)))
        Log4jUsageLogger.usageTracker.append(record)
      }
    }
  }

  def recordProductEvent(
      metric: MetricDefinition with CentralizableMetric,
      additionalTags: Map[TagDefinition, String] = Map.empty,
      blob: String = null,
      trimBlob: Boolean = true): Unit = {
    recordProductUsage(metric, 1, additionalTags, blob, trimBlob)
  }
}

trait CentralizableMetric
