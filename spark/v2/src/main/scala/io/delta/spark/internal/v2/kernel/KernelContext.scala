/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.spark.internal.v2.kernel

import org.apache.spark.sql.delta.storage.LogStore
import io.delta.kernel.defaults.engine.{DefaultEngine => KernelDefaultEngine}
import io.delta.kernel.engine.{Engine => KernelEngine}

import org.apache.spark.sql.SparkSession

/**
 * Table-scoped owner of Kernel resources and deferred Hadoop configuration materialization.
 *
 * It retains session-invariant filesystem options and the table manager's LogStore. Hadoop
 * configuration materialization combines those options with the active Spark session's settings.
 * Callers look up an engine when an operation needs it instead of retaining their own engine.
 */
final class KernelContext(
    private[v2] val sessionInvariantFsOptions: Map[String, String],
    private[v2] val logStore: LogStore) {
  require(sessionInvariantFsOptions != null, "sessionInvariantFsOptions must not be null")
  require(logStore != null, "logStore must not be null")

  private[v2] def materializeHadoopConf() =
    SparkSession.active.sessionState.newHadoopConfWithOptions(sessionInvariantFsOptions)

  private def createDefaultEngine(): KernelEngine = {
    val hadoopConf = materializeHadoopConf()
    KernelDefaultEngine.create(hadoopConf)
  }


  // The snapshot facade lives in a separate internal package in the standalone connector.
  def getDefaultEngine(): KernelEngine = {
    createDefaultEngine()
  }
}

private[v2] object KernelContext {
  def empty(logStore: LogStore): KernelContext = new KernelContext(Map.empty, logStore)

  def apply(sessionInvariantFsOptions: Map[String, String], logStore: LogStore): KernelContext =
    new KernelContext(sessionInvariantFsOptions, logStore)
}
