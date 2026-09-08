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

import io.delta.kernel.defaults.engine.{DefaultEngine => KernelDefaultEngine}
import io.delta.kernel.engine.{Engine => KernelEngine}

import org.apache.spark.sql.delta.storage.LogStore

import org.apache.spark.sql.SparkSession

/**
 * Table-scoped owner of Kernel resources and deferred Hadoop configuration materialization.
 *
 * It retains session-invariant filesystem options and the table manager's LogStore. When
 * filesystem I/O begins, it binds those options to the active Spark session so session-derived
 * settings and credentials are not retained by reusable connector state. Its LogStore and
 * lazily-created Engine remain stable across Spark sessions.
 */
private[v2] final class KernelContext(
    val sessionInvariantFsOptions: Map[String, String],
    val logStore: LogStore) {
  require(sessionInvariantFsOptions != null, "sessionInvariantFsOptions must not be null")
  require(logStore != null, "logStore must not be null")

  private[kernel] def materializeHadoopConf() =
    SparkSession.active.sessionState.newHadoopConfWithOptions(sessionInvariantFsOptions)

  private def createDefaultEngine(): KernelEngine =
    KernelDefaultEngine.create(materializeHadoopConf())

  private lazy val kernelDefaultEngine = createDefaultEngine()

  private[v2] def getDefaultEngine(): KernelEngine = kernelDefaultEngine
}

private[v2] object KernelContext {
  def empty(logStore: LogStore): KernelContext = new KernelContext(Map.empty, logStore)

  def apply(sessionInvariantFsOptions: Map[String, String], logStore: LogStore): KernelContext =
    new KernelContext(sessionInvariantFsOptions, logStore)
}
