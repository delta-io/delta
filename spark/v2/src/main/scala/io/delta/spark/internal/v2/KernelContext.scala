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

package io.delta.spark.internal.v2

import org.apache.spark.sql.SparkSession

/**
 * Immutable, cache-safe value for deferred Hadoop configuration materialization.
 *
 * It retains only session-invariant filesystem options. When filesystem I/O begins, it binds those
 * options to the active Spark session so session-derived settings and credentials are not retained
 * by reusable connector state.
 */
private[v2] final class KernelContext(val sessionInvariantFsOptions: Map[String, String]) {
  require(sessionInvariantFsOptions != null, "sessionInvariantFsOptions must not be null")

  private[v2] def materializeHadoopConf() =
    SparkSession.active.sessionState.newHadoopConfWithOptions(sessionInvariantFsOptions)
}

private[v2] object KernelContext {
  val empty: KernelContext = new KernelContext(Map.empty)

  def apply(): KernelContext = empty

  def apply(sessionInvariantFsOptions: Map[String, String]): KernelContext = {
    require(sessionInvariantFsOptions != null, "sessionInvariantFsOptions must not be null")
    if (sessionInvariantFsOptions.isEmpty) empty else new KernelContext(sessionInvariantFsOptions)
  }
}
