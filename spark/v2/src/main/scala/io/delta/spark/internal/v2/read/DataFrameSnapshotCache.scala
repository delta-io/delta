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
package io.delta.spark.internal.v2.read

import org.apache.spark.sql.DataFrame

/** Cached sorted AddFile DataFrame, keyed by version. Callers synchronize externally. */
private[v2] class DataFrameSnapshotCache(
    private[this] val version: Long,
    private[this] val sortedAddFiles: DataFrame)
    extends AutoCloseable {

  def getVersion(): Long = version

  def getSortedAddFiles(): DataFrame = sortedAddFiles

  override def close(): Unit = {
    sortedAddFiles.unpersist()
  }
}
