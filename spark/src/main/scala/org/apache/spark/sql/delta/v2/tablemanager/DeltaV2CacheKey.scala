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

package org.apache.spark.sql.delta.v2.tablemanager

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.util.DeltaFileSystemOptions
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Cache identity for the Delta V2 table-manager cache, aligned with [[DeltaLog.DeltaLogCacheKey]].
 *
 * @param path the Delta log directory path (`_delta_log`), derived from a fully-qualified data
 *   path. This is NOT the table data path -- it is `dataPath/_delta_log`. Must be absolute;
 *   scheme presence depends on the caller's qualification (catalog-resolved paths carry a scheme,
 *   local test paths may not).
 * @param sessionInvariantFsOptions filesystem-prefixed options (`fs.*`, `dfs.*`) extracted from
 *   reader/writer options and catalog storage properties. These are the credential-bearing options
 *   that distinguish cache entries for the same path accessed with different credentials. Called
 *   "session-invariant" because they are fixed at table-resolution time and do not change across
 *   requests to the same cached composite. Values are redacted in [[toString]] to prevent
 *   credential leakage in logs.
 */
case class DeltaV2CacheKey(
    path: Path,
    sessionInvariantFsOptions: Map[String, String]) {

  override def toString: String =
    s"DeltaV2CacheKey(path=$path,fsOptions=<redacted>)"
}

object DeltaV2CacheKey {

  /**
   * Constructs a cache key from caller-supplied table coordinates.
   *
   * @param spark the active SparkSession, used to resolve filesystem options via
   *   [[DeltaFileSystemOptions.buildFsOptions]]
   * @param dataPath the table's data directory path as a fully-qualified string (e.g.
   *   `s3://bucket/warehouse/db/table` or `/tmp/local-table`). Must NOT include the `_delta_log`
   *   suffix -- this method appends it. Callers (e.g. DeltaV2Table) are responsible for passing a
   *   path already resolved by the catalog or filesystem.
   * @param options reader/writer options (Java map). Only `fs.*` and `dfs.*` prefixed entries are
   *   retained as filesystem options; all others are filtered out.
   * @param catalogTableOpt optional catalog table whose storage properties contribute additional
   *   filesystem options (merged with `options`).
   */
  def from(
      spark: SparkSession,
      dataPath: String,
      options: java.util.Map[String, String],
      catalogTableOpt: Option[CatalogTable] = None): DeltaV2CacheKey = {
    val sessionInvariantFsOptions = DeltaFileSystemOptions.buildFsOptions(
      spark, options.asScala.toMap, catalogTableOpt)
    val logPath = DeltaLog.logPathFor(dataPath)
    DeltaV2CacheKey(logPath, sessionInvariantFsOptions)
  }
}
