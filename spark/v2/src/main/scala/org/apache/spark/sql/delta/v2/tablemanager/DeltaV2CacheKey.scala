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
package org.apache.spark.sql.delta.v2.tablemanager

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.util.{DeltaFileSystemOptions, PathWithFileSystem}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Cache identity for the Delta V2 table-manager cache, aligned with the V1
 * DeltaLogCacheKey.
 *
 * @param path the Delta log directory path (`_delta_log`), derived from a fully-qualified
 *   data path. This is NOT the table data path -- it is `dataPath/_delta_log`. Must be
 *   absolute; scheme presence depends on the caller's qualification (catalog-resolved paths
 *   carry a scheme, local test paths may not).
 * @param sessionInvariantFsOptions filesystem-prefixed options (`fs.*`, `dfs.*`) extracted
 *   from reader/writer options and catalog storage properties. These are the
 *   credential-bearing options that distinguish cache entries for the same path accessed
 *   with different credentials. Called "session-invariant" because they are fixed at
 *   table-resolution time and do not change across requests to the same cached composite.
 *   Values are redacted in [[toString]] to prevent credential leakage in logs.
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
   * Cache identity must follow DeltaLog's resolved filesystem identity rather than lexical
   * path spelling: two paths that resolve to the same filesystem location must share a
   * cache entry, and paths that differ by authority or mount point must remain distinct.
   *
   * To achieve this, `newHadoopConfWithOptions` snapshots the current session's Hadoop
   * configuration and overlays the derived session-invariant filesystem options. This
   * Configuration is used transiently to choose the filesystem, default authority, and
   * working directory, then call `FileSystem.makeQualified`. Neither the SparkSession nor
   * the Configuration is retained in the cache key or manager.
   *
   * Unqualified paths can therefore resolve differently under different session default
   * filesystems, while already-qualified absolute paths (e.g. `s3://bucket/path`) are
   * generally stable. Different authorities and mount points remain distinct; the code
   * intentionally does not lowercase or hand-normalize bucket or authority names.
   *
   * @param spark the active SparkSession, used to resolve filesystem options via
   *   [[DeltaFileSystemOptions.buildFsOptions]] and to qualify the cache key path through
   *   the filesystem.
   * @param dataPath the table's data directory path as a string (e.g.
   *   `s3://bucket/warehouse/db/table` or `/tmp/local-table`). Must NOT include the
   *   `_delta_log` suffix -- this method appends it.
   * @param options reader/writer options (Java map). Only `fs.*` and `dfs.*` prefixed
   *   entries are retained; others are filtered out.
   * @param catalogTableOpt optional catalog table whose storage properties contribute
   *   additional filesystem options.
   */
  def from(
      spark: SparkSession,
      dataPath: String,
      options: java.util.Map[String, String],
      catalogTableOpt: Option[CatalogTable] = None
  ): DeltaV2CacheKey = {
    val sessionInvariantFsOptions =
      DeltaFileSystemOptions.buildFsOptions(
        spark, options.asScala.toMap, catalogTableOpt)
    val rawLogPath =
      DeltaTableUtils.safeConcatPaths(new Path(dataPath), "_delta_log")
    // Snapshot the session's Hadoop config with fs options overlaid.
    // Used only for qualification; not retained in the key.
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState
      .newHadoopConfWithOptions(sessionInvariantFsOptions)
    // scalastyle:on deltahadoopconfiguration
    val qualifiedLogPath = PathWithFileSystem
      .withConf(rawLogPath, hadoopConf)
      .fs
      .makeQualified(rawLogPath)
    DeltaV2CacheKey(qualifiedLogPath, sessionInvariantFsOptions)
  }
}
