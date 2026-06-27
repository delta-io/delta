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

package io.delta.sharing.spark

import java.lang.ref.WeakReference

import io.delta.sharing.client.DeltaSharingClient
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import org.apache.hadoop.fs.Path

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.internal.Logging

object DeltaSharingDeltaLogBuilder extends Logging {

  /**
   * Build the synthetic Delta log for a single-version (snapshot) Delta Sharing read: issue the
   * getFiles RPC, fabricate a version-0 Delta log in the BlockManager (served back through the
   * synthetic `delta-sharing-log://` Hadoop FS), register the file-id -> presigned-url mapping with
   * the [[CachedTableManager]], and return the encoded `delta-sharing-log://` [[Path]].
   *
   * Snapshot construction is left to the caller: the V1 path ([[DeltaSharingFileIndex]]) feeds the
   * path to classic `DeltaLog.forTable`; the DSV2 scan feeds `.toString` to Delta Kernel's
   * `PathBasedSnapshotManager`, whose DefaultEngine reads the same synthetic Hadoop FS.
   *
   * @param client Delta Sharing client for the getFiles RPC and presigned-url refresh.
   * @param table the shared table to fetch.
   * @param options read options; supplies versionAsOf / timestampAsOf for the snapshot.
   * @param tablePath the shared table path; base of both the synthetic log path and the
   *   [[CachedTableManager]] key.
   * @param queryParamsHashId hash of the query parameters, appended as a suffix to the table path
   *   and the cache key so different query shapes on the same table stay distinct.
   * @param jsonPredicateHints optional server-side filter hints forwarded to getFiles.
   * @param limit optional row limit forwarded to getFiles.
   * @param anchor object whose lifetime bounds the [[CachedTableManager]] entry. A
   *   [[WeakReference]] to it is registered, so the cached presigned URLs become collectable once
   *   the anchor is unreachable. The anchor must be a per-query object: V1 passes the per-query
   *   [[DeltaSharingFileIndex]]; the DSV2 path must pass the per-query `Scan`, not the per-table
   *   `Table`. A `Table` is cached by the catalog and outlives the query, so anchoring to it would
   *   pin stale presigned URLs in the cache.
   * @param logPrefix caller tag for the fetch log line, so the shared loader does not hard-code a
   *   DSV2 tag onto plain V1 reads.
   */
  def buildSnapshotDeltaLogPath(
      client: DeltaSharingClient,
      table: DeltaSharingTable,
      options: DeltaSharingOptions,
      tablePath: String,
      queryParamsHashId: String,
      jsonPredicateHints: Option[String],
      limit: Option[Long],
      anchor: AnyRef,
      logPrefix: String): Path = {
    // 1. Call client.getFiles.
    val startTime = System.currentTimeMillis()
    val deltaTableFiles = client.getFiles(
      table = table,
      predicates = Nil,
      limit = limit,
      versionAsOf = options.versionAsOf,
      timestampAsOf = options.timestampAsOf,
      jsonPredicateHints = jsonPredicateHints,
      refreshToken = None,
      fileIdHash = None
    )
    logInfo(
      s"$logPrefix: fetched ${deltaTableFiles.lines.size} lines for table $table with version " +
      s"${deltaTableFiles.version} from delta sharing server, took " +
      s"${(System.currentTimeMillis() - startTime) / 1000.0}s."
    )

    // 2. Prepare a DeltaLog.
    val tablePathWithHashIdSuffix = DeltaSharingUtils.getTablePathWithIdSuffix(
      client.getProfileProvider.getCustomTablePath(tablePath),
      queryParamsHashId
    )
    val deltaLogMetadata = DeltaSharingLogFileSystem.constructLocalDeltaLogAtVersionZero(
      deltaTableFiles.lines,
      tablePathWithHashIdSuffix
    )

    // 3. Register parquet file id to url mapping.
    CachedTableManager.INSTANCE.register(
      // Using tablePath directly because it will be customized within CachedTableManager.
      tablePath = DeltaSharingUtils.getTablePathWithIdSuffix(tablePath, queryParamsHashId),
      idToUrl = deltaLogMetadata.idToUrl,
      refs = Seq(new WeakReference(anchor)),
      profileProvider = client.getProfileProvider,
      refresher = DeltaSharingUtils.getRefresherForGetFiles(
        client = client,
        table = table,
        predicates = Nil,
        limit = limit,
        versionAsOf = options.versionAsOf,
        timestampAsOf = options.timestampAsOf,
        jsonPredicateHints = jsonPredicateHints,
        useRefreshToken = true
      ),
      expirationTimestamp =
        if (CachedTableManager.INSTANCE
            .isValidUrlExpirationTime(deltaLogMetadata.minUrlExpirationTimestamp)) {
          deltaLogMetadata.minUrlExpirationTimestamp.get
        } else {
          System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
        },
      refreshToken = deltaTableFiles.refreshToken
    )

    DeltaSharingLogFileSystem.encode(tablePathWithHashIdSuffix)
  }
}
