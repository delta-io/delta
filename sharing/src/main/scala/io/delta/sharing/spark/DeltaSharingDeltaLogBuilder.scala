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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingProfileProvider}
import io.delta.sharing.client.model.{Table => DeltaSharingTable}
import io.delta.sharing.client.util.ConfUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

object DeltaSharingDeltaLogBuilder extends Logging {

  /**
   * A built synthetic Delta log with its [[CachedTableManager]] registration deferred to
   * [[registerPreSignedUrls]]. Pure data: it carries exactly what that call hands to
   * `CachedTableManager.register`, already computed. Splitting registration out lets the DSV2 path
   * build the Kernel scan first and anchor the cache entry to the scan's snapshot manager (see
   * [[DeltaSharingDSV2Utils.buildScan]]); safe to defer because nothing between building the log
   * and executor read consults the presigned-url cache.
   *
   * @param encodedPath the encoded `delta-sharing-log://` [[Path]] to open.
   * @param registrationTablePath table path + hash, the cache key (CachedTableManager customizes
   *   it).
   * @param idToUrl the file-id -> presigned-url mapping to cache.
   * @param minUrlExpirationTimestamp the earliest url expiration, for the cache entry's expiration.
   * @param refreshToken optional refresh token from getFiles, forwarded to the refresher.
   * @param profileProvider the sharing client's profile provider.
   * @param refresher the presigned-url refresher, built next to getFiles so the two cannot drift.
   */
  case class UnregisteredSnapshotDeltaLog private[spark] (
      encodedPath: Path,
      registrationTablePath: String,
      idToUrl: Map[String, String],
      minUrlExpirationTimestamp: Option[Long],
      refreshToken: Option[String],
      profileProvider: DeltaSharingProfileProvider,
      refresher: DeltaSharingUtils.RefresherFunction)

  private def isDeltaLogReuseEnabled: Boolean =
    SQLConf.get.getConf(DeltaSQLConf.DELTA_SHARING_SNAPSHOT_DELTA_LOG_REUSE_ENABLED)

  /**
   * Whether reuse is possible. Only query-specific cache entries take another query's state, and
   * CachedTableManager.register creates them only with the IO cache on and a query id; registering
   * into them also needs a refresher wrapper.
   */
  private def canReuseSnapshotDeltaLog(profileProvider: DeltaSharingProfileProvider): Boolean = {
    isDeltaLogReuseEnabled && ConfUtils.sparkParquetIOCacheEnabled(SQLConf.get) &&
      profileProvider.getCustomQueryId().isDefined &&
      profileProvider.getCustomRefresherWrapper().isDefined
  }

  /**
   * Reuse the log an identical earlier query built (same table and `queryParamsHashId`), and
   * register this query in its cache entry so its urls stay refreshed. This query's getMetadata
   * has already checked access. Requires [[canReuseSnapshotDeltaLog]].
   *
   * @return the log's path if reused, else None.
   */
  private def tryReuseSnapshotDeltaLog(
      client: DeltaSharingClient,
      tablePath: String,
      queryParamsHashId: String,
      anchor: AnyRef,
      logPrefix: String): Option[Path] = {
    val profileProvider = client.getProfileProvider
    // Key of the log's blocks and urls: getCustomTablePath doesn't change it with the IO cache on.
    val tablePathWithHash = DeltaSharingUtils.getTablePathWithIdSuffix(tablePath, queryParamsHashId)
    if (!DeltaSharingLogFileSystem.isSnapshotDeltaLogReusable(tablePathWithHash)) {
      return None
    }
    CachedTableManager.INSTANCE.registerQueryStatesInQuerySpecificCachedTable(
      tablePath = tablePathWithHash,
      refs = Seq(new WeakReference(anchor)),
      profileProvider = profileProvider)
    logInfo(s"$logPrefix: reusing the synthetic delta log and presigned urls of " +
      s"$tablePathWithHash.")
    Some(DeltaSharingLogFileSystem.encode(tablePathWithHash))
  }

  /**
   * Build the synthetic Delta log for a single-version (snapshot) Delta Sharing read: issue the
   * getFiles RPC and fabricate a version-0 Delta log in the BlockManager (served back through the
   * synthetic `delta-sharing-log://` Hadoop FS). The [[CachedTableManager]] registration is
   * deferred to [[registerPreSignedUrls]] so the caller can pick the anchor once it holds a durable
   * per-query object.
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
   * @param logPrefix caller tag for the fetch log line, so the shared loader does not hard-code a
   *   DSV2 tag onto plain V1 reads.
   */
  def buildSnapshotDeltaLog(
      client: DeltaSharingClient,
      table: DeltaSharingTable,
      options: DeltaSharingOptions,
      tablePath: String,
      queryParamsHashId: String,
      jsonPredicateHints: Option[String],
      limit: Option[Long],
      logPrefix: String): UnregisteredSnapshotDeltaLog = {
    // 1. Call client.getFiles, and build the matching refresher next to it so the two share the
    // same params.
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
    val refresher = DeltaSharingUtils.getRefresherForGetFiles(
      client = client,
      table = table,
      predicates = Nil,
      limit = limit,
      versionAsOf = options.versionAsOf,
      timestampAsOf = options.timestampAsOf,
      jsonPredicateHints = jsonPredicateHints,
      useRefreshToken = true
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

    // 3. Carry the (pre-computed) registration inputs; registerPreSignedUrls does the register.
    UnregisteredSnapshotDeltaLog(
      encodedPath = DeltaSharingLogFileSystem.encode(tablePathWithHashIdSuffix),
      // Using tablePath directly because it will be customized within CachedTableManager.
      registrationTablePath =
        DeltaSharingUtils.getTablePathWithIdSuffix(tablePath, queryParamsHashId),
      idToUrl = deltaLogMetadata.idToUrl,
      minUrlExpirationTimestamp = deltaLogMetadata.minUrlExpirationTimestamp,
      refreshToken = deltaTableFiles.refreshToken,
      profileProvider = client.getProfileProvider,
      refresher = refresher
    )
  }

  /**
   * Register parquet file-id to presigned-url mapping with the [[CachedTableManager]], keyed to
   * the lifetime of `anchor` via a [[WeakReference]]: the cached URLs (and their background
   * refresh) become collectable once `anchor` is unreachable.
   *
   * @param anchor a per-query object bounding the cache entry: V1 passes its
   *   [[DeltaSharingFileIndex]], DSV2 the scan's snapshot manager. A per-table object (e.g. a
   *   catalog-cached `Table`) would outlive the query and pin stale URLs.
   */
  def registerPreSignedUrls(unregistered: UnregisteredSnapshotDeltaLog, anchor: AnyRef): Unit = {
    CachedTableManager.INSTANCE.register(
      tablePath = unregistered.registrationTablePath,
      idToUrl = unregistered.idToUrl,
      refs = Seq(new WeakReference(anchor)),
      profileProvider = unregistered.profileProvider,
      refresher = unregistered.refresher,
      expirationTimestamp =
        if (CachedTableManager.INSTANCE
            .isValidUrlExpirationTime(unregistered.minUrlExpirationTimestamp)) {
          unregistered.minUrlExpirationTimestamp.get
        } else {
          System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
        },
      refreshToken = unregistered.refreshToken,
      // With reuse on, keep the urls after this query ends, for later identical queries.
      keepUrlsAfterRefsGone = isDeltaLogReuseEnabled
    )
  }

  /**
   * Reuse an identical earlier query's synthetic Delta log, or build it and register its presigned
   * URLs against `anchor`, returning the encoded `delta-sharing-log://` [[Path]]. Used by the V1
   * path ([[DeltaSharingFileIndex]]), whose anchor (its FileIndex) already exists at call time.
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
    def build(): Path = {
      val unregistered = buildSnapshotDeltaLog(
        client = client,
        table = table,
        options = options,
        tablePath = tablePath,
        queryParamsHashId = queryParamsHashId,
        jsonPredicateHints = jsonPredicateHints,
        limit = limit,
        logPrefix = logPrefix
      )
      registerPreSignedUrls(unregistered, anchor)
      unregistered.encodedPath
    }
    if (canReuseSnapshotDeltaLog(client.getProfileProvider)) {
      tryReuseSnapshotDeltaLog(client, tablePath, queryParamsHashId, anchor, logPrefix)
        .getOrElse(build())
    } else {
      build()
    }
  }
}
