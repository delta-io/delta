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
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.delta.{
  DeltaErrors,
  DeltaLog,
  DeltaOptions,
  SnapshotDescriptor
}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.{
  DeltaDataSource,
  DeltaSource,
  DeltaSourceOffset
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.sharing.client.{DeltaSharingClient, DeltaSharingRestClient}
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.client.model.{DeltaTableFiles, DeltaTableMetadata, Table => DeltaSharingTable}

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{
  ReadLimit,
  SupportsAdmissionControl,
  SupportsTriggerAvailableNow
}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * A streaming source for a Delta Sharing table.
 *
 * This class wraps a DeltaSource to read data out of locally constructed delta log.
 * When a new stream is started, delta sharing starts by fetching delta log from the server side,
 * constructing a local delta log, and call delta source apis to compute offset or read data.
 *
 * TODO: Support SupportsConcurrentExecution.
 */
case class DeltaFormatSharingSource(
    spark: SparkSession,
    client: DeltaSharingClient,
    table: DeltaSharingTable,
    options: DeltaSharingOptions,
    parameters: Map[String, String],
    sqlConf: SQLConf,
    metadataPath: String)
    extends Source
    with SupportsAdmissionControl
    with SupportsTriggerAvailableNow
    with DeltaLogging {

  private val sourceId = Some(UUID.randomUUID().toString().split('-').head)

  private var tableId: String = "unset_table_id"

  private val tablePath = options.options.getOrElse(
    "path",
    throw DeltaSharingErrors.pathNotSpecifiedException
  )

  // A unique string composed of a formatted timestamp and an uuid.
  // Used as a suffix for the table name and its delta log path of a delta sharing table in a
  // streaming job, to avoid overwriting the delta log from multiple references of the same delta
  // sharing table in one streaming job.
  private val timestampWithUUID = DeltaSharingUtils.getFormattedTimestampWithUUID()
  private val customTablePathWithUUIDSuffix = DeltaSharingUtils.getTablePathWithIdSuffix(
    client.getProfileProvider.getCustomTablePath(tablePath),
    timestampWithUUID
  )
  private val deltaLogPath =
    s"${DeltaSharingLogFileSystem.encode(customTablePathWithUUIDSuffix).toString}/_delta_log"

  // The latest metadata of the shared table, fetched at the initialization time of the
  // DeltaFormatSharingSource, used to initialize the wrapped DeltaSource.
  private lazy val deltaSharingTableMetadata =
    DeltaSharingUtils.getDeltaSharingTableMetadata(client, table)

  private lazy val deltaSource = initDeltaSource()

  private def initDeltaSource(): DeltaSource = {
    val (localDeltaLog, snapshotDescriptor) = DeltaSharingUtils.getDeltaLogAndSnapshotDescriptor(
      spark,
      deltaSharingTableMetadata,
      customTablePathWithUUIDSuffix
    )
    // Delta sharing delta log doesn't have catalog table and `localDeltaLog` is not binded to
    // catalog table.
    val schemaTrackingLogOpt =
      DeltaDataSource.getMetadataTrackingLogForDeltaSource(
        spark,
        snapshotDescriptor,
        catalogTableOpt = None,
        parameters,
        // Pass in the metadata path opt so we can use it for validation
        sourceMetadataPathOpt = Some(metadataPath)
      )

    val readSchema = schemaTrackingLogOpt
      .flatMap(_.getCurrentTrackedMetadata.map(_.dataSchema))
      .getOrElse(snapshotDescriptor.schema)

    if (readSchema.isEmpty) {
      throw DeltaErrors.schemaNotSetException
    }

    // Catalog table represents the table's catalog metadata and it's managed by Unity Catalog.
    // Delta sharing delta log doesn't have it and `localDeltaLog` is not bound to it.
    DeltaSource(
      spark = spark,
      deltaLog = localDeltaLog,
      catalogTableOpt = None,
      options = new DeltaOptions(parameters, sqlConf),
      snapshotAtSourceInit = snapshotDescriptor,
      metadataPath = metadataPath,
      metadataTrackingLog = schemaTrackingLogOpt
    )
  }

  // schema of the streaming source, based on the latest metadata of the shared table.
  override val schema: StructType = {
    val schemaWithoutCDC = deltaSharingTableMetadata.metadata.schema
    tableId = deltaSharingTableMetadata.metadata.deltaMetadata.id
    if (options.readChangeFeed) {
      CDCReader.cdcReadSchema(schemaWithoutCDC)
    } else {
      schemaWithoutCDC
    }
  }

  // When restarting from a legacy DeltaSharingSourceOffset checkpoint that is still in the
  // initial snapshot (isStartingVersion=true), file index ordering differs between
  // DeltaSharingSource (sort by file ID) and DeltaSource (sort by modificationTime and path).
  // We delegate to a DeltaSharingSource instance during the snapshot phase, then switch
  // back to normal DeltaFormatSharingSource logic once the snapshot completes.
  //
  // (snapshotVersion, DeltaSharingSource)
  private var legacyDeltaSharingSourceOpt: Option[(Long, DeltaSharingSource)] = None

  /**
   * Create the legacy DeltaSharingSource for snapshot delegation.
   *
   * @param snapshotVersion the initial snapshot version from the legacy checkpoint offset,
   *                        used to fetch metadata at that version so RemoteDeltaLog is
   *                        initialized with the correct schema.
   */
  private def getOrCreateLegacySource(snapshotVersion: Long): DeltaSharingSource = {
    // Safeguard: this error should never happen, as we only delegate to the legacy
    // source for the initial snapshot.
    legacyDeltaSharingSourceOpt.foreach { case (existingVersion, _) =>
      if (existingVersion != snapshotVersion) {
        throw new IllegalStateException(
          s"Legacy DeltaSharingSource was created for snapshot version $existingVersion " +
            s"but is now requested for version $snapshotVersion")
      }
    }
    legacyDeltaSharingSourceOpt.map(_._2).getOrElse {
      logInfo(s"Initializing legacy DeltaSharingSource for snapshot delegation at " +
        s"version $snapshotVersion," + getTableInfoForLogging)
      // Create a parquet-format client to fetch metadata, since RemoteDeltaLog
      // expects parquet-format DeltaTableMetadata (with protocol/metadata fields
      // set, not just lines).
      val parsedPath = DeltaSharingRestClient.parsePath(
        tablePath, options.shareCredentialsOptions)
      val parquetClient = DeltaSharingRestClient(
        profileFile = parsedPath.profileFile,
        shareCredentialsOptions = options.shareCredentialsOptions,
        forStreaming = true,
        responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_PARQUET
      )
      val deltaTableMetadata = try {
        parquetClient.getMetadata(table, versionAsOf = Some(snapshotVersion))
      } finally {
        parquetClient match {
          case restClient: DeltaSharingRestClient => restClient.close()
          case _ =>
        }
      }
      val deltaLog = RemoteDeltaLog(
        path = tablePath,
        shareCredentialsOptions = options.shareCredentialsOptions,
        forStreaming = true,
        responseFormat = DeltaSharingOptions.RESPONSE_FORMAT_PARQUET,
        initDeltaTableMetadata = Some(deltaTableMetadata),
        callerOrg = options.callerOrg
      )
      val source = DeltaSharingSource(spark, deltaLog, options)
      legacyDeltaSharingSourceOpt = Some((snapshotVersion, source))
      source
    }
  }

  // Latest endOffset of the getBatch call, used to compute startingOffset which will then be used
  // to compare with the the latest table version on server to decide whether to fetch new data.
  private var latestProcessedEndOffsetOption: Option[DeltaSourceOffset] = None

  // Latest table version for the data fetched from the delta sharing server, and stored in the
  // local delta log. Used to check whether all fetched files are processed by the DeltaSource.
  private var latestTableVersionInLocalDeltaLogOpt: Option[Long] = None

  // This is needed because DeltaSource is not advancing the offset to the next version
  // automatically when scanning through a snapshot, so DeltaFormatSharingSource needs to count the
  // number of files in the min version and advance the offset to the next version when the offset
  // is at the last index of the version.
  private var numFileActionsInStartingSnapshotOpt: Option[Int] = None

  // Latest timestamp for getTableVersion rpc from the server, used to compare with the current
  // timestamp, to ensure the gap QUERY_TABLE_VERSION_INTERVAL_MILLIS between two rpcs, to avoid
  // a high traffic load to the server.
  private var lastTimestampForGetVersionFromServer: Long = -1

  // The minimum gap between two getTableVersion rpcs, to avoid a high traffic load to the server.
  private val QUERY_TABLE_VERSION_INTERVAL_MILLIS = {
    val intervalSeconds = ConfUtils.MINIMUM_TABLE_VERSION_INTERVAL_SECONDS.max(
      ConfUtils.streamingQueryTableVersionIntervalSeconds(spark.sessionState.conf)
    )
    logInfo(s"Configured queryTableVersionIntervalSeconds:${intervalSeconds}," +
      getTableInfoForLogging)
    if (intervalSeconds < ConfUtils.MINIMUM_TABLE_VERSION_INTERVAL_SECONDS) {
      throw new IllegalArgumentException(s"QUERY_TABLE_VERSION_INTERVAL_MILLIS($intervalSeconds) " +
        s"must not be less than ${ConfUtils.MINIMUM_TABLE_VERSION_INTERVAL_SECONDS} seconds,"
        + getTableInfoForLogging)
    }
    TimeUnit.SECONDS.toMillis(intervalSeconds)
  }

  // Maximum number of versions of getFiles() rpc when fetching files from the server. Used to
  // reduce the number of files returned to avoid timeout of the rpc on the server.
  private val maxVersionsPerRpc: Int = options.maxVersionsPerRpc.getOrElse(
    DeltaSharingOptions.MAX_VERSIONS_PER_RPC_DEFAULT
  )

  private lazy val getTableInfoForLogging: String =
    s" for table(id:$tableId, name:${table.toString}, source:$sourceId)"

  private def getQueryIdForLogging: String = {
    s", with queryId(${client.getQueryId})"
  }

  // Whether this query is running in Trigger.AvailableNow mode.
  @volatile private var isTriggerAvailableNow: Boolean = false

  // The server version captured at query start for Trigger.AvailableNow. All processing is capped
  // at this version. Not persisted -- re-captured fresh on every query start.
  @volatile private var frozenServerVersionForAvailableNow: Long = -1

  // A variable to store the latest table version on server, returned from the getTableVersion rpc.
  // Used to store the latest table version for getOrUpdateLatestTableVersion when not getting
  // updates from the server.
  // For all other callers, please use getOrUpdateLatestTableVersion instead of this variable.
  private var latestTableVersionOnServer: Long = -1

  override def prepareForTriggerAvailableNow(): Unit = {
    if (frozenServerVersionForAvailableNow != -1) {
      logWarning(
        s"prepareForTriggerAvailableNow called but frozenServerVersionForAvailableNow is " +
          s"already set to $frozenServerVersionForAvailableNow." +
          getTableInfoForLogging
      )
    }
    // Capture the frozen version here rather than lazily in getOrUpdateLatestTableVersion to keep
    // that method simple (single early-return guard). Lazy capture would require two conditionals
    // inside getOrUpdateLatestTableVersion (check if already frozen + freeze after RPC).
    //
    // Call getOrUpdateLatestTableVersion BEFORE setting isTriggerAvailableNow, so the guard
    // inside getOrUpdateLatestTableVersion doesn't short-circuit and we get a real RPC.
    //
    // DeltaFormatSharingSource is always freshly instantiated per streaming query start, so
    // lastTimestampForGetVersionFromServer is -1 and the throttle never suppresses this RPC.
    frozenServerVersionForAvailableNow = getOrUpdateLatestTableVersion
    // No require(frozenServerVersionForAvailableNow >= 0) needed here:
    // getOrUpdateLatestTableVersion already throws IllegalStateException for negative versions.
    isTriggerAvailableNow = true
    logInfo(s"Prepared for Trigger.AvailableNow with frozenServerVersionForAvailableNow=" +
      s"$frozenServerVersionForAvailableNow," + getTableInfoForLogging)
  }

  /**
   * Check the latest table version from the delta sharing server through the client.getTableVersion
   * RPC. Adding a minimum interval of QUERY_TABLE_VERSION_INTERVAL_MILLIS between two consecutive
   * rpcs to avoid traffic jam on the delta sharing server.
   *
   * @return the latest table version on the server.
   */
  private def getOrUpdateLatestTableVersion: Long = {
    if (isTriggerAvailableNow) {
      require(frozenServerVersionForAvailableNow >= 0,
        s"frozenServerVersionForAvailableNow must be >= 0 when isTriggerAvailableNow is true, " +
          s"but got $frozenServerVersionForAvailableNow." + getTableInfoForLogging)
      return frozenServerVersionForAvailableNow
    }
    val currentTimeMillis = System.currentTimeMillis()
    if ((currentTimeMillis - lastTimestampForGetVersionFromServer) >=
      QUERY_TABLE_VERSION_INTERVAL_MILLIS) {
      val serverVersion = client.getTableVersion(table)
      if (serverVersion < 0) {
        throw new IllegalStateException(s"Delta Sharing Server returning negative table version:" +
          s"$serverVersion," + getTableInfoForLogging)
      } else if (serverVersion < latestTableVersionOnServer) {
        logWarning(
          s"Delta Sharing Server returning smaller table version: $serverVersion < " +
          s"$latestTableVersionOnServer," + getTableInfoForLogging
        )
      }
      logInfo(s"Got table version $serverVersion from Delta Sharing Server,$getTableInfoForLogging")
      latestTableVersionOnServer = serverVersion
      lastTimestampForGetVersionFromServer = currentTimeMillis
    }
    latestTableVersionOnServer
  }

  /**
   * NOTE: need to match with the logic in DeltaSource.extractStartingState().
   *
   * Get the starting offset used to send rpc to delta sharing server, to fetch needed files.
   * Use input startOffset when it's defined, otherwise use user defined starting version, otherwise
   * use input endOffset if it's defined, the least option is the latest table version returned from
   * the delta sharing server (which is usually used when a streaming query starts from scratch).
   *
   * @param startOffsetOption optional start offset, return it if defined. It's empty when the
   *                          streaming query starts from scratch. It's set for following calls.
   * @param endOffsetOption   optional end offset. It's set when the function is called from
   *                          getBatch and is empty when called from latestOffset.
   * @return The starting offset.
   */
  private def getStartingOffset(
      startOffsetOption: Option[DeltaSourceOffset],
      endOffsetOption: Option[DeltaSourceOffset]): DeltaSourceOffset = {
    if (startOffsetOption.isEmpty) {
      val (version, isInitialSnapshot) = getStartingVersion match {
        case Some(v) => (v, false)
        case None =>
          if (endOffsetOption.isDefined) {
            if (endOffsetOption.get.isInitialSnapshot) {
              (endOffsetOption.get.reservoirVersion, true)
            } else {
              assert(
                endOffsetOption.get.reservoirVersion > 0,
                s"invalid reservoirVersion in endOffset: ${endOffsetOption.get}"
              )
              // Load from snapshot `endOffset.reservoirVersion - 1L` so that `index` in `endOffset`
              // is still valid.
              // It's OK to use the previous version as the updated initial snapshot, even if the
              // initial snapshot might have been different from the last time when this starting
              // offset was computed.
              (endOffsetOption.get.reservoirVersion - 1L, true)
            }
          } else {
            (getOrUpdateLatestTableVersion, true)
          }
      }
      // Constructed the same way as DeltaSource.buildOffsetFromIndexedFile
      DeltaSourceOffset(
        reservoirId = tableId,
        reservoirVersion = version,
        index = DeltaSourceOffset.BASE_INDEX,
        isInitialSnapshot = isInitialSnapshot
      )
    } else {
      startOffsetOption.get
    }
  }

  /**
   * Converts an offset from the checkpoint to DeltaSourceOffset, and returns whether it was
   * converted from legacy format (DeltaSharingSourceOffset or legacy JSON tableId/tableVersion).
   * @return (DeltaSourceOffset, fromLegacy)
   * Visible for testing (private[spark]).
   */
  private[spark] def forceToDeltaSourceOffset(
      offset: streaming.Offset): (DeltaSourceOffset, Boolean) = {
    if (offset == null) {
      throw new IllegalArgumentException("offset cannot be null")
    }
    offset match {
      case o: DeltaSourceOffset =>
        (o, false)
      case o: DeltaSharingSourceOffset =>
        (convertDeltaSharingSourceOffsetToDeltaSourceOffset(o), true)
      case _ =>
        // For JSON (SerializedOffset): parse as DeltaSourceOffset first,
        // if that throws or yields empty reservoirId, parse as legacy DeltaSharingSourceOffset.
        try {
          val deltaOffset = deltaSource.toDeltaSourceOffset(offset)
          val reservoirIdEmpty =
            deltaOffset.reservoirId == null || deltaOffset.reservoirId.isEmpty
          if (reservoirIdEmpty) {
            // Throw to let the catcher handle the exception.
            throw new IllegalArgumentException(s"Invalid offset format: $offset")
          } else {
            logInfo("Offset JSON parsed as Delta format")
            (deltaOffset, false)
          }
        } catch {
          // Parsing legacy Offset JSON using DeltaSourceOffset
          // yields a null reservoirId, causing an exception
          // since toDeltaSourceOffset expects it to match tableId.
          case e: Exception =>
            val autoResolve = sqlConf.getConf(
              DeltaSQLConf.DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT)
            if (!autoResolve) {
              throw e
            }
            logInfo(s"Offset JSON not valid Delta format, parsing as legacy: ${e.getMessage}")
            (toDeltaSourceOffsetFromLegacyJson(offset), true)
        }
    }
  }

  /** Returns the file ID hash option based on auto-resolve config and whether MD5 is needed. */
  private def resolveFileIdHash(useParquetHash: Boolean): Option[String] = {
    val autoResolve = sqlConf.getConf(
      DeltaSQLConf.DELTA_SHARING_STREAMING_AUTO_RESOLVE_RESPONSE_FORMAT)
    if (!autoResolve) {
      None
    } else if (useParquetHash) {
      Some(DeltaSharingRestClient.FILEIDHASH_PARQUET)
    } else {
      Some(DeltaSharingRestClient.FILEIDHASH_DELTA)
    }
  }

  /** Parse the given offset as DeltaSharingSourceOffset (tableId/tableVersion)
   * and convert to DeltaSourceOffset.
   */
  private def toDeltaSourceOffsetFromLegacyJson(offset: streaming.Offset): DeltaSourceOffset = {
    val legacy = DeltaSharingSourceOffset(tableId, offset)
    convertDeltaSharingSourceOffsetToDeltaSourceOffset(legacy)
  }

  private def convertDeltaSharingSourceOffsetToDeltaSourceOffset(
      o: DeltaSharingSourceOffset): DeltaSourceOffset = {
    // Legacy DeltaSharingSourceOffset hardcodes -1 as the
    // version boundary index, but DeltaSourceOffset uses
    // BASE_INDEX (which varies across versions) and rejects
    // -1. Convert the index to BASE_INDEX for compatibility.
    val index = if (o.index == -1L) DeltaSourceOffset.BASE_INDEX else o.index
    logInfo(s"Converted DeltaSharingSourceOffset to DeltaSourceOffset: reservoirId=${o.tableId}, " +
      s"reservoirVersion=${o.tableVersion}, index=$index, isInitialSnapshot=${o.isStartingVersion}")
    DeltaSourceOffset(
      reservoirId = o.tableId,
      reservoirVersion = o.tableVersion,
      index = index,
      isInitialSnapshot = o.isStartingVersion
    )
  }

  /**
   * Convert a DeltaSourceOffset back to legacy DeltaSharingSourceOffset. Used
   * when transitioning from legacy checkpoints: mid-version offsets are kept
   * in legacy format until the stream reaches a version boundary.
   */
  private def convertDeltaSourceOffsetToLegacyOffset(
      o: DeltaSourceOffset
  ): streaming.Offset = {
    val legacyOffset = DeltaSharingSourceOffset(
      sourceVersion = DeltaSharingSourceOffset.VERSION_1,
      tableId = tableId,
      tableVersion = o.reservoirVersion,
      index = o.index,
      isStartingVersion = o.isInitialSnapshot
    )
    logInfo(
      s"Converting DeltaSourceOffset back to legacy: ${legacyOffset.json}"
    )
    legacyOffset
  }

  /**
   * The ending version used in rpc is restricted by both the latest table version and
   * maxVersionsPerRpc, to avoid loading too many files from the server to cause a timeout.
   * @param startingOffset The start offset used in the rpc.
   * @param latestTableVersion The latest table version at the server.
   * @return the ending version used in the rpc.
   */
  private def getEndingVersionForRpc(
      startingOffset: DeltaSourceOffset,
      latestTableVersion: Long): Long = {
    if (startingOffset.isInitialSnapshot) {
      // ending version is the same as starting version for snapshot query.
      return startingOffset.reservoirVersion
    }
    // using "startVersion + maxVersionsPerRpc - 1" because the endingVersion is inclusive.
    val endingVersionForQuery = latestTableVersion.min(
      startingOffset.reservoirVersion + maxVersionsPerRpc - 1
    )
    if (endingVersionForQuery < latestTableVersion) {
      logInfo(
        s"Reducing ending version for delta sharing rpc from latestTableVersion(" +
        s"$latestTableVersion) to endingVersionForQuery($endingVersionForQuery), " +
        s"startVersion:${startingOffset.reservoirVersion}, maxVersionsPerRpc:$maxVersionsPerRpc, " +
          getTableInfoForLogging
      )
    }
    endingVersionForQuery
  }

  override def getDefaultReadLimit: ReadLimit = {
    deltaSource.getDefaultReadLimit
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    logInfo(s"latestOffset with startOffset($startOffset), limit($limit)")
    // startOffset is null for initialSnapshot.
    val (startDeltaSourceOffsetOpt, wasConvertedFromLegacy) =
      Option(startOffset).map(forceToDeltaSourceOffset) match {
        case Some((offset, fromLegacy)) => (Some(offset), fromLegacy)
        case None => (None, false)
      }

    // Delegate to legacy source if start offset is from a legacy checkpoint and still in the
    // initial snapshot. Index ordering differs: DeltaSharingSource sorts by fileId, while
    // DeltaSource sorts by modificationTime and path. Format conversion starts after the
    // initial snapshot completes.
    if (wasConvertedFromLegacy && startDeltaSourceOffsetOpt.exists(_.isInitialSnapshot)) {
      logInfo(s"Delegating latestOffset to legacy DeltaSharingSource," + getTableInfoForLogging)
      return getOrCreateLegacySource(
        startDeltaSourceOffsetOpt.get.reservoirVersion).latestOffset(startOffset, limit)
    }

    // The engine always calls getBatch for priming on restart, so
    // latestProcessedEndOffsetOption is normally valid here and points to the last processed end
    // offset in legacy format.
    // As a safeguard in case the engine changes behavior, fall back to
    // the starting offset from the legacy offset. This matters when DVs are enabled on a shared
    // table with an existing LegacySource streaming query: the query fails, and on restart we
    // must use the legacy offset to fetch files.
    val deltaSourceOffset = startDeltaSourceOffsetOpt match {
      case Some(offset) if latestProcessedEndOffsetOption.isEmpty && wasConvertedFromLegacy =>
        offset
      case _ =>
        getStartingOffset(latestProcessedEndOffsetOption, None)
    }

    if (deltaSourceOffset.reservoirVersion < 0) {
      return null
    }

    val latestTableVersion = getOrUpdateLatestTableVersion
    // Legacy conversion edge case when the table has no version beyond the initial snapshot:
    // - Batch 0: v1, index=0, isStartingVersion=true (mid-snapshot at v1).
    // - Batch 1: v2, index=-1, isStartingVersion=false (snapshot finished; end offset is
    //   startVersion+1 at the version boundary).
    // The table only has v1 (no newer versions). getBatch priming delegates to the legacy source
    // because the batch 0 is still in the initial snapshot, latestProcessedEndOffsetOption
    // is empty, and latestOffset here advances to v2 with index=-1. The server has no v2, so
    // latestOffset must return null instead of fetching files for v2 from the server.
    if (wasConvertedFromLegacy && deltaSourceOffset.reservoirVersion > latestTableVersion) {
      return null
    }
    val (endingVersion, fileIdHash) = determineVersionAndHashFromLatestOffset(
      deltaSourceOffset, wasConvertedFromLegacy, latestTableVersion)
    maybeGetLatestFileChangesFromServer(
      deltaSourceOffset, latestTableVersion, endingVersion, fileIdHash)

    val endOffset =
      maybeMoveToNextVersion(
        deltaSource.latestOffset(startDeltaSourceOffsetOpt.orNull, limit)
      )

    // When restarting from a legacy checkpoint mid-version, the
    // current version may be too large for a single batch to
    // finish. If both the start and end offsets are still
    // mid-version, convert the end offset back to legacy format
    // so the next batch continues with the legacy file-id hash.
    // If the start offset is already at a version boundary
    // (lucky case) or the end offset reaches one, return a
    // DeltaSourceOffset to complete the transition.
    val needsLegacyConversion = wasConvertedFromLegacy &&
      startDeltaSourceOffsetOpt.exists(
        _.index != DeltaSourceOffset.BASE_INDEX
      ) &&
      endOffset != null &&
      endOffset.index != DeltaSourceOffset.BASE_INDEX
    if (needsLegacyConversion) {
      convertDeltaSourceOffsetToLegacyOffset(endOffset)
    } else {
      endOffset
    }
  }

  // Advance the DeltaSourceOffset to the next version when the offset is at the last index of the
  // version.
  // This is because DeltaSource is not advancing the offset automatically when processing a
  // snapshot (isStartingVersion = true), and advancing the offset is necessary for delta sharing
  // streaming to fetch new files from the delta sharing server.
  private def maybeMoveToNextVersion(
      latestOffsetFromDeltaSource: streaming.Offset): DeltaSourceOffset = {
    val deltaLatestOffset = deltaSource.toDeltaSourceOffset(latestOffsetFromDeltaSource)
    if (deltaLatestOffset.isInitialSnapshot &&
      (numFileActionsInStartingSnapshotOpt.exists(_ == deltaLatestOffset.index + 1))) {
      DeltaSourceOffset(
        reservoirId = deltaLatestOffset.reservoirId,
        reservoirVersion = deltaLatestOffset.reservoirVersion + 1,
        index = DeltaSourceOffset.BASE_INDEX,
        isInitialSnapshot = false
      )
    } else {
      deltaLatestOffset
    }
  }

  /**
   * Whether need to fetch new files from the delta sharing server.
   * @param startingOffset  the startingOffset of the next batch asked by spark streaming engine.
   * @param latestTableVersion  the latest table version on the delta sharing server.
   * @return whether need to fetch new files from the delta sharing server, this is needed when all
   *         files are processed in the local delta log, and there are new files on the delta
   *         sharing server.
   *         And we avoid fetching new files when files in the delta log are not fully processed.
   */
  private def needNewFilesFromServer(
      startingOffset: DeltaSourceOffset,
      latestTableVersion: Long): Boolean = {
    // In AvailableNow mode, stop fetching once the local log has all data up to the frozen version.
    // Using >= defensively; in practice the local log version should only equal the frozen version
    // since fetches are capped at frozenServerVersionForAvailableNow via getEndingVersionForRpc.
    // The frozenServerVersionForAvailableNow >= 0 check is defense-in-depth: it is logically
    // guaranteed to be >= 0 here because getOrUpdateLatestTableVersion throws for negative values,
    // but we keep it explicit to avoid a subtle early-return if the invariant were ever violated.
    if (isTriggerAvailableNow &&
        frozenServerVersionForAvailableNow >= 0 &&
        latestTableVersionInLocalDeltaLogOpt.exists(_ >= frozenServerVersionForAvailableNow)) {
      return false
    }
    if (latestTableVersionInLocalDeltaLogOpt.isEmpty) {
      return true
    }

    val allLocalFilesProcessed = latestTableVersionInLocalDeltaLogOpt.exists(
      _ < startingOffset.reservoirVersion
    )
    val newChangesOnServer = latestTableVersionInLocalDeltaLogOpt.exists(_ < latestTableVersion)
    allLocalFilesProcessed && newChangesOnServer
  }

  /**
   * Determine the ending version and file ID hash for a getBatch call, handling both
   * legacy-to-new format transitions and regular delta format processing.
   *
   * Examples:
   *  - start=None, end=legacy(v5): initial snapshot priming -> endingVersion=5, hash=MD5
   *  - start=legacy(v3), end=legacy(v5): post-snapshot priming -> endingVersion=5, hash=MD5
   *  - start=legacy(v3,mid), end=new(v4,-1): mid-version conversion -> endingVersion=3, hash=MD5
   *  - start=legacy(v3,-1), end=new(v5): boundary conversion -> endingVersion=5, hash=SHA256
   *  - start=new(v5), end=new(v7): regular delta processing -> endingVersion=latest, hash=SHA256
   *
   * @param startingOffset the starting offset for the batch.
   * @param startConvertedFromLegacy true when startingOffset was converted from legacy format.
   * @param endOffset the end offset from getBatch (must be defined).
   * @param endConvertedFromLegacy true when endOffset was converted from legacy format.
   * @param latestTableVersion the latest table version from the server.
   * @return (endingVersionForQuery, fileIdHash)
   */
  private[spark] def determineVersionAndHashFromGetBatch(
      startingOffset: DeltaSourceOffset,
      startConvertedFromLegacy: Boolean,
      endOffset: DeltaSourceOffset,
      endConvertedFromLegacy: Boolean,
      latestTableVersion: Long): (Long, Option[String]) = {
    val (endingVersionForQuery, useParquetHash) = if (endConvertedFromLegacy) {
      // getBatch priming during legacy to new format transition:
      // 1. Both start and end offsets are from legacy checkpoints.
      // 2. Start offset is None and end offset is from a legacy checkpoint.
      // We need to build DataFrame from start to end version using MD5 for all past
      // processed versions. If index == BASE_INDEX (-1), it means all files before
      // endOffset.reservoirVersion have been processed, so we use reservoirVersion - 1
      // as the ending version. In the next latestOffset call we can safely start using
      // the new format with SHA256 from endVersion to endVersion + 100.
      val endVersion = if (endOffset.index == DeltaSourceOffset.BASE_INDEX) {
        endOffset.reservoirVersion - 1
      } else {
        endOffset.reservoirVersion
      }
      (endVersion, true)
    } else if (startConvertedFromLegacy &&
        startingOffset.index != DeltaSourceOffset.BASE_INDEX) {
      // Start is legacy format mid-version, end is next version (index=-1). Restrict
      // to this version so we finish processing it with MD5 hashing. latestOffset
      // should have already fetched the current version; this is a safeguard.
      (startingOffset.reservoirVersion, true)
    } else {
      // Regular delta format batch, or start offset is already at a version boundary.
      (getEndingVersionForRpc(startingOffset, latestTableVersion), false)
    }

    val fileIdHash = resolveFileIdHash(useParquetHash)

    (endingVersionForQuery, fileIdHash)
  }

  /**
   * Determine the ending version and file ID hash for a latestOffset call.
   *
   * Examples:
   *  - start=legacy(v3,mid): mid-version conversion -> endingVersion=3, hash=MD5
   *  - start=legacy(v3,-1): boundary conversion -> endingVersion=latest, hash=SHA256
   *  - start=new(v3): regular batch -> endingVersion=latest, hash=SHA256
   *
   * @param startingOffset the starting offset.
   * @param startConvertedFromLegacy true when startingOffset was converted from legacy format.
   * @param latestTableVersion the latest table version from the server.
   * @return (endingVersionForQuery, fileIdHash)
   */
  private[spark] def determineVersionAndHashFromLatestOffset(
      startingOffset: DeltaSourceOffset,
      startConvertedFromLegacy: Boolean,
      latestTableVersion: Long): (Long, Option[String]) = {
    val (endingVersionForQuery, useParquetHash) =
      if (startConvertedFromLegacy &&
          startingOffset.index != DeltaSourceOffset.BASE_INDEX) {
        // Transitioning from parquet streaming source to delta streaming source.
        // The start offset is in legacy format and indicates unprocessed files in
        // the current version. Restrict to this version so we finish processing it
        // with MD5 hashing. The next version will switch to SHA256 and delta offsets.
        (startingOffset.reservoirVersion, true)
      } else {
        // Regular delta format batch, or start offset is already at a version boundary.
        (getEndingVersionForRpc(startingOffset, latestTableVersion), false)
      }

    val fileIdHash = resolveFileIdHash(useParquetHash)

    (endingVersionForQuery, fileIdHash)
  }

  /**
   * Check whether we need to fetch new files from the server and calls getTableFileChanges if true.
   *
   * @param startingOffset the starting offset used to fetch files.
   * @param latestTableVersion the latest table version from the server.
   * @param endingVersionForQuery the ending version for the RPC query, determined by the caller
   *                              via [[determineVersionAndHashFromGetBatch]] or
   *                              [[determineVersionAndHashFromLatestOffset]].
   * @param fileIdHash the file ID hash algorithm to use, determined by the caller.
   */
  private def maybeGetLatestFileChangesFromServer(
      startingOffset: DeltaSourceOffset,
      latestTableVersion: Long,
      endingVersionForQuery: Long,
      fileIdHash: Option[String]): Unit = {
    if (needNewFilesFromServer(startingOffset, latestTableVersion)) {
      logInfo(s"Legacy transition state: " +
        s"fileIdHash=$fileIdHash, " +
        s"endingVersionForQuery=$endingVersionForQuery")

      if (startingOffset.isInitialSnapshot || !options.readChangeFeed) {
        getTableFileChanges(startingOffset, endingVersionForQuery, fileIdHash)
      } else {
        // No flag check here: DELTA_SHARING_ENABLE_DELTA_FORMAT_CDF_STREAMING is already
        // enforced in DeltaSharingDataSource.createSource before this source is instantiated.
        getTableCDFFileChanges(startingOffset, endingVersionForQuery, fileIdHash)
      }
    }
  }

  /**
   * Fetch the table changes from delta sharing server starting from (version, index) of the
   * startingOffset, and store them in locally constructed delta log.
   *
   * @param startingOffset  Includes a reservoirVersion, an index of a file within the same version,
   *                        and an isInitialSnapshot.
   *                        If isInitialSnapshot is true, will load startingOffset.reservoirVersion
   *                        as a table snapshot (including files from previous versions). If false,
   *                        it will only load files since startingOffset.reservoirVersion.
   * @param endingVersionForQuery The ending version used for the query, always smaller than
   *                              the latest table version on server.
   * @param fileIdHash            Optional hash algorithm to use for file ID hashing.
   */
  private def getTableFileChanges(
      startingOffset: DeltaSourceOffset,
      endingVersionForQuery: Long,
      fileIdHash: Option[String] = None): Unit = {
    logInfo(
      s"Fetching files with table version(${startingOffset.reservoirVersion}), " +
      s"index(${startingOffset.index}), isInitialSnapshot(${startingOffset.isInitialSnapshot})," +
      s" endingVersionForQuery($endingVersionForQuery), " +
      s"server version($latestTableVersionOnServer)," + getTableInfoForLogging
    )

    val (tableFiles, refreshFunc) = if (startingOffset.isInitialSnapshot) {
      // If isInitialSnapshot is true, it means to fetch the snapshot at the fromVersion, which may
      // include table changes from previous versions.
      val tableFiles = client.getFiles(
        table = table,
        predicates = Nil,
        limit = None,
        versionAsOf = Some(startingOffset.reservoirVersion),
        timestampAsOf = None,
        jsonPredicateHints = None,
        refreshToken = None,
        fileIdHash = fileIdHash
      )
      val refreshFunc = DeltaSharingUtils.getRefresherForGetFiles(
        client = client,
        table = table,
        predicates = Nil,
        limit = None,
        versionAsOf = Some(startingOffset.reservoirVersion),
        timestampAsOf = None,
        jsonPredicateHints = None,
        useRefreshToken = false,
        fileIdHash = fileIdHash
      )
      logInfo(
        s"Fetched ${tableFiles.lines.size} lines for table version ${tableFiles.version} from" +
        " delta sharing server." + getTableInfoForLogging + getQueryIdForLogging
      )
      (tableFiles, refreshFunc)
    } else {
      // If isStartingVersion is false, it means to fetch files for data changes since fromVersion,
      // not including files from previous versions.
      val tableFiles = client.getFiles(
        table = table,
        startingVersion = startingOffset.reservoirVersion,
        endingVersion = Some(endingVersionForQuery),
        fileIdHash = fileIdHash
      )
      val refreshFunc = DeltaSharingUtils.getRefresherForGetFilesWithStartingVersion(
        client = client,
        table = table,
        startingVersion = startingOffset.reservoirVersion,
        endingVersion = Some(endingVersionForQuery),
        fileIdHash = fileIdHash
      )
      logInfo(
        s"Fetched ${tableFiles.lines.size} lines from startingVersion " +
        s"${startingOffset.reservoirVersion} to enedingVersion ${endingVersionForQuery} from " +
        "delta sharing server," + getTableInfoForLogging + getQueryIdForLogging
      )
      (tableFiles, refreshFunc)
    }

    applyTableFileChanges(tableFiles, refreshFunc, startingOffset, endingVersionForQuery)
  }

  /**
   * Fetch the CDF file changes from delta sharing server for the given version range,
   * and store them in the locally constructed delta log.
   *
   * This is the CDF streaming counterpart to getTableFileChanges. It calls getCDFFiles
   * instead of getFiles to retrieve AddCDCFile, AddFile, and RemoveFile actions that
   * represent row-level changes. includeHistoricalMetadata=true ensures metadata actions
   * are included so that mid-range schema changes break the stream correctly.
   *
   * @param startingOffset The starting offset with the reservoirVersion to fetch from.
   * @param endingVersionForQuery The ending version (inclusive) for the query.
   */
  private def getTableCDFFileChanges(
      startingOffset: DeltaSourceOffset,
      endingVersionForQuery: Long,
      fileIdHash: Option[String] = None): Unit = {
    logInfo(
      s"Fetching CDF files with table version(${startingOffset.reservoirVersion}), " +
      s"index(${startingOffset.index}), " +
      s"endingVersionForQuery($endingVersionForQuery), " +
      s"server version($latestTableVersionOnServer)," + getTableInfoForLogging
    )

    val cdfOptions = Map(
      DeltaSharingOptions.CDF_START_VERSION -> startingOffset.reservoirVersion.toString,
      DeltaSharingOptions.CDF_END_VERSION -> endingVersionForQuery.toString
    )
    val tableFiles = client.getCDFFiles(
      table = table,
      cdfOptions = cdfOptions,
      // Requests Metadata actions for schema-changing versions in the range so DeltaSource
      // can detect both backward-compatible and non-backward-compatible schema changes.
      includeHistoricalMetadata = true,
      fileIdHash = fileIdHash
    )
    val refreshFunc = DeltaSharingUtils.getRefresherForGetCDFFiles(
      client = client,
      table = table,
      cdfOptions = cdfOptions,
      fileIdHash = fileIdHash
    )
    logInfo(
      s"Fetched ${tableFiles.lines.size} CDF lines from startingVersion " +
      s"${startingOffset.reservoirVersion} to endingVersion $endingVersionForQuery from " +
      "delta sharing server," + getTableInfoForLogging + getQueryIdForLogging
    )

    applyTableFileChanges(tableFiles, refreshFunc, startingOffset, endingVersionForQuery)
  }

  /**
   * Shared post-RPC logic for both getTableFileChanges and getTableCDFFileChanges.
   * Constructs the local delta log from the server response, registers file URLs in
   * CachedTableManager, and updates version tracking state.
   */
  private def applyTableFileChanges(
      tableFiles: DeltaTableFiles,
      refreshFunc: DeltaSharingUtils.RefresherFunction,
      startingOffset: DeltaSourceOffset,
      endingVersionForQuery: Long): Unit = {
    val deltaLogMetadata = DeltaSharingLogFileSystem.constructLocalDeltaLogAcrossVersions(
      lines = tableFiles.lines,
      customTablePath = customTablePathWithUUIDSuffix,
      startingVersionOpt = Some(startingOffset.reservoirVersion),
      endingVersionOpt = Some(endingVersionForQuery)
    )
    assert(
      deltaLogMetadata.maxVersion > 0,
      s"Invalid table version in delta sharing response: ${tableFiles.lines}."
    )
    latestTableVersionInLocalDeltaLogOpt = Some(deltaLogMetadata.maxVersion)
    logInfo(s"Setting latestTableVersionInLocalDeltaLogOpt to ${deltaLogMetadata.maxVersion}" +
      getTableInfoForLogging)
    assert(
      deltaLogMetadata.numFileActionsInMinVersionOpt.isDefined,
      "numFileActionsInMinVersionOpt missing after constructed delta log."
    )
    if (startingOffset.isInitialSnapshot) {
      numFileActionsInStartingSnapshotOpt = deltaLogMetadata.numFileActionsInMinVersionOpt
    }

    CachedTableManager.INSTANCE.register(
      tablePath = DeltaSharingUtils.getTablePathWithIdSuffix(tablePath, timestampWithUUID),
      idToUrl = deltaLogMetadata.idToUrl,
      refs = Seq(new WeakReference(this)),
      profileProvider = client.getProfileProvider,
      refresher = refreshFunc,
      expirationTimestamp =
        if (CachedTableManager.INSTANCE
            .isValidUrlExpirationTime(deltaLogMetadata.minUrlExpirationTimestamp)) {
          deltaLogMetadata.minUrlExpirationTimestamp.get
        } else {
          System.currentTimeMillis() + CachedTableManager.INSTANCE.preSignedUrlExpirationMs
        },
      refreshToken = tableFiles.refreshToken
    )
  }

  // Streaming query execution flow varies by trigger type:
  //
  // Trigger.Once (batch-style triggers):
  //   Each restart replays the last committed offset before advancing:
  //   - Start 1: latestOffset(None) -> getBatch(None, offset1)
  //   - Restart 2: getBatch(None, offset1)
  //       -> latestOffset(offset1) -> getBatch(offset1, offset2)
  //   - Restart 3: getBatch(offset1, offset2)
  //       -> latestOffset(offset2) -> getBatch(offset2, offset3)
  //
  // Trigger.ProcessingTime / Trigger.Continuous / Trigger.AvailableNow (continuous triggers):
  //   The query runs until interrupted; each micro-batch advances the offset:
  //   - Start 1: latestOffset(None)        -> getBatch(None,    offset1)
  //   - Batch 2: latestOffset(offset1)     -> getBatch(offset1, offset2)determineVersionAndHashFrom
  //   - Batch 3: latestOffset(offset2)     -> getBatch(offset2, offset3)
  //   - On restart: resumes from last committed offset (getBatch priming applies here too)
  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    logInfo(s"getBatch with startOffsetOption($startOffsetOption) and end($end)," +
      getTableInfoForLogging)
    // When DVs are enabled on a shared table with an existing
    // LegacySource streaming query still reading an initial
    // snapshot, startOffsetOption is None and endOffset
    // specifies the starting version. On restart, this Source
    // is instantiated, so we convert legacy offsets to
    // DeltaSourceOffset.
    val (endOffset, endConvertedFromLegacy) = forceToDeltaSourceOffset(end)

    // When the query is past the initial snapshot,
    // startOffsetOption is defined and contains the starting
    // version. Convert from legacy offset if needed.
    val (startDeltaOffsetOption, startConvertedFromLegacy) =
      startOffsetOption.map(forceToDeltaSourceOffset) match {
        case Some((offset, fromLegacy)) => (Some(offset), fromLegacy)
        case None => (None, false)
      }

    // Delegate to legacy source if either offset is from a legacy checkpoint and either is
    // still in the initial snapshot. Index ordering differs: DeltaSharingSource sorts by
    // fileId, while DeltaSource sorts by modificationTime and path.
    // Format conversion starts after the initial snapshot completes.
    //
    // Possible streaming restart priming cases for legacy conversion:
    // 1. start=None, end=legacy(initial)
    // 2. start=legacy(initial), end=legacy(initial)
    // 3. start=legacy(initial), end=legacy(post-snapshot)
    val needsDelegation =
      (startConvertedFromLegacy && startDeltaOffsetOption.exists(_.isInitialSnapshot)) ||
      (endConvertedFromLegacy && endOffset.isInitialSnapshot)
    if (needsDelegation) {
      val snapshotVersion = startDeltaOffsetOption.map(_.reservoirVersion)
        .getOrElse(endOffset.reservoirVersion)
      logInfo(s"Delegating getBatch to legacy DeltaSharingSource," + getTableInfoForLogging)
      // Need to use table metadata at the snapshot version,
      // and pass in legacy offset to prime the legacy source.
      return getOrCreateLegacySource(snapshotVersion).getBatch(startOffsetOption, end)
    }

    val startingOffset = getStartingOffset(startDeltaOffsetOption, Some(endOffset))

    val latestTableVersion = getOrUpdateLatestTableVersion
    val (endingVersion, fileIdHash) = determineVersionAndHashFromGetBatch(
      startingOffset, startConvertedFromLegacy,
      endOffset, endConvertedFromLegacy, latestTableVersion)
    maybeGetLatestFileChangesFromServer(
      startingOffset, latestTableVersion, endingVersion, fileIdHash)

    // Reset latestProcessedEndOffsetOption only when endOffset is larger.
    // Because with microbatch pipelining, we may get getBatch requests out of order.
    if (latestProcessedEndOffsetOption.isEmpty ||
      endOffset.reservoirVersion > latestProcessedEndOffsetOption.get.reservoirVersion ||
      (endOffset.reservoirVersion == latestProcessedEndOffsetOption.get.reservoirVersion &&
      endOffset.index > latestProcessedEndOffsetOption.get.index)) {
      latestProcessedEndOffsetOption = Some(endOffset)
      logInfo(s"Setting latestProcessedEndOffsetOption to $endOffset," + getTableInfoForLogging)
    }

    deltaSource.getBatch(startDeltaOffsetOption, endOffset)
  }

  override def getOffset: Option[Offset] = {
    throw new UnsupportedOperationException(
      "latestOffset(Offset, ReadLimit) should be called instead of this method."
    )
  }

  /**
   * Extracts whether users provided the option to time travel a relation. If a query restarts from
   * a checkpoint and the checkpoint has recorded the offset, this method should never been called.
   */
  private lazy val getStartingVersion: Option[Long] = {

    /** DeltaOption validates input and ensures that only one is provided. */
    if (options.startingVersion.isDefined) {
      val v = options.startingVersion.get match {
        case StartingVersionLatest =>
          getOrUpdateLatestTableVersion + 1
        case StartingVersion(version) =>
          version
      }
      Some(v)
    } else if (options.startingTimestamp.isDefined) {
      Some(client.getTableVersion(table, options.startingTimestamp))
    } else {
      None
    }
  }

  override def stop(): Unit = {
    legacyDeltaSharingSourceOpt.foreach(_._2.stop())
    deltaSource.stop()

    DeltaSharingLogFileSystem.tryToCleanUpDeltaLog(deltaLogPath)
  }

  // Calls deltaSource.commit for checks related to column mapping.
  override def commit(end: Offset): Unit = {
    logInfo(s"Commit end offset: $end," + getTableInfoForLogging)
    val (endOffset, endConvertedFromLegacy) = forceToDeltaSourceOffset(end)

    if (endConvertedFromLegacy && endOffset.isInitialSnapshot) {
      // During legacy snapshot delegation, the batch was produced by DeltaSharingSource,
      // so delegate commit to the legacy source and return. DeltaSharingSource doesn't
      // implement commit(), so this is a no-op for now.
      legacyDeltaSharingSourceOpt.foreach(_._2.commit(end))
      return
    }

    // If DeltaSource detects a metadata change at endOffset
    // version, deltaSource.commit throws an exception so the
    // stream restarts from the checkpoint with the new schema.
    deltaSource.commit(endOffset)

    // Clean up processed versions in block manager regardless
    // of whether endOffset is from legacy format.
    DeltaSharingLogFileSystem.tryToCleanUpPreviousBlocks(
      deltaLogPath,
      endOffset.reservoirVersion - 1
    )
  }

  override def toString(): String = s"DeltaFormatSharingSource[${table.toString}]"
}
