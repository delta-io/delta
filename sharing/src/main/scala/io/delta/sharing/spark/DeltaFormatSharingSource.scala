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
import io.delta.sharing.client.DeltaSharingClient
import io.delta.sharing.client.util.ConfUtils
import io.delta.sharing.client.model.{Table => DeltaSharingTable}

import org.apache.spark.delta.sharing.CachedTableManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SupportsAdmissionControl}
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
 * TODO: Support CDC Streaming, SupportsTriggerAvailableNow and SupportsConcurrentExecution.
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
    with DeltaLogging {

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
    val schemaTrackingLogOpt =
      DeltaDataSource.getMetadataTrackingLogForDeltaSource(
        spark,
        snapshotDescriptor,
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

    DeltaSource(
      spark = spark,
      deltaLog = localDeltaLog,
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
    logInfo(s"Configured queryTableVersionIntervalSeconds:${intervalSeconds}.")
    if (intervalSeconds < ConfUtils.MINIMUM_TABLE_VERSION_INTERVAL_SECONDS) {
      throw new IllegalArgumentException(s"QUERY_TABLE_VERSION_INTERVAL_MILLIS($intervalSeconds) " +
        s"must not be less than ${ConfUtils.MINIMUM_TABLE_VERSION_INTERVAL_SECONDS} seconds.")
    }
    TimeUnit.SECONDS.toMillis(intervalSeconds)
  }

  // Maximum number of versions of getFiles() rpc when fetching files from the server. Used to
  // reduce the number of files returned to avoid timeout of the rpc on the server.
  private val maxVersionsPerRpc: Int = options.maxVersionsPerRpc.getOrElse(
    DeltaSharingOptions.MAX_VERSIONS_PER_RPC_DEFAULT
  )

  // A variable to store the latest table version on server, returned from the getTableVersion rpc.
  // Used to store the latest table version for getOrUpdateLatestTableVersion when not getting
  // updates from the server.
  // For all other callers, please use getOrUpdateLatestTableVersion instead of this variable.
  private var latestTableVersionOnServer: Long = -1

  /**
   * Check the latest table version from the delta sharing server through the client.getTableVersion
   * RPC. Adding a minimum interval of QUERY_TABLE_VERSION_INTERVAL_MILLIS between two consecutive
   * rpcs to avoid traffic jam on the delta sharing server.
   *
   * @return the latest table version on the server.
   */
  private def getOrUpdateLatestTableVersion: Long = {
    val currentTimeMillis = System.currentTimeMillis()
    if ((currentTimeMillis - lastTimestampForGetVersionFromServer) >=
      QUERY_TABLE_VERSION_INTERVAL_MILLIS) {
      val serverVersion = client.getTableVersion(table)
      if (serverVersion < 0) {
        throw new IllegalStateException(
          s"Delta Sharing Server returning negative table version: " +
          s"$serverVersion."
        )
      } else if (serverVersion < latestTableVersionOnServer) {
        logWarning(
          s"Delta Sharing Server returning smaller table version: $serverVersion < " +
          s"$latestTableVersionOnServer."
        )
      }
      logInfo(s"Delta Sharing Server returning $serverVersion for getTableVersion.")
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
        s"for table(id:$tableId, name:${table.toString})."
      )
    }
    endingVersionForQuery
  }

  override def getDefaultReadLimit: ReadLimit = {
    deltaSource.getDefaultReadLimit
  }

  override def latestOffset(startOffset: streaming.Offset, limit: ReadLimit): streaming.Offset = {
    val deltaSourceOffset = getStartingOffset(latestProcessedEndOffsetOption, None)

    if (deltaSourceOffset.reservoirVersion < 0) {
      return null
    }

    maybeGetLatestFileChangesFromServer(deltaSourceOffset)

    maybeMoveToNextVersion(deltaSource.latestOffset(startOffset, limit))
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
   * Check whether we need to fetch new files from the server and calls getTableFileChanges if true.
   *
   * @param startingOffset the starting offset used to fetch files, the 3 parameters will be useful:
   *                       - reservoirVersion: initially would be the startingVersion or the latest
   *                         table version.
   *                       - index: index of a file within the same version.
   *                       - isInitialSnapshot: If true, will load fromVersion as a table snapshot(
   *                         including files from previous versions). If false, will only load files
   *                         since fromVersion.
   *                       2 usages: 1) used to compare with latestTableVersionInLocalDeltaLogOpt to
   *                       check whether new files are needed. 2) used for getTableFileChanges,
   *                       check more details in the function header.
   */
  private def maybeGetLatestFileChangesFromServer(startingOffset: DeltaSourceOffset): Unit = {
    // Use a local variable to avoid a difference in the two usages below.
    val latestTableVersion = getOrUpdateLatestTableVersion

    if (needNewFilesFromServer(startingOffset, latestTableVersion)) {
      val endingVersionForQuery =
        getEndingVersionForRpc(startingOffset, latestTableVersion)

      if (startingOffset.isInitialSnapshot || !options.readChangeFeed) {
        getTableFileChanges(startingOffset, endingVersionForQuery)
      } else {
        throw new UnsupportedOperationException("CDF Streaming is not supported yet.")
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
   */
  private def getTableFileChanges(
      startingOffset: DeltaSourceOffset,
      endingVersionForQuery: Long): Unit = {
    logInfo(
      s"Fetching files with table version(${startingOffset.reservoirVersion}), " +
      s"index(${startingOffset.index}), isInitialSnapshot(${startingOffset.isInitialSnapshot})," +
      s" endingVersionForQuery($endingVersionForQuery), for table(id:$tableId, " +
      s"name:${table.toString}) with latest version on server($latestTableVersionOnServer)."
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
        refreshToken = None
      )
      val refreshFunc = DeltaSharingUtils.getRefresherForGetFiles(
        client = client,
        table = table,
        predicates = Nil,
        limit = None,
        versionAsOf = Some(startingOffset.reservoirVersion),
        timestampAsOf = None,
        jsonPredicateHints = None
      )
      logInfo(
        s"Fetched ${tableFiles.lines.size} lines for table version ${tableFiles.version} from" +
        " delta sharing server."
      )
      (tableFiles, refreshFunc)
    } else {
      // If isStartingVersion is false, it means to fetch files for data changes since fromVersion,
      // not including files from previous versions.
      val tableFiles = client.getFiles(
        table = table,
        startingVersion = startingOffset.reservoirVersion,
        endingVersion = Some(endingVersionForQuery)
      )
      val refreshFunc = DeltaSharingUtils.getRefresherForGetFilesWithStartingVersion(
        client = client,
        table = table,
        startingVersion = startingOffset.reservoirVersion,
        endingVersion = Some(endingVersionForQuery)
      )
      logInfo(
        s"Fetched ${tableFiles.lines.size} lines from startingVersion " +
        s"${startingOffset.reservoirVersion} to enedingVersion ${endingVersionForQuery} from " +
        "delta sharing server."
      )
      (tableFiles, refreshFunc)
    }

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
    logInfo(s"Setting latestTableVersionInLocalDeltaLogOpt to ${deltaLogMetadata.maxVersion}")
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

  override def getBatch(startOffsetOption: Option[Offset], end: Offset): DataFrame = {
    val endOffset = deltaSource.toDeltaSourceOffset(end)
    val startDeltaOffsetOption = startOffsetOption.map(deltaSource.toDeltaSourceOffset)
    val startingOffset = getStartingOffset(startDeltaOffsetOption, Some(endOffset))

    maybeGetLatestFileChangesFromServer(startingOffset = startingOffset)
    // Reset latestProcessedEndOffsetOption only when endOffset is larger.
    // Because with microbatch pipelining, we may get getBatch requests out of order.
    if (latestProcessedEndOffsetOption.isEmpty ||
      endOffset.reservoirVersion > latestProcessedEndOffsetOption.get.reservoirVersion ||
      (endOffset.reservoirVersion == latestProcessedEndOffsetOption.get.reservoirVersion &&
      endOffset.index > latestProcessedEndOffsetOption.get.index)) {
      latestProcessedEndOffsetOption = Some(endOffset)
      logInfo(s"Setting latestProcessedEndOffsetOption to $endOffset")
    }

    deltaSource.getBatch(startOffsetOption, end)
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
    deltaSource.stop()

    DeltaSharingLogFileSystem.tryToCleanUpDeltaLog(deltaLogPath)
  }

  // Calls deltaSource.commit for checks related to column mapping.
  override def commit(end: Offset): Unit = {
    logInfo(s"Commit end offset: $end.")
    deltaSource.commit(end)

    // Clean up previous blocks after commit.
    val endOffset = deltaSource.toDeltaSourceOffset(end)
    DeltaSharingLogFileSystem.tryToCleanUpPreviousBlocks(
      deltaLogPath,
      endOffset.reservoirVersion - 1
    )
  }

  override def toString(): String = s"DeltaFormatSharingSource[${table.toString}]"
}
