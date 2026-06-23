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

package org.apache.spark.sql.delta.icebergShaded

import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CommittedTransaction, CurrentTransactionInfo, DeltaErrors, DeltaFileNotFoundException, DeltaFileProviderUtils, DeltaLog, DeltaOperations, DummySnapshot, IcebergCompat, IcebergConstants, Snapshot, SnapshotDescriptor, UniversalFormat, UniversalFormatConverter}
import org.apache.spark.sql.delta.DeltaOperations.OPTIMIZE_OPERATION_NAME
import org.apache.spark.sql.delta.RowId.RowTrackingMetadataDomain
import org.apache.spark.sql.delta.RowTracking
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, DomainMetadata, FileAction, InMemoryLogReplay, Metadata, Protocol, RemoveFile}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.hooks.IcebergConverterHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.TransactionHelper
import io.delta.storage.commit.{CommitFailedException => DeltaCommitFailedException}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{Table => IcebergTable, TableProperties}
import shadedForDelta.org.apache.iceberg.BaseTable
import shadedForDelta.org.apache.iceberg.exceptions.CommitFailedException
import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables
import shadedForDelta.org.apache.iceberg.hive.{HiveCatalog, HiveTableOperations}
import shadedForDelta.org.apache.iceberg.util.LocationUtil

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, V1Table}

object IcebergConverter {


  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the delta commit version # that it corresponds to.
   */
  val DELTA_VERSION_PROPERTY = "delta-version"

  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the timestamp (milliseconds) of the delta commit that it corresponds to.
   */
  val DELTA_TIMESTAMP_PROPERTY = "delta-timestamp"

  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the base delta commit version # that the conversion started from
   */
  val BASE_DELTA_VERSION_PROPERTY = "base-delta-version"
  /**
   * Property to be set in translated Iceberg metadata files.
   * Indicates the delta highWaterMark # that it corresponds to.
   */
  val DELTA_HIGH_WATER_MARK_PROPERTY = "delta-high-water-mark"

  def getLastConvertedDeltaVersion(table: Option[IcebergTable]): Option[Long] =
    table.flatMap(_.properties().asScala.get(DELTA_VERSION_PROPERTY)).map(_.toLong)

  def getLastConvertedDeltaTimestamp(table: Option[IcebergTable]): Option[Long] =
    table.flatMap(_.properties().asScala.get(DELTA_TIMESTAMP_PROPERTY)).map(_.toLong)
}


/**
 * This class manages the transformation of delta snapshots into their Iceberg equivalent.
 */
class IcebergConverter
  extends UniversalFormatConverter
  with DeltaLogging {

  // Save an atomic reference of the snapshot being converted, and the txn that triggered
  // resulted in the specified snapshot
  protected val currentConversion =
    new AtomicReference[(Snapshot, CommittedTransaction)]()
  protected val standbyConversion =
    new AtomicReference[(Snapshot, CommittedTransaction)]()

  // Whether our async converter thread is active. We may already have an alive thread that is
  // about to shutdown, but in such cases this value should return false.
  @GuardedBy("asyncThreadLock")
  private var asyncConverterThreadActive: Boolean = false
  private val asyncThreadLock = new Object

  private[icebergShaded]var targetSnapshot: SnapshotDescriptor = _
  /**
   * Enqueue the specified snapshot to be converted to Iceberg. This will start an async
   * job to run the conversion, unless there already is an async conversion running for
   * this table. In that case, it will queue up the provided snapshot to be run after
   * the existing job completes.
   * Note that if there is another snapshot already queued, the previous snapshot will get
   * removed from the wait queue. Only one snapshot is queued at any point of time.
   *
   */
  override def enqueueSnapshotForConversion(
      snapshotToConvert: Snapshot,
      txn: CommittedTransaction): Unit = {
    throw new IllegalStateException("enqueueSnapshotForConversion is no longer supported")
  }

  /**
   * Convert the specified snapshot into Iceberg for the given catalogTable
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param catalogTable the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, catalogTable: CatalogTable): Option[(Long, Long)] = {
    throw new IllegalStateException("convertSnapshot is no longer supported")
  }

  /**
   * Convert the specified snapshot into Iceberg when performing an OptimisticTransaction
   * on a delta table.
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param txn               the transaction that triggers the conversion. It must
   *                          contain the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, txn: CommittedTransaction): Option[(Long, Long)] = {
    throw new IllegalStateException("convertSnapshot is no longer supported")
  }


  // Used for tracking last converted Iceberg metadata information
  // It would be used for incremental conversion for all Iceberg conversion modes
  protected case class LastConvertedIcebergInfo(
    icebergTable: Option[IcebergTable],
    icebergSnapshotId: Option[Long],
    deltaVersionConverted: Option[Long],
    baseMetadataLocationOpt: Option[String]
  )

  /**
   * Used for tracking Iceberg Conversion Context by Conversion Mode
   * UNIFORM_POST_COMMIT_MODE => no context required
   * @param conversionMode
   * @param additionalDeltaActionsToCommit
   */
  protected class ConversionContext(
    val conversionMode: IcebergConversionMode,
    val additionalDeltaActionsToCommit: Option[Seq[Action]],
    val opType: String
  ) {
    validate()
    // Validation on parameters
    def validate(): Unit = {
      conversionMode match {
        case UNIFORM_CC_MODE =>
          assert(additionalDeltaActionsToCommit.nonEmpty)
        case _ =>
          assert(additionalDeltaActionsToCommit.isEmpty)
      }
    }

    def hasAdditionalDeltaActionsToCommit: Boolean = {
      additionalDeltaActionsToCommit.nonEmpty
    }

    def getAdditionalDeltaActionsToCommit: Seq[Action] = {
      additionalDeltaActionsToCommit.get
    }
  }

  /**
   * Convert the specified txnInfo into Iceberg
   * This is for the case where Iceberg conversion is needed when
   *  a txn doesn't commit yet (for example, for atomic Delta UniForm)
   * NOTE: 1. This operation is blocking
   * @param txnInfo the txnInfo with the txn that needs to be converted to Iceberg
   * @param deltaLog the associated deltaLog
   * @param deltaAttemptVersion the attempt delta version txn targets at
   * @param catalogTable the catalogTable this conversion targets
   * @return (Iceberg metadata path, last converted Delta version)
   */
  override def convertUncommitedTxn(
      txnInfo: CurrentTransactionInfo,
      deltaAttemptVersion: Long,
      deltaLog: DeltaLog,
      catalogTable: CatalogTable): (String, Option[Long]) =
    recordFrameProfile("Delta", "IcebergConverter.convertUncommitedTxn") {
      val refreshedTable = refreshCatalogTableIfNeeded(txnInfo, deltaAttemptVersion, catalogTable)
      val snapshotToConvert = getSnapshotForUncommitedTxn(
        deltaLog, txnInfo, deltaAttemptVersion, catalogTable
      )
      val enablingUniForm =
        UniversalFormat.icebergEnabled(txnInfo.metadata) &&
          !UniversalFormat.icebergEnabled(txnInfo.readSnapshot.metadata)
      val lastConvertedInfo =
        fetchLastConvertedIcebergInfo(refreshedTable, deltaLog, enablingUniForm)
      val icebergTxn = convertSnapshotInternal(
        snapshotToConvert,
        readSnapshotOpt = Some(txnInfo.readSnapshot),
        lastConvertedInfo = lastConvertedInfo,
        conversionContext = new ConversionContext(
          conversionMode = UNIFORM_CC_MODE,
          additionalDeltaActionsToCommit = Some(txnInfo.finalActionsToCommit),
          opType = "delta.iceberg.conversion.convertUncommitedTxn"
        ),
        refreshedTable
      )

      val (newMetadataPath, _) = icebergTxn.getConvertedIcebergMetadata
      (newMetadataPath, lastConvertedInfo.deltaVersionConverted)
    }

  protected def refreshCatalogTableIfNeeded(
      txnInfo: CurrentTransactionInfo,
      deltaAttemptVersion: Long,
      catalogTable: CatalogTable): CatalogTable = {
    val lastDeltaVersionConvertedOpt =
      catalogTable.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP)
        .map(_.toLong)
    val needsRefreshCatalogTable =
      deltaAttemptVersion > txnInfo.readSnapshot.version + 1 &&
        UniversalFormat.icebergEnabled(txnInfo.metadata) &&
        lastDeltaVersionConvertedOpt.nonEmpty
    if (!needsRefreshCatalogTable) {
      return catalogTable
    }
    val refreshedTable = reloadCatalogTable(catalogTable)
    if (refreshedTable.storage.locationUri != catalogTable.storage.locationUri) {
      throw new DeltaCommitFailedException(
        false, // retryable
        true, // conflict
        s"Table location changed when refreshing catalogTable. " +
          s"catalogTable location: ${catalogTable.storage.locationUri} " +
          s"refreshedTable location: ${refreshedTable.storage.locationUri}")
    }
    val refreshedLastDeltaVersionConvertedOpt =
      refreshedTable.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP)
        .map(_.toLong)
    val oldBaseMetadataLocation =
      catalogTable.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP)
    val newBaseMetadataLocation =
      refreshedTable.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP)
    logInfo(s"refresh CatalogTable for UniForm for table ${catalogTable.identifier}: " +
      s"oldLastDeltaVersionConvertedOpt=$lastDeltaVersionConvertedOpt " +
      s"refreshedLastDeltaVersionConvertedOpt=$refreshedLastDeltaVersionConvertedOpt " +
      s"oldBaseMetadataLocation=$oldBaseMetadataLocation " +
      s"newBaseMetadataLocation=$newBaseMetadataLocation")
    if (
      refreshedLastDeltaVersionConvertedOpt.exists(_ >= deltaAttemptVersion)
    ) {
      throw new DeltaCommitFailedException(
        true, // retryable
        true, // conflict
        s"Attempts to commit Delta version $deltaAttemptVersion while the last converted delta " +
          s"version is already $refreshedLastDeltaVersionConvertedOpt")
    }
    refreshedTable
  }

  /**
   * Reloads a CatalogTable via the V2 catalog API
   * The V1 SessionCatalog only reaches HMS and
   * fails for UC tables whose schema doesn't exist there
   */
  protected def reloadCatalogTable(catalogTable: CatalogTable): CatalogTable = {
    val catalog = catalogTable.identifier.catalog
      .map(spark.sessionState.catalogManager.catalog)
      .getOrElse(spark.sessionState.catalogManager.v2SessionCatalog)
    val ident = Identifier.of(
      catalogTable.identifier.database.toArray,
      catalogTable.identifier.table)
    CatalogV2Util.loadTable(catalog, ident)
      .flatMap {
        case dt: DeltaTableV2 => dt.catalogTable
        case other => throw new DeltaCommitFailedException(
          false, // retryable
          false, // conflict
          s"Expected DeltaTableV2 when reloading catalogTable for ${catalogTable.identifier}, " +
            s"got ${other.getClass.getName}")
      }
      .getOrElse(throw new DeltaCommitFailedException(
        false, // retryable
        false, // conflict
        s"Table ${catalogTable.identifier} not found when reloading catalogTable through $catalog"))
  }

  /**
   * Reads the last converted Iceberg state from the catalogTable storage properties.
   * The catalogTable does not have a deltaUniformIceberg field; instead the metadata location
   * and last converted delta version are stored as plain table properties as a workaround
   */
  protected def fetchLastConvertedIcebergInfo(
      catalogTable: CatalogTable,
      deltaLog: DeltaLog,
      enablingUniForm: Boolean): LastConvertedIcebergInfo = {
    val baseMetadataLocation =
      catalogTable.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP)
    val lastDeltaVersionConverted =
      catalogTable.storage.properties
        .get(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP)
        .map(_.toLong)
    val lastConvertedIcebergTable =
      baseMetadataLocation.map(new HadoopTables(deltaLog.newDeltaHadoopConf()).load(_))
    val lastConvertedIcebergSnapshotId =
      lastConvertedIcebergTable.flatMap(it => Option(it.currentSnapshot())).map(_.snapshotId())

    LastConvertedIcebergInfo(
      icebergTable = lastConvertedIcebergTable,
      icebergSnapshotId = lastConvertedIcebergSnapshotId,
      deltaVersionConverted = lastDeltaVersionConverted,
      baseMetadataLocationOpt = baseMetadataLocation
    )
  }

  /**
   * The core implementation of convertSnapshot
   * 'delta.iceberg.conversion.convertSnapshot' ->
   *    Convert Iceberg Metadata for a complete snapshot. Used for conversion
   *    after delta commits and in create table
   */
  protected def convertSnapshotInternal(
      snapshotToConvert: Snapshot,
      readSnapshotOpt: Option[Snapshot],
      lastConvertedInfo: LastConvertedIcebergInfo,
      conversionContext: ConversionContext,
      catalogTable: CatalogTable
  ): IcebergConversionTransaction =
    recordFrameProfile("Delta", "IcebergConverter.convertSnapshotImpl") {
      val conversionMode = conversionContext.conversionMode
      val log = snapshotToConvert.deltaLog
      targetSnapshot = snapshotToConvert
      val lastConvertedIcebergTable = lastConvertedInfo.icebergTable
      val lastConvertedIcebergSnapshotId = lastConvertedInfo.icebergSnapshotId
      val lastDeltaVersionConverted = lastConvertedInfo.deltaVersionConverted
      val baseMetadataLocation = lastConvertedInfo.baseMetadataLocationOpt
      val maxCommitsToConvert =
        spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_COMMITS_TO_CONVERT)

      val conversionStartTime = System.currentTimeMillis()
      val prevConvertedSnapshotOpt = (lastDeltaVersionConverted, readSnapshotOpt) match {
        // The provided Snapshot is the last converted Snapshot
        case (Some(version), Some(readSnapshot)) if version == readSnapshot.version =>
          Some(readSnapshot)
        // Some snapshots are pending conversion since last conversion
        case (Some(version), _) if snapshotToConvert.version - version <= maxCommitsToConvert =>
          try {
            Some(log.getSnapshotAt(version, catalogTableOpt = Some(catalogTable)))
          } catch {
            // If we can't load the file since the last time Iceberg was converted, it's likely that
            // the commit file expired. Treat this like a new Iceberg table conversion.
            case _: DeltaFileNotFoundException => None
          }
        // Never converted before
        case _ => None
      }

      val rowTrackingJustEnabled =
        isRowTrackingJustEnabled(
          snapshotToConvert, prevConvertedSnapshotOpt, lastConvertedIcebergTable)
      val tableOp = (lastDeltaVersionConverted, prevConvertedSnapshotOpt) match {
        case (Some(_), _) if rowTrackingJustEnabled => REPLACE_TABLE
        case (Some(_), Some(_)) => WRITE_TABLE
        case (Some(_), None) => REPLACE_TABLE
        case (None, None) => CREATE_TABLE
      }

      val icebergTxn = new IcebergConversionTransaction(
        spark, catalogTable, log.newDeltaHadoopConf(), snapshotToConvert, tableOp,
        lastConvertedIcebergSnapshotId, lastDeltaVersionConverted, baseMetadataLocation
      )

      val convertedCommits: Seq[Option[CommitInfo]] = prevConvertedSnapshotOpt match {
        // If `rowTrackingBackfillRequired` is enabled, the Row Tracking Backfill command
        // will first be triggered, adding one or more ROW TRACKING BACKFILL commits
        // to the table. After backfilling completes, here we must regenerate the entire
        // Iceberg metadata to keep the snapshot version in sync.
        case _ if rowTrackingJustEnabled =>
          val commitInfos = createSnapshotsForReplaceTable(
            snapshotToConvert, prevConvertedSnapshotOpt, icebergTxn, catalogTable,
            snapshotToConvert.version, useRowTrackingVersion = true)
          prevConvertedSnapshotOpt.foreach { prevSnapshot =>
            icebergTxn.updateTableMetadata(prevSnapshot.metadata)
          }
          commitInfos
        case Some(prevSnapshot) =>
          // Read the actions directly from the delta json files.
          // TODO: Run this as a spark job on executors
          val endVersion = conversionMode match {
            case UNIFORM_CC_MODE =>
              snapshotToConvert.version - 1
            case _ => snapshotToConvert.version
          }
          val deltaFiles = DeltaFileProviderUtils.getDeltaFilesInVersionRange(
            spark = spark,
            deltaLog = log,
            startVersion = prevSnapshot.version + 1,
            endVersion = endVersion,
            catalogTableOpt = Some(catalogTable))

          recordDeltaEvent(
            snapshotToConvert.deltaLog,
            "delta.iceberg.conversion.deltaCommitRange",
            data = Map(
              "fromVersion" -> (prevSnapshot.version + 1),
              "toVersion" -> snapshotToConvert.version,
              "numDeltaFiles" -> deltaFiles.length
            )
          )

          val actionsToConvert = DeltaFileProviderUtils.parallelReadAndParseDeltaFilesAsIterator(
            log, spark, deltaFiles)
          var deltaVersion = prevSnapshot.version
          val commitInfos = actionsToConvert.map { actionsIter =>
            try {
              deltaVersion += 1
              runIcebergConversionForActions(
                icebergTxn,
                actionsIter.map(Action.fromJson).toSeq,
                prevConvertedSnapshotOpt,
                deltaVersion)
            } finally {
              actionsIter.close()
            }
          }
          val additionalCommitInfo = if (conversionContext.hasAdditionalDeltaActionsToCommit) {
            runIcebergConversionForActions(
              icebergTxn,
              actionsToCommit = conversionContext.getAdditionalDeltaActionsToCommit,
              prevConvertedSnapshotOpt,
              deltaVersion + 1
            )
          } else {
            None
          }
          // If the metadata hasn't changed, this will no-op.
          icebergTxn.updateTableMetadata(prevSnapshot.metadata)
          commitInfos :+ additionalCommitInfo
        case None =>
          // If we don't have a snapshot of the last converted version, get all the AddFiles
          // (via state reconstruction).
          // Batch is always disabled but we still want to reuse the event for conversion
          recordDeltaEvent(
            snapshotToConvert.deltaLog,
            "delta.iceberg.conversion.batch",
            data = Map(
              "version" -> snapshotToConvert.version,
              "numOfFiles" -> snapshotToConvert.numOfFiles,
              "actionBatchSize" -> -1, // This param is ignored as batch is deprecated
              "numOfPartitions" -> 1
            )
          )
          runIcebergConversionForActions(
            icebergTxn,
            snapshotToConvert.allFiles.toLocalIterator().asScala.toSeq,
            None,
            snapshotToConvert.version)

          // Always attempt to update table metadata (schema/properties) for REPLACE_TABLE
          if (tableOp == REPLACE_TABLE) {
            icebergTxn.updateTableMetadata(snapshotToConvert.metadata)
          }
          Nil
      }


      // OPTIMIZE will trigger snapshot expiration for iceberg table
      val OPR_TRIGGER_EXPIRE = Set(DeltaOperations.OPTIMIZE_OPERATION_NAME)
      val needsExpireSnapshot =
        OPR_TRIGGER_EXPIRE.intersect(convertedCommits.flatten.map(_.operation).toSet).nonEmpty
      if (needsExpireSnapshot) {
        logInfo(log"Committing iceberg snapshot expiration for uniform table " +
          log"[path = ${MDC(DeltaLogKeys.PATH, log.logPath)}] tableId=" +
          log"${MDC(DeltaLogKeys.TABLE_ID, log.unsafeVolatileTableId)}]")
        expireIcebergSnapshot(snapshotToConvert, icebergTxn)
      }

      icebergTxn.commit()
      logInfo(s"icebergTxn committed for table ${Option(catalogTable).map(_.identifier)} " +
        s"with converted delta version ${snapshotToConvert.version}")

      recordDeltaEvent(
        snapshotToConvert.deltaLog,
        conversionContext.opType,
        data = Map(
          "deltaVersion" -> snapshotToConvert.version,
          "compatVersion" -> IcebergCompat.getEnabledVersion(snapshotToConvert.metadata)
            .getOrElse(0),
          "elapsedTimeMs" -> (System.currentTimeMillis() - conversionStartTime)
        )
      )

      icebergTxn
    }

  private def getSnapshotForUncommitedTxn(
      deltaLog: DeltaLog,
      txnInfo: CurrentTransactionInfo,
      deltaAttemptVersion: Long,
      catalogTable: CatalogTable): Snapshot = {
    val rowTrackingHighWaterMark = if (IcebergCompat.isGeqEnabled(txnInfo.metadata, 3)) {
      txnInfo.finalActionsToCommit.collectFirst {
        case RowTrackingMetadataDomain(domain) => domain.rowIdHighWaterMark
      }.orElse(Some(txnInfo.readRowIdHighWatermark))
    } else {
      None
    }
    val domainMetadata = if (IcebergCompat.isGeqEnabled(txnInfo.metadata, 3)) {
      rowTrackingHighWaterMark.map { highWaterMark =>
        Seq(RowTrackingMetadataDomain(rowIdHighWaterMark = highWaterMark).toDomainMetadata)
      }
    } else {
      None
    }

    new DummySnapshotWithAllFilesSupport(
      deltaLog.logPath,
      deltaLog,
      txnInfo.metadata,
      deltaAttemptVersion,
      domainMetadata,
      Some(txnInfo.protocol),
      txnInfo,
      Some(catalogTable)
    )
  }

  /**
   * Helper function to execute and commit Iceberg snapshot expiry
   * @param snapshotToConvert the Delta snapshot that needs to be converted to Iceberg
   * @param icebergTxn the IcebergConversionTransaction created in convertSnapshot, used
   *                   to create a table object and expiration helper
   */
  private def expireIcebergSnapshot(
      snapshotToConvert: Snapshot,
      icebergTxn: IcebergConversionTransaction): Unit = {
    val expireSnapshotHelper = icebergTxn.getExpireSnapshotHelper()
    val table = icebergTxn.txn.table()
    val tableLocation = LocationUtil.stripTrailingSlash(table.location)
    val defaultWriteMetadataLocation = s"$tableLocation/metadata"
    val writeMetadataLocation = LocationUtil.stripTrailingSlash(
      table.properties().getOrDefault(
        TableProperties.WRITE_METADATA_LOCATION, defaultWriteMetadataLocation))

    val shouldKeepPhysicalFiles =
      // Don't attempt any file cleanup in the edge-case configuration
      // that the data location (in Uniform the table root location)
      // is the same as the Iceberg metadata location
      (snapshotToConvert.path.toString == writeMetadataLocation)
    if (shouldKeepPhysicalFiles) {
      expireSnapshotHelper.cleanExpiredFiles(false)
    } else {
      expireSnapshotHelper.deleteWith(path => {
        if (path.startsWith(writeMetadataLocation)) {
          table.io().deleteFile(path)
        }
      })
    }

    expireSnapshotHelper.commit(snapshotToConvert.version)
  }

  // This is for newly enabling uniform table to
  // start a new history line for iceberg metadata
  // so that if a uniform table is corrupted,
  // user can unset and re-enable to unblock
  private def cleanCatalogTableIfEnablingUniform(
      table: CatalogTable,
      snapshotToConvert: Snapshot,
      txnOpt: Option[CommittedTransaction]): CatalogTable = {
    val disabledIceberg = txnOpt.map(txn =>
      !UniversalFormat.icebergEnabled(txn.readSnapshot.metadata)
    ).getOrElse(!UniversalFormat.icebergEnabled(table.properties))
    val enablingUniform =
      disabledIceberg && UniversalFormat.icebergEnabled(snapshotToConvert.metadata)
    if (enablingUniform) {
      clearDeltaUniformMetadata(table)
    } else {
      table
    }
  }

  protected def clearDeltaUniformMetadata(table: CatalogTable): CatalogTable = {
    val metadata_key = IcebergConstants.ICEBERG_TBLPROP_METADATA_LOCATION
    if (table.properties.contains(metadata_key)) {
      val cleanedCatalogTable = table.copy(properties = table.properties
        - metadata_key
        - IcebergConverter.DELTA_VERSION_PROPERTY
        - IcebergConverter.DELTA_TIMESTAMP_PROPERTY
      )
      spark.sessionState.catalog.alterTable(cleanedCatalogTable)
      cleanedCatalogTable
    } else {
      table
    }
  }

  override def loadLastDeltaVersionConverted(
      snapshot: Snapshot, catalogTable: CatalogTable): Option[Long] =
    recordFrameProfile("Delta", "IcebergConverter.loadLastDeltaVersionConverted") {
      IcebergConverter.getLastConvertedDeltaVersion(loadIcebergTable(snapshot, catalogTable))
    }

  protected def loadIcebergTable(
      snapshot: Snapshot, catalogTable: CatalogTable): Option[IcebergTable] = {
    recordFrameProfile("Delta", "IcebergConverter.loadLastConvertedIcebergTable") {
      val hiveCatalog = IcebergTransactionUtils
        .createHiveCatalog(snapshot.deltaLog.newDeltaHadoopConf())
      val icebergTableId = IcebergTransactionUtils
        .convertSparkTableIdentifierToIcebergHive(catalogTable.identifier)
      if (hiveCatalog.tableExists(icebergTableId)) {
        Some(hiveCatalog.loadTable(icebergTableId))
      } else {
        None
      }
    }
  }

  /**
   * Returns true iff Row Tracking has just been turned on for this table
   *
   * A "just enabled" state is detected when **all** of the following hold:
   *   1) Row Tracking is enabled on `snapshotToConvert`, and
   *   2) IcebergCompat V3 is enabled on `snapshotToConvert.metadata`, and
   *   3) Either:
   *        - there is no previously converted snapshot (i.e., first time enable icebergCompat), or
   *        - the last converted Iceberg table exists and its current Iceberg
   *          format version is <= 2 (i.e., the table was on a lower compat version).
   */
  protected def isRowTrackingJustEnabled(
      snapshotToConvert: Snapshot,
      prevConvertedSnapshotOpt: Option[Snapshot],
      lastConvertedIcebergTable: Option[IcebergTable]): Boolean = {
    val rowTrackingEnabledOnSnapshotToConvert = RowTracking.isEnabled(
      snapshotToConvert.protocol, snapshotToConvert.metadata)
    rowTrackingEnabledOnSnapshotToConvert &&
      IcebergCompat.isGeqEnabled(snapshotToConvert.metadata, 3) &&
      // Either this is first time to enable Uniform or upgrade from lower version
      (prevConvertedSnapshotOpt.isEmpty || lastConvertedIcebergTable.exists(
        _.asInstanceOf[BaseTable].operations().current().formatVersion() <= 2))
  }

  /**
   * Replays all AddFile actions from a Delta snapshot into Iceberg by committing
   * them version by version within a replace table transaction.
   *
   * Groups all AddFile actions from snapshotToConvert by commit version and
   * commits each group to icebergTxn at its respective version. The commit version
   * is determined either by each file's defaultRowCommitVersion (when
   * useRowTrackingVersion is true), or by the provided deltaVersion as a fallback.
   *
   * @param snapshotToConvert The Delta snapshot whose AddFiles are being replayed
   * @param prevConvertedSnapshotOpt Optional previous converted Iceberg snapshot
   * @param icebergTxn The Iceberg conversion transaction
   * @param catalogTable The catalog table metadata
   * @param fallbackVersion Fallback Delta version to use when
   *                        file-level commit versions are missing
   * @param useRowTrackingVersion Whether to derive commit version from each file's
   *                              defaultRowCommitVersion (if available)
   * @return A sequence of optional CommitInfo objects for each replayed commit
   * @throws IllegalStateException if no actions are found for a commit version
   */
  protected def createSnapshotsForReplaceTable(
      snapshotToConvert: Snapshot,
      prevConvertedSnapshotOpt: Option[Snapshot],
      icebergTxn: IcebergConversionTransaction,
      catalogTable: CatalogTable,
      fallbackVersion: Long,
      useRowTrackingVersion: Boolean): Seq[Option[CommitInfo]] = {
    val versionActionPairs = snapshotToConvert.allFiles.rdd.map { add =>
      val version = if (useRowTrackingVersion) {
        add.defaultRowCommitVersion.getOrElse(fallbackVersion)
      } else {
        fallbackVersion
      }
      val action = add.copy(dataChange = true).wrap.unwrap
      version -> action
    }
    val allVersions = versionActionPairs.keys.distinct().collect().sorted
    val groupedRdd = versionActionPairs.groupByKey().cache()

    // Process one version at a time
    val commitInfos = allVersions.toSeq.map { version =>
      // Turn JSON back into Actions
      val actionsToAdd: Seq[Action] =
        groupedRdd
          .lookup(version)
          .headOption
          .map(_.iterator.toSeq)
          .getOrElse(throw new IllegalStateException(s"No actions found for version $version"))
      runIcebergConversionForActions(
        icebergTxn,
        actionsToAdd,
        prevConvertedSnapshotOpt,
        version
      )
    }
    commitInfos
  }

  /**
   * Commit the set of changes into an Iceberg snapshot. Each call to this function will
   * build exactly one Iceberg Snapshot.
   *
   * We determine what type of [[IcebergConversionTransaction.TransactionHelper]] to use
   * (and what type of Iceberg snapshot to create) based on the types of actions and
   * whether they contain data change. An [[UnsupportedOperationException]] will be
   * thrown for cases not listed in the table below. It means the combination of actions are
   * not recognized/supported. IcebergConverter will do a re-try with REPLACE TABLE, which
   * collects all valid data files from the target Delta snapshot and commit to Iceberg.
   *
   * Some Delta operations only update metadata on existing files. They rely on Delta's
   * dedup in state reconstruction and cannot be action-to-action translated to Iceberg.
   * Iceberg lacks dedup abilities. These metadata-only commits are skipped here before
   * choosing a TransactionHelper. The enclosing transaction still updates table properties
   * such as delta-version and writes new metadata to keep Iceberg conversion state
   * in sync.
   *
   * The following table demonstrates how to choose the appropriate TransactionHelper.
   * The conditions can overlap and should be checked in order.
   * +-------------------+---------------+---------------------+--------------------+
   * |  Type of actions  |  Data Change  |   TransactionHelper | Example / Note     |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Create table     |  Any          |   AppendHelper      |  Note 1            |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Create with DV   |  Any          |   RowDeltaHelper    |                    |
   * +-------------------+---------------+---------------------+--------------------+
   * |                   |  All          |   AppendHelper      |  INSERT            |
   * |  Add only         +---------------+---------------------+--------------------+
   * |                   |  None         |   NullHelper        |  Note 2            |
   * |                   |               |                     |  Add Tag           |
   * |                   +---------------+---------------------+--------------------+
   * |                   |  Some         |   Unsupported       |  (unknown)         |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Add only         |  All          |   RowDeltaHelper    |  Smart Clone       |
   * |  with DV          +---------------+---------------------+--------------------+
   * |                   |  None         |   same as no dv     |  Note 2            |
   * |                   +---------------+---------------------+--------------------+
   * |                   |  Some         |   Unsupported       |  (unknown)         |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Remove only      |  Any          |   RemoveHelper      |  DELETE            |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Remove only      |  Any          |   RowDeltaHelper    |  DELETE            |
   * |  with DV          |               |                     |                    |
   * +-------------------+---------------+---------------------+--------------------+
   * |                   |  All          |   OverwriteHelper   |  UPDATE            |
   * |  Add + Remove     +---------------+---------------------+--------------------+
   * |                   |  None         |   RewriteHelper     |  OPTIMIZE          |
   * |                   +---------------+---------------------+--------------------+
   * |                   |  Some         |   Unsupported       |  (unknown)         |
   * +-------------------+---------------+---------------------+--------------------+
   * |  Add + Remove     |  All          |   RowDeltaHelper    |  UPDATE            |
   * |  with DV          +---------------+---------------------+--------------------+
   * |                   |  None         |   RewriteHelper     |  OPTIMIZE          |
   * |                   +---------------+---------------------+--------------------+
   * |                   |  Some         |   Unsupported       |  (unknown)         |
   * +-------------------+---------------+---------------------+--------------------+
   * Note:
   * 1. We assume a Create/Replace table operation will only contain AddFiles.
   * 2. DV is allowed but ignored for no-data changes.
   */
  private[delta] def runIcebergConversionForActions(
      icebergTxn: IcebergConversionTransaction,
      actionsToCommit: Seq[Action],
      prevSnapshotOpt: Option[SnapshotDescriptor],
      deltaVersion: Long): Option[CommitInfo] = {

    var commitInfo: Option[CommitInfo] = None
    var addFiles: Seq[AddFile] = Nil
    var removeFiles: Seq[RemoveFile] = Nil
    // Determining what txnHelper to use for this group of Actions requires a full-scan
    // of [[actionsToCommit]], which is not too expensive as the actions are already in-memory.

    val txnHelper = prevSnapshotOpt match {
      // Having no previous Snapshot implies that the table is either being created or replaced.
      // This guarantees that the actions are fetched via [[Snapshot.allFiles]] and are unique.
      case None =>
        addFiles = actionsToCommit.asInstanceOf[Seq[AddFile]]
        if (addFiles.isEmpty) {
          icebergTxn.getNullHelper
        } else if (addFiles.exists(_.deletionVector != null)) {
          icebergTxn.getRowDeltaHelper
        } else {
          icebergTxn.getAppendOnlyHelper
        }
      case Some(_) =>
        commitInfo = actionsToCommit.collectFirst { case c: CommitInfo => c }
        val isComputeStats = commitInfo.exists(_.operation == "COMPUTE STATS")

        if (isComputeStats
        ) {
          icebergTxn.getNullHelper
        } else {
          val addBuffer = new ArrayBuffer[AddFile]()
          val removeBuffer = new ArrayBuffer[RemoveFile]()
          // Scan the actions to collect info needed to determine which txnHelper to use
          object DataChange extends Enumeration {
            val Empty = Value(0, "Empty")
            val None = Value(1, "None")
            val All = Value(2, "All")
            val Some = Value(3, "Some")
          }
          var dataChangeBits = 0
          var hasDv: Boolean = false

          actionsToCommit.foreach {
            case file: FileAction =>
              addBuffer ++= Option(file.wrap.add)
              removeBuffer ++= Option(file.wrap.remove)
              if (file.wrap.add != null || file.wrap.remove != null) {
                // We only care about data changes in add and remove actions
                dataChangeBits |= (1 << (if (file.dataChange) 1 else 0))
              }
              hasDv |= file.deletionVector != null
            case _ => // Ignore other actions
          }
          addFiles = addBuffer.toSeq
          removeFiles = removeBuffer.toSeq
          val dataChange = DataChange(dataChangeBits)

          (addFiles.nonEmpty, removeFiles.nonEmpty, dataChange) match {
          case (true, false, DataChange.All) if !hasDv =>
            icebergTxn.getAppendOnlyHelper
          case (true, false, DataChange.All) if hasDv =>
            icebergTxn.getRowDeltaHelper
          case (true, false, DataChange.None) =>
            icebergTxn.getNullHelper // Ignore
          case (false, true, _) if hasDv =>
            icebergTxn.getRowDeltaHelper
          case (false, true, _) =>
            icebergTxn.getRemoveOnlyHelper
          case (true, true, DataChange.All) if hasDv =>
            icebergTxn.getRowDeltaHelper
          case (true, true, DataChange.None) if hasDv =>
            icebergTxn.getRewriteHelper
          case (true, true, DataChange.All) if !hasDv =>
            icebergTxn.getOverwriteHelper
          case (true, true, DataChange.None) if !hasDv =>
            icebergTxn.getRewriteHelper
          case (false, false, _) =>
            icebergTxn.getNullHelper
          case _ =>
            recordDeltaEvent(
              targetSnapshot,
              "delta.iceberg.conversion.unsupportedActions",
              data = Map(
                "version" -> targetSnapshot.version,
                "commitInfo" -> commitInfo.map(_.operation).getOrElse(""),
                "hasAdd" -> addFiles.nonEmpty.toString,
                "hasRemove" -> removeFiles.nonEmpty.toString,
                "dataChange" -> dataChange.toString,
                "hasDv" -> hasDv.toString
              )
            )
            logError(
              s"""Unsupported combination of actions for incremental conversion. Context:
                 |version -> ${targetSnapshot.version},
                 |commitInfo -> ${commitInfo.map(_.operation).getOrElse("")},
                 |hasAdd -> ${addFiles.nonEmpty.toString},
                 |hasRemove -> ${removeFiles.nonEmpty.toString},
                 |dataChange -> ${dataChange.toString},
                 |hasDv -> ${hasDv.toString}""".stripMargin)
            throw new UnsupportedOperationException(
              "Unsupported combination of actions for incremental conversion.")
          }
        }
    }
    recordDeltaEvent(
      targetSnapshot,
      "delta.iceberg.conversion.convertActions",
      data = Map(
        "version" -> targetSnapshot.version,
        "commitInfo" -> commitInfo.map(_.operation).getOrElse(""),
        "txnHelper" -> txnHelper.getClass.getSimpleName
      )
    )

    removeFiles.foreach(txnHelper.add)
    addFiles.foreach(txnHelper.add)
    // Make sure the next snapshot sequence number is deltaVersion
    txnHelper.commit(deltaVersion)

    commitInfo
  }

  /**
   * Validate the Iceberg conversion by comparing the number of files and size in bytes
   * between the converted Iceberg table and the Delta table.
   * TODO: throw exception and proactively abort conversion transaction
   */
  private def validateIcebergCommit(snapshotToConvert: Snapshot, catalogTable: CatalogTable) = {
    val table = loadIcebergTable(snapshotToConvert, catalogTable)
    val lastConvertedDeltaVersion = IcebergConverter.getLastConvertedDeltaVersion(table)
    table.map {t =>
      if (lastConvertedDeltaVersion.contains(snapshotToConvert.version) &&
          t.currentSnapshot() != null) {
        val icebergNumOfFiles = t.currentSnapshot().summary().asScala
          .getOrElse("total-data-files", "-1").toLong
        val icebergTotalBytes = t.currentSnapshot().summary().asScala
          .getOrElse("total-files-size", "-1").toLong

        if (icebergNumOfFiles != snapshotToConvert.numOfFiles ||
            icebergTotalBytes != snapshotToConvert.sizeInBytes) {
          recordDeltaEvent(
            snapshotToConvert.deltaLog, "delta.iceberg.conversion.mismatch",
            data = Map(
              "lastConvertedDeltaVersion" -> snapshotToConvert.version,
              "numOfFiles" -> snapshotToConvert.numOfFiles,
              "icebergNumOfFiles" -> icebergNumOfFiles,
              "sizeInBytes" -> snapshotToConvert.sizeInBytes,
              "icebergTotalBytes" -> icebergTotalBytes
            )
          )
        }
      }
    }
  }
}

/**
 * The dummy snapshot that could compute allFiles using state-reconstruction and log reply
 * This is used for Delta UniForm only
 */
class DummySnapshotWithAllFilesSupport(
    override val logPath: Path,
    override val deltaLog: DeltaLog,
    override val metadata: Metadata,
    override val version: Long = -1,
    val domainMetadataOpt: Option[Seq[DomainMetadata]] = None,
    val protocolOpt: Option[Protocol] = None,
    val txnInfo: CurrentTransactionInfo,
    override val catalogTable: Option[CatalogTable])
  extends DummySnapshot(
    logPath,
    deltaLog,
    metadata,
    domainMetadataOpt,
    protocolOpt
  ) {
  override def allFiles: Dataset[AddFile] = {
    SparkSession.getActiveSession.map { spark =>
      import org.apache.spark.sql.delta.implicits._
      val replay = new InMemoryLogReplay(None, None)
      val baseVersion = version - 1
      if (baseVersion < 0) { // No prior commit exists
        replay.append(0, txnInfo.finalActionsToCommit.iterator)
      } else { // construct allFiles from baseSnapshot
        val baseSnapshot = baseVersion match {
          case txnInfo.readSnapshot.version => txnInfo.readSnapshot
          case _ => deltaLog.getSnapshotAt(baseVersion, catalogTableOpt = catalogTable)
        }
        val existingFiles = baseSnapshot.allFiles.collect().toIterator
        replay.append(0, existingFiles)
        replay.append(1, txnInfo.finalActionsToCommit.iterator)
      }
      val allFiles: Seq[AddFile] = replay.allFiles
      spark.createDataset(allFiles)
    }.getOrElse(super.allFiles)
  }
}
