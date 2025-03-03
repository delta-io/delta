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
import scala.util.control.Breaks._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaErrors, DeltaFileNotFoundException, DeltaFileProviderUtils, DeltaTransaction, IcebergConstants, Snapshot, UniversalFormat, UniversalFormatConverter}
import org.apache.spark.sql.delta.DeltaOperations.OPTIMIZE_OPERATION_NAME
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.hooks.IcebergConverterHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.{Table => IcebergTable}
import shadedForDelta.org.apache.iceberg.exceptions.CommitFailedException
import shadedForDelta.org.apache.iceberg.hive.{HiveCatalog, HiveTableOperations}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

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

  def getLastConvertedDeltaVersion(table: Option[IcebergTable]): Option[Long] =
    table.flatMap(_.properties().asScala.get(DELTA_VERSION_PROPERTY)).map(_.toLong)

  def getLastConvertedDeltaTimestamp(table: Option[IcebergTable]): Option[Long] =
    table.flatMap(_.properties().asScala.get(DELTA_TIMESTAMP_PROPERTY)).map(_.toLong)
}

/**
 * This class manages the transformation of delta snapshots into their Iceberg equivalent.
 */
class IcebergConverter(spark: SparkSession)
    extends UniversalFormatConverter(spark)
    with DeltaLogging {

  // Save an atomic reference of the snapshot being converted, and the txn that triggered
  // resulted in the specified snapshot
  protected val currentConversion =
    new AtomicReference[(Snapshot, DeltaTransaction)]()
  protected val standbyConversion =
    new AtomicReference[(Snapshot, DeltaTransaction)]()

  // Whether our async converter thread is active. We may already have an alive thread that is
  // about to shutdown, but in such cases this value should return false.
  @GuardedBy("asyncThreadLock")
  private var asyncConverterThreadActive: Boolean = false
  private val asyncThreadLock = new Object

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
      txn: DeltaTransaction): Unit = {
    if (!UniversalFormat.icebergEnabled(snapshotToConvert.metadata)) {
      return
    }
    val log = snapshotToConvert.deltaLog
    // Replace any previously queued snapshot
    val previouslyQueued = standbyConversion.getAndSet((snapshotToConvert, txn))
    asyncThreadLock.synchronized {
      if (!asyncConverterThreadActive) {
        val threadName = IcebergConverterHook.ASYNC_ICEBERG_CONVERTER_THREAD_NAME +
          s" [id=${snapshotToConvert.metadata.id}]"
        val asyncConverterThread: Thread = new Thread(threadName) {
          setDaemon(true)

          override def run(): Unit =
              try {
                var snapshotAndTxn = getNextSnapshot
                  while (snapshotAndTxn != null) {
                    val snapshotVal = snapshotAndTxn._1
                    val prevTxn = snapshotAndTxn._2
                    try {
                      logInfo(log"Converting Delta table [path=" +
                        log"${MDC(DeltaLogKeys.PATH, log.logPath)}, " +
                        log"tableId=${MDC(DeltaLogKeys.TABLE_ID, log.tableId)}, version=" +
                        log"${MDC(DeltaLogKeys.VERSION, snapshotVal.version)}] into Iceberg")
                      convertSnapshot(snapshotVal, prevTxn)
                    } catch {
                      case NonFatal(e) =>
                        logWarning(log"Error when writing Iceberg metadata asynchronously", e)
                        recordDeltaEvent(
                          log,
                          "delta.iceberg.conversion.async.error",
                          data = Map(
                            "exception" -> ExceptionUtils.getMessage(e),
                            "stackTrace" -> ExceptionUtils.getStackTrace(e)
                          )
                        )
                    }
                    currentConversion.set(null)
                    // Pick next snapshot to convert if there's a new one
                    snapshotAndTxn = getNextSnapshot
                  }
              } finally {
                // shuttingdown thread
                asyncThreadLock.synchronized {
                  asyncConverterThreadActive = false
                }
              }

          // Get a snapshot to convert from the icebergQueue. Sets the queue to null after.
          private def getNextSnapshot: (Snapshot, DeltaTransaction) =
            asyncThreadLock.synchronized {
              val potentialSnapshotAndTxn = standbyConversion.get()
              currentConversion.set(potentialSnapshotAndTxn)
              standbyConversion.compareAndSet(potentialSnapshotAndTxn, null)
              if (potentialSnapshotAndTxn == null) {
                asyncConverterThreadActive = false
              }
              potentialSnapshotAndTxn
            }
        }
        asyncConverterThread.start()
        asyncConverterThreadActive = true
      }
    }

    // If there already was a snapshot waiting to be converted, log that snapshot info.
    if (previouslyQueued != null) {
//      previouslyQueued._1.uncache()
      recordDeltaEvent(
        snapshotToConvert.deltaLog,
        "delta.iceberg.conversion.async.backlog",
        data = Map(
          "newVersion" -> snapshotToConvert.version,
          "replacedVersion" -> previouslyQueued._1.version)
      )
    }
  }

  /**
   * Convert the specified snapshot into Iceberg for the given catalogTable
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param catalogTable the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, catalogTable: CatalogTable): Option[(Long, Long)] = {
    try {
      convertSnapshotWithRetry(snapshotToConvert, None, catalogTable)
    } catch {
      case NonFatal(e) =>
        logError(log"Error when converting to Iceberg metadata", e)
        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.iceberg.conversion.error",
          data = Map(
            "exception" -> ExceptionUtils.getMessage(e),
            "stackTrace" -> ExceptionUtils.getStackTrace(e)
          )
        )
        throw e
    }
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
      snapshotToConvert: Snapshot, txn: DeltaTransaction): Option[(Long, Long)] = {
    try {
      txn.catalogTable match {
        case Some(table) => convertSnapshotWithRetry(snapshotToConvert, Some(txn), table)
        case _ =>
          val msg = s"CatalogTable for table ${snapshotToConvert.deltaLog.tableId} " +
            s"is empty in txn. Skip iceberg conversion."
          throw DeltaErrors.universalFormatConversionFailedException(
            snapshotToConvert.version, "iceberg", msg)
      }
    } catch {
      case NonFatal(e) =>
        logError(log"Error when converting to Iceberg metadata", e)
        recordDeltaEvent(
          txn.deltaLog,
          "delta.iceberg.conversion.error",
          data = Map(
            "exception" -> ExceptionUtils.getMessage(e),
            "stackTrace" -> ExceptionUtils.getStackTrace(e)
          )
        )
        throw e
    }
  }

  /**
   *  Convert the specified snapshot into Iceberg with retry
   */
  private def convertSnapshotWithRetry(
      snapshotToConvert: Snapshot,
      txnOpt: Option[DeltaTransaction],
      catalogTable: CatalogTable,
      maxRetry: Int =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_UNIFORM_ICEBERG_RETRY_TIMES)
  ): Option[(Long, Long)] = {
    var retryAttempt = 0
    while (retryAttempt < maxRetry) {
      try {
        return convertSnapshot(snapshotToConvert, txnOpt, catalogTable)
      } catch {
        case e: CommitFailedException if retryAttempt < maxRetry =>
          retryAttempt += 1
          val lastConvertedIcebergTable = loadIcebergTable(snapshotToConvert, catalogTable)
          val lastDeltaVersionConverted = IcebergConverter
            .getLastConvertedDeltaVersion(lastConvertedIcebergTable)
          val lastConvertedDeltaTimestamp = IcebergConverter
            .getLastConvertedDeltaTimestamp(lastConvertedIcebergTable)
          // Do not retry if the current or higher Delta version is already converted
          (lastDeltaVersionConverted, lastConvertedDeltaTimestamp) match {
            case (Some(version), Some(timestamp)) if version >= snapshotToConvert.version =>
              return Some(version, timestamp)
            case _ =>
              logWarning(s"CommitFailedException when converting to Iceberg metadata;" +
                s" retry count $retryAttempt", e)
          }
      }
    }
    throw new IllegalStateException("should not happen")
  }

  /**
   * Convert the specified snapshot into Iceberg. NOTE: This operation is blocking. Call
   * enqueueSnapshotForConversion to run the operation asynchronously.
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param txnOpt the OptimisticTransaction that created snapshotToConvert.
   *            Used as a hint to avoid recomputing old metadata.
   * @param catalogTable the catalogTable this conversion targets
   * @return Converted Delta version and commit timestamp
   */
  private def convertSnapshot(
      snapshotToConvert: Snapshot,
      txnOpt: Option[DeltaTransaction],
      catalogTable: CatalogTable): Option[(Long, Long)] =
      recordFrameProfile("Delta", "IcebergConverter.convertSnapshot") {
    val cleanedCatalogTable =
      cleanCatalogTableIfEnablingUniform(catalogTable, snapshotToConvert, txnOpt)
    val log = snapshotToConvert.deltaLog
    val lastConvertedIcebergTable = loadIcebergTable(snapshotToConvert, cleanedCatalogTable)
    val lastConvertedIcebergSnapshotId =
      lastConvertedIcebergTable.flatMap(it => Option(it.currentSnapshot())).map(_.snapshotId())
    val lastDeltaVersionConverted = IcebergConverter
      .getLastConvertedDeltaVersion(lastConvertedIcebergTable)
    val maxCommitsToConvert =
      spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_COMMITS_TO_CONVERT)

    // Nth to convert
    if (lastDeltaVersionConverted.contains(snapshotToConvert.version)) {
      return None
    }

    // Get the most recently converted delta snapshot, if applicable
    val prevConvertedSnapshotOpt = (lastDeltaVersionConverted, txnOpt) match {
      case (Some(version), Some(txn)) if version == txn.snapshot.version =>
        Some(txn.snapshot)
      // Check how long it has been since we last converted to Iceberg. If outside the threshold,
      // fall back to state reconstruction to get the actions, to protect driver from OOMing.
      case (Some(version), _) if snapshotToConvert.version - version <= maxCommitsToConvert =>
        try {
          // TODO: We can optimize this by providing a checkpointHint to getSnapshotAt. Check if
          //  txn.snapshot.version < version. If true, use txn.snapshot's checkpoint as a hint.
          Some(log.getSnapshotAt(version, catalogTableOpt = Some(catalogTable)))
        } catch {
          // If we can't load the file since the last time Iceberg was converted, it's likely that
          // the commit file expired. Treat this like a new Iceberg table conversion.
          case _: DeltaFileNotFoundException => None
        }
      case (_, _) => None
    }

    val tableOp = (lastDeltaVersionConverted, prevConvertedSnapshotOpt) match {
      case (Some(_), Some(_)) => WRITE_TABLE
      case (Some(_), None) => REPLACE_TABLE
      case (None, None) => CREATE_TABLE
    }

    val icebergTxn = new IcebergConversionTransaction(
      spark, cleanedCatalogTable, log.newDeltaHadoopConf(), snapshotToConvert, tableOp,
      lastConvertedIcebergSnapshotId, lastDeltaVersionConverted)

    // Write out the actions taken since the last conversion (or since table creation).
    // This is done in batches, with each batch corresponding either to one delta file,
    // or to the specified batch size.
    val actionBatchSize =
      spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_ACTIONS_TO_CONVERT)
    // If there exists any OPTIMIZE action inside actions to convert,
    // It will trigger snapshot expiration for iceberg table
    var needsExpireSnapshot = false

    prevConvertedSnapshotOpt match {
      case Some(prevSnapshot) =>
        // Read the actions directly from the delta json files.
        // TODO: Run this as a spark job on executors
        val deltaFiles = DeltaFileProviderUtils.getDeltaFilesInVersionRange(
          spark, log, prevSnapshot.version + 1, snapshotToConvert.version)

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
        actionsToConvert.foreach { actionsIter =>
          try {
            actionsIter.grouped(actionBatchSize).foreach { actionStrs =>
              val actions = actionStrs.map(Action.fromJson)
              needsExpireSnapshot ||= existsOptimize(actions)

              runIcebergConversionForActions(
                icebergTxn,
                actions,
                log.dataPath,
                prevConvertedSnapshotOpt)
            }
          } finally {
            actionsIter.close()
          }
        }
        // If the metadata hasn't changed, this will no-op.
        icebergTxn.updateTableMetadata(snapshotToConvert.metadata, prevSnapshot.metadata)

      // If we don't have a snapshot of the last converted version, get all the table addFiles
      // (via state reconstruction).
      case None =>
        val numPartitions = math.max(snapshotToConvert.numOfFiles / actionBatchSize, 1)
        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.iceberg.conversion.batch",
          data = Map(
            "version" -> snapshotToConvert.version,
            "numOfFiles" -> snapshotToConvert.numOfFiles,
            "actionBatchSize" -> actionBatchSize,
            "numOfPartitions" -> numPartitions
          )
        )

        snapshotToConvert.allFiles
          .repartition(numPartitions.toInt)
          .toLocalIterator
          .asScala
          .grouped(actionBatchSize)
          .foreach { actions =>
            needsExpireSnapshot ||= existsOptimize(actions)
            runIcebergConversionForActions(icebergTxn, actions, log.dataPath, None)
        }

        // Always attempt to update table metadata (schema/properties) for REPLACE_TABLE
        if (tableOp == REPLACE_TABLE) {
          icebergTxn.updateTableMetadata(snapshotToConvert.metadata, snapshotToConvert.metadata)
        }
    }
    if (needsExpireSnapshot) {
      logInfo(log"Committing iceberg snapshot expiration for uniform table " +
        log"[path = ${MDC(DeltaLogKeys.PATH, log.logPath)}] tableId=" +
        log"${MDC(DeltaLogKeys.TABLE_ID, log.tableId)}]")
      val expireSnapshotHelper = icebergTxn.getExpireSnapshotHelper()
      expireSnapshotHelper.commit()
    }

    icebergTxn.commit()
    validateIcebergCommit(snapshotToConvert, cleanedCatalogTable)
    Some(snapshotToConvert.version, snapshotToConvert.timestamp)
  }

  // This is for newly enabling uniform table to
  // start a new history line for iceberg metadata
  // so that if a uniform table is corrupted,
  // user can unset and re-enable to unblock
  private def cleanCatalogTableIfEnablingUniform(
      table: CatalogTable,
      snapshotToConvert: Snapshot,
      txnOpt: Option[DeltaTransaction]): CatalogTable = {
    val disabledIceberg = txnOpt.map(txn =>
      !UniversalFormat.icebergEnabled(txn.snapshot.metadata)
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
   * Build an iceberg TransactionHelper from the provided txn, and commit the set of changes
   * specified by the actionsToCommit.
   */
  private[delta] def runIcebergConversionForActions(
      icebergTxn: IcebergConversionTransaction,
      actionsToCommit: Seq[Action],
      dataPath: Path,
      prevSnapshotOpt: Option[Snapshot]): Unit = {
    prevSnapshotOpt match {
      case None =>
        // If we don't have a previous snapshot, that implies that the table is either being
        // created or replaced. We can assume that the actions have already been deduped, and
        // only addFiles are present.
        val appendHelper = icebergTxn.getAppendOnlyHelper()
        actionsToCommit.foreach {
          case a: AddFile => appendHelper.add(a)
          case _ => throw new IllegalStateException(s"Must provide only AddFiles when creating " +
            s"or replacing an Iceberg Table $dataPath.")
        }
        appendHelper.commit()

      case Some(_) =>
        // We have to go through the seq of actions twice, once to figure out the TransactionHelper
        // to use, and then again to commit the actions. This is not too expensive, since the max #
        // of actions is <= min(max # actions in delta json, ICEBERG_MAX_ACTIONS_TO_CONVERT)
        var hasAdds = false
        var hasRemoves = false
        var hasDataChange = false
        var hasCommitInfo = false
        var commitInfo: Option[CommitInfo] = None
        breakable {
          for (action <- actionsToCommit) {
            action match {
              case a: AddFile =>
                hasAdds = true
                if (a.dataChange) hasDataChange = true
              case r: RemoveFile =>
                hasRemoves = true
                if (r.dataChange) hasDataChange = true
              case ci: CommitInfo =>
                commitInfo = Some(ci)
                hasCommitInfo = true
              case _ => // Do nothing
            }
            if (hasAdds && hasRemoves && hasDataChange && hasCommitInfo) break // Short-circuit
          }
        }

        // We want to know whether all actions in the commit are contained in this `actionsToCommit`
        // group. If yes, then we can safely determine whether the operation is a rewrite, delete,
        // append, overwrite, etc. If not, then we can't make any assumptions since we have
        // incomplete information, and we default to a rewrite.
        val allDeltaActionsCaptured = hasCommitInfo && actionsToCommit.size <
          spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_ACTIONS_TO_CONVERT)

        val addsAndRemoves = actionsToCommit
          .map(_.wrap)
          .filter(sa => sa.remove != null || sa.add != null)

        if (hasAdds && hasRemoves && !hasDataChange && allDeltaActionsCaptured) {
          val rewriteHelper = icebergTxn.getRewriteHelper()
          val split = addsAndRemoves.partition(_.add == null)
          rewriteHelper.rewrite(removes = split._1.map(_.remove), adds = split._2.map(_.add))
          rewriteHelper.commit()
        } else if ((hasAdds && hasRemoves) || !allDeltaActionsCaptured) {
          val overwriteHelper = icebergTxn.getOverwriteHelper()
          addsAndRemoves.foreach { action =>
            if (action.add != null) {
              overwriteHelper.add(action.add)
            } else {
              overwriteHelper.remove(action.remove)
            }
          }
          overwriteHelper.commit()
        } else if (hasAdds) {
          if (!hasRemoves && !hasDataChange && allDeltaActionsCaptured) {
            logInfo(log"Skip Iceberg conversion for commit that only has AddFiles " +
              log"without any RemoveFiles or data change. CommitInfo: " +
              log"${MDC(DeltaLogKeys.DELTA_COMMIT_INFO, commitInfo)}")
          } else {
            val appendHelper = icebergTxn.getAppendOnlyHelper()
              addsAndRemoves.foreach(action => appendHelper.add(action.add))
              appendHelper.commit()
          }
        } else if (hasRemoves) {
          val removeHelper = icebergTxn.getRemoveOnlyHelper()
          addsAndRemoves.foreach(action => removeHelper.remove(action.remove))
          removeHelper.commit()
        }
    }
  }

  private def existsOptimize(actions: Seq[Action]): Boolean = {
    actions.exists { action =>
      val sa = action.wrap
      sa.commitInfo != null && sa.commitInfo.operation == OPTIMIZE_OPERATION_NAME
    }
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
