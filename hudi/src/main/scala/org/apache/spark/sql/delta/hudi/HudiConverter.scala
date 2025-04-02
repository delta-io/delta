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

package org.apache.spark.sql.delta.hudi

import java.io.{IOException, UncheckedIOException}
import java.util.concurrent.atomic.AtomicReference
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.OptimisticTransactionImpl
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.UniversalFormatConverter
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.hooks.HudiConverterHook
import org.apache.spark.sql.delta.hudi.HudiTransactionUtils._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieReplaceCommitMetadata}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

object HudiConverter {
  /**
   * Property to be set in translated Hudi commit metadata.
   * Indicates the delta commit version # that it corresponds to.
   */
  val DELTA_VERSION_PROPERTY = "delta-version"

  /**
   * Property to be set in translated Hudi commit metadata.
   * Indicates the timestamp (milliseconds) of the delta commit that it corresponds to.
   */
  val DELTA_TIMESTAMP_PROPERTY = "delta-timestamp"
}

/**
 * This class manages the transformation of delta snapshots into their Hudi equivalent.
 */
class HudiConverter(spark: SparkSession)
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
   * Enqueue the specified snapshot to be converted to Hudi. This will start an async
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
    if (!UniversalFormat.hudiEnabled(snapshotToConvert.metadata)) {
      return
    }
    val log = snapshotToConvert.deltaLog
    // Replace any previously queued snapshot
    val previouslyQueued = standbyConversion.getAndSet((snapshotToConvert, txn))
    asyncThreadLock.synchronized {
      if (!asyncConverterThreadActive) {
        val threadName = HudiConverterHook.ASYNC_HUDI_CONVERTER_THREAD_NAME +
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
                        log"tableId=${MDC(DeltaLogKeys.TABLE_ID, log.tableId)}, " +
                        log"version=${MDC(DeltaLogKeys.VERSION, snapshotVal.version)}] into Hudi")
                      convertSnapshot(snapshotVal, prevTxn)
                    } catch {
                      case NonFatal(e) =>
                        logWarning(log"Error when writing Hudi metadata asynchronously", e)
                        recordDeltaEvent(
                          log,
                          "delta.hudi.conversion.async.error",
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

          // Get a snapshot to convert from the hudiQueue. Sets the queue to null after.
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
      recordDeltaEvent(
        snapshotToConvert.deltaLog,
        "delta.hudi.conversion.async.backlog",
        data = Map(
          "newVersion" -> snapshotToConvert.version,
          "replacedVersion" -> previouslyQueued._1.version)
      )
    }
  }

  /**
   * Convert the specified snapshot into Hudi for the given catalogTable
   * @param snapshotToConvert the snapshot that needs to be converted to Hudi
   * @param catalogTable the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, catalogTable: CatalogTable): Option[(Long, Long)] = {
    if (!UniversalFormat.hudiEnabled(snapshotToConvert.metadata)) {
      return None
    }
    convertSnapshot(snapshotToConvert, None, Some(catalogTable))
  }

  /**
   * Convert the specified snapshot into Hudi when performing an OptimisticTransaction
   * on a delta table.
   * @param snapshotToConvert the snapshot that needs to be converted to Hudi
   * @param txn               the transaction that triggers the conversion. It must
   *                          contain the catalogTable this conversion targets.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot, txn: DeltaTransaction): Option[(Long, Long)] = {
    if (!UniversalFormat.hudiEnabled(snapshotToConvert.metadata)) {
      return None
    }
    convertSnapshot(snapshotToConvert, Some(txn), txn.catalogTable)
  }

  /**
   * Convert the specified snapshot into Hudi. NOTE: This operation is blocking. Call
   * enqueueSnapshotForConversion to run the operation asynchronously.
   * @param snapshotToConvert the snapshot that needs to be converted to Hudi
   * @param txnOpt the OptimisticTransaction that created snapshotToConvert.
   *            Used as a hint to avoid recomputing old metadata.
   * @param catalogTable the catalogTable this conversion targets
   * @return Converted Delta version and commit timestamp
   */
  private def convertSnapshot(
      snapshotToConvert: Snapshot,
      txnOpt: Option[DeltaTransaction],
      catalogTable: Option[CatalogTable]): Option[(Long, Long)] =
      recordFrameProfile("Delta", "HudiConverter.convertSnapshot") {
    val log = snapshotToConvert.deltaLog
    val metaClient = loadTableMetaClient(
      snapshotToConvert.deltaLog.dataPath.toString,
      catalogTable.flatMap(ct => Option(ct.identifier.table)),
      snapshotToConvert.metadata.partitionColumns,
      new HadoopStorageConfiguration(log.newDeltaHadoopConf()))
    val lastDeltaVersionConverted: Option[Long] = loadLastDeltaVersionConverted(metaClient)
    val maxCommitsToConvert =
      spark.sessionState.conf.getConf(DeltaSQLConf.HUDI_MAX_COMMITS_TO_CONVERT)

    // Nth to convert
    if (lastDeltaVersionConverted.exists(_ == snapshotToConvert.version)) {
      return None
    }

    // Get the most recently converted delta snapshot, if applicable
    val prevConvertedSnapshotOpt = (lastDeltaVersionConverted, txnOpt) match {
      case (Some(version), Some(txn)) if version == txn.snapshot.version =>
        Some(txn.snapshot)
      // Check how long it has been since we last converted to Hudi. If outside the threshold,
      // fall back to state reconstruction to get the actions, to protect driver from OOMing.
      case (Some(version), _) if snapshotToConvert.version - version <= maxCommitsToConvert =>
        try {
          // TODO: We can optimize this by providing a checkpointHint to getSnapshotAt. Check if
          //  txn.snapshot.version < version. If true, use txn.snapshot's checkpoint as a hint.
          Some(log.getSnapshotAt(version, catalogTableOpt = catalogTable))
        } catch {
          // If we can't load the file since the last time Hudi was converted, it's likely that
          // the commit file expired. Treat this like a new Hudi table conversion.
          case _: DeltaFileNotFoundException => None
        }
      case (_, _) => None
    }

    val hudiTxn = new HudiConversionTransaction(log.newDeltaHadoopConf(),
      snapshotToConvert, metaClient, lastDeltaVersionConverted)

    // Write out the actions taken since the last conversion (or since table creation).
    // This is done in batches, with each batch corresponding either to one delta file,
    // or to the specified batch size.
    val actionBatchSize =
      spark.sessionState.conf.getConf(DeltaSQLConf.HUDI_MAX_COMMITS_TO_CONVERT)
    prevConvertedSnapshotOpt match {
      case Some(prevSnapshot) =>
        // Read the actions directly from the delta json files.
        // TODO: Run this as a spark job on executors
        val deltaFiles = DeltaFileProviderUtils.getDeltaFilesInVersionRange(
          spark = spark,
          deltaLog = log,
          startVersion = prevSnapshot.version + 1,
          endVersion = snapshotToConvert.version,
        )

        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.hudi.conversion.deltaCommitRange",
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
              runHudiConversionForActions(
                hudiTxn,
                actionStrs.map(Action.fromJson))
            }
          } finally {
            actionsIter.close()
          }
        }

      // If we don't have a snapshot of the last converted version, get all the table addFiles
      // (via state reconstruction).
      case None =>
        val actionsToConvert = snapshotToConvert.allFiles.toLocalIterator().asScala

        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.hudi.conversion.batch",
          data = Map(
            "version" -> snapshotToConvert.version,
            "numDeltaFiles" -> snapshotToConvert.numOfFiles
          )
        )

        actionsToConvert.grouped(actionBatchSize)
          .foreach { actions =>
            runHudiConversionForActions(hudiTxn, actions)
          }
    }
    hudiTxn.commit()
    Some(snapshotToConvert.version, snapshotToConvert.timestamp)
  }

  def loadLastDeltaVersionConverted(snapshot: Snapshot, table: CatalogTable): Option[Long] = {
    val metaClient = loadTableMetaClient(snapshot.deltaLog.dataPath.toString,
      Option.apply(table.identifier.table), snapshot.metadata.partitionColumns,
      new HadoopStorageConfiguration(snapshot.deltaLog.newDeltaHadoopConf()))
    loadLastDeltaVersionConverted(metaClient)
  }

  private def loadLastDeltaVersionConverted(metaClient: HoodieTableMetaClient): Option[Long] = {
    val lastCompletedCommit = metaClient.getCommitsTimeline.filterCompletedInstants.lastInstant
    if (!lastCompletedCommit.isPresent) {
      return None
    }
    val extraMetadata = parseCommitExtraMetadata(lastCompletedCommit.get(), metaClient)
    extraMetadata.get(HudiConverter.DELTA_VERSION_PROPERTY).map(_.toLong)
  }

  private def parseCommitExtraMetadata(instant: HoodieInstant,
                                       metaClient: HoodieTableMetaClient): Map[String, String] = {
    try {
      if (instant.getAction == HoodieTimeline.REPLACE_COMMIT_ACTION) {
        HoodieReplaceCommitMetadata.fromBytes(
          metaClient.getActiveTimeline.getInstantDetails(instant).get,
          classOf[HoodieReplaceCommitMetadata]).getExtraMetadata.asScala.toMap
      } else {
        HoodieCommitMetadata.fromBytes(
          metaClient.getActiveTimeline.getInstantDetails(instant).get,
          classOf[HoodieCommitMetadata]).getExtraMetadata.asScala.toMap
      }
    } catch {
      case ex: IOException =>
        throw new UncheckedIOException("Unable to read Hudi commit metadata", ex)
    }
  }

  private[delta] def runHudiConversionForActions(
      hudiTxn: HudiConversionTransaction,
      actionsToCommit: Seq[Action]): Unit = {
    hudiTxn.setCommitFileUpdates(actionsToCommit)
  }
}
