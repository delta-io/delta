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

import org.apache.spark.sql.delta.{DeltaFileNotFoundException, DeltaFileProviderUtils, OptimisticTransactionImpl, Snapshot, UniversalFormatConverter}
import org.apache.spark.sql.delta.actions.{Action, AddFile, CommitInfo, RemoveFile}
import org.apache.spark.sql.delta.hooks.IcebergConverterHook
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path
import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables

import org.apache.spark.sql.SparkSession

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
    new AtomicReference[(Snapshot, Option[OptimisticTransactionImpl])]()
  protected val standbyConversion =
    new AtomicReference[(Snapshot, Option[OptimisticTransactionImpl])]()

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
      txn: Option[OptimisticTransactionImpl]): Unit = {
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
                      logInfo(s"Converting Delta table [path=${log.logPath}, " +
                        s"tableId=${log.tableId}, version=${snapshotVal.version}] into Iceberg")
                      convertSnapshot(snapshotVal, prevTxn)
                    } catch {
                      case NonFatal(e) =>
                        logWarning(s"Error when writing Iceberg metadata asynchronously", e)
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
          private def getNextSnapshot: (Snapshot, Option[OptimisticTransactionImpl]) =
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
   * Convert the specified snapshot into Iceberg. NOTE: This operation is blocking. Call
   * enqueueSnapshotForConversion to run the operation asynchronously.
   * @param snapshotToConvert the snapshot that needs to be converted to Iceberg
   * @param txnOpt the OptimisticTransaction that created snapshotToConvert.
   *            Used as a hint to avoid recomputing old metadata.
   * @return Converted Delta version and commit timestamp
   */
  override def convertSnapshot(
      snapshotToConvert: Snapshot,
      txnOpt: Option[OptimisticTransactionImpl]): Option[(Long, Long)] =
      recordFrameProfile("Delta", "IcebergConverter.convertSnapshot") {
    val log = snapshotToConvert.deltaLog
    val lastDeltaVersionConverted: Option[Long] =
      loadLastDeltaVersionConverted(snapshotToConvert)
    val maxCommitsToConvert =
      spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_COMMITS_TO_CONVERT)

    // Nth to convert
    if (lastDeltaVersionConverted.exists(_ == snapshotToConvert.version)) {
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
          Some(log.getSnapshotAt(version))
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
      log.newDeltaHadoopConf(), snapshotToConvert, tableOp, lastDeltaVersionConverted)

    // Write out the actions taken since the last conversion (or since table creation).
    // This is done in batches, with each batch corresponding either to one delta file,
    // or to the specified batch size.
    val actionBatchSize =
      spark.sessionState.conf.getConf(DeltaSQLConf.ICEBERG_MAX_ACTIONS_TO_CONVERT)
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
              runIcebergConversionForActions(
                icebergTxn,
                actionStrs.map(Action.fromJson),
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
        val actionsToConvert = snapshotToConvert.allFiles.toLocalIterator().asScala

        recordDeltaEvent(
          snapshotToConvert.deltaLog,
          "delta.iceberg.conversion.batch",
          data = Map(
            "version" -> snapshotToConvert.version,
            "numDeltaFiles" -> snapshotToConvert.numOfFiles
          )
        )

        actionsToConvert.grouped(actionBatchSize)
          .foreach { actions =>
            runIcebergConversionForActions(icebergTxn, actions, log.dataPath, None)
          }
    }
    icebergTxn.commit()
    Some(snapshotToConvert.version, snapshotToConvert.timestamp)
  }

  override def loadLastDeltaVersionConverted(snapshot: Snapshot): Option[Long] =
    recordFrameProfile("Delta", "IcebergConverter.loadLastDeltaVersionConverted") {
      val deltaLog = snapshot.deltaLog
      val hadoopTables = new HadoopTables(deltaLog.newDeltaHadoopConf())
      if (hadoopTables.exists(deltaLog.dataPath.toString)) {
        hadoopTables
          .load(deltaLog.dataPath.toString)
          .properties()
          .asScala
          .get(IcebergConverter.DELTA_VERSION_PROPERTY)
          .map(_.toLong)
      } else None
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
        breakable {
          for (action <- actionsToCommit) {
            action match {
              case a: AddFile =>
                hasAdds = true
                if (a.dataChange) hasDataChange = true
              case r: RemoveFile =>
                hasRemoves = true
                if (r.dataChange) hasDataChange = true
              case _: CommitInfo => hasCommitInfo = true
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
          val appendHelper = icebergTxn.getAppendOnlyHelper()
          addsAndRemoves.foreach(action => appendHelper.add(action.add))
          appendHelper.commit()
        } else if (hasRemoves) {
          val removeHelper = icebergTxn.getRemoveOnlyHelper()
          addsAndRemoves.foreach(action => removeHelper.remove(action.remove))
          removeHelper.commit()
        }
    }
  }
}
