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

package org.apache.spark.sql.delta.util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.{CatalogManagedTableFeature, CommitStats, CommittedTransaction, CoordinatedCommitsStats, CoordinatedCommitType, DeltaConfigs, DeltaLog, IsolationLevel, Snapshot}
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, CommitInfo, DomainMetadata, Metadata, Protocol, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogManagedTableUtils, TableCommitCoordinatorClient}
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Contains helper methods for Delta transactions.
 */
trait TransactionHelper extends DeltaLogging {
  def deltaLog: DeltaLog
  def catalogTable: Option[CatalogTable]
  def snapshot: Snapshot

  /** Unique identifier for the transaction */
  def txnId: String

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction.
   */
  def metadata: Metadata

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol

  /**
   * Default [[IsolationLevel]] as set in table metadata.
   */
  private[delta] def getDefaultIsolationLevel(): IsolationLevel = {
    DeltaConfigs.ISOLATION_LEVEL.fromMetaData(metadata)
  }

  /**
   * Return the user-defined metadata for the operation.
   */
  def getUserMetadata(op: Operation): Option[String] = {
    // option wins over config if both are set
    op.userMetadata match {
      case data @ Some(_) => data
      case None => spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_USER_METADATA)
    }
  }

  /** The current spark session */
  protected def spark: SparkSession = SparkSession.active

  private[delta] lazy val readSnapshotTableCommitCoordinatorClientOpt:
      Option[TableCommitCoordinatorClient] = {
    if (snapshot.isCatalogManaged) {
      // Catalog managed table's commit coordinator is always determined by the catalog.
      CatalogManagedTableUtils.populateTableCommitCoordinatorFromCatalog(
        spark, catalogTable, snapshot)
    } else {
      // The commit-coordinator of a table shouldn't change. If it is changed by a concurrent
      // commit, then it will be detected as a conflict and the transaction will anyway fail.
      snapshot.getTableCommitCoordinatorForWrites
    }
  }

  def createCoordinatedCommitsStats(newProtocol: Option[Protocol]) : CoordinatedCommitsStats = {
    val (coordinatedCommitsType, metadataToUse) =
      readSnapshotTableCommitCoordinatorClientOpt match {
        // TODO: Capture the CM -> FS downgrade case when we start
        //       supporting downgrade for CM.
        case Some(_) if snapshot.isCatalogManaged =>                             // CM commit
          (CoordinatedCommitType.CO_COMMIT, snapshot.metadata)
        case Some(_) if metadata.coordinatedCommitsCoordinatorName.isEmpty =>  // CC -> FS
          (CoordinatedCommitType.CC_TO_FS_DOWNGRADE_COMMIT, snapshot.metadata)
        // Only the 0th commit to a table can be a FS -> CM upgrade for now.
        // Upgrading an existing FS table to CM through ALTER TABLE is not supported yet.
        case None if newProtocol.exists(_.readerAndWriterFeatureNames
            .contains(CatalogManagedTableFeature.name)) =>                       // FS -> CM
          (CoordinatedCommitType.FS_TO_CO_UPGRADE_COMMIT, metadata)
        case None if metadata.coordinatedCommitsCoordinatorName.isDefined =>   // FS -> CC
          (CoordinatedCommitType.FS_TO_CC_UPGRADE_COMMIT, metadata)
        case Some(_) =>                                                        // CC commit
          (CoordinatedCommitType.CC_COMMIT, snapshot.metadata)
        case None =>                                                           // FS commit
          (CoordinatedCommitType.FS_COMMIT, snapshot.metadata)
        // Errors out in rest of the cases.
        case _ =>
          throw new IllegalStateException(
            "Unexpected state found when trying " +
            s"to generate CoordinatedCommitsStats for table ${deltaLog.logPath}. " +
            s"$readSnapshotTableCommitCoordinatorClientOpt, " +
            s"$metadata, $snapshot, $catalogTable")
      }
    CoordinatedCommitsStats(
      coordinatedCommitsType = coordinatedCommitsType.toString,
      commitCoordinatorName = if (Set(CoordinatedCommitType.CO_COMMIT,
        CoordinatedCommitType.FS_TO_CO_UPGRADE_COMMIT).contains(coordinatedCommitsType)) {
        // The catalog for FS -> CM upgrade commit would be
        // "CATALOG_EMPTY" because `catalogTable` is not available
        // for the 0th FS commit.
        catalogTable.flatMap { ct =>
          CatalogManagedTableUtils.getCatalogName(
            spark,
            identifier = ct.identifier)
        }.getOrElse("CATALOG_MISSING")
      } else {
        metadataToUse.coordinatedCommitsCoordinatorName.getOrElse("NONE")
      },
      // For Catalog-Managed table, the coordinator conf for UC-CC is [[Map.empty]]
      // so we don't distinguish between CM/CC here.
      commitCoordinatorConf = metadataToUse.coordinatedCommitsCoordinatorConf)
  }

  /**
   * Determines if we should checkpoint the version that has just been committed.
   */
  protected def isCheckpointNeeded(
      committedVersion: Long, postCommitSnapshot: Snapshot): Boolean = {
    def checkpointInterval = deltaLog.checkpointInterval(postCommitSnapshot.metadata)
    committedVersion != 0 && committedVersion % checkpointInterval == 0
  }

  /** Runs a post-commit hook, handling any exceptions that occur. */
  protected def runPostCommitHook(
      hook: PostCommitHook,
      committedTransaction: CommittedTransaction): Unit = {
    val version = committedTransaction.committedVersion
    try {
      hook.run(spark, committedTransaction)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error when executing post-commit hook " +
          log"${MDC(DeltaLogKeys.HOOK_NAME, hook.name)} " +
          log"for commit ${MDC(DeltaLogKeys.VERSION, version)}", e)
        recordDeltaEvent(deltaLog, "delta.commit.hook.failure", data = Map(
          "hook" -> hook.name,
          "version" -> version,
          "exception" -> e.toString
        ))
        hook.handleError(spark, e, version)
    }
  }

  /**
   * Generates a timestamp which is greater than the commit timestamp
   * of the last snapshot. Note that this is only needed when the
   * feature `inCommitTimestamps` is enabled.
   */
  protected[delta] def generateInCommitTimestampForFirstCommitAttempt(
      currentTimestamp: Long): Option[Long] =
    Option.when(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(metadata)) {
      val lastCommitTimestamp = snapshot.timestamp
      math.max(currentTimestamp, lastCommitTimestamp + 1)
    }

  /**
   * Computes and emits commit stats for the current transaction.
   */
  class CommitStatsComputer {
    private var bytesNew: Long = 0L
    private var numAdd: Int = 0
    private var numOfDomainMetadatas: Int = 0
    private var numRemove: Int = 0
    private var numSetTransaction: Int = 0
    private var numCdcFiles: Int = 0
    private var cdcBytesNew: Long = 0L
    private var numAbsolutePaths = 0
    // We don't expect commits to have more than 2 billion actions
    private var numActions: Int = 0
    private val partitionsAdded = mutable.HashSet.empty[Map[String, String]]
    private var newProtocolOpt = Option.empty[Protocol]
    private var newMetadataOpt = Option.empty[Metadata]

    private var inputActionsIteratorOpt = Option.empty[Iterator[Action]]


    private def assertStateBeforeFinalization(): Unit = {
      assert(
        inputActionsIteratorOpt.isDefined,
        "addToCommitStats must be called before finalizing commit stats")
      assert(
        !inputActionsIteratorOpt.get.hasNext,
        "The actions iterator must be consumed before finalizing commit stats")
    }

    /**
     * Takes in an iterator of actions and processes them to compute commit stats.
     * The commit stats are computed as a side effect of the iterator processing
     * and are only populated after the returned iterator is fully consumed.
     * Note that this function will not consume the input iterator.
     * @param actions An iterator of actions that are being committed in this transaction.
     * @return An iterator of actions. This is will return the same actions as the
     *         input iterator, but with the commit stats computed as a side effect.
     */
    def addToCommitStats(actions: Iterator[Action]): Iterator[Action] = {
      assert(inputActionsIteratorOpt.isEmpty,
        "addToCommitStats should only be called once per transaction")
      inputActionsIteratorOpt = Some(actions)
      actions.map { action =>
        numActions += 1
        action match {
          case a: AddFile =>
            numAdd += 1
            if (a.pathAsUri.isAbsolute) numAbsolutePaths += 1
            partitionsAdded += a.partitionValues
            if (a.dataChange) bytesNew += a.size
          case r: RemoveFile =>
            numRemove += 1
          case c: AddCDCFile =>
            numCdcFiles += 1
            cdcBytesNew += c.size
          case _: SetTransaction =>
            numSetTransaction += 1
          case _: DomainMetadata =>
            numOfDomainMetadatas += 1
          case m: Metadata =>
            newMetadataOpt = Some(m)
          case p: Protocol =>
            newProtocolOpt = Some(p)
          case _ => ()
        }
        action
      }
    }

    // scalastyle:off argcount
    /**
     * Finalizes the commit stats and emits them as a Delta event.
     * This must be called after
     * 1. [[addToCommitStats]] has been called on the actions iterator AND
     * 2. after the actions iterator returned by [[addToCommitStats]]
     *  has been fully consumed.
     * @param spark The Spark session.
     * @param attemptVersion The version of the table which is being written to the log.
     * @param startVersion The version of the table which was read at the start of the transaction.
     * @param commitDurationMs The duration of the commit in milliseconds.
     * @param fsWriteDurationMs The duration of the file system write of the commit in milliseconds.
     * @param txnExecutionTimeMs The total execution time of the transaction in milliseconds.
     * @param stateReconstructionDurationMs The duration of post commit snapshot construction in
     *   milliseconds.
     * @param postCommitSnapshot The snapshot constructed after the commit.
     * @param computedNeedsCheckpoint Whether a checkpoint needs to be created after this commit.
     *  Computed in `setNeedsCheckpoint`.
     * @param isolationLevel The isolation level used for this transaction.
     * @param commitSizeBytes The total size of the commit in bytes, computed as the sum of
     *  the JSON sizes of all actions in the commit.
     * @param commitInfo The commit info for this transaction.
     * @return A HashSet containing the partitions that were added in the transaction.
     */
    def finalizeAndEmitCommitStats(
        spark: SparkSession,
        attemptVersion: Long,
        startVersion: Long,
        commitDurationMs: Long,
        fsWriteDurationMs: Long,
        txnExecutionTimeMs: Long,
        stateReconstructionDurationMs: Long,
        postCommitSnapshot: Snapshot,
        computedNeedsCheckpoint: Boolean,
        isolationLevel: IsolationLevel,
        commitInfoOpt: Option[CommitInfo],
        commitSizeBytes: Long): Unit = {
      assertStateBeforeFinalization()

      val doCollectCommitStats =
        computedNeedsCheckpoint ||
          spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_FORCE_ALL_COMMIT_STATS)
      // Stats that force an expensive snapshot state reconstruction:
      val numFilesTotal = if (doCollectCommitStats) postCommitSnapshot.numOfFiles else -1L
      val sizeInBytesTotal = if (doCollectCommitStats) postCommitSnapshot.sizeInBytes else -1L
      val commitInfoToEmit = commitInfoOpt match {
        case Some(ci) => ci.copy(readVersion = None, isolationLevel = None)
        case None => null
      }
      val stats = CommitStats(
        startVersion = startVersion,
        commitVersion = attemptVersion,
        readVersion = postCommitSnapshot.version,
        txnDurationMs = txnExecutionTimeMs,
        commitDurationMs = commitDurationMs,
        fsWriteDurationMs = fsWriteDurationMs,
        stateReconstructionDurationMs = stateReconstructionDurationMs,
        numAdd = numAdd,
        numRemove = numRemove,
        numSetTransaction = numSetTransaction,
        bytesNew = bytesNew,
        numFilesTotal = numFilesTotal,
        sizeInBytesTotal = sizeInBytesTotal,
        numCdcFiles = numCdcFiles,
        cdcBytesNew = cdcBytesNew,
        protocol = postCommitSnapshot.protocol,
        commitSizeBytes = commitSizeBytes,
        checkpointSizeBytes = postCommitSnapshot.checkpointSizeInBytes(),
        totalCommitsSizeSinceLastCheckpoint = postCommitSnapshot.deltaFileSizeInBytes(),
        checkpointAttempt = computedNeedsCheckpoint,
        info = commitInfoToEmit,
        newMetadata = newMetadataOpt,
        numAbsolutePathsInAdd = numAbsolutePaths,
        numDistinctPartitionsInAdd = partitionsAdded.size,
        numPartitionColumnsInTable = postCommitSnapshot.metadata.partitionColumns.size,
        isolationLevel = isolationLevel.toString,
        coordinatedCommitsInfo = createCoordinatedCommitsStats(newProtocolOpt),
        numOfDomainMetadatas = numOfDomainMetadatas,
        txnId = Some(txnId))
      recordDeltaEvent(deltaLog, DeltaLogging.DELTA_COMMIT_STATS_OPTYPE, data = stats)
    }

    /**
     * Returns the partitions that were added in this transaction.
     */
    def getPartitionsAddedByTransaction: mutable.HashSet[Map[String, String]] = {
      assertStateBeforeFinalization()
      partitionsAdded
    }

    /**
     * Returns the number of actions that were added in this transaction.
     */
    def getNumActions: Int = {
      assertStateBeforeFinalization()
      numActions
    }
  }
  // scalastyle:on argcount
}
