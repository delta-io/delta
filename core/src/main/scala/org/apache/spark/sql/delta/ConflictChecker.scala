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

package org.apache.spark.sql.delta

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionSet, Or}
import org.apache.spark.sql.types.StructType

/**
 * A class representing different attributes of current transaction needed for conflict detection.
 *
 * @param readPredicates predicates by which files have been queried by the transaction
 * @param readFiles files that have been seen by the transaction
 * @param readWholeTable whether the whole table was read during the transaction
 * @param readAppIds appIds that have been seen by the transaction
 * @param metadata table metadata for the transaction
 * @param actions delta log actions that the transaction wants to commit
 * @param readSnapshot read [[Snapshot]] used for the transaction
 * @param commitInfo [[CommitInfo]] for the commit
 */
private[delta] case class CurrentTransactionInfo(
    val txnId: String,
    val readPredicates: Seq[DeltaTableReadPredicate],
    val readFiles: Set[AddFile],
    val readWholeTable: Boolean,
    val readAppIds: Set[String],
    val metadata: Metadata,
    val protocol: Protocol,
    val actions: Seq[Action],
    val readSnapshot: Snapshot,
    val commitInfo: Option[CommitInfo],
    val readRowIdHighWatermark: RowIdHighWaterMark,
    val domainMetadatas: Seq[DomainMetadata]) {

  /**
   * Final actions to commit - including the [[CommitInfo]] which should always come first so we can
   * extract it easily from a commit without having to parse an arbitrarily large file.
   *
   * TODO: We might want to cluster all non-file actions at the front, for similar reasons.
   */
  lazy val finalActionsToCommit: Seq[Action] = commitInfo ++: actions

  /** Whether this transaction wants to make any [[Metadata]] update */
  lazy val metadataChanged: Boolean = actions.exists {
    case _: Metadata => true
    case _ => false
  }


  /**
   * Partition schema corresponding to the read snapshot for this transaction.
   * NOTE: In conflict detection, we should be careful around whether we want to use the new schema
   * which this txn wants to update OR the old schema from the read snapshot.
   * e.g. the ConcurrentAppend check makes sure that no new files have been added concurrently
   * that this transaction should have read. So this should use the read snapshot partition schema
   * and not the new partition schema which this txn is introducing. Using the new schema can cause
   * issues.
   */
  val partitionSchemaAtReadTime: StructType = readSnapshot.metadata.partitionSchema

  def isConflict(winningTxn: SetTransaction): Boolean = readAppIds.contains(winningTxn.appId)
}

/**
 * Summary of the Winning commit against which we want to check the conflict
 * @param actions - delta log actions committed by the winning commit
 * @param commitVersion - winning commit version
 */
private[delta] class WinningCommitSummary(val actions: Seq[Action], val commitVersion: Long) {

  val metadataUpdates: Seq[Metadata] = actions.collect { case a: Metadata => a }
  val appLevelTransactions: Seq[SetTransaction] = actions.collect { case a: SetTransaction => a }
  val protocol: Seq[Protocol] = actions.collect { case a: Protocol => a }
  val commitInfo: Option[CommitInfo] = actions.collectFirst { case a: CommitInfo => a }.map(
    ci => ci.copy(version = Some(commitVersion)))
  val removedFiles: Seq[RemoveFile] = actions.collect { case a: RemoveFile => a }
  val addedFiles: Seq[AddFile] = actions.collect { case a: AddFile => a }
  val isBlindAppendOption: Option[Boolean] = commitInfo.flatMap(_.isBlindAppend)
  val blindAppendAddedFiles: Seq[AddFile] = if (isBlindAppendOption.getOrElse(false)) {
    addedFiles
  } else {
    Seq()
  }
  val changedDataAddedFiles: Seq[AddFile] = if (isBlindAppendOption.getOrElse(false)) {
    Seq()
  } else {
    addedFiles
  }
  val onlyAddFiles: Boolean = actions.collect { case f: FileAction => f }
    .forall(_.isInstanceOf[AddFile])

}

private[delta] class ConflictChecker(
    spark: SparkSession,
    initialCurrentTransactionInfo: CurrentTransactionInfo,
    winningCommitVersion: Long,
    isolationLevel: IsolationLevel) extends DeltaLogging {

  protected val startTimeMs = System.currentTimeMillis()
  protected val timingStats = mutable.HashMap[String, Long]()
  protected val deltaLog = initialCurrentTransactionInfo.readSnapshot.deltaLog

  protected var currentTransactionInfo: CurrentTransactionInfo = initialCurrentTransactionInfo

  protected val winningCommitSummary: WinningCommitSummary = createWinningCommitSummary()

  /**
   * This function checks conflict of the `initialCurrentTransactionInfo` against the
   * `winningCommitVersion` and returns an updated [[CurrentTransactionInfo]] that represents
   * the transaction as if it had started while reading the `winningCommitVersion`.
   */
  def checkConflicts(): CurrentTransactionInfo = {
    checkProtocolCompatibility()
    checkNoMetadataUpdates()
    checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn()
    checkForDeletedFilesAgainstCurrentTxnReadFiles()
    checkForDeletedFilesAgainstCurrentTxnDeletedFiles()
    checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn()
    reassignOverlappingRowIds()
    checkIfDomainMetadataConflict()
    logMetrics()
    currentTransactionInfo
  }

  /**
   * Initializes [[WinningCommitSummary]] for the already committed
   * transaction (winning transaction).
   */
  protected def createWinningCommitSummary(): WinningCommitSummary = {
    recordTime("initialize-old-commit") {
      val winningCommitActions = deltaLog.store.read(
        FileNames.deltaFile(deltaLog.logPath, winningCommitVersion),
        deltaLog.newDeltaHadoopConf()
      ).map(Action.fromJson)
      new WinningCommitSummary(winningCommitActions, winningCommitVersion)
    }
  }

  /**
   * Asserts that the client is up to date with the protocol and is allowed to read and write
   * against the protocol set by the committed transaction.
   */
  protected def checkProtocolCompatibility(): Unit = {
    if (winningCommitSummary.protocol.nonEmpty) {
      winningCommitSummary.protocol.foreach { p =>
        deltaLog.protocolRead(p)
        deltaLog.protocolWrite(p)
      }
      if (currentTransactionInfo.actions.exists(_.isInstanceOf[Protocol])) {
        throw DeltaErrors.protocolChangedException(winningCommitSummary.commitInfo)
      }
    }
  }

  /**
   * Check if the committed transaction has changed metadata.
   */
  protected def checkNoMetadataUpdates(): Unit = {
    // Fail if the metadata is different than what the txn read.
    if (winningCommitSummary.metadataUpdates.nonEmpty) {
      throw DeltaErrors.metadataChangedException(winningCommitSummary.commitInfo)
    }
  }

  /**
   * Filters the [[files]] list with the partition predicates of the current transaction
   * and returns the first file that is matching.
   */
  protected def getFirstFileMatchingPartitionPredicates(files: Seq[AddFile]): Option[AddFile] = {
    import org.apache.spark.sql.delta.implicits._
    val filesDf = files.toDF(spark)

    // we need to canonicalize the partition predicates per each group of rewrites vs. nonRewrites
    val canonicalPredicates = currentTransactionInfo.readPredicates
      .partition(_.shouldRewriteFilter) match {
        case (rewrites, nonRewrites) =>
          val canonicalRewrites =
            ExpressionSet(rewrites.map(_.partitionPredicate)).map { e =>
              DeltaTableReadPredicate(partitionPredicates = Seq(e))
            }
          val canonicalNonRewrites =
            ExpressionSet(nonRewrites.map(_.partitionPredicate)).map { e =>
              DeltaTableReadPredicate(partitionPredicates = Seq(e), shouldRewriteFilter = false)
            }
          canonicalRewrites ++ canonicalNonRewrites
      }

    val filesMatchingPartitionPredicates = canonicalPredicates.iterator
      .flatMap { readPredicate =>
        DeltaLog.filterFileList(
          partitionSchema = currentTransactionInfo.partitionSchemaAtReadTime,
          files = filesDf,
          partitionFilters = readPredicate.partitionPredicates,
          shouldRewritePartitionFilters = readPredicate.shouldRewriteFilter
        ).as[AddFile].head(1).headOption
      }.take(1).toArray

    filesMatchingPartitionPredicates.headOption
  }

  /**
   * Check if the new files added by the already committed transactions should have been read by
   * the current transaction.
   */
  protected def checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn(): Unit = {
    recordTime("checked-appends") {
      // Fail if new files have been added that the txn should have read.
      val addedFilesToCheckForConflicts = isolationLevel match {
        case WriteSerializable if !currentTransactionInfo.metadataChanged =>
          winningCommitSummary.changedDataAddedFiles // don't conflict with blind appends
        case Serializable | WriteSerializable =>
          winningCommitSummary.changedDataAddedFiles ++ winningCommitSummary.blindAppendAddedFiles
        case SnapshotIsolation =>
          Seq.empty
      }

      val fileMatchingPartitionReadPredicates =
        getFirstFileMatchingPartitionPredicates(addedFilesToCheckForConflicts)

      if (fileMatchingPartitionReadPredicates.nonEmpty) {
        val isWriteSerializable = isolationLevel == WriteSerializable

        val retryMsg = if (isWriteSerializable && winningCommitSummary.onlyAddFiles &&
          winningCommitSummary.isBlindAppendOption.isEmpty) {
          // The transaction was made by an older version which did not set `isBlindAppend` flag
          // So even if it looks like an append, we don't know for sure if it was a blind append
          // or not. So we suggest them to upgrade all there workloads to latest version.
          Some(
            "Upgrading all your concurrent writers to use the latest Delta Lake may " +
              "avoid this error. Please upgrade and then retry this operation again.")
        } else None
        throw DeltaErrors.concurrentAppendException(
          winningCommitSummary.commitInfo,
          getPrettyPartitionMessage(fileMatchingPartitionReadPredicates.get.partitionValues),
          retryMsg)
      }
    }
  }

  /**
   * Check if [[RemoveFile]] actions added by already committed transactions conflicts with files
   * read by the current transaction.
   */
  protected def checkForDeletedFilesAgainstCurrentTxnReadFiles(): Unit = {
    recordTime("checked-deletes") {
      // Fail if files have been deleted that the txn read.
      val readFilePaths = currentTransactionInfo.readFiles.map(
        f => f.path -> f.partitionValues).toMap
      val deleteReadOverlap = winningCommitSummary.removedFiles
        .find(r => readFilePaths.contains(r.path))
      if (deleteReadOverlap.nonEmpty) {
        val filePath = deleteReadOverlap.get.path
        val partition = getPrettyPartitionMessage(readFilePaths(filePath))
        throw DeltaErrors.concurrentDeleteReadException(
          winningCommitSummary.commitInfo, s"$filePath in $partition")
      }
      if (winningCommitSummary.removedFiles.nonEmpty && currentTransactionInfo.readWholeTable) {
        val filePath = winningCommitSummary.removedFiles.head.path
        throw DeltaErrors.concurrentDeleteReadException(
          winningCommitSummary.commitInfo, s"$filePath")
      }
    }
  }

  /**
   * Check if [[RemoveFile]] actions added by already committed transactions conflicts with
   * [[RemoveFile]] actions this transaction is trying to add.
   */
  protected def checkForDeletedFilesAgainstCurrentTxnDeletedFiles(): Unit = {
    recordTime("checked-2x-deletes") {
      // Fail if a file is deleted twice.
      val txnDeletes = currentTransactionInfo.actions
        .collect { case r: RemoveFile => r }
        .map(_.path).toSet
      val deleteOverlap = winningCommitSummary.removedFiles.map(_.path).toSet intersect txnDeletes
      if (deleteOverlap.nonEmpty) {
        throw DeltaErrors.concurrentDeleteDeleteException(
          winningCommitSummary.commitInfo, deleteOverlap.head)
      }
    }
  }

  /**
   * Checks [[DomainMetadata]] to capture whether the current transaction conflicts with the
   * winning transaction at any domain.
   *     1. Accept the current transaction if its set of metadata domains do not overlap with the
   *        winning transaction's set of metadata domains.
   *     2. Otherwise, fail the current transaction unless each conflicting domain is associated
   *        with a table feature that defines a domain-specific way of resolving the conflict.
   */
  private def checkIfDomainMetadataConflict(): Unit = {
    if (!DomainMetadataUtils.domainMetadataSupported(currentTransactionInfo.protocol)) {
      return
    }
    // Note [[currentTransactionInfo.domainMetadatas]] can't have duplicate domains otherwise
    // it would have failed [[validateDuplicateDomainMetadata]] at beginning of the transaction's
    // validation.
    val currentDomainMetadatas =
      currentTransactionInfo.domainMetadatas.map(action => action.domain -> action)
    val winningDomainMetadataMap =
      DomainMetadataUtils.extractDomainMetadatasMap(winningCommitSummary.actions)

    /**
     * Any new well-known domains that need custom conflict resolution need to add new cases in
     * below case match clause. E.g.
     * case ("delta.example.monotonicCounter", domain, Some(conflictDomain)) =>
     *   domain.copy(value = Math.max(domain.value, conflictingDomain.value))
     */
    val mergedDomainMetadatas = currentDomainMetadatas
      .map { case (name, domain) =>
        (name, domain, winningDomainMetadataMap.get(name)) match {
          // No-conflict case.
          case (_, domain, None) => domain
          case (name, _, Some(_)) =>
            // Any conflict not specifically handled by a previous case must fail the transaction.
            throw new io.delta.exceptions.ConcurrentTransactionException(
              s"A conflicting metadata domain $name is added.")
        }
      }

    // Update the [[DomainMetadata]] to the merged one.
    val updatedActions = currentTransactionInfo.actions.map {
      case domainMetadata: DomainMetadata =>
        // Note the find will always succeed, because [[mergedDomainMetadatas]] already contains
        // every domain from the transaction.
        mergedDomainMetadatas.find(_.domain == domainMetadata.domain).getOrElse(domainMetadata)
      case other => other
    }
    currentTransactionInfo = currentTransactionInfo.copy(
      domainMetadatas = mergedDomainMetadatas,
      actions = updatedActions)
  }

  /**
   * Checks if the winning transaction corresponds to some AppId on which current transaction
   * also depends.
   */
  protected def checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn(): Unit = {
    // Fail if the appIds seen by the current transaction has been updated by the winning
    // transaction i.e. the winning transaction have [[SetTransaction]] corresponding to
    // some appId on which current transaction depends on. Example - This can happen when
    // multiple instances of the same streaming query are running at the same time.
    if (winningCommitSummary.appLevelTransactions.exists(currentTransactionInfo.isConflict(_))) {
      throw DeltaErrors.concurrentTransactionException(winningCommitSummary.commitInfo)
    }
  }

  /**
   * Checks whether the Row IDs assigned by the current transaction overlap with the Row IDs
   * assigned by the winning transaction. I.e. this function checks whether both the winning and the
   * current transaction assigned new Row IDs. If this the case, then this check assigns new Row IDs
   * to the new files added by the current transaction so that they no longer overlap.
   */
  private def reassignOverlappingRowIds(): Unit = {
    // The current transaction should only assign Row Ids if they are supported.
    if (!RowId.isSupported(currentTransactionInfo.protocol)) return

    winningCommitSummary.actions.collectFirst {
      case RowIdHighWaterMark(winningHighWaterMark) =>
        // The winning transaction assigned conflicting Row IDs. Adjust the Row IDs assigned by the
        // current transaction as if it had read the result of the winning transaction.
        val readHighWaterMark = currentTransactionInfo.readRowIdHighWatermark.highWaterMark
        assert(winningHighWaterMark >= readHighWaterMark)
        val watermarkDiff = winningHighWaterMark - readHighWaterMark

        val actionsWithReassignedRowIds = currentTransactionInfo.actions.map {
          // We should only update the row IDs that were assigned by this transaction, and not the
          // row IDs that were assigned by an earlier transaction and merely copied over to a new
          // AddFile as part of this transaction. I.e., we should only update the base row IDs
          // that are larger than the read high watermark.
          case a: AddFile if a.baseRowId.exists(_ > readHighWaterMark) =>
            val newBaseRowId = a.baseRowId.map(_ + watermarkDiff)
            a.copy(baseRowId = newBaseRowId)

          case waterMark @ RowIdHighWaterMark(v) =>
            waterMark.copy(highWaterMark = v + watermarkDiff)

          case a => a
        }
      currentTransactionInfo = currentTransactionInfo.copy(
        actions = actionsWithReassignedRowIds,
        readRowIdHighWatermark = RowIdHighWaterMark(winningHighWaterMark))
    }
  }

  /** A helper function for pretty printing a specific partition directory. */
  protected def getPrettyPartitionMessage(partitionValues: Map[String, String]): String = {
    val partitionColumns = currentTransactionInfo.partitionSchemaAtReadTime
    if (partitionColumns.isEmpty) {
      "the root of the table"
    } else {
      val partition = partitionColumns.map { field =>
        s"${field.name}=${partitionValues(DeltaColumnMapping.getPhysicalName(field))}"
      }.mkString("[", ", ", "]")
      s"partition ${partition}"
    }
  }

  protected def recordTime[T](phase: String)(f: => T): T = {
    val startTimeNs = System.nanoTime()
    val ret = f
    val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
    timingStats += phase -> timeTakenMs
    ret
  }

  protected def logMetrics(): Unit = {
    val totalTimeTakenMs = System.currentTimeMillis() - startTimeMs
    val timingStr = timingStats.keys.toSeq.sorted.map(k => s"$k=${timingStats(k)}").mkString(",")
    logInfo(s"[$logPrefix] Timing stats against $winningCommitVersion " +
      s"[$timingStr, totalTimeTakenMs: $totalTimeTakenMs]")
  }

  protected lazy val logPrefix: String = {
    def truncate(uuid: String): String = uuid.split("-").head
    s"[tableId=${truncate(initialCurrentTransactionInfo.readSnapshot.metadata.id)}," +
      s"txnId=${truncate(initialCurrentTransactionInfo.txnId)}] "
  }
}
