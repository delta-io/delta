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
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionSet}

/**
 * A class representing different attributes of current transaction needed for conflict detection.
 *
 * @param readPredicates partition predicates by which files have been queried by the transaction
 * @param readFiles files that have been seen by the transaction
 * @param readWholeTable whether the whole table was read during the transaction
 * @param readAppIds appIds that have been seen by the transaction
 * @param metadata table metadata for the transaction
 * @param actions delta log actions that the transaction wants to commit
 * @param readSnapshot read [[Snapshot]] used for the transaction
 * @param commitInfo [[CommitInfo]] for the commit
 */
private[delta] class CurrentTransactionInfo(
    val readPredicates: Seq[Expression],
    val readFiles: Set[AddFile],
    val readWholeTable: Boolean,
    val readAppIds: Set[String],
    val metadata: Metadata,
    val actions: Seq[Action],
    val readSnapshot: Snapshot,
    val commitInfo: Option[CommitInfo]) {

  /** Final actions to commit - including the [[CommitInfo]] */
  lazy val finalActionsToCommit: Seq[Action] = actions ++ commitInfo
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
    isolationLevel: IsolationLevel,
    logPrefixStr: String) extends DeltaLogging {

  protected val timingStats = mutable.HashMap[String, Long]()
  protected val deltaLog = initialCurrentTransactionInfo.readSnapshot.deltaLog

  def currentTransactionInfo: CurrentTransactionInfo = initialCurrentTransactionInfo
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
    reportMetrics()
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
      currentTransactionInfo.actions.foreach {
        case Protocol(_, _) =>
          throw DeltaErrors.protocolChangedException(winningCommitSummary.commitInfo)
        case _ =>
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
   * Check if the new files added by the already committed transactions should have been read by
   * the current transaction.
   */
  protected def checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn(): Unit = {
    recordTime("checked-appends") {
      // Fail if new files have been added that the txn should have read.
      val addedFilesToCheckForConflicts = isolationLevel match {
        case Serializable =>
          winningCommitSummary.changedDataAddedFiles ++ winningCommitSummary.blindAppendAddedFiles
        case WriteSerializable =>
          winningCommitSummary.changedDataAddedFiles // don't conflict with blind appends
        case SnapshotIsolation =>
          Seq.empty
      }

      import spark.implicits._
      val predicatesMatchingAddedFiles = ExpressionSet(
        currentTransactionInfo.readPredicates).iterator.flatMap { p =>
        val conflictingFile = DeltaLog.filterFileList(
          currentTransactionInfo.metadata.partitionSchema,
          addedFilesToCheckForConflicts.toDF(), p :: Nil).as[AddFile].take(1)

        conflictingFile.headOption.map(f => getPrettyPartitionMessage(f.partitionValues))
      }.take(1).toArray

      if (predicatesMatchingAddedFiles.nonEmpty) {
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
          predicatesMatchingAddedFiles.head,
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
   * Checks if the winning transaction corresponds to some AppId on which current transaction
   * also depends.
   */
  protected def checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn(): Unit = {
    // Fail if the appIds seen by the current transaction has been updated by the winning
    // transaction i.e. the winning transaction have [[SetTransaction]] corresponding to
    // some appId on which current transaction depends on. Example - This can happen when
    // multiple instances of the same streaming query are running at the same time.
    val txnOverlap = winningCommitSummary.appLevelTransactions.map(_.appId).toSet intersect
      currentTransactionInfo.readAppIds
    if (txnOverlap.nonEmpty) {
      throw DeltaErrors.concurrentTransactionException(winningCommitSummary.commitInfo)
    }
  }

  /** A helper function for pretty printing a specific partition directory. */
  protected def getPrettyPartitionMessage(partitionValues: Map[String, String]): String = {
    val partitionColumns = currentTransactionInfo.metadata.partitionSchema
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

  protected def reportMetrics(): Unit = {
    val timingStr = timingStats.keys.toSeq.sorted.map(k => s"$k=${timingStats(k)}").mkString(",")
    logInfo(s"[$logPrefixStr] Timing stats against $winningCommitVersion [$timingStr]")
  }
}
