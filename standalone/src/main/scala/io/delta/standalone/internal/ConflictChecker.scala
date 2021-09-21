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

package io.delta.standalone.internal

import io.delta.standalone.expressions.Expression
import io.delta.standalone.internal.actions._
import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.util.FileNames

/**
 * A class representing different attributes of current transaction needed for conflict detection.
 *
 * @param readPredicates - partition predicates by which files have been queried by the transaction
 * @param readFiles - specific files that have been seen by the transaction
 * @param readWholeTable - whether the whole table was read during the transaction
 * @param readAppIds - appIds that have been seen by the transaction
 * @param metadata - table metadata for the transaction
 * @param actions - delta log actions that the transaction wants to commit
 * @param deltaLog - [[DeltaLogImpl]] corresponding to the target table
 */
private[internal] case class CurrentTransactionInfo(
    readPredicates: Seq[Expression],
    readFiles: Set[AddFile],
    readWholeTable: Boolean,
    readAppIds: Set[String],
    metadata: Metadata,
    actions: Seq[Action],
    deltaLog: DeltaLogImpl)

/**
 * Summary of the Winning commit against which we want to check the conflict
 * @param actions - delta log actions committed by the winning commit
 * @param commitVersion - winning commit version
 */
private[internal] case class WinningCommitSummary(actions: Seq[Action], commitVersion: Long) {
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

private[internal] class ConflictChecker(
    currentTransactionInfo: CurrentTransactionInfo,
    winningCommitVersion: Long,
    isolationLevel: IsolationLevel) {

  private val winningCommitSummary: WinningCommitSummary = createWinningCommitSummary()

  def checkConflicts(): Unit = {
    //    TODO checkProtocolCompatibility()
    //    TODO checkNoMetadataUpdates()
    checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn()
    checkForDeletedFilesAgainstCurrentTxnReadFiles()
    //    TODO checkForDeletedFilesAgainstCurrentTxnDeletedFiles()
    //    TODO checkForUpdatedApplicationTransactionIdsThatCurrentTxnDependsOn()
  }

  /**
   * Initializes [[WinningCommitSummary]] for the already committed
   * transaction (winning transaction).
   */
  private def createWinningCommitSummary(): WinningCommitSummary = {
    val deltaLog = currentTransactionInfo.deltaLog
    val winningCommitActions = deltaLog.store.read(
      FileNames.deltaFile(deltaLog.logPath, winningCommitVersion)).map(Action.fromJson)
    WinningCommitSummary(winningCommitActions, winningCommitVersion)
  }

  /**
   * Check if the new files added by the already committed transactions should have been read by
   * the current transaction.
   */
  def checkForAddedFilesThatShouldHaveBeenReadByCurrentTxn(): Unit = {
    // Fail if new files have been added that the txn should have read.
    val addedFilesToCheckForConflicts = isolationLevel match {
      case Serializable =>
        winningCommitSummary.changedDataAddedFiles ++ winningCommitSummary.blindAppendAddedFiles
      case SnapshotIsolation =>
        Seq.empty
    }

    // TODO
  }

  /**
   * Check if [[RemoveFile]] actions added by already committed transactions conflicts with files
   * read by the current transaction.
   */
  def checkForDeletedFilesAgainstCurrentTxnReadFiles(): Unit = {
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

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  /** A helper function for pretty printing a specific partition directory. */
  private def getPrettyPartitionMessage(partitionValues: Map[String, String]): String = {
    val partitionColumns = currentTransactionInfo.metadata.partitionColumns
    if (partitionColumns.isEmpty) {
      "the root of the table"
    } else {
      val partition = partitionColumns.map { name =>
        s"$name=${partitionValues(name)}"
      }.mkString("[", ", ", "]")
      s"partition ${partition}"
    }
  }
}
