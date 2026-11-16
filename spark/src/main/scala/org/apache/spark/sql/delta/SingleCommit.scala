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

import org.apache.spark.sql.delta.actions.{Action, AddFile, Checkpoint, CommitInfo, FileAction, RemoveFile}
import org.apache.spark.sql.delta.amt.AMTCheckpointProvider
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.{ClosableIterator, SupportsRewinding}
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession

/**
 * A handle to a single commit in the Delta log, standing in for the raw commit log file that
 * [[DeltaLog.getChangeLogFiles]] used to expose as a [[FileStatus]] -- now hidden so the log's
 * physical layout stays internal, a prerequisite for the Adaptive Metadata Tree (AMT).
 */
class SingleCommit private (
    private val deltaLog: DeltaLog,
    override val version: Long,
    private val fileStatus: FileStatus
) extends DeltaSingleCommit {
  // Read the persisted ICT for this version, so commits before ICT enablement still use mtime.
  override lazy val timestamp: Long = getActionsIterator().processAndClose { actions =>
    actions.collectFirst { case commitInfo: CommitInfo => commitInfo }
      .flatMap(_.inCommitTimestamp)
      .getOrElse(fileModificationTimestamp)
  }

  /**
   * Open a fresh iterator over all of this commit's logical actions. For an inline AMT commit,
   * inline AddFile and RemoveFile actions are replaced by actions reconstructed from the tree.
   * Every other inline action is preserved. The AMT does not record a metadata-only re-add of
   * an already-live file (e.g. a stats backfill), so such AddFiles are omitted.
   *
   * @param maxInMemoryFileSizeBytes commits whose log file is smaller than this are read into
   *        memory once and replayed on every `rewind()`.
   */
  override def getActionsIterator(
      maxInMemoryFileSizeBytes: Long = 0)
      : ClosableIterator[Action] with SupportsRewinding[Action] = {
    if (SparkSession.active.sessionState.conf.getConf(
        DeltaSQLConf.AMT_SINGLE_COMMIT_READ_ENABLED)) {
      getAMTAwareActionsIterator(maxInMemoryFileSizeBytes)
    } else {
      getJsonActionsIterator(maxInMemoryFileSizeBytes)
    }
  }

  /**
   * Get this commit's logical actions, the same actions in the same order as
   * [[getActionsIterator]] with single-commit read on.
   *
   * @param skipNoDataChangeFileActions whether to omit the AddFile and RemoveFile actions with
   *                                    `dataChange = false`.
   */
  def getActions(skipNoDataChangeFileActions: Boolean = false): Seq[Action] = {
    val commitJsonActions =
      getJsonActionsIterator(maxInMemoryFileSizeBytes = 0).processAndClose(_.toList)
    def computeTreeFileActions(
        commitInfo: CommitInfo, checkpointOpt: Option[Checkpoint]): Iterator[Action] = {
      amtProviderFromCheckpointOpt(checkpointOpt)
        .reconstructFileActions(
          deltaLog, commitInfo.getDataChangeOrThrow, commitInfo.getCommitTimestamp)
        .iterator
    }
    toLogicalActions(
      commitJsonActions.iterator, skipNoDataChangeFileActions, computeTreeFileActions).toList
  }

  /**
   * Like [[getActionsIterator]], but asserts this commit is a log commit.
   *
   * @param maxInMemoryFileSizeBytes see [[getActionsIterator]].
   */
  private[delta] def getLogCommitActionsIteratorUnsafe(
      maxInMemoryFileSizeBytes: Long = 0)
      : ClosableIterator[Action] with SupportsRewinding[Action] = {
    val underlying = getJsonActionsIterator(maxInMemoryFileSizeBytes)
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      override def hasNext: Boolean = underlying.hasNext
      override def next(): Action = underlying.next() match {
        case _: Checkpoint =>
          throw new IllegalStateException(
            s"commit $version is a manifest commit and cannot be read as a log commit")
        case action => action
      }
      override def close(): Unit = underlying.close()
      override def rewind(): Unit = underlying.rewind()
    }
  }

  /**
   * The modification time of this commit's log file.
   */
  private[delta] def fileModificationTimestamp: Long = fileStatus.getModificationTime

  /**
   * The size of this commit's log file, in bytes.
   */
  private[delta] def commitFileSizeInBytes: Long = fileStatus.getLen

  //////////////////////
  // Helper functions //
  //////////////////////

  /**
   * The actions in this commit's log file, as written.
   *
   * @param maxInMemoryFileSizeBytes see [[getActionsIterator]].
   */
  private def getJsonActionsIterator(
      maxInMemoryFileSizeBytes: Long): ClosableIterator[Action] with SupportsRewinding[Action] =
    SingleCommit.createRewindableActionIterator(deltaLog, fileStatus, maxInMemoryFileSizeBytes)

  /**
   * The actions in this commit's log file, with an inline AMT commit's AddFile and RemoveFile
   * actions read from its tree instead (see [[toLogicalActions]]).
   *
   * @param maxInMemoryFileSizeBytes see [[getActionsIterator]].
   */
  private def getAMTAwareActionsIterator(
      maxInMemoryFileSizeBytes: Long): ClosableIterator[Action] with SupportsRewinding[Action] = {
    val commitJsonIter = getJsonActionsIterator(maxInMemoryFileSizeBytes)
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      private def computeTreeFileActions(
          commitInfo: CommitInfo, checkpointOpt: Option[Checkpoint]): Iterator[Action] =
        amtProviderFromCheckpointOpt(checkpointOpt).reconstructFileActionsIterator(
          deltaLog, commitInfo.getDataChangeOrThrow, commitInfo.getCommitTimestamp)
      private var underlying = toLogicalActions(
        commitJsonIter, skipNoDataChangeFileActions = false, computeTreeFileActions)
      override def hasNext: Boolean = underlying.hasNext
      override def next(): Action = underlying.next()
      override def close(): Unit = commitJsonIter.close()
      override def rewind(): Unit = {
        commitJsonIter.rewind()
        underlying = toLogicalActions(
          commitJsonIter, skipNoDataChangeFileActions = false, computeTreeFileActions)
      }
    }
  }

  /**
   * Wrap the Json commit iterator with AMT specific handling. This includes:
   * 1. Skip all Add and Removes if skipNoDataChangeFileActions is set and dataChange is false.
   * 2. Returns iterator immediately for non inline manifest commits.
   * 3. Reconstruct file actions from the tree if the commit is an inline manifest commit.
   * Reconstruction happens after log commit is exhausted.
   *
   * @param commitJsonIter the actions in this commit's log file, as written.
   * @param skipNoDataChangeFileActions whether to omit the AddFile and RemoveFile actions with
   *                                    `dataChange = false`.
   * @param computeTreeFileActions reads the tree's file actions, given the commit's CommitInfo
   *                               and Checkpoint action.
   */
  private def toLogicalActions(
      commitJsonIter: Iterator[Action],
      skipNoDataChangeFileActions: Boolean,
      computeTreeFileActions: (CommitInfo, Option[Checkpoint]) => Iterator[Action])
      : Iterator[Action] = {
    val bufferedIter = commitJsonIter.buffered
    val amtCommitInfoOpt = bufferedIter.headOption.collect {
      case commitInfo: CommitInfo if commitInfo.isAMTInlineManifestCommit(version) => commitInfo
    }
    amtCommitInfoOpt match {
      case None =>
        // No AMT tree is present. Log commit actions are filtered if skipNoDataChangeFileActions.
        if (skipNoDataChangeFileActions) {
          bufferedIter.filter {
            case add: AddFile => add.dataChange
            case remove: RemoveFile => remove.dataChange
            case _ => true
          }
        } else {
          bufferedIter
        }
      case Some(commitInfo) =>
        // Always take tree actions when present.
        var checkpointOpt: Option[Checkpoint] = None
        val logFileActionsIter = bufferedIter.filter(!FileAction.isAddOrRemove(_)).map {
          case checkpoint: Checkpoint if checkpointOpt.isEmpty =>
            checkpointOpt = Some(checkpoint)
            checkpoint
          case action => action
        }
        // `++` takes the tree's actions by name and opens them only once the log file's actions
        // run out, so the Checkpoint has been read by then.
        logFileActionsIter ++ {
          if (skipNoDataChangeFileActions && !commitInfo.getDataChangeOrThrow) {
            // A commit that changed no data has no data-changing file actions, so skip its tree.
            Iterator.empty
          } else {
            computeTreeFileActions(commitInfo, checkpointOpt)
          }
        }
    }
  }

  /** The tree of this manifest commit, which its Checkpoint action locates. */
  private def amtProviderFromCheckpointOpt(
      checkpointOpt: Option[Checkpoint]): AMTCheckpointProvider = {
    val checkpoint = checkpointOpt.filter(_.contentRoot.version == version).getOrElse(
      throw new IllegalStateException(
        s"Commit $version's CommitInfo makes it a manifest commit at its own version, but it " +
          s"has no Checkpoint action whose content root is at that version."))
    AMTCheckpointProvider.fromCheckpoint(SparkSession.active, deltaLog, checkpoint, version)
  }
}

object SingleCommit {
  /**
   * Wrap a commit's log file into a [[DeltaSingleCommit]] handle. The raw [[FileStatus]] stays
   * hidden inside the handle so the log's physical layout is not exposed to callers.
   */
  private[delta] def apply(
      deltaLog: DeltaLog,
      version: Long,
      fileStatus: FileStatus): DeltaSingleCommit =
    new SingleCommit(deltaLog, version, fileStatus)

  /**
   * Read a single commit's actions into a rewindable, closable iterator, respecting memory
   * constraints. If the commit's log file is smaller than `maxInMemoryFileSizeBytes`, its actions
   * are read into memory once and replayed on every `rewind()`; otherwise the file is re-read on
   * each pass.
   */
  private def createRewindableActionIterator(
      deltaLog: DeltaLog,
      fileStatus: FileStatus,
      maxInMemoryFileSizeBytes: Long): ClosableIterator[Action] with SupportsRewinding[Action] = {
    lazy val actions =
      deltaLog.store.read(fileStatus, deltaLog.newDeltaHadoopConf()).map(Action.fromJson)
    // If the commit is smaller than the threshold, read it into memory once and iterate over that
    // buffer on every pass; otherwise re-read the file each time.
    val shouldLoadIntoMemory = fileStatus.getLen < maxInMemoryFileSizeBytes
    def createClosableIterator(): ClosableIterator[Action] = if (shouldLoadIntoMemory) {
      actions.toIterator.toClosable
    } else {
      deltaLog.store.readAsIterator(fileStatus, deltaLog.newDeltaHadoopConf()).withClose {
        _.map(Action.fromJson)
      }
    }
    new ClosableIterator[Action] with SupportsRewinding[Action] {
      private var delegatedIterator: ClosableIterator[Action] = createClosableIterator()
      override def hasNext: Boolean = delegatedIterator.hasNext
      override def next(): Action = delegatedIterator.next()
      override def close(): Unit = delegatedIterator.close()
      override def rewind(): Unit = {
        // Close the outgoing delegate before replacing it.
        delegatedIterator.close()
        delegatedIterator = createClosableIterator()
      }
    }
  }
}

/**
 * A commit reader paired with the inclusive version range its actions cover.
 *
 * Compacted files omit CommitInfo, so `commit.timestamp` is the compacted file's modification
 * time, not the timestamp of any individual source commit. The reader is used only for actions;
 * `commit.version` identifies the end of the range.
 */
private[delta] class RangedSingleCommit private (
  val commit: DeltaSingleCommit,
  val startVersion: Long,
  val endVersion: Long)

private[delta] object RangedSingleCommit {
  def apply(deltaLog: DeltaLog, file: FileStatus): RangedSingleCommit = {
    val (startVersion, endVersion) = file match {
      case FileNames.CompactedDeltaFile(_, start, end) => (start, end)
      case _ =>
        val version = FileNames.deltaVersion(file)
        (version, version)
    }
    val commit = SingleCommit(deltaLog, endVersion, file)
    new RangedSingleCommit(commit, startVersion, endVersion)
  }
}
