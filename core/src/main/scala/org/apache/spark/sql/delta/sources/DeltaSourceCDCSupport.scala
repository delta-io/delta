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

package org.apache.spark.sql.delta.sources

import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, CommitInfo, FileAction, Metadata, Protocol, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.storage.ClosableIterator._

import org.apache.spark.sql.DataFrame

/**
 * Helper functions for CDC-specific handling for DeltaSource.
 */
trait DeltaSourceCDCSupport { self: DeltaSource =>

  /////////////////////////
  // Nested helper class //
  /////////////////////////

  /**
   * This class represents an iterator of Change metadata(AddFile, RemoveFile, AddCDCFile)
   * for a particular version.
   * @param fileActionsItr - Iterator of IndexedFiles for a particular commit.
   * @param isInitialSnapshot - Indicates whether the commit version is the initial snapshot or not.
   */
  class IndexedChangeFileSeq(
      fileActionsItr: Iterator[IndexedFile],
      isInitialSnapshot: Boolean) {

    private def moreThanFrom(
        indexedFile: IndexedFile, fromVersion: Long, fromIndex: Long): Boolean = {
      // we need to filter out files so that we get only files after the startingOffset
      indexedFile.version > fromVersion || indexedFile.index > fromIndex
    }

    private def lessThanEnd(
        indexedFile: IndexedFile,
        endOffset: Option[DeltaSourceOffset]): Boolean = {
      // we need to filter out files so that they are within the end offsets.
      if (endOffset.isEmpty) {
        true
      } else {
        indexedFile.version < endOffset.get.reservoirVersion ||
          (indexedFile.version <= endOffset.get.reservoirVersion &&
            indexedFile.index <= endOffset.get.index)
      }
    }

    private def noMatchesRegex(indexedFile: IndexedFile): Boolean = {
      if (hasNoFileActionAndStartIndex(indexedFile)) return true

      excludeRegex.forall(_.findFirstIn(indexedFile.getFileAction.path).isEmpty)
    }

    private def hasFileAction(indexedFile: IndexedFile): Boolean = {
      indexedFile.getFileAction != null
    }

    private def hasNoFileActionAndStartIndex(indexedFile: IndexedFile): Boolean = {
      !indexedFile.hasFileAction && indexedFile.index == DeltaSourceOffset.BASE_INDEX
    }

    private def hasAddsOrRemoves(indexedFile: IndexedFile): Boolean = {
      indexedFile.add != null || indexedFile.remove != null
    }

    private def isSchemaChangeIndexedFile(indexedFile: IndexedFile): Boolean = {
      indexedFile.index == DeltaSourceOffset.SCHEMA_CHANGE_INDEX ||
        indexedFile.index == DeltaSourceOffset.POST_SCHEMA_CHANGE_INDEX
    }

    private def isValidIndexedFile(
        indexedFile: IndexedFile,
        fromVersion: Long,
        fromIndex: Long,
        endOffset: Option[DeltaSourceOffset]): Boolean = {
      !indexedFile.shouldSkip &&
        (hasFileAction(indexedFile) ||
          hasNoFileActionAndStartIndex(indexedFile) ||
          isSchemaChangeIndexedFile(indexedFile)) &&
        moreThanFrom(indexedFile, fromVersion, fromIndex) &&
        lessThanEnd(indexedFile, endOffset) && noMatchesRegex(indexedFile) &&
        lessThanEnd(indexedFile, lastOffsetForTriggerAvailableNow)
    }

    /**
     * Returns the IndexedFiles for particular commit version after rate-limiting and filtering
     * out based on version boundaries.
     */
    def filterFiles(
        fromVersion: Long,
        fromIndex: Long,
        limits: Option[AdmissionLimits],
        endOffset: Option[DeltaSourceOffset] = None): Iterator[IndexedFile] = {

      if (limits.isEmpty) {
        return fileActionsItr.filter(isValidIndexedFile(_, fromVersion, fromIndex, endOffset))
      }
      val admissionControl = limits.get
      if (isInitialSnapshot) {
        // In this case we only have AddFiles as we are returning the snapshot of the table.
        // NOTE: the initial snapshot can be huge hence we do not do a toSeq here.
        fileActionsItr.filter(isValidIndexedFile(_, fromVersion, fromIndex, endOffset))
          .takeWhile { indexedFile =>
            admissionControl.admit(Some(indexedFile.add))
          }
      } else {
        // Change data for a commit can be either recorded by a Seq[AddCDCFiles] or
        // a Seq[AddFile]/ Seq[RemoveFile]
        val fileActions = fileActionsItr.toSeq

        // If there exists a stopping iterator for this version, we should return right-away
        fileActions.find(isSchemaChangeIndexedFile) match {
          case Some(schemaChangeBarrier) =>
            return Seq(schemaChangeBarrier).toIterator
          case _ =>
        }

        val cdcFiles = fileActions.filter(_.cdc != null) // get only cdc commits.
        if (cdcFiles.nonEmpty) {
          // CDC of commit is represented by AddCDCFile
          val filteredFiles = cdcFiles
            .filter(isValidIndexedFile(_, fromVersion, fromIndex, endOffset))
          // For CDC commits we either admit the entire commit or nothing at all.
          // This is to avoid returning `update_preimage` and `update_postimage` in separate
          // batches.
          if (admissionControl.admit(filteredFiles.map(_.cdc))) {
            filteredFiles.toIterator
          } else {
            Iterator()
          }
        } else {
          // CDC is recorded as AddFile or RemoveFile
          // We allow entries with no file actions and index as [[DeltaSourceOffset.BASE_INDEX]]
          // that are used primarily to update latest offset when no other
          // file action based entries are present.
          fileActions.filter(indexedFile => hasAddsOrRemoves(indexedFile) ||
            hasNoFileActionAndStartIndex(indexedFile))
            .filter(
              isValidIndexedFile(_, fromVersion, fromIndex, endOffset)
            ).takeWhile { indexedFile =>
            admissionControl.admit(Option(indexedFile.getFileAction))
          }.toIterator
        }
      }
    }
  }

  ///////////////////////////////
  // Util methods for children //
  ///////////////////////////////

  /**
   * Get the changes from startVersion, startIndex to the end for CDC case. We need to call
   * CDCReader to get the CDC DataFrame.
   *
   * @param startVersion - calculated starting version
   * @param startIndex - calculated starting index
   * @param isStartingVersion - whether the stream has to return the initial snapshot or not
   * @param endOffset - Offset that signifies the end of the stream.
   * @return the DataFrame containing the file changes (AddFile, RemoveFile, AddCDCFile)
   */
  protected def getCDCFileChangesAndCreateDataFrame(
      startVersion: Long,
      startIndex: Long,
      isStartingVersion: Boolean,
      endOffset: DeltaSourceOffset): DataFrame = {
    val changes: Iterator[(Long, Iterator[IndexedFile])] =
      getFileChangesForCDC(startVersion, startIndex, isStartingVersion, None, Some(endOffset))

    val groupedFileActions: Iterator[(Long, Seq[FileAction])] =
      changes.map { case (v, indexFiles) =>
        (v, indexFiles.filter(_.hasFileAction).map { _.getFileAction }.toSeq)
      }

    val cdcInfo = CDCReader.changesToDF(
      readSchemaSnapshotDescriptor,
      startVersion,
      endOffset.reservoirVersion,
      groupedFileActions,
      spark,
      isStreaming = true
    )

    cdcInfo.fileChangeDf
  }

  /**
   * Get the changes starting from (fromVersion, fromIndex). fromVersion is included.
   * It returns an iterator of (log_version, fileActions)
   *
   * If verifyMetadataAction = true, we will break the stream when we detect any read-incompatible
   * metadata changes.
   */
  protected def getFileChangesForCDC(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean,
      limits: Option[AdmissionLimits],
      endOffset: Option[DeltaSourceOffset],
      verifyMetadataAction: Boolean = true): Iterator[(Long, Iterator[IndexedFile])] = {

    /** Returns matching files that were added on or after startVersion among delta logs. */
    def filterAndIndexDeltaLogs(startVersion: Long): Iterator[(Long, IndexedChangeFileSeq)] = {
      // TODO: handle the case when failOnDataLoss = false and we are missing change log files
      //    in that case, we need to recompute the start snapshot and evolve the schema if needed
      require(options.failOnDataLoss || !trackingSchemaChange,
        "Using schema from schema tracking log cannot tolerate missing commit files.")
      deltaLog.getChanges(startVersion, options.failOnDataLoss).map { case (version, actions) =>
        // skipIndexedFile must be applied after creating IndexedFile so that
        // IndexedFile.index is consistent across all versions.
        val (fileActions, skipIndexedFile, metadataOpt) =
          filterCDCActions(actions, version, verifyMetadataAction && !trackingSchemaChange)
        val itr =
            Iterator(IndexedFile(version, DeltaSourceOffset.BASE_INDEX, null)) ++
              getSchemaChangeIndexedFileIterator(metadataOpt, version) ++
              fileActions
            .zipWithIndex.map {
              case (action: AddFile, index) =>
                IndexedFile(
                  version,
                  index.toLong,
                  action,
                  isLast = index + 1 == fileActions.size,
                  shouldSkip = skipIndexedFile)
              case (cdcFile: AddCDCFile, index) =>
                IndexedFile(
                  version,
                  index.toLong,
                  add = null,
                  cdc = cdcFile,
                  isLast = index + 1 == fileActions.size,
                  shouldSkip = skipIndexedFile)
              case (remove: RemoveFile, index) =>
                IndexedFile(
                  version,
                  index.toLong,
                  add = null,
                  remove = remove,
                  isLast = index + 1 == fileActions.size,
                  shouldSkip = skipIndexedFile)
            }
        (version, new IndexedChangeFileSeq(itr, isInitialSnapshot = false))
      }
    }

    val iter: Iterator[(Long, IndexedChangeFileSeq)] = if (isStartingVersion) {
      // If we are reading change data from the start of the table we need to
      // get the latest snapshot of the table as well.
      val snapshot: Iterator[IndexedFile] = getSnapshotAt(fromVersion).map { m =>
        // When we get the snapshot the dataChange is false for the AddFile actions
        // We need to set it to true for it to be considered by the CDCReader.
        if (m.add != null) {
          m.copy(add = m.add.copy(dataChange = true))
        } else {
          m
        }
      }
      val snapshotItr: Iterator[(Long, IndexedChangeFileSeq)] = Iterator((
        fromVersion,
        new IndexedChangeFileSeq(snapshot, isInitialSnapshot = true)
      ))

      snapshotItr ++ filterAndIndexDeltaLogs(fromVersion + 1)
    } else {
      filterAndIndexDeltaLogs(fromVersion)
    }

    // In this case, filterFiles will consume the available capacity. We use takeWhile
    // to stop the iteration when we reach the limit which will save us from reading
    // unnecessary log files.
    iter.takeWhile(_ => limits.forall(_.hasCapacity)).map { case (version, indexItr) =>
      (version, indexItr.filterFiles(fromVersion, fromIndex, limits, endOffset))
    }
  }

  /////////////////////
  // Private methods //
  /////////////////////

  /**
   * Filter out non CDC actions and only return CDC ones. This will either be AddCDCFiles
   * or AddFile and RemoveFiles
   *
   * If verifyMetadataAction = true, we will break the stream when we detect any read-incompatible
   * metadata changes.
   */
  private def filterCDCActions(
      actions: Seq[Action],
      version: Long,
      verifyMetadataAction: Boolean = true): (Seq[FileAction], Boolean, Option[Metadata]) = {
    var shouldSkipIndexedFile = false
    var metadataAction: Option[Metadata] = None
    if (actions.exists(_.isInstanceOf[AddCDCFile])) {
      (actions.filter(_.isInstanceOf[AddCDCFile]).asInstanceOf[Seq[FileAction]],
       shouldSkipIndexedFile, metadataAction)
    } else {
      (actions.filter {
        case a: AddFile =>
          a.dataChange
        case r: RemoveFile =>
          r.dataChange
        case cdc: AddCDCFile =>
          false
        case m: Metadata =>
          if (verifyMetadataAction) {
            checkReadIncompatibleSchemaChanges(m, version)
          }
          assert(metadataAction.isEmpty,
            "Should not encounter two metadata actions in the same commit")
          metadataAction = Some(m)
          false
        case protocol: Protocol =>
          deltaLog.protocolRead(protocol)
          false
        case commitInfo: CommitInfo =>
          shouldSkipIndexedFile = CDCReader.shouldSkipFileActionsInCommit(commitInfo)
          false
        case _: SetTransaction =>
          false
        case null => // Some crazy future feature. Ignore
          false
      }.asInstanceOf[Seq[FileAction]], shouldSkipIndexedFile, metadataAction)
    }
  }
}
