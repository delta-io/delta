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

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, CommitInfo, FileAction, Metadata, Protocol, RemoveFile, SetTransaction}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.schema.SchemaUtils

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
      indexedFile.version > fromVersion ||
        (indexedFile.index == -1 || indexedFile.index > fromIndex)
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
      excludeRegex.forall(_.findFirstIn(indexedFile.getFileAction.path).isEmpty)
    }

    private def hasFileAction(indexedFile: IndexedFile): Boolean = {
      indexedFile.getFileAction != null
    }

    private def hasAddsOrRemoves(indexedFile: IndexedFile): Boolean = {
      indexedFile.add != null || indexedFile.remove != null
    }

    private def isValidIndexedFile(
        indexedFile: IndexedFile,
        fromVersion: Long,
        fromIndex: Long,
        endOffset: Option[DeltaSourceOffset]): Boolean = {
      hasFileAction(indexedFile) && moreThanFrom(indexedFile, fromVersion, fromIndex) &&
        lessThanEnd(indexedFile, endOffset) && noMatchesRegex(indexedFile) &&
        lessThanEnd(indexedFile, Option(lastOffsetForTriggerAvailableNow))
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
          fileActions.filter(hasAddsOrRemoves(_))
            .filter(
              isValidIndexedFile(_, fromVersion, fromIndex, endOffset)
            ).takeWhile { indexedFile =>
            admissionControl.admit(Some(indexedFile.getFileAction))
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
        (v, indexFiles.map { _.getFileAction }.toSeq)
      }

    val cdcInfo = CDCReader.changesToDF(
      deltaLog,
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
   * It returns an  iterator of (log_version, fileActions)
   */
  protected def getFileChangesForCDC(
      fromVersion: Long,
      fromIndex: Long,
      isStartingVersion: Boolean,
      limits: Option[AdmissionLimits],
      endOffset: Option[DeltaSourceOffset]): Iterator[(Long, Iterator[IndexedFile])] = {

    /** Returns matching files that were added on or after startVersion among delta logs. */
    def filterAndIndexDeltaLogs(startVersion: Long): Iterator[(Long, IndexedChangeFileSeq)] = {
      deltaLog.getChanges(startVersion, options.failOnDataLoss).map { case (version, actions) =>
        val fileActions = filterCDCActions(actions, version)
        val itr = Iterator(IndexedFile(version, -1, null)) ++ fileActions
          .zipWithIndex.map {
          case (action: AddFile, index) =>
            IndexedFile(version, index.toLong, action, isLast = index + 1 == fileActions.size)
          case (cdcFile: AddCDCFile, index) =>
            IndexedFile(
              version,
              index.toLong,
              add = null,
              cdc = cdcFile,
              isLast = index + 1 == fileActions.size)
          case (remove: RemoveFile, index) =>
            IndexedFile(
              version,
              index.toLong,
              add = null,
              remove = remove,
              isLast = index + 1 == fileActions.size)
        }
        (version,
          new IndexedChangeFileSeq(itr, isInitialSnapshot = false))
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

    iter.map { case (version, indexItr) =>
      (version, indexItr.filterFiles(fromVersion, fromIndex, limits, endOffset))
    }
  }

  /////////////////////
  // Private methods //
  /////////////////////

  /**
   * Filter out non CDC actions and only return CDC ones. This will either be AddCDCFiles
   * or AddFile and RemoveFiles
   */
  private def filterCDCActions(
      actions: Seq[Action],
      version: Long): Seq[FileAction] = {
    if (actions.exists(_.isInstanceOf[AddCDCFile])) {
      actions.filter(_.isInstanceOf[AddCDCFile]).asInstanceOf[Seq[FileAction]]
    } else {
      actions.filter {
        case a: AddFile =>
          a.dataChange
        case r: RemoveFile =>
          r.dataChange
        case cdc: AddCDCFile =>
          false
        case m: Metadata =>
          val cdcSchema = CDCReader.cdcReadSchema(m.schema)
          if (!SchemaUtils.isReadCompatible(cdcSchema, schema)) {
            throw DeltaErrors.schemaChangedException(schema, cdcSchema, false)
          }
          false
        case protocol: Protocol =>
          deltaLog.protocolRead(protocol)
          false
        case _: SetTransaction | _: CommitInfo =>
          false
        case null => // Some crazy future feature. Ignore
          false
      }.asInstanceOf[Seq[FileAction]]
    }
  }
}
