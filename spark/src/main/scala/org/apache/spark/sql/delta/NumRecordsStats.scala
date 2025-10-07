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

import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.util.ScalaExtensions._

/**
 * Container class for statistics related to number of records in a Delta commit.
 */
case class NumRecordsStats (
    // Number of logical records in AddFile actions with numRecords.
    numLogicalRecordsAddedPartial: Long,
    // Number of logical records in RemoveFile actions with numRecords.
    numLogicalRecordsRemovedPartial: Long,
    numDeletionVectorRecordsAdded: Long,
    numDeletionVectorRecordsRemoved: Long,
    numFilesAddedWithoutNumRecords: Long,
    numFilesRemovedWithoutNumRecords: Long) {

  def allFilesHaveNumRecords: Boolean =
    numFilesAddedWithoutNumRecords == 0 && numFilesRemovedWithoutNumRecords == 0

  /**
   * The number of logical records in all AddFile actions or None if any file does not contain
   * statistics.
   */
  def numLogicalRecordsAdded: Option[Long] = Option.when(numFilesAddedWithoutNumRecords == 0)(
    numLogicalRecordsAddedPartial)

  /**
   * The number of logical records in all RemoveFile actions or None if any file does not contain
   * statistics.
   */
  def numLogicalRecordsRemoved: Option[Long] = Option.when(numFilesRemovedWithoutNumRecords == 0)(
    numLogicalRecordsRemovedPartial)
}

object NumRecordsStats {
  def fromActions(actions: Seq[Action]): NumRecordsStats = {
    var numFilesAdded = 0L
    var numFilesRemoved = 0L
    var numFilesAddedWithoutNumRecords = 0L
    var numFilesRemovedWithoutNumRecords = 0L
    var numLogicalRecordsAddedPartial: Long = 0L
    var numLogicalRecordsRemovedPartial: Long = 0L
    var numDeletionVectorRecordsAdded = 0L
    var numDeletionVectorRecordsRemoved = 0L

    actions.foreach {
      case a: AddFile =>
        numFilesAdded += 1
        numLogicalRecordsAddedPartial += a.numLogicalRecords.getOrElse {
          numFilesAddedWithoutNumRecords += 1
          0L
        }
        numDeletionVectorRecordsAdded += a.numDeletedRecords
      case r: RemoveFile =>
        numFilesRemoved += 1
        numLogicalRecordsRemovedPartial += r.numLogicalRecords.getOrElse {
          numFilesRemovedWithoutNumRecords += 1
          0L
        }
        numDeletionVectorRecordsRemoved += r.numDeletedRecords
      case _ =>
        // Do nothing
    }
    NumRecordsStats(
      numLogicalRecordsAddedPartial = numLogicalRecordsAddedPartial,
      numLogicalRecordsRemovedPartial = numLogicalRecordsRemovedPartial,
      numDeletionVectorRecordsAdded = numDeletionVectorRecordsAdded,
      numDeletionVectorRecordsRemoved = numDeletionVectorRecordsRemoved,
      numFilesAddedWithoutNumRecords = numFilesAddedWithoutNumRecords,
      numFilesRemovedWithoutNumRecords = numFilesRemovedWithoutNumRecords)
  }
}
