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

package org.apache.spark.sql.delta.commands.optimize

import org.apache.spark.sql.delta.actions.AddFile

/**
 * Wrapper over an [AddFile] and its stats:
 * @param numPhysicalRecords The number of records physically present in the file.
 *                           Equivalent to `addFile.numTotalRecords`.
 * @param numLogicalRecords The physical number of records minus the Deletion Vector cardinality.
 *                          Equivalent to `addFile.numRecords`.
 */
case class AddFileWithNumRecords(
  addFile: AddFile,
  numPhysicalRecords: java.lang.Long,
  numLogicalRecords: java.lang.Long) {

  /** Returns the approx size of the remaining records after excluding the deleted ones. */
  def estLogicalFileSize: Long = {
    (addFile.size * logicalToPhysicalRecordsRatio).toLong
  }

  /** Returns the ratio of the logical number of records to the total number of records. */
  def logicalToPhysicalRecordsRatio: Double = {
    if (numLogicalRecords == null || numPhysicalRecords == null || numPhysicalRecords == 0) {
      1.0d
    } else {
      numLogicalRecords.toDouble / numPhysicalRecords.toDouble
    }
  }

  /** Returns the ratio of the deleted number of records to the total number of records */
  def deletedToPhysicalRecordsRatio: Double = {
    1.0d - logicalToPhysicalRecordsRatio
  }
}

object AddFileWithNumRecords {
  def createFromFile(file: AddFile): AddFileWithNumRecords = {
    val numPhysicalRecords = file.numPhysicalRecords.getOrElse(0L)
    val numLogicalRecords = file.numLogicalRecords.getOrElse(0L)
    AddFileWithNumRecords(file, numPhysicalRecords, numLogicalRecords)
  }
}
