/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.exception

import java.io.FileNotFoundException

import org.apache.hadoop.fs.Path

import io.delta.standalone.types.StructType

/** A holder object for Delta errors. */
private [internal] object DeltaErrors {

  /** Thrown when the protocol version of a table is greater than supported by this client. */
  case class InvalidProtocolVersionException(
      clientProtocolVersion: Int,
      tableProtocolVersion: Int) extends RuntimeException(
    s"Delta protocol version $tableProtocolVersion is too new for this version of Delta " +
      s"Standalone Reader $clientProtocolVersion. Please upgrade to a newer release.")

  def deltaVersionsNotContiguousException(deltaVersions: Seq[Long]): Throwable = {
    new IllegalStateException(s"Versions ($deltaVersions) are not contiguous.")
  }

  def actionNotFoundException(action: String, version: Long): Throwable = {
    new IllegalStateException(
      s"""
         |The $action of your Delta table couldn't be recovered while Reconstructing
         |version: ${version.toString}. Did you manually delete files in the _delta_log directory?
       """.stripMargin)
  }

  def emptyDirectoryException(directory: String): Throwable = {
    new FileNotFoundException(s"No file found in the directory: $directory.")
  }

  def logFileNotFoundException(
      path: Path,
      version: Long): Throwable = {
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy ")
  }

  def missingPartFilesException(version: Long, e: Exception): Throwable = {
    new IllegalStateException(
      s"Couldn't find all part files of the checkpoint version: $version", e)
  }

  def noReproducibleHistoryFound(logPath: Path): Throwable = {
    new RuntimeException(s"No reproducible commits found at $logPath")
  }

  def timestampEarlierThanTableFirstCommit(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp): Throwable = {
    new IllegalArgumentException(
      s"""The provided timestamp ($userTimestamp) is before the earliest version available to this
         |table ($commitTs). Please use a timestamp greater than or equal to $commitTs.
       """.stripMargin)
  }

  def timestampLaterThanTableLastCommit(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp): Throwable = {
    new IllegalArgumentException(
      s"""The provided timestamp ($userTimestamp) is after the latest version available to this
         |table ($commitTs). Please use a timestamp less than or equal to $commitTs.
       """.stripMargin)
  }

  def noHistoryFound(logPath: Path): Throwable = {
    new RuntimeException(s"No commits found at $logPath")
  }

  def versionNotExistException(userVersion: Long, earliest: Long, latest: Long): Throwable = {
    new IllegalArgumentException(s"Cannot time travel Delta table to version $userVersion. " +
      s"Available versions: [$earliest, $latest].")
  }

  def nullValueFoundForNonNullSchemaField(fieldName: String, schema: StructType): Throwable = {
    new NullPointerException(s"Read a null value for field $fieldName, yet schema indicates " +
      s"that this field can't be null. Schema: ${schema.getTreeString}")
  }
}
