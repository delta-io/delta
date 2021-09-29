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

import java.io.{FileNotFoundException, IOException}

import io.delta.standalone.internal.actions.{CommitInfo, Protocol}
import io.delta.standalone.internal.util.JsonUtils
import io.delta.standalone.types.StructType
import io.delta.standalone.exceptions._

import org.apache.hadoop.fs.Path

/** A holder object for Delta errors. */
private[internal] object DeltaErrors {

  /**
   * Thrown when the protocol version of a table is greater than the one supported by this client
   */
  class InvalidProtocolVersionException(
      clientProtocol: Protocol,
      tableProtocol: Protocol) extends RuntimeException(
    s"""
       |Delta protocol version ${tableProtocol.simpleString} is too new for this version of Delta
       |Standalone Reader/Writer ${clientProtocol.simpleString}. Please upgrade to a newer release.
       |""".stripMargin)

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
    // TODO: AnalysisException ?
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
    // TODO: AnalysisException ?
    new RuntimeException(s"No commits found at $logPath")
  }

  def versionNotExistException(userVersion: Long, earliest: Long, latest: Long): Throwable = {
    new IllegalArgumentException(s"Cannot time travel Delta table to version $userVersion. " +
      s"Available versions: [$earliest, $latest].")
  }

  def nullValueFoundForPrimitiveTypes(fieldName: String): Throwable = {
    new NullPointerException(s"Read a null value for field $fieldName which is a primitive type")
  }

  def nullValueFoundForNonNullSchemaField(fieldName: String, schema: StructType): Throwable = {
    new NullPointerException(s"Read a null value for field $fieldName, yet schema indicates " +
      s"that this field can't be null. Schema: ${schema.getTreeString}")
  }

  def failOnDataLossException(expectedVersion: Long, seenVersion: Long): Throwable = {
    new IllegalStateException(
      s"""The stream from your Delta table was expecting process data from version $expectedVersion,
         |but the earliest available version in the _delta_log directory is $seenVersion. The files
         |in the transaction log may have been deleted due to log cleanup.
         |
         |If you would like to ignore the missed data and continue your stream from where it left
         |off, you can set the .option("failOnDataLoss", "false") as part
         |of your readStream statement.
       """.stripMargin
    )
  }

  def metadataAbsentException(): Throwable = {
    new IllegalStateException(
      "Couldn't find Metadata while committing the first version of the Delta table.")
  }

  def addFilePartitioningMismatchException(
      addFilePartitions: Seq[String],
      metadataPartitions: Seq[String]): Throwable = {
    new IllegalStateException(
      s"""
         |The AddFile contains partitioning schema different from the table's partitioning schema
         |expected: ${DeltaErrors.formatColumnList(metadataPartitions)}
         |actual: ${DeltaErrors.formatColumnList(addFilePartitions)}
      """.stripMargin)
  }

  def modifyAppendOnlyTableException: Throwable = {
    new UnsupportedOperationException(
      "This table is configured to only allow appends. If you would like to permit " +
        s"updates or deletes, use 'ALTER TABLE <table_name> SET TBLPROPERTIES " +
        s"(appendOnly=false)'.")
  }

  def invalidColumnName(name: String): Throwable = {
    // TODO: AnalysisException ??
    new RuntimeException(
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  // TODO: AnalysisException ??
  def invalidPartitionColumn(e: RuntimeException): Throwable = {
    // TODO: AnalysisException ??
    new RuntimeException(
      """Found partition columns having invalid character(s) among " ,;{}()\n\t=". Please """ +
        "change the name to your partition columns. This check can be turned off by setting " +
        """spark.conf.set("spark.databricks.delta.partitionColumnValidity.enabled", false) """ +
        "however this is not recommended as other features of Delta may not work properly.",
      e)
  }

  def incorrectLogStoreImplementationException(cause: Throwable): Throwable = {
    new IOException(s"""
     |The error typically occurs when the default LogStore implementation, that
     |is, HDFSLogStore, is used to write into a Delta table on a non-HDFS storage system.
     |In order to get the transactional ACID guarantees on table updates, you have to use the
     |correct implementation of LogStore that is appropriate for your storage system.
     |See https://docs.delta.io/latest/delta-storage.html for details.
      """.stripMargin, cause)
  }

  def concurrentModificationExceptionMsg(
      baseMessage: String,
      commit: Option[CommitInfo]): String = {
    baseMessage +
      commit.map(ci => s"\nConflicting commit: ${JsonUtils.toJson(ci)}").getOrElse("") +
      s"\nRefer to https://docs.delta.io/latest/concurrency-control.html for more details."
  }

  def metadataChangedException(
      conflictingCommit: Option[CommitInfo]): MetadataChangedException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      "The metadata of the Delta table has been changed by a concurrent update. " +
        "Please try the operation again.",
      conflictingCommit)
    new MetadataChangedException(message)
  }

  def protocolChangedException(conflictingCommit: Option[CommitInfo]): ProtocolChangedException = {
    val additionalInfo = conflictingCommit.map { v =>
      if (v.version.getOrElse(-1) == 0) {
        "This happens when multiple writers are writing to an empty directory. " +
          "Creating the table ahead of time will avoid this conflict. "
      } else {
        ""
      }
    }.getOrElse("")

    val message = DeltaErrors.concurrentModificationExceptionMsg(
      "The protocol version of the Delta table has been changed by a concurrent update. " +
        additionalInfo + "Please try the operation again.",
      conflictingCommit)
    new ProtocolChangedException(message)
  }

  def concurrentAppendException(
      conflictingCommit: Option[CommitInfo],
      partition: String): ConcurrentAppendException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      s"Files were added to $partition by a concurrent update. " +
        s"Please try the operation again.",
      conflictingCommit)
    new ConcurrentAppendException(message)
  }

  def concurrentDeleteReadException(
      conflictingCommit: Option[CommitInfo],
      file: String): ConcurrentDeleteReadException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      "This transaction attempted to read one or more files that were deleted" +
        s" (for example $file) by a concurrent update. Please try the operation again.",
      conflictingCommit)
    new ConcurrentDeleteReadException(message)
  }

  def concurrentDeleteDeleteException(
      conflictingCommit: Option[CommitInfo],
      file: String): ConcurrentDeleteDeleteException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      "This transaction attempted to delete one or more files that were deleted " +
        s"(for example $file) by a concurrent update. Please try the operation again.",
      conflictingCommit)
    new ConcurrentDeleteDeleteException(message)
  }

  def concurrentTransactionException(
      conflictingCommit: Option[CommitInfo]): ConcurrentTransactionException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      s"This error occurs when multiple streaming queries are using the same checkpoint to write " +
        "into this table. Did you run multiple instances of the same streaming query" +
        " at the same time?",
      conflictingCommit)
    new ConcurrentTransactionException(message)
  }

  def maxCommitRetriesExceededException(
      attemptNumber: Int,
      attemptVersion: Long,
      initAttemptVersion: Long,
      numActions: Int,
      totalCommitAttemptTime: Long): Throwable = {
    new IllegalStateException(
      s"""This commit has failed as it has been tried $attemptNumber times but did not succeed.
         |This can be caused by the Delta table being committed continuously by many concurrent
         |commits.
         |
         |Commit started at version: $initAttemptVersion
         |Commit failed at version: $attemptVersion
         |Number of actions attempted to commit: $numActions
         |Total time spent attempting this commit: $totalCommitAttemptTime ms
       """.stripMargin)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Helper Methods
  ///////////////////////////////////////////////////////////////////////////

  private def formatColumn(colName: String): String = s"`$colName`"

  private def formatColumnList(colNames: Seq[String]): String =
    colNames.map(formatColumn).mkString("[", ", ", "]")
}
