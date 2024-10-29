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

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.util.Utils

/**
 * Stats calculated within a snapshot, which we store along individual transactions for
 * verification.
 *
 * @param txnId Optional transaction identifier
 * @param tableSizeBytes The size of the table in bytes
 * @param numFiles Number of `AddFile` actions in the snapshot
 * @param numMetadata Number of `Metadata` actions in the snapshot
 * @param numProtocol Number of `Protocol` actions in the snapshot
 * @param histogramOpt Optional file size histogram
 */
case class VersionChecksum(
    txnId: Option[String],
    tableSizeBytes: Long,
    numFiles: Long,
    numMetadata: Long,
    numProtocol: Long,
    @JsonDeserialize(contentAs = classOf[Long])
    inCommitTimestampOpt: Option[Long],
    setTransactions: Option[Seq[SetTransaction]],
    domainMetadata: Option[Seq[DomainMetadata]],
    metadata: Metadata,
    protocol: Protocol,
    histogramOpt: Option[FileSizeHistogram],
    allFiles: Option[Seq[AddFile]])

/**
 * Record the state of the table as a checksum file along with a commit.
 */
trait RecordChecksum extends DeltaLogging {
  val deltaLog: DeltaLog
  protected def spark: SparkSession

  private lazy val writer =
    CheckpointFileManager.create(deltaLog.logPath, deltaLog.newDeltaHadoopConf())

  protected def writeChecksumFile(txnId: String, snapshot: Snapshot): Unit = {
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)) {
      return
    }

    val version = snapshot.version
    val checksum = snapshot.computeChecksum.copy(txnId = Some(txnId))
    val eventData = mutable.Map[String, Any]("operationSucceeded" -> false)
    eventData("numAddFileActions") = checksum.allFiles.map(_.size).getOrElse(-1)
    eventData("numSetTransactionActions") = checksum.setTransactions.map(_.size).getOrElse(-1)
    val startTimeMs = System.currentTimeMillis()
    try {
      val toWrite = JsonUtils.toJson(checksum) + "\n"
      eventData("jsonSerializationTimeTakenMs") = System.currentTimeMillis() - startTimeMs
      eventData("checksumLength") = toWrite.length
      val stream = writer.createAtomic(
        FileNames.checksumFile(deltaLog.logPath, version),
        overwriteIfPossible = false)
      try {
        stream.write(toWrite.getBytes(UTF_8))
        stream.close()
        eventData("overallTimeTakenMs") = System.currentTimeMillis() - startTimeMs
        eventData("operationSucceeded") = true
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to write the checksum for version: " +
            log"${MDC(DeltaLogKeys.VERSION, version)}", e)
          stream.cancel()
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to write the checksum for version: " +
          log"${MDC(DeltaLogKeys.VERSION, version)}", e)
    }
    recordDeltaEvent(
      deltaLog,
      opType = "delta.checksum.write",
      data = eventData)
  }

  /**
   * Incrementally derive checksum for the just-committed or about-to-be committed snapshot.
   * @param spark The SparkSession
   * @param deltaLog The DeltaLog
   * @param versionToCompute The version for which we want to compute the checksum
   * @param actions The actions corresponding to the version `versionToCompute`
   * @param metadata The metadata corresponding to the version `versionToCompute`
   * @param protocol The protocol corresponding to the version `versionToCompute`
   * @param operationName The operation name corresponding to the version `versionToCompute`
   * @param txnIdOpt The transaction identifier for the version `versionToCompute`
   * @param previousVersionState Contains either the versionChecksum corresponding to
   *                             `versionToCompute - 1` or a snapshot. Note that the snapshot may
   *                             belong to any version and this method will only use the snapshot if
   *                             it corresponds to `versionToCompute - 1`.
   * @return Either the new checksum or an error code string if the checksum could not be computed.
   */
  // scalastyle:off argcount
  def incrementallyDeriveChecksum(
      spark: SparkSession,
      deltaLog: DeltaLog,
      versionToCompute: Long,
      actions: Seq[Action],
      metadata: Metadata,
      protocol: Protocol,
      operationName: String,
      txnIdOpt: Option[String],
      previousVersionState: Either[Snapshot, VersionChecksum]
  ): Either[String, VersionChecksum] = {
    // scalastyle:on argcount
    if (!deltaLog.incrementalCommitEnabled) {
      return Left("INCREMENTAL_COMMITS_DISABLED")
    }

    // Do not incrementally derive checksum for ManualUpdate operations since it may
    // include actions that violate delta protocol invariants.
    if (operationName == DeltaOperations.ManualUpdate.name) {
      return Left("INVALID_OPERATION_MANUAL_UPDATE")
    }

    // Try to incrementally compute a VersionChecksum for the just-committed snapshot.
    val expectedVersion = versionToCompute - 1
    val (oldVersionChecksum, oldSnapshot) = previousVersionState match {
      case Right(checksum) => checksum -> None
      case Left(snapshot) if snapshot.version == expectedVersion =>
        // The original snapshot is still fresh so use it directly. Note this could trigger
        // a state reconstruction if there is not an existing checksumOpt in the snapshot
        // or if the existing checksumOpt contains missing information e.g.
        // a null valued metadata or protocol. However, if we do not obtain a checksum here,
        // then we cannot incrementally derive a new checksum for the new snapshot.
        logInfo(log"Incremental commit: starting with snapshot version " +
          log"${MDC(DeltaLogKeys.VERSION, expectedVersion)}")
        snapshot.computeChecksum.copy(numMetadata = 1, numProtocol = 1) -> Some(snapshot)
      case _ =>
        previousVersionState.swap.foreach { snapshot =>
          // Occurs when snapshot is no longer fresh due to concurrent writers.
          // Read CRC file and validate checksum information is complete.
          recordDeltaEvent(deltaLog, opType = "delta.commit.snapshotAgedOut", data = Map(
            "snapshotVersion" -> snapshot.version,
            "commitAttemptVersion" -> versionToCompute
          ))
        }
        val oldCrcOpt = deltaLog.readChecksum(expectedVersion)
        if (oldCrcOpt.isEmpty) {
          return Left("MISSING_OLD_CRC")
        }
        val oldCrcFiltered = oldCrcOpt
          .filterNot(_.metadata == null)
          .filterNot(_.protocol == null)

        val oldCrc = oldCrcFiltered.getOrElse {
          return Left("OLD_CRC_INCOMPLETE")
        }
        oldCrc -> None
    }

    // Incrementally compute the new version checksum, if the old one is available.
    val ignoreAddFilesInOperation =
      RecordChecksum.operationNamesWhereAddFilesIgnoredForIncrementalCrc.contains(operationName)

    computeNewChecksum(
      versionToCompute,
      operationName,
      txnIdOpt,
      oldVersionChecksum,
      oldSnapshot,
      actions,
      ignoreAddFilesInOperation
    )
  }

  /**
   * Incrementally derive new checksum from old checksum + actions.
   *
   * @param attemptVersion commit attempt version for which we want to generate CRC.
   * @param operationName operation name for the attempted commit.
   * @param txnId transaction identifier.
   * @param oldVersionChecksum from previous commit (attemptVersion - 1).
   * @param oldSnapshot snapshot representing previous commit version (i.e. attemptVersion - 1),
   *                    None if not available.
   * @param actions used to incrementally compute new checksum.
   * @param ignoreAddFiles for transactions whose add file actions refer to already-existing files
   *                       e.g., [[DeltaOperations.ComputeStats]] transactions.
   * @return Either the new checksum or error code string if the checksum could not be computed
   *         incrementally due to some reason.
   */
  // scalastyle:off argcount
  private[delta] def computeNewChecksum(
      attemptVersion: Long,
      operationName: String,
      txnIdOpt: Option[String],
      oldVersionChecksum: VersionChecksum,
      oldSnapshot: Option[Snapshot],
      actions: Seq[Action],
      ignoreAddFiles: Boolean
  ) : Either[String, VersionChecksum] = {
    // scalastyle:on argcount
    oldSnapshot.foreach(s => require(s.version == (attemptVersion - 1)))
    var tableSizeBytes = oldVersionChecksum.tableSizeBytes
    var numFiles = oldVersionChecksum.numFiles
    var protocol = oldVersionChecksum.protocol
    var metadata = oldVersionChecksum.metadata

    var inCommitTimestamp : Option[Long] = None
    actions.foreach {
      case a: AddFile if !ignoreAddFiles =>
        tableSizeBytes += a.size
        numFiles += 1


      // extendedFileMetadata == true implies fields partitionValues, size, and tags are present
      case r: RemoveFile if r.extendedFileMetadata == Some(true) =>
        val size = r.size.get
        tableSizeBytes -= size
        numFiles -= 1


      case r: RemoveFile =>
        // Report the failure to usage logs.
        val msg = s"A remove action with a missing file size was detected in file ${r.path} " +
          "causing incremental commit to fallback to state reconstruction."
        recordDeltaEvent(
          this.deltaLog,
          "delta.checksum.compute",
          data = Map("error" -> msg))
        return Left("ENCOUNTERED_REMOVE_FILE_MISSING_SIZE")
      case p: Protocol =>
        protocol = p
      case m: Metadata =>
        metadata = m
      case ci: CommitInfo =>
        inCommitTimestamp = ci.inCommitTimestamp
      case _ =>
    }

    Right(VersionChecksum(
      txnId = txnIdOpt,
      tableSizeBytes = tableSizeBytes,
      numFiles = numFiles,
      numMetadata = 1,
      numProtocol = 1,
      inCommitTimestampOpt = inCommitTimestamp,
      metadata = metadata,
      protocol = protocol,
      setTransactions = None,
      domainMetadata = None,
      histogramOpt = None,
      allFiles = None
    ))
  }

}

object RecordChecksum {
  // Operations where we should ignore AddFiles in the incremental checksum computation.
  val operationNamesWhereAddFilesIgnoredForIncrementalCrc = Set(
    // The transaction that computes stats is special -- it re-adds files that already exist, in
    // order to update their min/max stats. We should not count those against the totals.
    DeltaOperations.ComputeStats(Seq.empty).name,
    // Backfill/Tagging re-adds existing AddFiles without changing the underlying data files.
    // Incremental commits should ignore backfill commits.
    DeltaOperations.RowTrackingBackfill().name
  )
}

/**
 * Read checksum files.
 */
trait ReadChecksum extends DeltaLogging { self: DeltaLog =>

  val logPath: Path
  private[delta] def store: LogStore

  private[delta] def readChecksum(
      version: Long,
      checksumFileStatusHintOpt: Option[FileStatus] = None): Option[VersionChecksum] = {
    recordDeltaOperation(self, "delta.readChecksum") {
      val checksumFilePath = FileNames.checksumFile(logPath, version)
      val verifiedChecksumFileStatusOpt =
        checksumFileStatusHintOpt.filter(_.getPath == checksumFilePath)
      var exception: Option[String] = None
      val content = try Some(
        verifiedChecksumFileStatusOpt
          .map(store.read(_, newDeltaHadoopConf()))
          .getOrElse(store.read(checksumFilePath, newDeltaHadoopConf()))
      ) catch {
        case NonFatal(e) =>
          // We expect FileNotFoundException; if it's another kind of exception, we still catch them
          // here but we log them in the checksum error event below.
          if (!e.isInstanceOf[FileNotFoundException]) {
            exception = Some(Utils.exceptionString(e))
          }
          None
      }

      if (content.isEmpty) {
        // We may not find the checksum file in two cases:
        //  - We just upgraded our Spark version from an old one
        //  - Race conditions where we commit a transaction, and before we can write the checksum
        //    this reader lists the new version, and uses it to create the snapshot.
        recordDeltaEvent(
          this,
          "delta.checksum.error.missing",
          data = Map("version" -> version) ++ exception.map("exception" -> _))

        return None
      }
      val checksumData = content.get
      if (checksumData.isEmpty) {
        recordDeltaEvent(
          this,
          "delta.checksum.error.empty",
          data = Map("version" -> version))
        return None
      }
      try {
        Option(JsonUtils.mapper.readValue[VersionChecksum](checksumData.head))
      } catch {
        case NonFatal(e) =>
          recordDeltaEvent(
            this,
            "delta.checksum.error.parsing",
            data = Map("exception" -> Utils.exceptionString(e)))
          None
      }
    }
  }
}

