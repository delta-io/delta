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
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path

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
          logWarning(s"Failed to write the checksum for version: $version", e)
          stream.cancel()
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to write the checksum for version: $version", e)
    }
    recordDeltaEvent(
      deltaLog,
      opType = "delta.checksum.write",
      data = eventData)
  }
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

