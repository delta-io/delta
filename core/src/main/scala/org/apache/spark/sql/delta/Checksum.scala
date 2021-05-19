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

package org.apache.spark.sql.delta

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.util.Utils

/**
 * Stats calculated within a snapshot, which we store along individual transactions for
 * verification.
 *
 * @param tableSizeBytes The size of the table in bytes
 * @param numFiles Number of `AddFile` actions in the snapshot
 * @param numMetadata Number of `Metadata` actions in the snapshot
 * @param numProtocol Number of `Protocol` actions in the snapshot
 * @param numTransactions Number of `SetTransaction` actions in the snapshot
 */
case class VersionChecksum(
    tableSizeBytes: Long,
    numFiles: Long,
    numMetadata: Long,
    numProtocol: Long,
    numTransactions: Long)

/**
 * Record the state of the table as a checksum file along with a commit.
 */
trait RecordChecksum extends DeltaLogging {
  val deltaLog: DeltaLog
  protected def spark: SparkSession

  private lazy val writer =
    CheckpointFileManager.create(deltaLog.logPath, spark.sessionState.newHadoopConf())

  protected def writeChecksumFile(snapshot: Snapshot): Unit = {
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED)) {
      return
    }
    val version = snapshot.version
    val checksum = VersionChecksum(
      tableSizeBytes = snapshot.sizeInBytes,
      numFiles = snapshot.numOfFiles,
      numMetadata = snapshot.numOfMetadata,
      numProtocol = snapshot.numOfProtocol,
      numTransactions = snapshot.numOfSetTransactions)
    try {
      recordDeltaOperation(deltaLog, "delta.checksum.write") {
        val stream = writer.createAtomic(
          FileNames.checksumFile(deltaLog.logPath, version),
          overwriteIfPossible = false)
        try {
          val toWrite = JsonUtils.toJson(checksum) + "\n"
          stream.write(toWrite.getBytes(UTF_8))
          stream.close()
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to write the checksum for version: $version", e)
            stream.cancel()
        }
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to write the checksum for version: $version", e)
    }
  }
}

/**
 * Read checksum files.
 */
trait ReadChecksum extends DeltaLogging { self: DeltaLog =>

  val logPath: Path
  private[delta] def store: LogStore

  protected def readChecksum(version: Long): Option[VersionChecksum] = {
    val checksumFile = FileNames.checksumFile(logPath, version)

    var exception: Option[String] = None
    val content = try Some(store.read(checksumFile)) catch {
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

/**
 * Verify the state of the table using the checksum information.
 */
trait ValidateChecksum extends DeltaLogging { self: Snapshot =>

  def validateChecksum(): Unit = checksumOpt.foreach { checksum =>
    val mismatchStringOpt = checkMismatch(checksum)
    if (mismatchStringOpt.isDefined) {
      // Report the failure to usage logs.
      recordDeltaEvent(
        this.deltaLog,
        "delta.checksum.invalid",
        data = Map("error" -> mismatchStringOpt.get))
      // We get the active SparkSession, which may be different than the SparkSession of the
      // Snapshot that was created, since we cache `DeltaLog`s.
      val spark = SparkSession.getActiveSession.getOrElse {
        throw new IllegalStateException("Active SparkSession not set.")
      }
      val conf = DeltaSQLConf.DELTA_STATE_CORRUPTION_IS_FATAL
      if (spark.sessionState.conf.getConf(conf)) {
        throw new IllegalStateException(
          "The transaction log has failed integrity checks. We recommend you contact " +
            s"Databricks support for assistance. To disable this check, set ${conf.key} to " +
            s"false. Failed verification at version $version of:\n${mismatchStringOpt.get}"
        )
      }
    }
  }

  private def checkMismatch(checksum: VersionChecksum): Option[String] = {
    val result = new ArrayBuffer[String]()
    def compare(expected: Long, found: Long, title: String): Unit = {
      if (expected != found) {
        result += s"$title - Expected: $expected Computed: $found"
      }
    }
    compare(checksum.tableSizeBytes, computedState.sizeInBytes, "Table size (bytes)")
    compare(checksum.numFiles, computedState.numOfFiles, "Number of files")
    compare(checksum.numMetadata, computedState.numOfMetadata, "Metadata updates")
    compare(checksum.numProtocol, computedState.numOfProtocol, "Protocol updates")
    compare(checksum.numTransactions, computedState.numOfSetTransactions, "Transactions")
    if (result.isEmpty) None else Some(result.mkString("\n"))
  }
}
