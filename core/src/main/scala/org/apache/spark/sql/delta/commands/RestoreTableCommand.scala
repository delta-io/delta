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

package org.apache.spark.sql.delta.commands

import java.sql.Timestamp

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, RemoveFile}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.IGNORE_MISSING_FILES
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * Perform restore of delta table to a specified version or timestamp
 *
 * Algorithm:
 * 1) Read the latest snapshot of the table.
 * 2) Read snapshot for version or timestamp to restore
 * 3) Compute files available in snapshot for restoring (files were removed by some commit)
 * but missed in the latest. Add these files into commit as AddFile action.
 * 4) Compute files available in the latest snapshot (files were added after version to restore)
 * but missed in the snapshot to restore. Add these files into commit as RemoveFile action.
 * 5) If SQLConf.IGNORE_MISSING_FILES option is false (default value) check availability of AddFile
 * in file system.
 * 6) Commit metadata, Protocol, all RemoveFile and AddFile actions
 * into delta log using `commitLarge` (commit will be failed in case of parallel transaction)
 * 7) If table was modified in parallel then ignore restore and raise exception.
 *
 */
case class RestoreTableCommand(
    sourceTable: DeltaTableV2,
    targetIdent: TableIdentifier) extends LeafRunnableCommand with DeltaCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val deltaLog = sourceTable.deltaLog
    val version = sourceTable.timeTravelOpt.get.version
    val timestamp = getTimestamp()
    recordDeltaOperation(deltaLog, "delta.restore") {
      require(version.isEmpty ^ timestamp.isEmpty,
        "Either the version or timestamp should be provided for restore")

      val versionToRestore = version.getOrElse {
        deltaLog
          .history
          .getActiveCommitAtTime(parseStringToTs(timestamp), canReturnLastCommit = true)
          .version
      }

      val latestVersion = deltaLog.update().version

      require(versionToRestore < latestVersion, s"Version to restore ($versionToRestore)" +
        s"should be less then last available version ($latestVersion)")

      deltaLog.withNewTransaction { txn =>
        val latestSnapshot = txn.snapshot
        val snapshotToRestore = deltaLog.getSnapshotAt(versionToRestore)
        val latestSnapshotFiles = latestSnapshot.allFiles
        val snapshotToRestoreFiles = snapshotToRestore.allFiles

        import spark.implicits._

        val filesToAdd = snapshotToRestoreFiles
          .join(
            latestSnapshotFiles,
            snapshotToRestoreFiles("path") === latestSnapshotFiles("path"),
            "left_anti")
          .as[AddFile]
          .map(_.copy(dataChange = true))

        val filesToRemove = latestSnapshotFiles
          .join(
            snapshotToRestoreFiles,
            latestSnapshotFiles("path") === snapshotToRestoreFiles("path"),
            "left_anti")
          .as[AddFile]
          .map(_.removeWithTimestamp())

        val ignoreMissingFiles = spark
            .sessionState
            .conf
            .getConf(IGNORE_MISSING_FILES)

        if (!ignoreMissingFiles) {
          checkSnapshotFilesAvailability(deltaLog, filesToAdd, versionToRestore)
        }

          // Commit files, metrics, protocol and metadata to delta log
        val metrics = withDescription("metrics") {
          computeMetrics(filesToAdd, filesToRemove, snapshotToRestore)
        }
        val addActions = withDescription("add actions") {
          filesToAdd.toLocalIterator().asScala
        }
        val removeActions = withDescription("remove actions") {
          filesToRemove.toLocalIterator().asScala
        }

        txn.updateMetadata(snapshotToRestore.metadata)

        commitLarge(
          spark,
          txn,
          addActions ++ removeActions,
          DeltaOperations.Restore(version, timestamp),
          Map.empty,
          metrics)
      }

      Seq.empty[Row]
    }
  }

  private def withDescription[T](action: String)(f: => T): T =
    withStatusCode("DELTA",
      s"RestoreTableCommand: compute $action  (table path ${sourceTable.deltaLog.dataPath})") {
    f
  }

  private def parseStringToTs(timestamp: Option[String]): Timestamp = {
    Try {
      timestamp.flatMap { tsStr =>
        val tz = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
        val utfStr = UTF8String.fromString(tsStr)
        DateTimeUtils.stringToTimestamp(utfStr, tz)
      }
    } match {
      case Success(Some(tsMicroseconds)) => new Timestamp(tsMicroseconds / 1000)
      case _ => throw DeltaErrors.timestampInvalid(Literal(timestamp.get))
    }
  }

  private def computeMetrics(
    toAdd: Dataset[AddFile],
    toRemove: Dataset[RemoveFile],
    snapshot: Snapshot
  ): Map[String, String] = {
    import toAdd.sparkSession.implicits._

    val (numRestoredFiles, restoredFilesSize) = toAdd
      .agg("size" -> "count", "size" -> "sum").as[(Long, Option[Long])].head()

    val (numRemovedFiles, removedFilesSize) = toRemove
      .agg("size" -> "count", "size" -> "sum").as[(Long, Option[Long])].head()

    Map(
      "numRestoredFiles" -> numRestoredFiles,
      "restoredFilesSize" -> restoredFilesSize.getOrElse(0),
      "numRemovedFiles" -> numRemovedFiles,
      "removedFilesSize" -> removedFilesSize.getOrElse(0),
      "numOfFilesAfterRestore" -> snapshot.numOfFiles,
      "tableSizeAfterRestore" -> snapshot.sizeInBytes
    ).mapValues(_.toString).toMap
  }

  /* Prevent users from running restore to table version with missed
   * data files (manually deleted or vacuumed). Restoring to this version partially
   * is still possible if spark.sql.files.ignoreMissingFiles is set to true
   */
  private def checkSnapshotFilesAvailability(
    deltaLog: DeltaLog,
    files: Dataset[AddFile],
    version: Long): Unit = withDescription("missing files validation") {

    implicit val spark: SparkSession = files.sparkSession

    val path = deltaLog.dataPath
    val hadoopConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(deltaLog.newDeltaHadoopConf()))

    import spark.implicits._

    val missedFiles = files
      .mapPartitions { files =>
        val fs = path.getFileSystem(hadoopConf.value.value)
        val pathStr = path.toUri.getPath
        files.filterNot(f => fs.exists(absolutePath(pathStr, f.path)))
      }
      .map(_.path)
      .head(100)

    if (missedFiles.nonEmpty) {
      throw DeltaErrors.restoreMissedDataFilesError(missedFiles, version)
    }
  }

  /** If available get the timestamp referring to a snapshot in the source table timeline */
  private def getTimestamp(): Option[String] = {
    if (sourceTable.timeTravelOpt.get.timestamp.isDefined) {
      Some(sourceTable.timeTravelOpt.get.getTimestamp(conf).toString)
    } else {
      None
    }
  }
}
