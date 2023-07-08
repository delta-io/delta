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
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, RemoveFile}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions.{column, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.IGNORE_MISSING_FILES
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/** Base trait class for RESTORE. Defines command output schema and metrics. */
trait RestoreTableCommandBase {

  // RESTORE operation metrics
  val TABLE_SIZE_AFTER_RESTORE = "tableSizeAfterRestore"
  val NUM_OF_FILES_AFTER_RESTORE = "numOfFilesAfterRestore"
  val NUM_REMOVED_FILES = "numRemovedFiles"
  val NUM_RESTORED_FILES = "numRestoredFiles"
  val REMOVED_FILES_SIZE = "removedFilesSize"
  val RESTORED_FILES_SIZE = "restoredFilesSize"

  // SQL way column names for RESTORE command output
  private val COLUMN_TABLE_SIZE_AFTER_RESTORE = "table_size_after_restore"
  private val COLUMN_NUM_OF_FILES_AFTER_RESTORE = "num_of_files_after_restore"
  private val COLUMN_NUM_REMOVED_FILES = "num_removed_files"
  private val COLUMN_NUM_RESTORED_FILES = "num_restored_files"
  private val COLUMN_REMOVED_FILES_SIZE = "removed_files_size"
  private val COLUMN_RESTORED_FILES_SIZE = "restored_files_size"

  val outputSchema: Seq[Attribute] = Seq(
    AttributeReference(COLUMN_TABLE_SIZE_AFTER_RESTORE, LongType)(),
    AttributeReference(COLUMN_NUM_OF_FILES_AFTER_RESTORE, LongType)(),
    AttributeReference(COLUMN_NUM_REMOVED_FILES, LongType)(),
    AttributeReference(COLUMN_NUM_RESTORED_FILES, LongType)(),
    AttributeReference(COLUMN_REMOVED_FILES_SIZE, LongType)(),
    AttributeReference(COLUMN_RESTORED_FILES_SIZE, LongType)()
  )
}

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
    targetIdent: TableIdentifier)
  extends LeafRunnableCommand with DeltaCommand with RestoreTableCommandBase {

  override val output: Seq[Attribute] = outputSchema

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

        import org.apache.spark.sql.delta.implicits._

        // If either source version or destination version contains DVs,
        // we have to take them into account during deduplication.
        val targetMayHaveDVs = DeletionVectorUtils.deletionVectorsReadable(latestSnapshot)
        val sourceMayHaveDVs = DeletionVectorUtils.deletionVectorsReadable(snapshotToRestore)

        val normalizedSourceWithoutDVs = snapshotToRestoreFiles.mapPartitions { files =>
          files.map(file => (file, file.path))
        }.toDF("srcAddFile", "srcPath")
        val normalizedTargetWithoutDVs = latestSnapshotFiles.mapPartitions { files =>
          files.map(file => (file, file.path))
        }.toDF("tgtAddFile", "tgtPath")

        def addDVsToNormalizedDF(
          mayHaveDVs: Boolean,
          dvIdColumnName: String,
          dvAccessColumn: Column,
          normalizedDf: DataFrame): DataFrame = {
          if (mayHaveDVs) {
            normalizedDf.withColumn(
              dvIdColumnName,
              DeletionVectorDescriptor.uniqueIdExpression(dvAccessColumn))
          } else {
            normalizedDf.withColumn(dvIdColumnName, lit(null))
          }
        }

        val normalizedSource = addDVsToNormalizedDF(
          mayHaveDVs = sourceMayHaveDVs,
          dvIdColumnName = "srcDeletionVectorId",
          dvAccessColumn = column("srcAddFile.deletionVector"),
          normalizedDf = normalizedSourceWithoutDVs)

        val normalizedTarget = addDVsToNormalizedDF(
          mayHaveDVs = targetMayHaveDVs,
          dvIdColumnName = "tgtDeletionVectorId",
          dvAccessColumn = column("tgtAddFile.deletionVector"),
          normalizedDf = normalizedTargetWithoutDVs)

        val joinExprs =
          column("srcPath") === column("tgtPath") and
            // Use comparison operator where NULL == NULL
            column("srcDeletionVectorId") <=> column("tgtDeletionVectorId")

        val filesToAdd = normalizedSource
          .join(normalizedTarget, joinExprs, "left_anti")
          .select(column("srcAddFile").as[AddFile])
          .map(_.copy(dataChange = true))

        val filesToRemove = normalizedTarget
          .join(normalizedSource, joinExprs, "left_anti")
          .select(column("tgtAddFile").as[AddFile])
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

        val sourceProtocol = snapshotToRestore.protocol
        val targetProtocol = latestSnapshot.protocol
        // Only upgrade the protocol, never downgrade (unless allowed by flag), since that may break
        // time travel.
        val protocolDowngradeAllowed =
          conf.getConf(DeltaSQLConf.RESTORE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED)
        val newProtocol = if (protocolDowngradeAllowed) {
          sourceProtocol
        } else {
          sourceProtocol.merge(targetProtocol)
        }

        txn.commitLarge(
          spark,
          Iterator.single(newProtocol) ++ addActions ++ removeActions,
          DeltaOperations.Restore(version, timestamp),
          Map.empty,
          metrics.mapValues(_.toString).toMap)

        Seq(Row(
          metrics.get(TABLE_SIZE_AFTER_RESTORE),
          metrics.get(NUM_OF_FILES_AFTER_RESTORE),
          metrics.get(NUM_REMOVED_FILES),
          metrics.get(NUM_RESTORED_FILES),
          metrics.get(REMOVED_FILES_SIZE),
          metrics.get(RESTORED_FILES_SIZE)))
      }
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
  ): Map[String, Long] = {
    // scalastyle:off sparkimplicits
    import toAdd.sparkSession.implicits._
    // scalastyle:on sparkimplicits

    val (numRestoredFiles, restoredFilesSize) = toAdd
      .agg("size" -> "count", "size" -> "sum").as[(Long, Option[Long])].head()

    val (numRemovedFiles, removedFilesSize) = toRemove
      .agg("size" -> "count", "size" -> "sum").as[(Long, Option[Long])].head()

    Map(
      NUM_RESTORED_FILES -> numRestoredFiles,
      RESTORED_FILES_SIZE -> restoredFilesSize.getOrElse(0),
      NUM_REMOVED_FILES -> numRemovedFiles,
      REMOVED_FILES_SIZE -> removedFilesSize.getOrElse(0),
      NUM_OF_FILES_AFTER_RESTORE -> snapshot.numOfFiles,
      TABLE_SIZE_AFTER_RESTORE -> snapshot.sizeInBytes
    )
  }

  /* Prevent users from running restore to table version with missed
   * data files (manually deleted or vacuumed). Restoring to this version partially
   * is still possible if spark.sql.files.ignoreMissingFiles is set to true
   */
  private def checkSnapshotFilesAvailability(
    deltaLog: DeltaLog,
    files: Dataset[AddFile],
    version: Long): Unit = withDescription("missing files validation") {

    val spark: SparkSession = files.sparkSession

    val pathString = deltaLog.dataPath.toString
    val hadoopConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(deltaLog.newDeltaHadoopConf()))

    import org.apache.spark.sql.delta.implicits._

    val missedFiles = files
      .mapPartitions { files =>
        val path = new Path(pathString)
        val fs = path.getFileSystem(hadoopConf.value.value)
        files.filterNot(f => fs.exists(absolutePath(pathString, f.path)))
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

object RestoreTableCommand {
  // Op name used by RESTORE command
  val OP_NAME = "RESTORE"
}
