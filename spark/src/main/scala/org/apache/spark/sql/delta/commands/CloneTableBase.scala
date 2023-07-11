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

// scalastyle:off import.ordering.noEmptyLine
import java.io.Closeable
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Clock, SerializableConfiguration}
// scalastyle:on import.ordering.noEmptyLine

/**
 * An interface of the source table to be cloned from.
 */
trait CloneSource extends Closeable {
  /** The format of the source table */
  def format: String

  /** The source table's protocol */
  def protocol: Protocol

  /** A system clock */
  def clock: Clock

  /** The source table name */
  def name: String

  /** The path of the source table */
  def dataPath: Path

  /** The source table schema */
  def schema: StructType

  /** The catalog table of the source table, if exists */
  def catalogTable: Option[CatalogTable]

  /** The time travel spec of the source table, if exists */
  def timeTravelOpt: Option[DeltaTimeTravelSpec]

  /** A snapshot of the source table, if exists */
  def snapshot: Option[Snapshot]

  /** The metadata of the source table */
  def metadata: Metadata

  /** All of the files present in the source table */
  def allFiles: Dataset[AddFile]

  /** Total size of data files in bytes */
  def sizeInBytes: Long

  /** Total number of data files */
  def numOfFiles: Long

  /** Describe this clone source */
  def description: String
}

// Clone source table formats
object CloneSourceFormat {
  val DELTA = "Delta"
  val ICEBERG = "Iceberg"
  val PARQUET = "Parquet"
  val UNKNOWN = "Unknown"
}

trait CloneTableBaseUtils extends DeltaLogging
{

  import CloneTableCommand._

  /** Make a map of operation metrics for the executed command for DeltaLog commits */
  protected def getOperationMetricsForDeltaLog(
      opMetrics: SnapshotOverwriteOperationMetrics): Map[String, Long] = {
    Map(
      SOURCE_TABLE_SIZE -> opMetrics.sourceSnapshotSizeInBytes,
      SOURCE_NUM_OF_FILES -> opMetrics.sourceSnapshotFileCount,
      NUM_REMOVED_FILES -> 0L,
      NUM_COPIED_FILES -> 0L,
      REMOVED_FILES_SIZE -> 0L,
      COPIED_FILES_SIZE -> 0L
    )
  }

  /**
   * Make a map of operation metrics for the executed command for recording events.
   * Any command can extend to overwrite or add new metrics
   */
  protected def getOperationMetricsForEventRecord(
      opMetrics: SnapshotOverwriteOperationMetrics): Map[String, Long] =
    getOperationMetricsForDeltaLog(opMetrics)

  /** Make a output Seq[Row] of metrics for the executed command */
  protected def getOutputSeq(operationMetrics: Map[String, Long]): Seq[Row]

  protected def checkColumnMappingMode(beforeMetadata: Metadata, afterMetadata: Metadata): Unit = {
    val beforeColumnMappingMode = beforeMetadata.columnMappingMode
    val afterColumnMappingMode = afterMetadata.columnMappingMode
    // can't switch column mapping mode
    if (beforeColumnMappingMode != afterColumnMappingMode) {
      throw DeltaErrors.changeColumnMappingModeNotSupported(
        beforeColumnMappingMode.name, afterColumnMappingMode.name)
    }
  }

  // Return a copy of the AddFiles with path being absolutized, indicating a SHALLOW CLONE
  protected def handleNewDataFiles(
      opName: String,
      datasetOfNewFilesToAdd: Dataset[AddFile],
      qualifiedSourceTableBasePath: String,
      destTable: DeltaLog
  ): Dataset[AddFile] = {
    recordDeltaOperation(destTable, s"delta.${opName.toLowerCase()}.makeAbsolute") {
      val absolutePaths = DeltaFileOperations.makePathsAbsolute(
        qualifiedSourceTableBasePath,
        datasetOfNewFilesToAdd)
      absolutePaths
    }
  }
}

abstract class CloneTableBase(
    sourceTable: CloneSource,
    tablePropertyOverrides: Map[String, String],
    targetPath: Path)
  extends LeafRunnableCommand with CloneTableBaseUtils
{

  import CloneTableBase._
  def dataChangeInFileAction: Boolean = true

  /**
   * Run the clone command
   *
   * @param spark [[SparkSession]] to use
   * @param opName Name of the operation used in log4j logs
   * @param deltaOperation [[DeltaOperations.Operation]] to use when commit changes to DeltaLog
   * @param fsOptions Extra filesystem options passed to target DeltaLog.
   * @return
   */
  def runInternal(
      spark: SparkSession,
      opName: String,
      hdpConf: Configuration,
      deltaOperation: DeltaOperations.Operation,
      fsOptions: Map[String, String]): Seq[Row] = {
    val targetFs = targetPath.getFileSystem(hdpConf)
    val qualifiedTarget = targetFs.makeQualified(targetPath).toString
    val qualifiedSource = {
      val sourcePath = sourceTable.dataPath
      val sourceFs = sourcePath.getFileSystem(hdpConf)
      sourceFs.makeQualified(sourcePath).toString
    }

    val destinationTable = DeltaLog.forTable(spark, targetPath, fsOptions)

    val txn = destinationTable.startTransaction()
    if (txn.readVersion < 0) {
      destinationTable.createLogDirectory()
    }

    val (
      datasetOfNewFilesToAdd
      ) = {
      // Make sure target table is empty before running clone
      if (txn.snapshot.allFiles.count() > 0) {
        throw DeltaErrors.cloneReplaceNonEmptyTable
      }
      sourceTable.allFiles
    }

    val metadataToUpdate = determineTargetMetadata(txn.snapshot, opName)
    // Don't merge in the default properties when cloning, or we'll end up with different sets of
    // properties between source and target.
    txn.updateMetadata(metadataToUpdate, ignoreDefaultProperties = true)

    val datasetOfAddedFileList = handleNewDataFiles(
      opName,
      datasetOfNewFilesToAdd,
      qualifiedSource,
      destinationTable)

    val addedFileList = datasetOfAddedFileList.collectAsList()

    val (addedFileCount, addedFilesSize) =
      (addedFileList.size.toLong, totalDataSize(addedFileList.iterator))

    val operationTimestamp = sourceTable.clock.getTimeMillis()


    val newProtocol = determineTargetProtocol(spark, txn, opName)

    try {
      var actions = Iterator.single(newProtocol) ++
        addedFileList.iterator.asScala.map { fileToCopy =>
          val copiedFile = fileToCopy.copy(dataChange = dataChangeInFileAction)
          opName match {
            case CloneTableCommand.OP_NAME =>
              // CLONE does not preserve Row IDs and Commit Versions
              copiedFile.copy(baseRowId = None, defaultRowCommitVersion = None)
            case RestoreTableCommand.OP_NAME =>
              // RESTORE preserves Row IDs and Commit Versions
              copiedFile
          }
        }
      val sourceName = sourceTable.name
      // Override source table metadata with user-defined table properties
      val context = Map[String, String]()
      val isReplaceDelta = txn.readVersion >= 0

      val opMetrics = SnapshotOverwriteOperationMetrics(
        sourceTable.sizeInBytes,
        sourceTable.numOfFiles,
        addedFileCount,
        addedFilesSize)
      val commitOpMetrics = getOperationMetricsForDeltaLog(opMetrics)

        recordDeltaOperation(destinationTable, s"delta.${opName.toLowerCase()}.commit") {
          txn.commitLarge(
            spark,
            actions,
            deltaOperation,
            context,
            commitOpMetrics.mapValues(_.toString()).toMap)
        }

      val cloneLogData = getOperationMetricsForEventRecord(opMetrics) ++ Map(
        SOURCE -> sourceName,
        SOURCE_FORMAT -> sourceTable.format,
        SOURCE_PATH -> qualifiedSource,
        TARGET -> qualifiedTarget,
        PARTITION_BY -> sourceTable.metadata.partitionColumns,
        IS_REPLACE_DELTA -> isReplaceDelta) ++
        sourceTable.snapshot.map(s => SOURCE_VERSION -> s.version)
      recordDeltaEvent(destinationTable, s"delta.${opName.toLowerCase()}", data = cloneLogData)

      getOutputSeq(commitOpMetrics)
    } finally {
      sourceTable.close()
    }
  }

  /**
   * Prepares the source metadata by making it compatible with the existing target metadata.
   */
  private def prepareSourceMetadata(
      targetSnapshot: SnapshotDescriptor,
      opName: String): Metadata = {
      sourceTable.metadata.copy(
        id = UUID.randomUUID().toString,
        name = targetSnapshot.metadata.name,
        description = targetSnapshot.metadata.description)
  }

  /**
   * Verifies metadata invariants.
   */
  private def verifyMetadataInvariants(
      targetSnapshot: SnapshotDescriptor,
      updatedMetadataWithOverrides: Metadata): Unit = {
    // TODO: we have not decided on how to implement switching column mapping modes
    //  so we block this feature for now
    // 1. Validate configuration overrides
    //    this checks if columnMapping.maxId is unexpected set in the properties
    DeltaConfigs.validateConfigurations(tablePropertyOverrides)
    // 2. Check for column mapping mode conflict with the source metadata w/ tablePropertyOverrides
    checkColumnMappingMode(sourceTable.metadata, updatedMetadataWithOverrides)
    // 3. Checks for column mapping mode conflicts with existing metadata if there's any
    if (targetSnapshot.version >= 0) {
      checkColumnMappingMode(targetSnapshot.metadata, updatedMetadataWithOverrides)
    }
  }

  /**
   * Determines the expected metadata of the target.
   */
  private def determineTargetMetadata(
      targetSnapshot: SnapshotDescriptor,
      opName: String) : Metadata = {
    var metadata = prepareSourceMetadata(targetSnapshot, opName)
    val validatedConfigurations = DeltaConfigs.validateConfigurations(tablePropertyOverrides)
    // Merge source configuration and table property overrides
    metadata = metadata.copy(
      configuration = metadata.configuration ++ validatedConfigurations)
    verifyMetadataInvariants(targetSnapshot, metadata)
    metadata
  }

  /**
   * Determines the final protocol of the target. The metadata of the `txn` must be updated before
   * determining the protocol.
   */
  private def determineTargetProtocol(
      spark: SparkSession,
      txn: OptimisticTransaction,
      opName: String): Protocol = {
    val sourceProtocol = sourceTable.protocol
    // Pre-transaction version of the target table.
    val targetProtocol = txn.snapshot.protocol
    // Overriding properties during the CLONE can change the minimum required protocol for target.
    // We need to look at the metadata of the transaction to see the entire set of table properties
    // for the post-transaction state and decide a version based on that. We also need to re-add
    // the table property overrides as table features set by it won't be in the transaction
    // metadata anymore.
    val validatedConfigurations = DeltaConfigs.validateConfigurations(tablePropertyOverrides)
    val configWithOverrides = txn.metadata.configuration ++ validatedConfigurations
    val metadataWithOverrides = txn.metadata.copy(configuration = configWithOverrides)
    var (minReaderVersion, minWriterVersion, enabledFeatures) =
      Protocol.minProtocolComponentsFromMetadata(spark, metadataWithOverrides)

    // Only upgrade the protocol, never downgrade (unless allowed by flag), since that may break
    // time travel.
    val protocolDowngradeAllowed =
    conf.getConf(DeltaSQLConf.RESTORE_TABLE_PROTOCOL_DOWNGRADE_ALLOWED) ||
      // It's not a real downgrade if the table doesn't exist before the CLONE.
      txn.snapshot.version == -1

    if (protocolDowngradeAllowed) {
      minReaderVersion = minReaderVersion.max(sourceProtocol.minReaderVersion)
      minWriterVersion = minWriterVersion.max(sourceProtocol.minWriterVersion)
      val minProtocol = Protocol(minReaderVersion, minWriterVersion).withFeatures(enabledFeatures)
      sourceProtocol.merge(minProtocol)
    } else {
      // Take the maximum of all protocol versions being merged to ensure that table features
      // from table property overrides are correctly added to the table feature list or are only
      // implicitly enabled
      minReaderVersion =
        Seq(targetProtocol.minReaderVersion, sourceProtocol.minReaderVersion, minReaderVersion).max
      minWriterVersion = Seq(
        targetProtocol.minWriterVersion, sourceProtocol.minWriterVersion, minWriterVersion).max
      val minProtocol = Protocol(minReaderVersion, minWriterVersion).withFeatures(enabledFeatures)
      targetProtocol.merge(sourceProtocol, minProtocol)
    }
  }
}

object CloneTableBase extends Logging {

  val SOURCE = "source"
  val SOURCE_FORMAT = "sourceFormat"
  val SOURCE_PATH = "sourcePath"
  val SOURCE_VERSION = "sourceVersion"
  val TARGET = "target"
  val IS_REPLACE_DELTA = "isReplaceDelta"
  val PARTITION_BY = "partitionBy"

  /** Utility method returns the total size of all files in the given iterator */
  private def totalDataSize(fileList: java.util.Iterator[AddFile]): Long = {
    var totalSize = 0L
    fileList.asScala.foreach { f =>
      totalSize += f.size
    }
    totalSize
  }
}

/**
 * Metrics of snapshot overwrite operation.
 * @param sourceSnapshotSizeInBytes Total size of the data in the source snapshot.
 * @param sourceSnapshotFileCount Number of data files in the source snapshot.
 * @param destSnapshotAddedFileCount Number of new data files added to the destination
 *                                   snapshot as part of the execution.
 * @param destSnapshotAddedFilesSizeInBytes Total size (in bytes) of the data files that were
 *                                          added to the destination snapshot.
 */
case class SnapshotOverwriteOperationMetrics(
    sourceSnapshotSizeInBytes: Long,
    sourceSnapshotFileCount: Long,
    destSnapshotAddedFileCount: Long,
    destSnapshotAddedFilesSizeInBytes: Long)
