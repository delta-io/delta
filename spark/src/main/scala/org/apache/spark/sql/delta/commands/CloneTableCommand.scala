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
import java.io.FileNotFoundException

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTimeTravelSpec, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.DeltaOperations.Clone
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.Protocol.extractAutomaticallyEnabledFeatures
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.convert.{ConvertTargetTable, ConvertUtils}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.util.{Clock, SerializableConfiguration, SystemClock}
// scalastyle:on import.ordering.noEmptyLine

/**
 * Clones a Delta table to a new location with a new table id.
 * The clone can be performed as a shallow clone (i.e. shallow = true),
 * where we do not copy the files, but just point to them.
 * If a table exists at the given targetPath, that table will be replaced.
 *
 * @param sourceTable is the table to be cloned
 * @param targetIdent destination table identifier to clone to
 * @param tablePropertyOverrides user-defined table properties that should override any properties
 *                       with the same key from the source table
 * @param targetPath the actual destination
 */
case class CloneTableCommand(
    sourceTable: CloneSource,
    targetIdent: TableIdentifier,
    tablePropertyOverrides: Map[String, String],
    targetPath: Path
) extends CloneTableBase(sourceTable, tablePropertyOverrides, targetPath) {

  import CloneTableCommand._


  /** Return the CLONE command output from the execution metrics */
  override protected def getOutputSeq(operationMetrics: Map[String, Long]): Seq[Row] = {
    Seq(Row(
      operationMetrics.get(SOURCE_TABLE_SIZE),
      operationMetrics.get(SOURCE_NUM_OF_FILES),
      operationMetrics.get(NUM_REMOVED_FILES),
      operationMetrics.get(NUM_COPIED_FILES),
      operationMetrics.get(REMOVED_FILES_SIZE),
      operationMetrics.get(COPIED_FILES_SIZE)
    ))
  }

  /**
   * Handles the transaction logic for the CLONE command.
   * @param txn [[OptimisticTransaction]] to use for the commit to the target table.
   * @param targetDeltaLog [[DeltaLog]] of the target table.
   * @return
   */
  def handleClone(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      targetDeltaLog: DeltaLog,
      commandMetrics: Option[Map[String, SQLMetric]] = None): Seq[Row] = {
    if (!targetPath.isAbsolute) {
      throw DeltaErrors.cloneOnRelativePath(targetIdent.toString)
    }

    /** Log clone command information */
    logInfo(log"Cloning ${MDC(DeltaLogKeys.TABLE_DESC, sourceTable.description)} to " +
      log"${MDC(DeltaLogKeys.PATH, targetPath)}")

    // scalastyle:off deltahadoopconfiguration
    val hdpConf = sparkSession.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    if (!sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_CLONE_REPLACE_ENABLED)) {
      val targetFs = targetPath.getFileSystem(hdpConf)
      try {
        val subFiles = targetFs.listStatus(targetPath)
        if (subFiles.nonEmpty) {
          throw DeltaErrors.cloneReplaceUnsupported(targetIdent)
        }
      } catch {
        case _: FileNotFoundException => // we want the path to not exist
          targetFs.mkdirs(targetPath)
      }
    }

    handleClone(
      sparkSession,
      txn,
      targetDeltaLog,
      hdpConf = hdpConf,
      deltaOperation = Clone(
        sourceTable.name, sourceTable.snapshot.map(_.version).getOrElse(-1)
      ),
      commandMetrics = commandMetrics)
  }
}

object CloneTableCommand {
  // Names of the metrics - added to the Delta commit log as part of Clone transaction
  val SOURCE_TABLE_SIZE = "sourceTableSize"
  val SOURCE_NUM_OF_FILES = "sourceNumOfFiles"
  val NUM_REMOVED_FILES = "numRemovedFiles"
  val NUM_COPIED_FILES = "numCopiedFiles"
  val REMOVED_FILES_SIZE = "removedFilesSize"
  val COPIED_FILES_SIZE = "copiedFilesSize"

  // SQL way column names for metrics in command execution output
  private val COLUMN_SOURCE_TABLE_SIZE = "source_table_size"
  private val COLUMN_SOURCE_NUM_OF_FILES = "source_num_of_files"
  private val COLUMN_NUM_REMOVED_FILES = "num_removed_files"
  private val COLUMN_NUM_COPIED_FILES = "num_copied_files"
  private val COLUMN_REMOVED_FILES_SIZE = "removed_files_size"
  private val COLUMN_COPIED_FILES_SIZE = "copied_files_size"

  val output: Seq[Attribute] = Seq(
    AttributeReference(COLUMN_SOURCE_TABLE_SIZE, LongType)(),
    AttributeReference(COLUMN_SOURCE_NUM_OF_FILES, LongType)(),
    AttributeReference(COLUMN_NUM_REMOVED_FILES, LongType)(),
    AttributeReference(COLUMN_NUM_COPIED_FILES, LongType)(),
    AttributeReference(COLUMN_REMOVED_FILES_SIZE, LongType)(),
    AttributeReference(COLUMN_COPIED_FILES_SIZE, LongType)()
  )
}

/** A delta table source to be cloned from */
class CloneDeltaSource(
    val sourceTable: DeltaTableV2,
    sourceSnapshot: Snapshot)
  extends CloneSource {

  def this(sourceTable: DeltaTableV2) = this(sourceTable, sourceTable.getFreshSnapshot())

  private def deltaLog = sourceSnapshot.deltaLog

  def format: String = CloneSourceFormat.DELTA

  def protocol: Protocol = sourceSnapshot.protocol

  def clock: Clock = deltaLog.clock

  def name: String = sourceTable.name()

  def dataPath: Path = deltaLog.dataPath

  def schema: StructType = sourceTable.schema()

  def catalogTable: Option[CatalogTable] = sourceTable.catalogTable

  def timeTravelOpt: Option[DeltaTimeTravelSpec] = sourceTable.timeTravelOpt

  def snapshot: Option[Snapshot] = Some(sourceSnapshot)

  def metadata: Metadata = sourceSnapshot.metadata

  def allFiles: Dataset[AddFile] = sourceSnapshot.allFiles

  def sizeInBytes: Long = sourceSnapshot.sizeInBytes

  def numOfFiles: Long = sourceSnapshot.numOfFiles

  def description: String = s"${format} table ${name} at version ${sourceSnapshot.version}"

  override def close(): Unit = {}
}

/** A convertible non-delta table source to be cloned from */
abstract class CloneConvertedSource(spark: SparkSession) extends CloneSource {

  // The converter which produces delta metadata from non-delta table, child class must implement
  // this converter.
  protected def convertTargetTable: ConvertTargetTable

  def format: String = CloneSourceFormat.UNKNOWN

  def protocol: Protocol = {
    // This is quirky but necessary to add table features such as column mapping if the default
    // protocol version supports table features.
    Protocol().withFeatures(extractAutomaticallyEnabledFeatures(spark, metadata, Protocol()))
  }

  override val clock: Clock = new SystemClock()

  def dataPath: Path = new Path(convertTargetTable.fileManifest.basePath)

  def schema: StructType = convertTargetTable.tableSchema

  def timeTravelOpt: Option[DeltaTimeTravelSpec] = None

  def snapshot: Option[Snapshot] = None

  override lazy val metadata: Metadata = {
    val conf = catalogTable
      // Hive adds some transient table properties which should be ignored
      .map(_.properties.filterKeys(_ != "transient_lastDdlTime").toMap)
      .foldRight(convertTargetTable.properties.toMap)(_ ++ _)

    {
      Metadata(
        schemaString = convertTargetTable.tableSchema.json,
        partitionColumns = convertTargetTable.partitionSchema.fieldNames,
        configuration = conf,
        createdTime = Some(System.currentTimeMillis()))
    }
  }

  override lazy val allFiles: Dataset[AddFile] = {
    import org.apache.spark.sql.delta.implicits._

    // scalastyle:off deltahadoopconfiguration
    val serializableConf = new SerializableConfiguration(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
    val baseDir = dataPath.toString
    val conf = spark.sparkContext.broadcast(serializableConf)
    val partitionSchema = convertTargetTable.partitionSchema

    {
      convertTargetTable.fileManifest.allFiles.mapPartitions { targetFile =>
        val basePath = new Path(baseDir)
        val fs = basePath.getFileSystem(conf.value.value)
        targetFile.map(ConvertUtils.createAddFile(
          _, basePath, fs, SQLConf.get, Some(partitionSchema)))
      }
    }
  }

  private lazy val fileStats = allFiles.select(
      coalesce(sum("size"), lit(0L)), count(new Column("*"))).first()

  def sizeInBytes: Long = fileStats.getLong(0)

  def numOfFiles: Long = fileStats.getLong(1)

  def description: String = s"${format} table ${name}"

  override def close(): Unit = convertTargetTable.fileManifest.close()
}

/**
 * A parquet table source to be cloned from
 */
case class CloneParquetSource(
    tableIdentifier: TableIdentifier,
    override val catalogTable: Option[CatalogTable],
    spark: SparkSession) extends CloneConvertedSource(spark) {

  override lazy val convertTargetTable: ConvertTargetTable = {
    val baseDir = catalogTable.map(_.location.toString).getOrElse(tableIdentifier.table)
    ConvertUtils.getParquetTable(spark, baseDir, catalogTable, None)
  }

  override def format: String = CloneSourceFormat.PARQUET

  override def name: String = catalogTable.map(_.identifier.unquotedString)
    .getOrElse(s"parquet.`${tableIdentifier.table}`")
}

/**
 * A iceberg table source to be cloned from
 */
case class CloneIcebergSource(
  tableIdentifier: TableIdentifier,
  sparkTable: Option[Table],
  tableSchema: Option[StructType],
  spark: SparkSession) extends CloneConvertedSource(spark) {

  override lazy val convertTargetTable: ConvertTargetTable =
    ConvertUtils.getIcebergTable(spark, tableIdentifier.table, sparkTable, tableSchema)

  override def format: String = CloneSourceFormat.ICEBERG

  override def name: String =
    sparkTable.map(_.name()).getOrElse(s"iceberg.`${tableIdentifier.table}`")

  override def catalogTable: Option[CatalogTable] = None
}
