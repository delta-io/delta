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

import java.util.UUID

import scala.collection.generic.Sizing

import org.apache.spark.sql.catalyst.expressions.aggregation.BitmapAggregator
import org.apache.spark.sql.delta.{DeltaLog, DeltaParquetFileFormat, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.DeltaParquetFileFormat._
import org.apache.spark.sql.delta.actions.{AddFile, DeletionVectorDescriptor, FileAction}
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, RoaringBitmapArrayFormat, StoredBitmap}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.StatsCollectionUtils
import org.apache.spark.sql.delta.storage.dv.DeletionVectorStore
import org.apache.spark.sql.delta.util.{BinPackingIterator, DeltaEncoder, PathWithFileSystem, Utils => DeltaUtils}
import org.apache.spark.sql.delta.util.DeltaFileOperations.absolutePath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.FileFormat.{FILE_PATH, METADATA_NAME}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{SerializableConfiguration, Utils => SparkUtils}


/**
 * Contains utility classes and method for performing DML operations with Deletion Vectors.
 */
object DMLWithDeletionVectorsHelper extends DeltaCommand {
  val SUPPORTED_DML_COMMANDS: Seq[String] = Seq("DELETE", "UPDATE")

  /**
   * Creates a DataFrame that can be used to scan for rows matching the condition in the given
   * files. Generally the given file list is a pruned file list using the stats based pruning.
   */
  def createTargetDfForScanningForMatches(
      spark: SparkSession,
      target: LogicalPlan,
      fileIndex: TahoeFileIndex): DataFrame = {
    Dataset.ofRows(spark, replaceFileIndex(spark, target, fileIndex))
  }

  /**
   * Replace the file index in a logical plan and return the updated plan.
   * It's a common pattern that, in Delta commands, we use data skipping to determine a subset of
   * files that can be affected by the command, so we replace the whole-table file index in the
   * original logical plan with a new index of potentially affected files, while everything else in
   * the original plan, e.g., resolved references, remain unchanged.
   *
   * In addition we also request a metadata column and a row index column from the Scan to help
   * generate the Deletion Vectors. When predicate pushdown is enabled, we only request the
   * metadata column. This is because we can utilize _metadata.row_index instead of generating a
   * custom one.
   *
   * @param spark the active spark session
   * @param target the logical plan in which we replace the file index
   * @param fileIndex the new file index
   */
  private def replaceFileIndex(
      spark: SparkSession,
      target: LogicalPlan,
      fileIndex: TahoeFileIndex): LogicalPlan = {
    val useMetadataRowIndex =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX)
    // This is only used when predicate pushdown is disabled.
    val rowIndexCol = AttributeReference(ROW_INDEX_COLUMN_NAME, ROW_INDEX_STRUCT_FIELD.dataType)()

    var fileMetadataCol: AttributeReference = null

    val newTarget = target.transformUp {
      case l @ LogicalRelation(
        hfsr @ HadoopFsRelation(_, _, _, _, format: DeltaParquetFileFormat, _), _, _, _) =>
        fileMetadataCol = format.createFileMetadataCol()
        // Take the existing schema and add additional metadata columns
        if (useMetadataRowIndex) {
          l.copy(
            relation = hfsr.copy(location = fileIndex)(hfsr.sparkSession),
            output = l.output :+ fileMetadataCol)
        } else {
          val newDataSchema =
            StructType(hfsr.dataSchema).add(ROW_INDEX_STRUCT_FIELD)
          val finalOutput = l.output ++ Seq(rowIndexCol, fileMetadataCol)
          // Disable splitting and filter pushdown in order to generate the row-indexes.
          val newFormat = format.copy(optimizationsEnabled = false)
          val newBaseRelation = hfsr.copy(
            location = fileIndex,
            dataSchema = newDataSchema,
            fileFormat = newFormat)(hfsr.sparkSession)

          l.copy(relation = newBaseRelation, output = finalOutput)
        }
      case p @ Project(projectList, _) =>
        if (fileMetadataCol == null) {
          throw new IllegalStateException("File metadata column is not yet created.")
        }
        val rowIndexColOpt = if (useMetadataRowIndex) None else Some(rowIndexCol)
        val additionalColumns = Seq(fileMetadataCol) ++ rowIndexColOpt
        p.copy(projectList = projectList ++ additionalColumns)
    }
    newTarget
  }

  /**
   * Find the target table files that contain rows that satisfy the condition and a DV attached
   * to each file that indicates a the rows marked as deleted from the file
   */
  def findTouchedFiles(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      hasDVsEnabled: Boolean,
      deltaLog: DeltaLog,
      targetDf: DataFrame,
      fileIndex: TahoeFileIndex,
      condition: Expression,
      opName: String): Seq[TouchedFileWithDV] = {
    require(
      SUPPORTED_DML_COMMANDS.contains(opName),
      s"Expecting opName to be one of ${SUPPORTED_DML_COMMANDS.mkString(", ")}, " +
        s"but got '$opName'.")

    recordDeltaOperation(deltaLog, opType = s"$opName.findTouchedFiles") {
      val candidateFiles = fileIndex match {
        case f: TahoeBatchFileIndex => f.addFiles
        case _ => throw new IllegalArgumentException("Unexpected file index found!")
      }

      val matchedRowIndexSets =
        DeletionVectorBitmapGenerator.buildRowIndexSetsForFilesMatchingCondition(
          sparkSession,
          txn,
          hasDVsEnabled,
          targetDf,
          candidateFiles,
          condition)

      val nameToAddFileMap = generateCandidateFileMap(txn.deltaLog.dataPath, candidateFiles)
      findFilesWithMatchingRows(txn, nameToAddFileMap, matchedRowIndexSets)
    }
  }

  /**
   * Finds the files in nameToAddFileMap in which rows were deleted by checking the row index set.
   */
  def findFilesWithMatchingRows(
      txn: OptimisticTransaction,
      nameToAddFileMap: Map[String, AddFile],
      matchedFileRowIndexSets: Seq[DeletionVectorResult]): Seq[TouchedFileWithDV] = {
    // Get the AddFiles using the touched file names and group them together with other
    // information we need for later phases.
    val dataPath = txn.deltaLog.dataPath
    val touchedFilesWithMatchedRowIndices = matchedFileRowIndexSets.map { fileRowIndex =>
      val filePath = fileRowIndex.filePath
      val addFile = getTouchedFile(dataPath, filePath, nameToAddFileMap)
      TouchedFileWithDV(
        filePath,
        addFile,
        fileRowIndex.deletionVector,
        fileRowIndex.matchedRowCount)
    }

    logTrace("findTouchedFiles: matched files:\n\t" +
      s"${touchedFilesWithMatchedRowIndices.map(_.inputFilePath).mkString("\n\t")}")

    touchedFilesWithMatchedRowIndices.filterNot(_.isUnchanged)
  }

  def processUnmodifiedData(
      spark: SparkSession,
      touchedFiles: Seq[TouchedFileWithDV],
      snapshot: Snapshot): (Seq[FileAction], Map[String, Long]) = {
    val numModifiedRows = touchedFiles.map(_.numberOfModifiedRows).sum.toLong
    val numRemovedFiles = touchedFiles.count(_.isFullyReplaced()).toLong

    val (fullyRemovedFiles, notFullyRemovedFiles) = touchedFiles.partition(_.isFullyReplaced())

    val timestamp = System.currentTimeMillis()
    val fullyRemoved = fullyRemovedFiles.map(_.fileLogEntry.removeWithTimestamp(timestamp))

    val dvUpdates = notFullyRemovedFiles.map { fileWithDVInfo =>
      fileWithDVInfo.fileLogEntry.removeRows(
        deletionVector = fileWithDVInfo.newDeletionVector,
        updateStats = false
      )}
    val (dvAddFiles, dvRemoveFiles) = dvUpdates.unzip
    val dvAddFilesWithStats = getActionsWithStats(spark, dvAddFiles, snapshot)

    var (numDeletionVectorsAdded, numDeletionVectorsRemoved, numDeletionVectorsUpdated) =
      dvUpdates.foldLeft((0L, 0L, 0L)) { case ((added, removed, updated), (addFile, removeFile)) =>
        (Option(addFile.deletionVector), Option(removeFile.deletionVector)) match {
          case (Some(_), Some(_)) => (added, removed, updated + 1)
          case (None, Some(_)) => (added, removed + 1, updated)
          case (Some(_), None) => (added + 1, removed, updated)
          case _ => (added, removed, updated)
        }
      }
    numDeletionVectorsRemoved += fullyRemoved.count(_.deletionVector != null)
    val metricMap = Map(
      "numModifiedRows" -> numModifiedRows,
      "numRemovedFiles" -> numRemovedFiles,
      "numDeletionVectorsAdded" -> numDeletionVectorsAdded,
      "numDeletionVectorsRemoved" -> numDeletionVectorsRemoved,
      "numDeletionVectorsUpdated" -> numDeletionVectorsUpdated)
    (fullyRemoved ++ dvAddFilesWithStats ++ dvRemoveFiles, metricMap)
  }

  /** Fetch stats for `addFiles`. */
  private def getActionsWithStats(
      spark: SparkSession,
      addFilesWithNewDvs: Seq[AddFile],
      snapshot: Snapshot): Seq[AddFile] = {
    import org.apache.spark.sql.delta.implicits._

    if (addFilesWithNewDvs.isEmpty) return Seq.empty

    val selectionPathAndStatsCols = Seq(col("path"), col("stats"))
    val addFilesWithNewDvsDf = addFilesWithNewDvs.toDF(spark)

    // These files originate from snapshot.filesForScan which resets column statistics.
    // Since these object don't carry stats and tags, if we were to use them as result actions of
    // the operation directly, we'd effectively be removing all stats and tags. To resolve this
    // we join the list of files with DVs with the log (allFiles) to retrieve statistics. This is
    // expected to have better performance than supporting full stats retrieval
    // in snapshot.filesForScan because it only affects a subset of the scanned files.

    // Find the current metadata with stats for all files with new DV
    val addFileWithStatsDf = snapshot.withStats
      .join(addFilesWithNewDvsDf.select("path"), "path")

    // Update the existing stats to set the tightBounds to false and also set the appropriate
    // null count. We want to set the bounds before the AddFile has DV descriptor attached.
    // Attaching the DV descriptor here, causes wrong logical records computation in
    // `updateStatsToWideBounds`.
    val statsColName = snapshot.getBaseStatsColumnName
    val addFilesWithWideBoundsDf = snapshot
      .updateStatsToWideBounds(addFileWithStatsDf, statsColName)

    val (filesWithNoStats, filesWithExistingStats) = {
      // numRecords is the only stat we really have to guarantee.
      // If the others are missing, we do not need to fetch them.
      addFilesWithWideBoundsDf.as[AddFile].collect().toSeq
        .partition(_.numPhysicalRecords.isEmpty)
    }

    // If we encounter files with no stats we fetch the stats from the parquet footer.
    // Files with persistent DVs *must* have (at least numRecords) stats according to the
    // Delta spec.
    val filesWithFetchedStats =
      if (filesWithNoStats.nonEmpty) {
        StatsCollectionUtils.computeStats(spark,
          conf = snapshot.deltaLog.newDeltaHadoopConf(),
          deltaLog = snapshot.deltaLog,
          snapshot = snapshot,
          addFiles = filesWithNoStats.toDS(spark),
          numFilesOpt = Some(filesWithNoStats.size),
          setBoundsToWide = true)
          .collect()
          .toSeq
      } else {
        Seq.empty
      }

    val allAddFilesWithUpdatedStats =
      (filesWithExistingStats ++ filesWithFetchedStats).toSeq.toDF(spark)

    // Now join the allAddFilesWithUpdatedStats with addFilesWithNewDvs
    // so that the updated stats are joined with the new DV info
    addFilesWithNewDvsDf.drop("stats")
      .join(
        allAddFilesWithUpdatedStats.select(selectionPathAndStatsCols: _*), "path")
      .as[AddFile]
      .collect()
      .toSeq
  }
}

object DeletionVectorBitmapGenerator {
  final val FILE_NAME_COL = "filePath"
  final val FILE_DV_ID_COL = "deletionVectorId"
  final val ROW_INDEX_COL = "rowIndexCol"
  final val DELETED_ROW_INDEX_BITMAP = "deletedRowIndexSet"
  final val DELETED_ROW_INDEX_COUNT = "deletedRowIndexCount"
  final val MAX_ROW_INDEX_COL = "maxRowIndexCol"

  private class DeletionVectorSet(
    spark: SparkSession,
    target: DataFrame,
    targetDeltaLog: DeltaLog,
    deltaTxn: OptimisticTransaction) {

    case object CardinalityAndBitmapStruct {
      val name: String = "CardinalityAndBitmapStruct"
      def cardinality: String = s"$name.cardinality"
      def bitmap: String = s"$name.bitmap"
    }

    def computeResult(): Seq[DeletionVectorResult] = {
      val aggregated = target
        .groupBy(col(FILE_NAME_COL), col(FILE_DV_ID_COL))
        .agg(aggColumns.head, aggColumns.tail: _*)
        .select(outputColumns: _*)

      import DeletionVectorResult.encoder
      val rowIndexData = aggregated.as[DeletionVectorData]
      val storedResults = rowIndexData.mapPartitions(bitmapStorageMapper())
      storedResults.as[DeletionVectorResult].collect()
    }

    protected def aggColumns: Seq[Column] = {
      Seq(createBitmapSetAggregator(col(ROW_INDEX_COL)).as(CardinalityAndBitmapStruct.name))
    }

    /** Create a bitmap set aggregator over the given column */
    private def createBitmapSetAggregator(indexColumn: Column): Column = {
      val func = new BitmapAggregator(indexColumn.expr, RoaringBitmapArrayFormat.Portable)
      Column(func.toAggregateExpression(isDistinct = false))
    }

    protected def outputColumns: Seq[Column] =
      Seq(
        col(FILE_NAME_COL),
        col(FILE_DV_ID_COL),
        col(CardinalityAndBitmapStruct.bitmap).as(DELETED_ROW_INDEX_BITMAP),
        col(CardinalityAndBitmapStruct.cardinality).as(DELETED_ROW_INDEX_COUNT)
      )

    protected def bitmapStorageMapper()
      : Iterator[DeletionVectorData] => Iterator[DeletionVectorResult] = {
      val prefixLen = DeltaUtils.getRandomPrefixLength(deltaTxn.metadata)
      DeletionVectorWriter.createMapperToStoreDeletionVectors(
        spark,
        targetDeltaLog.newDeltaHadoopConf(),
        targetDeltaLog.dataPath,
        prefixLen)
    }
  }

  /**
   * Build bitmap compressed sets of row indices for each file in [[target]] using
   * [[ROW_INDEX_COL]].
   * Write those sets out to temporary files and collect the file names,
   * together with some encoded metadata about the contents.
   *
   * @param target  DataFrame with expected schema [[FILE_NAME_COL]], [[ROW_INDEX_COL]],
   */
  def buildDeletionVectors(
      spark: SparkSession,
      target: DataFrame,
      targetDeltaLog: DeltaLog,
      deltaTxn: OptimisticTransaction): Seq[DeletionVectorResult] = {
    val rowIndexSet = new DeletionVectorSet(spark, target, targetDeltaLog, deltaTxn)
    rowIndexSet.computeResult()
  }

  def buildRowIndexSetsForFilesMatchingCondition(
      sparkSession: SparkSession,
      txn: OptimisticTransaction,
      tableHasDVs: Boolean,
      targetDf: DataFrame,
      candidateFiles: Seq[AddFile],
      condition: Expression,
      fileNameColumnOpt: Option[Column] = None,
      rowIndexColumnOpt: Option[Column] = None): Seq[DeletionVectorResult] = {
    val useMetadataRowIndexConf = DeltaSQLConf.DELETION_VECTORS_USE_METADATA_ROW_INDEX
    val useMetadataRowIndex = sparkSession.sessionState.conf.getConf(useMetadataRowIndexConf)
    val fileNameColumn = fileNameColumnOpt.getOrElse(col(s"${METADATA_NAME}.${FILE_PATH}"))
    val rowIndexColumn = if (useMetadataRowIndex) {
      rowIndexColumnOpt.getOrElse(col(s"${METADATA_NAME}.${ParquetFileFormat.ROW_INDEX}"))
    } else {
      rowIndexColumnOpt.getOrElse(col(ROW_INDEX_COLUMN_NAME))
    }
    val matchedRowsDf = targetDf
      .withColumn(FILE_NAME_COL, fileNameColumn)
      // Filter after getting input file name as the filter might introduce a join and we
      // cannot get input file name on join's output.
      .filter(Column(condition))
      .withColumn(ROW_INDEX_COL, rowIndexColumn)

    val df = if (tableHasDVs) {
      // When the table already has DVs, join the `matchedRowDf` above to attach for each matched
      // file its existing DeletionVectorDescriptor
      val basePath = txn.deltaLog.dataPath.toString
      val filePathToDV = candidateFiles.map { add =>
        val serializedDV = Option(add.deletionVector).map(_.serializeToBase64())
        // Paths in the metadata column are canonicalized. Thus we must canonicalize the DV path.
        FileToDvDescriptor(
          SparkPath.fromPath(absolutePath(basePath, add.path)).urlEncoded,
          serializedDV)
      }
      val filePathToDVDf = sparkSession.createDataset(filePathToDV)

      val joinExpr = filePathToDVDf("path") === matchedRowsDf(FILE_NAME_COL)
      // Perform leftOuter join to make sure we do not eliminate any rows because of path
      // encoding issues. If there is such an issue we will detect it during the aggregation
      // of the bitmaps.
      val joinedDf = matchedRowsDf.join(filePathToDVDf, joinExpr, "leftOuter")
        .drop(FILE_NAME_COL)
        .withColumnRenamed("path", FILE_NAME_COL)
      joinedDf
    } else {
      // When the table has no DVs, just add a column to indicate that the existing dv is null
      matchedRowsDf.withColumn(FILE_DV_ID_COL, lit(null))
    }

    DeletionVectorBitmapGenerator.buildDeletionVectors(sparkSession, df, txn.deltaLog, txn)
  }
}

/**
 * Holds a mapping from a file path (url-encoded) to an (optional) serialized Deletion Vector
 * descriptor.
 */
case class FileToDvDescriptor(path: String, deletionVectorId: Option[String])

object FileToDvDescriptor {
  private lazy val _encoder = new DeltaEncoder[FileToDvDescriptor]
  implicit def encoder: Encoder[FileToDvDescriptor] = _encoder.get
}

/**
 * Row containing the file path and its new deletion vector bitmap in memory
 *
 * @param filePath             Absolute path of the data file this DV result is generated for.
 * @param deletionVectorId     Existing [[DeletionVectorDescriptor]] serialized in JSON format.
 *                             This info is used to load the existing DV with the new DV.
 * @param deletedRowIndexSet   In-memory Deletion vector bitmap generated containing the newly
 *                             deleted row indexes from data file.
 * @param deletedRowIndexCount Count of rows marked as deleted using the [[deletedRowIndexSet]].
 */
case class DeletionVectorData(
    filePath: String,
    deletionVectorId: Option[String],
    deletedRowIndexSet: Array[Byte],
    deletedRowIndexCount: Long) extends Sizing {

  /** The size of the bitmaps to use in [[BinPackingIterator]]. */
  override def size: Int = deletedRowIndexSet.length
}

object DeletionVectorData {
  private lazy val _encoder = new DeltaEncoder[DeletionVectorData]
  implicit def encoder: Encoder[DeletionVectorData] = _encoder.get

  def apply(filePath: String, rowIndexSet: Array[Byte], rowIndexCount: Long): DeletionVectorData = {
    DeletionVectorData(
      filePath = filePath,
      deletionVectorId = None,
      deletedRowIndexSet = rowIndexSet,
      deletedRowIndexCount = rowIndexCount)
  }
}

/** Final output for each file containing the file path, DeletionVectorDescriptor and how many
 * rows are marked as deleted in this file as part of the this operation (doesn't include rows that
 * are already marked as deleted).
 *
 * @param filePath        Absolute path of the data file this DV result is generated for.
 * @param deletionVector  Deletion vector generated containing the newly deleted row indices from
 *                        data file.
 * @param matchedRowCount Number of rows marked as deleted using the [[deletionVector]].
 */
case class DeletionVectorResult(
    filePath: String,
    deletionVector: DeletionVectorDescriptor,
    matchedRowCount: Long) {
}

object DeletionVectorResult {
  private lazy val _encoder = new DeltaEncoder[DeletionVectorResult]
  implicit def encoder: Encoder[DeletionVectorResult] = _encoder.get

  def fromDeletionVectorData(
      data: DeletionVectorData,
      deletionVector: DeletionVectorDescriptor): DeletionVectorResult = {
    DeletionVectorResult(
      filePath = data.filePath,
      deletionVector = deletionVector,
      matchedRowCount = data.deletedRowIndexCount)
  }
}

case class TouchedFileWithDV(
    inputFilePath: String,
    fileLogEntry: AddFile,
    newDeletionVector: DeletionVectorDescriptor,
    deletedRows: Long) {
  /**
   * Checks the *sufficient* condition for a file being fully replaced by the current operation.
   * (That is, all rows are either being updated or deleted.)
   */
  def isFullyReplaced(): Boolean = {
    fileLogEntry.numLogicalRecords match {
      case Some(numRecords) => numRecords == numberOfModifiedRows
      case None => false // must make defensive assumption if no statistics are available
    }
  }

  /**
   * Checks if the file is unchanged by the current operation.
   * (That is no row has been updated or deleted.)
   */
  def isUnchanged: Boolean = {
    // If the bitmap is empty then no row would be removed during the rewrite,
    // thus the file is unchanged.
    numberOfModifiedRows == 0
  }

  /**
   * The number of rows that are modified in this file.
   */
  def numberOfModifiedRows: Long = newDeletionVector.cardinality - fileLogEntry.numDeletedRecords
}

/**
 * Utility methods to write the deletion vector to storage. If a particular file already
 * has an existing DV, it will be merged with the new deletion vector and written to storage.
 */
object DeletionVectorWriter extends DeltaLogging {
  /**
   * The context for [[createDeletionVectorMapper]] callback functions. Contains the DV writer that
   * is used by callback functions to write the new DVs.
   */
  case class DeletionVectorMapperContext(
      dvStore: DeletionVectorStore,
      writer: DeletionVectorStore.Writer,
      tablePath: Path,
      fileId: UUID,
      prefix: String)

  /**
   * Prepare a mapper function for storing deletion vectors.
   *
   * For each DeletionVector the writer will create a [[DeletionVectorMapperContext]] that contains
   * a DV writer that is used by to write the DV into a file.
   *
   * The result can be used with [[org.apache.spark.sql.Dataset.mapPartitions()]] and must thus be
   * serialized.
   */
  def createDeletionVectorMapper[InputT <: Sizing, OutputT](
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      table: Path,
      prefixLength: Int)
      (callbackFn: (DeletionVectorMapperContext, InputT) => OutputT)
    : Iterator[InputT] => Iterator[OutputT] = {
    val broadcastHadoopConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // hadoop.fs.Path is not Serializable, so close over the String representation instead
    val tablePathString = DeletionVectorStore.pathToEscapedString(table)
    val packingTargetSize =
      sparkSession.conf.get(DeltaSQLConf.DELETION_VECTOR_PACKING_TARGET_SIZE)

    // This is the (partition) mapper function we are returning
    (rowIterator: Iterator[InputT]) => {
      val dvStore = DeletionVectorStore.createInstance(broadcastHadoopConf.value.value)
      val tablePath = DeletionVectorStore.escapedStringToPath(tablePathString)
      val tablePathWithFS = dvStore.pathWithFileSystem(tablePath)

      val perBinFunction: Seq[InputT] => Seq[OutputT] = (rows: Seq[InputT]) => {
        val prefix = DeltaUtils.getRandomPrefix(prefixLength)
        val (writer, fileId) = createWriter(dvStore, tablePathWithFS, prefix)
        val ctx = DeletionVectorMapperContext(
          dvStore,
          writer,
          tablePath,
          fileId,
          prefix)
        val result = SparkUtils.tryWithResource(writer) { writer =>
          rows.map(r => callbackFn(ctx, r))
        }
        result
      }

      val binPackedRowIterator = new BinPackingIterator(rowIterator, packingTargetSize)
      binPackedRowIterator.flatMap(perBinFunction)
    }
  }

  /**
   * Creates a writer for writing multiple DVs in the same file.
   *
   * Returns the writer and the UUID of the new file.
   */
  def createWriter(
      dvStore: DeletionVectorStore,
      tablePath: PathWithFileSystem,
      prefix: String = ""): (DeletionVectorStore.Writer, UUID) = {
    val fileId = UUID.randomUUID()
    val writer = dvStore.createWriter(dvStore.generateFileNameInTable(tablePath, fileId, prefix))
    (writer, fileId)
  }

  /** Store the `bitmapData` on cloud storage. */
  def storeSerializedBitmap(
      ctx: DeletionVectorMapperContext,
      bitmapData: Array[Byte],
      cardinality: Long): DeletionVectorDescriptor = {
    if (cardinality == 0L) {
      DeletionVectorDescriptor.EMPTY
    } else {
      val dvRange = ctx.writer.write(bitmapData)
      DeletionVectorDescriptor.onDiskWithRelativePath(
        id = ctx.fileId,
        randomPrefix = ctx.prefix,
        sizeInBytes = bitmapData.length,
        cardinality = cardinality,
        offset = Some(dvRange.offset))
    }
  }

  /**
   * Prepares a mapper function that can be used by DML commands to store the Deletion Vectors
   * that are in described in [[DeletionVectorData]] and return their descriptors
   * [[DeletionVectorResult]].
   */
  def createMapperToStoreDeletionVectors(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      table: Path,
      prefixLength: Int): Iterator[DeletionVectorData] => Iterator[DeletionVectorResult] =
    createDeletionVectorMapper(sparkSession, hadoopConf, table, prefixLength) {
      (ctx, row) => storeBitmapAndGenerateResult(ctx, row)
    }

  /**
   * Helper to generate and store the deletion vector bitmap. The deletion vector is merged with
   * the file's already existing deletion vector before being stored.
   */
  def storeBitmapAndGenerateResult(ctx: DeletionVectorMapperContext, row: DeletionVectorData)
    : DeletionVectorResult = {
    // If a group with null path exists it means there was an issue while joining with the log to
    // fetch the DeletionVectorDescriptors.
    assert(row.filePath != null,
      s"""
         |Encountered a non matched file path.
         |It is likely that _metadata.file_path is not encoded by Spark as expected.
         |""".stripMargin)

    val fileDvDescriptor = row.deletionVectorId.map(DeletionVectorDescriptor.deserializeFromBase64)
    val finalDvDescriptor = fileDvDescriptor match {
      case Some(existingDvDescriptor) if row.deletedRowIndexCount > 0 =>
        // Load the existing bit map
        val existingBitmap =
          StoredBitmap.create(existingDvDescriptor, ctx.tablePath).load(ctx.dvStore)
        val newBitmap = RoaringBitmapArray.readFrom(row.deletedRowIndexSet)

        // Merge both the existing and new bitmaps into one, and finally persist on disk
        existingBitmap.merge(newBitmap)
        storeSerializedBitmap(
          ctx,
          existingBitmap.serializeAsByteArray(RoaringBitmapArrayFormat.Portable),
          existingBitmap.cardinality)
      case Some(existingDvDescriptor) =>
        existingDvDescriptor // This is already stored.
      case None =>
        // Persist the new bitmap
        storeSerializedBitmap(ctx, row.deletedRowIndexSet, row.deletedRowIndexCount)
    }
    DeletionVectorResult.fromDeletionVectorData(row, deletionVector = finalDvDescriptor)
  }
}
