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

import java.util.ConcurrentModificationException

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.immutable.ParVector

import org.apache.spark.sql.delta.skipping.MultiDimClustering
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.{Action, AddFile, DeletionVectorDescriptor, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.optimize._
import org.apache.spark.sql.delta.files.SQLMetricsReporting
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.sql.{AnalysisException, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.execution.command.{LeafRunnableCommand, RunnableCommand}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.types._
import org.apache.spark.util.{SystemClock, ThreadUtils}

/** Base class defining abstract optimize command */
abstract class OptimizeTableCommandBase extends RunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("path", StringType)(),
    AttributeReference("metrics", Encoders.product[OptimizeMetrics].schema)())

  /**
   * Validates ZOrderBy columns
   * - validates that partitions columns are not used in `unresolvedZOrderByCols`
   * - validates that we already collect stats for all the columns used in `unresolvedZOrderByCols`
   *
   * @param spark [[SparkSession]] to use
   * @param txn the [[OptimisticTransaction]] being used to optimize
   * @param unresolvedZOrderByCols Seq of [[UnresolvedAttribute]] corresponding to zOrderBy columns
   */
  def validateZorderByColumns(
      spark: SparkSession,
      txn: OptimisticTransaction,
      unresolvedZOrderByCols: Seq[UnresolvedAttribute]): Unit = {
    if (unresolvedZOrderByCols.isEmpty) return
    val metadata = txn.snapshot.metadata
    val partitionColumns = metadata.partitionColumns.toSet
    val dataSchema =
      StructType(metadata.schema.filterNot(c => partitionColumns.contains(c.name)))
    val df = spark.createDataFrame(new java.util.ArrayList[Row](), dataSchema)
    val checkColStat = spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK)
    val statCollectionSchema = txn.snapshot.statCollectionSchema
    val colsWithoutStats = ArrayBuffer[String]()

    unresolvedZOrderByCols.foreach { colAttribute =>
      val colName = colAttribute.name
      if (checkColStat) {
        try {
          SchemaUtils.findColumnPosition(colAttribute.nameParts, statCollectionSchema)
        } catch {
          case e: AnalysisException if e.getMessage.contains("Couldn't find column") =>
            colsWithoutStats.append(colName)
        }
      }
      val isNameEqual = spark.sessionState.conf.resolver
      if (partitionColumns.find(isNameEqual(_, colName)).nonEmpty) {
        throw DeltaErrors.zOrderingOnPartitionColumnException(colName)
      }
      if (df.queryExecution.analyzed.resolve(colAttribute.nameParts, isNameEqual).isEmpty) {
        throw DeltaErrors.zOrderingColumnDoesNotExistException(colName)
      }
    }
    if (checkColStat && colsWithoutStats.nonEmpty) {
      throw DeltaErrors.zOrderingOnColumnWithNoStatsException(
        colsWithoutStats.toSeq, spark)
    }
  }
}

/**
 * The `optimize` command implementation for Spark SQL. Example SQL:
 * {{{
 *    OPTIMIZE ('/path/to/dir' | delta.table) [WHERE part = 25];
 * }}}
 */
case class OptimizeTableCommand(
    path: Option[String],
    tableId: Option[TableIdentifier],
    userPartitionPredicates: Seq[String],
    options: Map[String, String],
    optimizeContext: DeltaOptimizeContext = DeltaOptimizeContext()
)(val zOrderBy: Seq[UnresolvedAttribute])
  extends OptimizeTableCommandBase with LeafRunnableCommand {

  override val otherCopyArgs: Seq[AnyRef] = zOrderBy :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = getDeltaLog(sparkSession, path, tableId, "OPTIMIZE", options)

    val txn = deltaLog.startTransaction()
    if (txn.readVersion == -1) {
      throw DeltaErrors.notADeltaTableException(deltaLog.dataPath.toString)
    }

    val partitionColumns = txn.snapshot.metadata.partitionColumns
    // Parse the predicate expression into Catalyst expression and verify only simple filters
    // on partition columns are present

    val partitionPredicates = userPartitionPredicates.flatMap { predicate =>
        val predicates = parsePredicates(sparkSession, predicate)
        verifyPartitionPredicates(
          sparkSession,
          partitionColumns,
          predicates)
        predicates
    }

    validateZorderByColumns(sparkSession, txn, zOrderBy)
    val zOrderByColumns = zOrderBy.map(_.name).toSeq

    new OptimizeExecutor(sparkSession, txn, partitionPredicates, zOrderByColumns, optimizeContext)
      .optimize()
  }
}

/**
 * Stored all runtime context information that can control the execution of optimize.
 *
 * @param isPurge Whether the rewriting task is only for purging soft-deleted data instead of
 *                for compaction. If [[isPurge]] is true, only files with DVs will be selected
 *                for compaction.
 * @param minFileSize Files which are smaller than this threshold will be selected for compaction.
 *                    If not specified, [[DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE]] will be used.
 *                    This parameter must be set to `0` when [[isPurge]] is true.
 * @param maxDeletedRowsRatio Files with a ratio of soft-deleted rows to the total rows larger than
 *                            this threshold will be rewritten by the OPTIMIZE command. If not
 *                            specified, [[DeltaSQLConf.DELTA_OPTIMIZE_MAX_DELETED_ROWS_RATIO]]
 *                            will be used. This parameter must be set to `0` when [[isPurge]] is
 *                            true.
 */
case class DeltaOptimizeContext(
    isPurge: Boolean = false,
    minFileSize: Option[Long] = None,
    maxDeletedRowsRatio: Option[Double] = None) {
  if (isPurge) {
    require(
      minFileSize.contains(0L) && maxDeletedRowsRatio.contains(0d),
      "minFileSize and maxDeletedRowsRatio must be 0 when running PURGE.")
  }
}

/**
 * Optimize job which compacts small files into larger files to reduce
 * the number of files and potentially allow more efficient reads.
 *
 * @param sparkSession Spark environment reference.
 * @param txn The transaction used to optimize this table
 * @param partitionPredicate List of partition predicates to select subset of files to optimize.
 */
class OptimizeExecutor(
    sparkSession: SparkSession,
    txn: OptimisticTransaction,
    partitionPredicate: Seq[Expression],
    zOrderByColumns: Seq[String],
    optimizeContext: DeltaOptimizeContext)
  extends DeltaCommand with SQLMetricsReporting with Serializable {

  /** Timestamp to use in [[FileAction]] */
  private val operationTimestamp = new SystemClock().getTimeMillis()

  private val isMultiDimClustering = zOrderByColumns.nonEmpty

  def optimize(): Seq[Row] = {
    recordDeltaOperation(txn.deltaLog, "delta.optimize") {
      val minFileSize = optimizeContext.minFileSize.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE))
      val maxFileSize =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE)
      val maxDeletedRowsRatio = optimizeContext.maxDeletedRowsRatio.getOrElse(
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_DELETED_ROWS_RATIO))

      val candidateFiles = txn.filterFiles(partitionPredicate, keepNumRecords = true)
      val partitionSchema = txn.metadata.partitionSchema

      val filesToProcess = pruneCandidateFileList(minFileSize, maxDeletedRowsRatio, candidateFiles)
      val partitionsToCompact = filesToProcess.groupBy(_.partitionValues).toSeq

      val jobs = groupFilesIntoBins(partitionsToCompact, maxFileSize)

      val maxThreads =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_THREADS)
      val updates = ThreadUtils.parmap(jobs, "OptimizeJob", maxThreads) { partitionBinGroup =>
        runOptimizeBinJob(txn, partitionBinGroup._1, partitionBinGroup._2, maxFileSize)
      }.flatten

      val addedFiles = updates.collect { case a: AddFile => a }
      val removedFiles = updates.collect { case r: RemoveFile => r }
      val removedDVs = filesToProcess.filter(_.deletionVector != null).map(_.deletionVector).toSeq
      if (addedFiles.size > 0) {
        val operation = DeltaOperations.Optimize(partitionPredicate, zOrderByColumns)
        val metrics = createMetrics(sparkSession.sparkContext, addedFiles, removedFiles, removedDVs)
        commitAndRetry(txn, operation, updates, metrics) { newTxn =>
          val newPartitionSchema = newTxn.metadata.partitionSchema
          val candidateSetOld = candidateFiles.map(_.path).toSet
          val candidateSetNew = newTxn.filterFiles(partitionPredicate).map(_.path).toSet

          // As long as all of the files that we compacted are still part of the table,
          // and the partitioning has not changed it is valid to continue to try
          // and commit this checkpoint.
          if (candidateSetOld.subsetOf(candidateSetNew) && partitionSchema == newPartitionSchema) {
            true
          } else {
            val deleted = candidateSetOld -- candidateSetNew
            logWarning(s"The following compacted files were delete " +
              s"during checkpoint ${deleted.mkString(",")}. Aborting the compaction.")
            false
          }
        }
      }

      val optimizeStats = OptimizeStats()
      optimizeStats.addedFilesSizeStats.merge(addedFiles)
      optimizeStats.removedFilesSizeStats.merge(removedFiles)
      optimizeStats.numPartitionsOptimized = jobs.map(j => j._1).distinct.size
      optimizeStats.numBatches = jobs.size
      optimizeStats.totalConsideredFiles = candidateFiles.size
      optimizeStats.totalFilesSkipped = optimizeStats.totalConsideredFiles - removedFiles.size
      optimizeStats.totalClusterParallelism = sparkSession.sparkContext.defaultParallelism
      val numTableColumns = txn.snapshot.metadata.schema.size
      optimizeStats.numTableColumns = numTableColumns
      optimizeStats.numTableColumnsWithStats =
        DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(txn.snapshot.metadata)
          .min(numTableColumns)
      if (removedDVs.size > 0) {
        optimizeStats.deletionVectorStats = Some(DeletionVectorStats(
          numDeletionVectorsRemoved = removedDVs.size,
          numDeletionVectorRowsRemoved = removedDVs.map(_.cardinality).sum))
      }

      if (isMultiDimClustering) {
        val inputFileStats =
          ZOrderFileStats(removedFiles.size, removedFiles.map(_.size.getOrElse(0L)).sum)
        optimizeStats.zOrderStats = Some(ZOrderStats(
          strategyName = "all", // means process all files in a partition
          inputCubeFiles = ZOrderFileStats(0, 0),
          inputOtherFiles = inputFileStats,
          inputNumCubes = 0,
          mergedFiles = inputFileStats,
          // There will one z-cube for each partition
          numOutputCubes = optimizeStats.numPartitionsOptimized))
      }

      return Seq(Row(txn.deltaLog.dataPath.toString, optimizeStats.toOptimizeMetrics))
    }
  }

  /**
   * Helper method to prune the list of selected files based on fileSize and ratio of
   * deleted rows according to the deletion vector in [[AddFile]].
   */
  private def pruneCandidateFileList(
      minFileSize: Long, maxDeletedRowsRatio: Double, files: Seq[AddFile]): Seq[AddFile] = {

    // Select all files in case of multi-dimensional clustering
    if (isMultiDimClustering) return files

    def shouldCompactBecauseOfDeletedRows(file: AddFile): Boolean = {
      // Always compact files with DVs but without numRecords stats.
      // This may be overly aggressive, but it fixes the problem in the long-term,
      // as the compacted files will have stats.
      (file.deletionVector != null && file.numPhysicalRecords.isEmpty) ||
          file.deletedToPhysicalRecordsRatio.getOrElse(0d) > maxDeletedRowsRatio
    }

    // Select files that are small or have too many deleted rows
    files.filter(
      addFile => addFile.size < minFileSize || shouldCompactBecauseOfDeletedRows(addFile))
  }

  /**
   * Utility methods to group files into bins for optimize.
   *
   * @param partitionsToCompact List of files to compact group by partition.
   *                            Partition is defined by the partition values (partCol -> partValue)
   * @param maxTargetFileSize Max size (in bytes) of the compaction output file.
   * @return Sequence of bins. Each bin contains one or more files from the same
   *         partition and targeted for one output file.
   */
  private def groupFilesIntoBins(
      partitionsToCompact: Seq[(Map[String, String], Seq[AddFile])],
      maxTargetFileSize: Long): Seq[(Map[String, String], Seq[AddFile])] = {
    partitionsToCompact.flatMap {
      case (partition, files) =>
        val bins = new ArrayBuffer[Seq[AddFile]]()

        val currentBin = new ArrayBuffer[AddFile]()
        var currentBinSize = 0L

        files.sortBy(_.size).foreach { file =>
          // Generally, a bin is a group of existing files, whose total size does not exceed the
          // desired maxFileSize. They will be coalesced into a single output file.
          // However, if isMultiDimClustering = true, all files in a partition will be read by the
          // same job, the data will be range-partitioned and numFiles = totalFileSize / maxFileSize
          // will be produced. See below.
          if (file.size + currentBinSize > maxTargetFileSize && !isMultiDimClustering) {
            bins += currentBin.toVector
            currentBin.clear()
            currentBin += file
            currentBinSize = file.size
          } else {
            currentBin += file
            currentBinSize += file.size
          }
        }

        if (currentBin.nonEmpty) {
          bins += currentBin.toVector
        }

        bins.filter { bin =>
          bin.size > 1 || // bin has more than one file or
          (bin.size == 1 && bin(0).deletionVector != null) || // single file in the bin has a DV or
          isMultiDimClustering // multi-clustering
        }.map(b => (partition, b))
    }
  }

  /**
   * Utility method to run a Spark job to compact the files in given bin
   *
   * @param txn [[OptimisticTransaction]] instance in use to commit the changes to DeltaLog.
   * @param partition Partition values of the partition that files in [[bin]] belongs to.
   * @param bin List of files to compact into one large file.
   * @param maxFileSize Targeted output file size in bytes
   */
  private def runOptimizeBinJob(
      txn: OptimisticTransaction,
      partition: Map[String, String],
      bin: Seq[AddFile],
      maxFileSize: Long): Seq[FileAction] = {
    val baseTablePath = txn.deltaLog.dataPath

    val input = txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize"))
    val repartitionDF = if (isMultiDimClustering) {
      val totalSize = bin.map(_.size).sum
      val approxNumFiles = Math.max(1, totalSize / maxFileSize).toInt
      MultiDimClustering.cluster(
        input,
        approxNumFiles,
        zOrderByColumns)
    } else {
      val useRepartition = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_REPARTITION_ENABLED)
      if (useRepartition) {
        input.repartition(numPartitions = 1)
      } else {
        input.coalesce(numPartitions = 1)
      }
    }

    val partitionDesc = partition.toSeq.map(entry => entry._1 + "=" + entry._2).mkString(",")

    val partitionName = if (partition.isEmpty) "" else s" in partition ($partitionDesc)"
    val description = s"$baseTablePath<br/>Optimizing ${bin.size} files" + partitionName
    sparkSession.sparkContext.setJobGroup(
      sparkSession.sparkContext.getLocalProperty(SPARK_JOB_GROUP_ID),
      description)

    val addFiles = txn.writeFiles(repartitionDF).collect {
      case a: AddFile =>
        a.copy(dataChange = false)
      case other =>
        throw new IllegalStateException(
          s"Unexpected action $other with type ${other.getClass}. File compaction job output" +
              s"should only have AddFiles")
    }
    val removeFiles = bin.map(f => f.removeWithTimestamp(operationTimestamp, dataChange = false))
    val updates = addFiles ++ removeFiles
    updates
  }

  /**
   * Attempts to commit the given actions to the log. In the case of a concurrent update,
   * the given function will be invoked with a new transaction to allow custom conflict
   * detection logic to indicate it is safe to try again, by returning `true`.
   *
   * This function will continue to try to commit to the log as long as `f` returns `true`,
   * otherwise throws a subclass of [[ConcurrentModificationException]].
   */
  private def commitAndRetry(
      txn: OptimisticTransaction,
      optimizeOperation: Operation,
      actions: Seq[Action],
      metrics: Map[String, SQLMetric])(f: OptimisticTransaction => Boolean): Unit = {
    try {
      txn.registerSQLMetrics(sparkSession, metrics)
      txn.commit(actions, optimizeOperation)
    } catch {
      case e: ConcurrentModificationException =>
        val newTxn = txn.deltaLog.startTransaction()
        if (f(newTxn)) {
          logInfo("Retrying commit after checking for semantic conflicts with concurrent updates.")
          commitAndRetry(newTxn, optimizeOperation, actions, metrics)(f)
        } else {
          logWarning("Semantic conflicts detected. Aborting operation.")
          throw e
        }
    }
  }

  /** Create a map of SQL metrics for adding to the commit history. */
  private def createMetrics(
      sparkContext: SparkContext,
      addedFiles: Seq[AddFile],
      removedFiles: Seq[RemoveFile],
      removedDVs: Seq[DeletionVectorDescriptor]): Map[String, SQLMetric] = {

    def setAndReturnMetric(description: String, value: Long) = {
      val metric = createMetric(sparkContext, description)
      metric.set(value)
      metric
    }

    def totalSize(actions: Seq[FileAction]): Long = {
      var totalSize = 0L
      actions.foreach { file =>
        val fileSize = file match {
          case addFile: AddFile => addFile.size
          case removeFile: RemoveFile => removeFile.size.getOrElse(0L)
          case default =>
            throw new IllegalArgumentException(s"Unknown FileAction type: ${default.getClass}")
        }
        totalSize += fileSize
      }
      totalSize
    }

    val (deletionVectorRowsRemoved, deletionVectorBytesRemoved) =
      removedDVs.map(dv => (dv.cardinality, dv.sizeInBytes.toLong))
        .reduceLeftOption((dv1, dv2) => (dv1._1 + dv2._1, dv1._2 + dv2._2))
        .getOrElse((0L, 0L))

    val dvMetrics: Map[String, SQLMetric] = Map(
      "numDeletionVectorsRemoved" ->
        setAndReturnMetric(
          "total number of deletion vectors removed",
          removedDVs.size),
      "numDeletionVectorRowsRemoved" ->
        setAndReturnMetric(
          "total number of deletion vector rows removed",
          deletionVectorRowsRemoved),
      "numDeletionVectorBytesRemoved" ->
        setAndReturnMetric(
          "total number of bytes of removed deletion vectors",
          deletionVectorBytesRemoved))

    val sizeStats = FileSizeStatsWithHistogram.create(addedFiles.map(_.size).sorted)
    Map[String, SQLMetric](
      "minFileSize" -> setAndReturnMetric("minimum file size", sizeStats.get.min),
      "p25FileSize" -> setAndReturnMetric("25th percentile file size", sizeStats.get.p25),
      "p50FileSize" -> setAndReturnMetric("50th percentile file size", sizeStats.get.p50),
      "p75FileSize" -> setAndReturnMetric("75th percentile file size", sizeStats.get.p75),
      "maxFileSize" -> setAndReturnMetric("maximum file size", sizeStats.get.max),
      "numAddedFiles" -> setAndReturnMetric("total number of files added.", addedFiles.size),
      "numRemovedFiles" -> setAndReturnMetric("total number of files removed.", removedFiles.size),
      "numAddedBytes" -> setAndReturnMetric("total number of bytes added", totalSize(addedFiles)),
      "numRemovedBytes" ->
        setAndReturnMetric("total number of bytes removed", totalSize(removedFiles))
    ) ++ dvMetrics
  }
}
