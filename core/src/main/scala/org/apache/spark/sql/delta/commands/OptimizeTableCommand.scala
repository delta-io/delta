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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.delta.commands.optimize.{FileSizeStatsWithHistogram, OptimizeMetrics, OptimizeStats}
import org.apache.spark.sql.delta.files.SQLMetricsReporting
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.SPARK_JOB_GROUP_ID
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.{SystemClock, ThreadUtils}

/** Base class defining abstract optimize command */
abstract class OptimizeTableCommandBase extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("path", StringType)(),
    AttributeReference("metrics", Encoders.product[OptimizeMetrics].schema)())
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
    partitionPredicate: Option[String])
  extends OptimizeTableCommandBase {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaLog = getDeltaLog(sparkSession, path, tableId, "OPTIMIZE")

    // Parse the predicate expression into Catalyst expression and verify only simple filters
    // on partition columns are present
    val partitionPredicates = partitionPredicate.map(predicate => {
      val predicates = parsePredicates(sparkSession, predicate)
      verifyPartitionPredicates(
        sparkSession,
        deltaLog.snapshot.metadata.partitionColumns,
        predicates)
      predicates
    }).getOrElse(Seq(Literal.TrueLiteral))

    new OptimizeExecutor(sparkSession, deltaLog, partitionPredicates)
        .optimize()
  }
}

/**
 * Optimize job which compacts small files into larger files to reduce
 * the number of files and potentially allow more efficient reads.
 *
 * @param sparkSession Spark environment reference.
 * @param deltaLog Delta table that is being optimized.
 * @param partitionPredicate List of partition predicates to select subset of files to optimize.
 */
class OptimizeExecutor(
    sparkSession: SparkSession,
    deltaLog: DeltaLog,
    partitionPredicate: Seq[Expression])
  extends DeltaCommand with SQLMetricsReporting with Serializable {

  /** Timestamp to use in [[FileAction]] */
  private val operationTimestamp = new SystemClock().getTimeMillis()

  def optimize(): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.optimize") {
      val minFileSize = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MIN_FILE_SIZE)
      val maxFileSize = sparkSession.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_OPTIMIZE_MAX_FILE_SIZE)
      require(minFileSize > 0, "minFileSize must be > 0")
      require(maxFileSize > 0, "maxFileSize must be > 0")

      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        throw DeltaErrors.notADeltaTableException(deltaLog.dataPath.toString)
      }

      val candidateFiles = txn.filterFiles(partitionPredicate)
      val partitionSchema = txn.metadata.partitionSchema

      val filesToCompact = candidateFiles.filter(_.size < minFileSize)
      val partitionsToCompact = filesToCompact.groupBy(_.partitionValues).toSeq

      val jobs = groupFilesIntoBins(partitionsToCompact, maxFileSize)

      val parallelJobCollection = new ParVector(jobs.toVector)

      // Create a task pool to parallelize the submission of optimization jobs to Spark.
      val threadPool = ThreadUtils.newForkJoinPool(
        "OptimizeJob",
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.DELTA_OPTIMIZE_MAX_THREADS))

      val updates = try {
        val forkJoinPoolTaskSupport = new ForkJoinTaskSupport(threadPool)
        parallelJobCollection.tasksupport = forkJoinPoolTaskSupport

        parallelJobCollection.flatMap(partitionBinGroup =>
          runCompactBinJob(txn, partitionBinGroup._1, partitionBinGroup._2)).seq
      } finally {
        threadPool.shutdownNow()
      }

      val addedFiles = updates.collect { case a: AddFile => a }
      val removedFiles = updates.collect { case r: RemoveFile => r }
      if (addedFiles.size > 0) {
        val operation = DeltaOperations.Optimize(partitionPredicate.map(_.sql))
        val metrics = createMetrics(sparkSession.sparkContext, addedFiles, removedFiles)
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

      return Seq(Row(deltaLog.dataPath.toString, optimizeStats.toOptimizeMetrics))
    }
  }

  /**
   * Utility methods to group files into bins for compaction.
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
          if (file.size + currentBinSize > maxTargetFileSize) {
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

        bins.map(b => (partition, b))
          .filter(_._2.size > 1) // select bins that have at least two files
    }
  }

  /**
   * Utility method to run a Spark job to compact the files in given bin
   *
   * @param txn [[OptimisticTransaction]] instance in use to commit the changes to DeltaLog.
   * @param partition Partition values of the partition that files in [[bin]] belongs to.
   * @param bin List of files to compact into one large file.
   */
  private def runCompactBinJob(
      txn: OptimisticTransaction,
      partition: Map[String, String],
      bin: Seq[AddFile]): Seq[FileAction] = {
    val baseTablePath = deltaLog.dataPath

    val input = txn.deltaLog.createDataFrame(txn.snapshot, bin, actionTypeOpt = Some("Optimize"))
    val repartitionDF = input.coalesce(numPartitions = 1)

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
      removedFiles: Seq[RemoveFile]): Map[String, SQLMetric] = {

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
        setAndReturnMetric("total number of bytes removed", totalSize(removedFiles)))
  }
}
