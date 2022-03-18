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

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.commands.MergeIntoCommand.totalBytesAndDistinctPartitionValues
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, Expression, InputFileName, Literal, Not}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{DeltaDelete, LogicalPlan}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.functions.udf

/**
 * Performs a Delete based on the search condition
 *
 * Algorithm:
 *   1) Scan all the files and determine which files have
 *      the rows that need to be deleted.
 *   2) Traverse the affected files and rebuild the touched files.
 *   3) Use the Delta protocol to atomically write the remaining rows to new files and remove
 *      the affected files that are identified in step 1.
 */
case class DeleteCommand(
    deltaLog: DeltaLog,
    target: LogicalPlan,
    condition: Option[Expression])
  extends LeafRunnableCommand with DeltaCommand {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  override lazy val metrics = Map[String, SQLMetric](
    "numRemovedFiles" -> createMetric(sc, "number of files removed."),
    "numAddedFiles" -> createMetric(sc, "number of files added."),
    "numDeletedRows" -> createMetric(sc, "number of rows deleted."),
    "numFilesBeforeSkipping" -> createMetric(sc, "number of files before skipping"),
    "numBytesBeforeSkipping" -> createMetric(sc, "number of bytes before skipping"),
    "numFilesAfterSkipping" -> createMetric(sc, "number of files after skipping"),
    "numBytesAfterSkipping" -> createMetric(sc, "number of bytes after skipping"),
    "numPartitionsAfterSkipping" -> createMetric(sc, "number of partitions after skipping"),
    "numPartitionsAddedTo" -> createMetric(sc, "number of partitions added"),
    "numPartitionsRemovedFrom" -> createMetric(sc, "number of partitions removed"),
    "numCopiedRows" -> createMetric(sc, "number of rows copied"),
    "numBytesAdded" -> createMetric(sc, "number of bytes added"),
    "numBytesRemoved" -> createMetric(sc, "number of bytes removed"),
    "executionTimeMs" -> createMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" -> createMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" -> createMetric(sc, "time taken to rewrite the matched files")
  )

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.delete") {
      deltaLog.assertRemovable()
      deltaLog.withNewTransaction { txn =>
        val deleteActions = performDelete(sparkSession, deltaLog, txn)
        if (deleteActions.nonEmpty) {
          txn.commit(deleteActions, DeltaOperations.Delete(condition.map(_.sql).toSeq))
        }
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }

    Seq.empty[Row]
  }

  def performDelete(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      txn: OptimisticTransaction): Seq[Action] = {
    import sparkSession.implicits._

    var numRemovedFiles: Long = 0
    var numAddedFiles: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0
    var numBytesAdded: Long = 0
    var numBytesRemoved: Long = 0
    var numFilesBeforeSkipping: Long = 0
    var numBytesBeforeSkipping: Long = 0
    var numFilesAfterSkipping: Long = 0
    var numBytesAfterSkipping: Long = 0
    var numPartitionsAfterSkipping: Option[Long] = None
    var numPartitionsRemovedFrom: Option[Long] = None
    var numPartitionsAddedTo: Option[Long] = None
    var numDeletedRows: Option[Long] = None
    var numCopiedRows: Option[Long] = None

    val startTime = System.nanoTime()
    val numFilesTotal = deltaLog.snapshot.numOfFiles

    val deleteActions: Seq[Action] = condition match {
      case None =>
        // Case 1: Delete the whole table if the condition is true
        val allFiles = txn.filterFiles(Nil)

        numRemovedFiles = allFiles.size
        scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
        val (numBytes, numPartitions) = totalBytesAndDistinctPartitionValues(allFiles)
        numBytesRemoved = numBytes
        numFilesBeforeSkipping = numRemovedFiles
        numBytesBeforeSkipping = numBytes
        numFilesAfterSkipping = numRemovedFiles
        numBytesAfterSkipping = numBytes
        if (txn.metadata.partitionColumns.nonEmpty) {
          numPartitionsAfterSkipping = Some(numPartitions)
        }
        val operationTimestamp = System.currentTimeMillis()
        metrics("numRemovedFiles").set(allFiles.size)
        allFiles.map(_.removeWithTimestamp(operationTimestamp))
      case Some(cond) =>
        val (metadataPredicates, otherPredicates) =
          DeltaTableUtils.splitMetadataAndDataPredicates(
            cond, txn.metadata.partitionColumns, sparkSession)

        numFilesBeforeSkipping = txn.snapshot.numOfFiles
        numBytesBeforeSkipping = txn.snapshot.sizeInBytes

        if (otherPredicates.isEmpty) {
          // Case 2: The condition can be evaluated using metadata only.
          //         Delete a set of files without the need of scanning any data files.
          val operationTimestamp = System.currentTimeMillis()
          val candidateFiles = txn.filterFiles(metadataPredicates)

          scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
          numRemovedFiles = candidateFiles.size
          numBytesRemoved = candidateFiles.map(_.size).sum
          numFilesAfterSkipping = candidateFiles.size
          val (numCandidateBytes, numCandidatePartitions) =
            totalBytesAndDistinctPartitionValues(candidateFiles)
          numBytesAfterSkipping = numCandidateBytes
          if (txn.metadata.partitionColumns.nonEmpty) {
            numPartitionsAfterSkipping = Some(numCandidatePartitions)
          }
          metrics("numRemovedFiles").set(numRemovedFiles)
          candidateFiles.map(_.removeWithTimestamp(operationTimestamp))
        } else {
          // Case 3: Delete the rows based on the condition.
          val candidateFiles = txn.filterFiles(metadataPredicates ++ otherPredicates)

          numFilesAfterSkipping = candidateFiles.size
          val (numCandidateBytes, numCandidatePartitions) =
            totalBytesAndDistinctPartitionValues(candidateFiles)
          numBytesAfterSkipping = numCandidateBytes
          if (txn.metadata.partitionColumns.nonEmpty) {
            numPartitionsAfterSkipping = Some(numCandidatePartitions)
          }

          numRemovedFiles = candidateFiles.size
          val nameToAddFileMap = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

          val fileIndex = new TahoeBatchFileIndex(
            sparkSession, "delete", candidateFiles, deltaLog, deltaLog.dataPath, txn.snapshot)
          // Keep everything from the resolved target except a new TahoeFileIndex
          // that only involves the affected files instead of all files.
          val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
          val data = Dataset.ofRows(sparkSession, newTarget)
          val deletedRowCount = metrics("numDeletedRows")
          val deletedRowUdf = udf { () =>
            deletedRowCount += 1
            true
          }.asNondeterministic()
          val filesToRewrite =
            withStatusCode("DELTA", s"Finding files to rewrite for DELETE operation") {
              if (numRemovedFiles == 0) {
                Array.empty[String]
              } else {
                data
                  .filter(new Column(cond))
                  .filter(deletedRowUdf())
                  .select(new Column(InputFileName())).distinct()
                  .as[String].collect()
              }
            }

          metrics("numRemovedFiles").set(filesToRewrite.size)
          scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
          if (filesToRewrite.isEmpty) {
            // Case 3.1: no row matches and no delete will be triggered
            Nil
          } else {
            // Case 3.2: some files need an update to remove the deleted files
            // Do the second pass and just read the affected files
            val baseRelation = buildBaseRelation(
              sparkSession, txn, "delete", deltaLog.dataPath, filesToRewrite, nameToAddFileMap)
            // Keep everything from the resolved target except a new TahoeFileIndex
            // that only involves the affected files instead of all files.
            val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
            val copiedRowCount = metrics("numCopiedRows")
            val copiedRowUdf = udf { () =>
              copiedRowCount += 1
              true
            }.asNondeterministic()
            val targetDF = Dataset.ofRows(sparkSession, newTarget)
            val filterCond = Not(EqualNullSafe(cond, Literal.TrueLiteral))
            val updatedDF = targetDF.filter(new Column(filterCond))
              .filter(copiedRowUdf())

            val rewrittenFiles = withStatusCode(
              "DELTA", s"Rewriting ${filesToRewrite.size} files for DELETE operation") {
              txn.writeFiles(updatedDF)
            }
            val removedFiles = filesToRewrite.map(f =>
              getTouchedFile(deltaLog.dataPath, f, nameToAddFileMap))
            val (removedBytes, removedPartitions) =
              totalBytesAndDistinctPartitionValues(removedFiles)
            numBytesRemoved = removedBytes
            val (rewrittenBytes, rewrittenPartitions) =
              totalBytesAndDistinctPartitionValues(rewrittenFiles)
            numBytesAdded = rewrittenBytes
            if (txn.metadata.partitionColumns.nonEmpty) {
              numPartitionsRemovedFrom = Some(removedPartitions)
              numPartitionsAddedTo = Some(rewrittenPartitions)
            }
            numAddedFiles = rewrittenFiles.size
            rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs
            numDeletedRows = Some(metrics("numDeletedRows").value)
            numCopiedRows = Some(metrics("numCopiedRows").value)

            val operationTimestamp = System.currentTimeMillis()
            removeFilesFromPaths(deltaLog, nameToAddFileMap, filesToRewrite, operationTimestamp) ++
              rewrittenFiles
          }
        }
    }
    metrics("numAddedFiles").set(numAddedFiles)
    val executionTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
    metrics("executionTimeMs").set(executionTimeMs)
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    metrics("numBytesAdded").set(numBytesAdded)
    metrics("numBytesRemoved").set(numBytesRemoved)
    metrics("numFilesBeforeSkipping").set(numFilesBeforeSkipping)
    metrics("numBytesBeforeSkipping").set(numBytesBeforeSkipping)
    metrics("numFilesAfterSkipping").set(numFilesAfterSkipping)
    metrics("numBytesAfterSkipping").set(numBytesAfterSkipping)
    numPartitionsAfterSkipping.foreach(metrics("numPartitionsAfterSkipping").set)
    numPartitionsAddedTo.foreach(metrics("numPartitionsAddedTo").set)
    numPartitionsRemovedFrom.foreach(metrics("numPartitionsRemovedFrom").set)
    txn.registerSQLMetrics(sparkSession, metrics)
    // This is needed to make the SQL metrics visible in the Spark UI
    val executionId = sparkSession.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkSession.sparkContext, executionId, metrics.values.toSeq)

    recordDeltaEvent(
      deltaLog,
      "delta.dml.delete.stats",
      data = DeleteMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numRemovedFiles,
        numAddedFiles,
        numRemovedFiles,
        numAddedFiles,
        numAddedChangeFiles = 0,
        numFilesBeforeSkipping,
        numBytesBeforeSkipping,
        numFilesAfterSkipping,
        numBytesAfterSkipping,
        numPartitionsAfterSkipping,
        numPartitionsAddedTo,
        numPartitionsRemovedFrom,
        numCopiedRows,
        numDeletedRows,
        numBytesAdded,
        numBytesRemoved,
        changeFileBytes = 0,
        scanTimeMs,
        rewriteTimeMs)
    )

    deleteActions
  }
}

object DeleteCommand {
  def apply(delete: DeltaDelete): DeleteCommand = {
    val index = EliminateSubqueryAliases(delete.child) match {
      case DeltaFullTable(tahoeFileIndex) =>
        tahoeFileIndex
      case o =>
        throw DeltaErrors.notADeltaSourceException("DELETE", Some(o))
    }
    DeleteCommand(index.deltaLog, delete.child, delete.condition)
  }

  val FILE_NAME_COLUMN = "_input_file_name_"
}

/**
 * Used to report details about delete.
 *
 * @param condition: what was the delete condition
 * @param numFilesTotal: how big is the table
 * @param numTouchedFiles: how many files did we touch. Alias for `numRemovedFiles`
 * @param numRewrittenFiles: how many files had to be rewritten. Alias for `numAddedFiles`
 * @param numRemovedFiles: how many files we removed. Alias for `numTouchedFiles`
 * @param numAddedFiles: how many files we added. Alias for `numRewrittenFiles`
 * @param numAddedChangeFiles: how many change files were generated
 * @param numFilesBeforeSkipping: how many candidate files before skipping
 * @param numBytesBeforeSkipping: how many candidate bytes before skipping
 * @param numFilesAfterSkipping: how many candidate files after skipping
 * @param numBytesAfterSkipping: how many candidate bytes after skipping
 * @param numPartitionsAfterSkipping: how many candidate partitions after skipping
 * @param numPartitionsAddedTo: how many new partitions were added
 * @param numPartitionsRemovedFrom: how many partitions were removed
 * @param numCopiedRows: how many rows were copied
 * @param numDeletedRows: how many rows were deleted
 * @param numBytesAdded: how many bytes were added
 * @param numBytesRemoved: how many bytes were removed
 * @param changeFileBytes: total size of change files generated
 * @param scanTimeMs: how long did finding take
 * @param rewriteTimeMs: how long did rewriting take
 *
 * @note All the time units are milliseconds.
 */
case class DeleteMetric(
    condition: String,
    numFilesTotal: Long,
    numTouchedFiles: Long,
    numRewrittenFiles: Long,
    numRemovedFiles: Long,
    numAddedFiles: Long,
    numAddedChangeFiles: Long,
    numFilesBeforeSkipping: Long,
    numBytesBeforeSkipping: Long,
    numFilesAfterSkipping: Long,
    numBytesAfterSkipping: Long,
    numPartitionsAfterSkipping: Option[Long],
    numPartitionsAddedTo: Option[Long],
    numPartitionsRemovedFrom: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numCopiedRows: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numDeletedRows: Option[Long],
    numBytesAdded: Long,
    numBytesRemoved: Long,
    changeFileBytes: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long
)
