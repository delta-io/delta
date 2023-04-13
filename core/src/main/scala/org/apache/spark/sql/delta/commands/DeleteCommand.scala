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
import org.apache.spark.sql.delta.actions.{Action, AddCDCFile, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.DeleteCommand.{rewritingFilesMsg, FINDING_TOUCHED_FILES_MSG}
import org.apache.spark.sql.delta.commands.MergeIntoCommand.totalBytesAndDistinctPartitionValues
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.Utils
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualNullSafe, Expression, If, Literal, Not}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{DeltaDelete, LogicalPlan}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.LongType

trait DeleteCommandMetrics { self: LeafRunnableCommand =>
  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()

  def createMetrics: Map[String, SQLMetric] = Map[String, SQLMetric](
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
    "numAddedBytes" -> createMetric(sc, "number of bytes added"),
    "numRemovedBytes" -> createMetric(sc, "number of bytes removed"),
    "executionTimeMs" ->
      createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
      createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createTimingMetric(sc, "time taken to rewrite the matched files"),
    "numAddedChangeFiles" -> createMetric(sc, "number of change data capture files generated"),
    "changeFileBytes" -> createMetric(sc, "total size of change data capture files generated"),
    "numTouchedRows" -> createMetric(sc, "number of rows touched")
  )

  def getDeletedRowsFromAddFilesAndUpdateMetrics(files: Seq[AddFile]) : Option[Long] = {
    if (!conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)) {
      return None;
    }
    // No file to get metadata, return none to be consistent with metadata stats disabled
    if (files.isEmpty) {
      return None
    }
    // Return None if any file does not contain numLogicalRecords status
    var count: Long = 0
    for (file <- files) {
      if (file.numLogicalRecords.isEmpty) {
        return None
      }
      count += file.numLogicalRecords.get
    }
    metrics("numDeletedRows").set(count)
    return Some(count)
  }
}

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
  extends LeafRunnableCommand with DeltaCommand with DeleteCommandMetrics {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  override val output: Seq[Attribute] = Seq(AttributeReference("num_affected_rows", LongType)())

  override lazy val metrics = createMetrics

  final override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaLog, "delta.dml.delete") {
      deltaLog.withNewTransaction { txn =>
        DeltaLog.assertRemovable(txn.snapshot)
        if (hasBeenExecuted(txn, sparkSession)) {
          sendDriverMetrics(sparkSession, metrics)
          return Seq.empty
        }

        val deleteActions = performDelete(sparkSession, deltaLog, txn)
        txn.commitIfNeeded(deleteActions, DeltaOperations.Delete(condition.map(_.sql).toSeq))
      }
      // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
      // this data source relation.
      sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    }

    // Adjust for deletes at partition boundaries. Deletes at partition boundaries is a metadata
    // operation, therefore we don't actually have any information around how many rows were deleted
    // While this info may exist in the file statistics, it's not guaranteed that we have these
    // statistics. To avoid any performance regressions, we currently just return a -1 in such cases
    if (metrics("numRemovedFiles").value > 0 && metrics("numDeletedRows").value == 0) {
      Seq(Row(-1L))
    } else {
      Seq(Row(metrics("numDeletedRows").value))
    }
  }

  def performDelete(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      txn: OptimisticTransaction): Seq[Action] = {
    import org.apache.spark.sql.delta.implicits._

    var numRemovedFiles: Long = 0
    var numAddedFiles: Long = 0
    var numAddedChangeFiles: Long = 0
    var scanTimeMs: Long = 0
    var rewriteTimeMs: Long = 0
    var numAddedBytes: Long = 0
    var changeFileBytes: Long = 0
    var numRemovedBytes: Long = 0
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
    val numFilesTotal = txn.snapshot.numOfFiles

    val deleteActions: Seq[Action] = condition match {
      case None =>
        // Case 1: Delete the whole table if the condition is true
        val reportRowLevelMetrics = conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)
        val allFiles = txn.filterFiles(Nil, keepNumRecords = reportRowLevelMetrics)

        numRemovedFiles = allFiles.size
        scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
        val (numBytes, numPartitions) = totalBytesAndDistinctPartitionValues(allFiles)
        numRemovedBytes = numBytes
        numFilesBeforeSkipping = numRemovedFiles
        numBytesBeforeSkipping = numBytes
        numFilesAfterSkipping = numRemovedFiles
        numBytesAfterSkipping = numBytes
        numDeletedRows = getDeletedRowsFromAddFilesAndUpdateMetrics(allFiles)

        if (txn.metadata.partitionColumns.nonEmpty) {
          numPartitionsAfterSkipping = Some(numPartitions)
          numPartitionsRemovedFrom = Some(numPartitions)
          numPartitionsAddedTo = Some(0)
        }
        val operationTimestamp = System.currentTimeMillis()
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
          val reportRowLevelMetrics = conf.getConf(DeltaSQLConf.DELTA_DML_METRICS_FROM_METADATA)
          val candidateFiles =
            txn.filterFiles(metadataPredicates, keepNumRecords = reportRowLevelMetrics)

          scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
          numRemovedFiles = candidateFiles.size
          numRemovedBytes = candidateFiles.map(_.size).sum
          numFilesAfterSkipping = candidateFiles.size
          val (numCandidateBytes, numCandidatePartitions) =
            totalBytesAndDistinctPartitionValues(candidateFiles)
          numBytesAfterSkipping = numCandidateBytes
          numDeletedRows = getDeletedRowsFromAddFilesAndUpdateMetrics(candidateFiles)

          if (txn.metadata.partitionColumns.nonEmpty) {
            numPartitionsAfterSkipping = Some(numCandidatePartitions)
            numPartitionsRemovedFrom = Some(numCandidatePartitions)
            numPartitionsAddedTo = Some(0)
          }
          candidateFiles.map(_.removeWithTimestamp(operationTimestamp))
        } else {
          // Case 3: Delete the rows based on the condition.

          // Should we write the DVs to represent the deleted rows?
          val shouldWriteDVs = shouldWritePersistentDeletionVectors(sparkSession, txn)

          val candidateFiles = txn.filterFiles(
            metadataPredicates ++ otherPredicates,
            keepNumRecords = shouldWriteDVs)
          // `candidateFiles` contains the files filtered using statistics and delete condition
          // They may or may not contains any rows that need to be deleted.

          numFilesAfterSkipping = candidateFiles.size
          val (numCandidateBytes, numCandidatePartitions) =
            totalBytesAndDistinctPartitionValues(candidateFiles)
          numBytesAfterSkipping = numCandidateBytes
          if (txn.metadata.partitionColumns.nonEmpty) {
            numPartitionsAfterSkipping = Some(numCandidatePartitions)
          }

          val nameToAddFileMap = generateCandidateFileMap(deltaLog.dataPath, candidateFiles)

          val fileIndex = new TahoeBatchFileIndex(
            sparkSession, "delete", candidateFiles, deltaLog, deltaLog.dataPath, txn.snapshot)
          if (shouldWriteDVs) {
            val targetDf = DeleteWithDeletionVectorsHelper.createTargetDfForScanningForMatches(
              sparkSession,
              target,
              fileIndex)

            // Does the target table already has DVs enabled? If so, we need to read the table
            // with deletion vectors.
            val mustReadDeletionVectors = DeletionVectorUtils.deletionVectorsReadable(txn.snapshot)

            val touchedFiles = DeleteWithDeletionVectorsHelper.findTouchedFiles(
              sparkSession,
              txn,
              mustReadDeletionVectors,
              deltaLog,
              targetDf,
              fileIndex,
              cond)

            if (touchedFiles.nonEmpty) {
              DeleteWithDeletionVectorsHelper.processUnmodifiedData(
                sparkSession,
                touchedFiles,
                txn.snapshot)
            } else {
              Nil // Nothing to update
            }
          } else {
            // Keep everything from the resolved target except a new TahoeFileIndex
            // that only involves the affected files instead of all files.
            val newTarget = DeltaTableUtils.replaceFileIndex(target, fileIndex)
            val data = Dataset.ofRows(sparkSession, newTarget)
            val deletedRowCount = metrics("numDeletedRows")
            val deletedRowUdf = DeltaUDF.boolean { () =>
              deletedRowCount += 1
              true
            }.asNondeterministic()
            val filesToRewrite =
              withStatusCode("DELTA", FINDING_TOUCHED_FILES_MSG) {
                if (candidateFiles.isEmpty) {
                  Array.empty[String]
                } else {
                  data.filter(new Column(cond))
                    .select(input_file_name())
                    .filter(deletedRowUdf())
                    .distinct()
                    .as[String]
                    .collect()
                }
              }

            numRemovedFiles = filesToRewrite.length
            scanTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
            if (filesToRewrite.isEmpty) {
              // Case 3.1: no row matches and no delete will be triggered
              if (txn.metadata.partitionColumns.nonEmpty) {
                numPartitionsRemovedFrom = Some(0)
                numPartitionsAddedTo = Some(0)
              }
              Nil
            } else {
              // Case 3.2: some files need an update to remove the deleted files
              // Do the second pass and just read the affected files
              val baseRelation = buildBaseRelation(
                sparkSession, txn, "delete", deltaLog.dataPath, filesToRewrite, nameToAddFileMap)
              // Keep everything from the resolved target except a new TahoeFileIndex
              // that only involves the affected files instead of all files.
              val newTarget = DeltaTableUtils.replaceFileIndex(target, baseRelation.location)
              val targetDF = Dataset.ofRows(sparkSession, newTarget)
              val filterCond = Not(EqualNullSafe(cond, Literal.TrueLiteral))
              val rewrittenActions = rewriteFiles(txn, targetDF, filterCond, filesToRewrite.length)
              val (changeFiles, rewrittenFiles) = rewrittenActions
                .partition(_.isInstanceOf[AddCDCFile])
              numAddedFiles = rewrittenFiles.size
              val removedFiles = filesToRewrite.map(f =>
                getTouchedFile(deltaLog.dataPath, f, nameToAddFileMap))
              val (removedBytes, removedPartitions) =
                totalBytesAndDistinctPartitionValues(removedFiles)
              numRemovedBytes = removedBytes
              val (rewrittenBytes, rewrittenPartitions) =
                totalBytesAndDistinctPartitionValues(rewrittenFiles)
              numAddedBytes = rewrittenBytes
              if (txn.metadata.partitionColumns.nonEmpty) {
                numPartitionsRemovedFrom = Some(removedPartitions)
                numPartitionsAddedTo = Some(rewrittenPartitions)
              }
              numAddedChangeFiles = changeFiles.size
              changeFileBytes = changeFiles.collect { case f: AddCDCFile => f.size }.sum
              rewriteTimeMs = (System.nanoTime() - startTime) / 1000 / 1000 - scanTimeMs
              numDeletedRows = Some(metrics("numDeletedRows").value)
              numCopiedRows =
                Some(metrics("numTouchedRows").value - metrics("numDeletedRows").value)

              val operationTimestamp = System.currentTimeMillis()
              removeFilesFromPaths(
                deltaLog, nameToAddFileMap, filesToRewrite, operationTimestamp) ++ rewrittenActions
            }
          }
        }
    }
    metrics("numRemovedFiles").set(numRemovedFiles)
    metrics("numAddedFiles").set(numAddedFiles)
    val executionTimeMs = (System.nanoTime() - startTime) / 1000 / 1000
    metrics("executionTimeMs").set(executionTimeMs)
    metrics("scanTimeMs").set(scanTimeMs)
    metrics("rewriteTimeMs").set(rewriteTimeMs)
    metrics("numAddedChangeFiles").set(numAddedChangeFiles)
    metrics("changeFileBytes").set(changeFileBytes)
    metrics("numAddedBytes").set(numAddedBytes)
    metrics("numRemovedBytes").set(numRemovedBytes)
    metrics("numFilesBeforeSkipping").set(numFilesBeforeSkipping)
    metrics("numBytesBeforeSkipping").set(numBytesBeforeSkipping)
    metrics("numFilesAfterSkipping").set(numFilesAfterSkipping)
    metrics("numBytesAfterSkipping").set(numBytesAfterSkipping)
    numPartitionsAfterSkipping.foreach(metrics("numPartitionsAfterSkipping").set)
    numPartitionsAddedTo.foreach(metrics("numPartitionsAddedTo").set)
    numPartitionsRemovedFrom.foreach(metrics("numPartitionsRemovedFrom").set)
    numCopiedRows.foreach(metrics("numCopiedRows").set)
    txn.registerSQLMetrics(sparkSession, metrics)
    sendDriverMetrics(sparkSession, metrics)

    recordDeltaEvent(
      deltaLog,
      "delta.dml.delete.stats",
      data = DeleteMetric(
        condition = condition.map(_.sql).getOrElse("true"),
        numFilesTotal,
        numFilesAfterSkipping,
        numAddedFiles,
        numRemovedFiles,
        numAddedFiles,
        numAddedChangeFiles = numAddedChangeFiles,
        numFilesBeforeSkipping,
        numBytesBeforeSkipping,
        numFilesAfterSkipping,
        numBytesAfterSkipping,
        numPartitionsAfterSkipping,
        numPartitionsAddedTo,
        numPartitionsRemovedFrom,
        numCopiedRows,
        numDeletedRows,
        numAddedBytes,
        numRemovedBytes,
        changeFileBytes = changeFileBytes,
        scanTimeMs,
        rewriteTimeMs)
    )

    if (deleteActions.nonEmpty) {
      createSetTransaction(sparkSession, deltaLog).toSeq ++ deleteActions
    } else {
      Seq.empty
    }
  }

  /**
   * Returns the list of [[AddFile]]s and [[AddCDCFile]]s that have been re-written.
   */
  private def rewriteFiles(
      txn: OptimisticTransaction,
      baseData: DataFrame,
      filterCondition: Expression,
      numFilesToRewrite: Long): Seq[FileAction] = {
    val shouldWriteCdc = DeltaConfigs.CHANGE_DATA_FEED.fromMetaData(txn.metadata)

    // number of total rows that we have seen / are either copying or deleting (sum of both).
    val numTouchedRows = metrics("numTouchedRows")
    val numTouchedRowsUdf = DeltaUDF.boolean { () =>
      numTouchedRows += 1
      true
    }.asNondeterministic()

    withStatusCode(
      "DELTA", rewritingFilesMsg(numFilesToRewrite)) {
      val dfToWrite = if (shouldWriteCdc) {
        import org.apache.spark.sql.delta.commands.cdc.CDCReader._
        // The logic here ends up being surprisingly elegant, with all source rows ending up in
        // the output. Recall that we flipped the user-provided delete condition earlier, before the
        // call to `rewriteFiles`. All rows which match this latest `filterCondition` are retained
        // as table data, while all rows which don't match are removed from the rewritten table data
        // but do get included in the output as CDC events.
        baseData
          .filter(numTouchedRowsUdf())
          .withColumn(
            CDC_TYPE_COLUMN_NAME,
            new Column(If(filterCondition, CDC_TYPE_NOT_CDC, CDC_TYPE_DELETE))
          )
      } else {
        baseData
          .filter(numTouchedRowsUdf())
          .filter(new Column(filterCondition))
      }

      txn.writeFiles(dfToWrite)
    }
  }

  def shouldWritePersistentDeletionVectors(
      spark: SparkSession, txn: OptimisticTransaction): Boolean = {
    // DELETE with DVs only enabled for tests.
    Utils.isTesting &&
      spark.conf.get(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS) &&
        DeletionVectorUtils.deletionVectorsWritable(txn.snapshot)
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

  val FILE_NAME_COLUMN: String = "_input_file_name_"
  val FINDING_TOUCHED_FILES_MSG: String = "Finding files to rewrite for DELETE operation"

  def rewritingFilesMsg(numFilesToRewrite: Long): String =
    s"Rewriting $numFilesToRewrite files for DELETE operation"
}

/**
 * Used to report details about delete.
 *
 * @param condition: what was the delete condition
 * @param numFilesTotal: how big is the table
 * @param numTouchedFiles: how many files did we touch. Alias for `numFilesAfterSkipping`
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
