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

import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction}
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSource, MergeIntoMaterializeSourceReason, MergeStats}
import org.apache.spark.sql.delta.files.{TahoeBatchFileIndex, TahoeFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

abstract class MergeIntoCommandBase extends LeafRunnableCommand
  with DeltaCommand
  with DeltaLogging
  with PredicateHelper
  with MergeIntoMaterializeSource {

  @transient val source: LogicalPlan
  @transient val target: LogicalPlan
  @transient val targetFileIndex: TahoeFileIndex
  val condition: Expression
  val matchedClauses: Seq[DeltaMergeIntoMatchedClause]
  val notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause]
  val notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause]
  val migratedSchema: Option[StructType]


  @transient protected lazy val sc: SparkContext = SparkContext.getOrCreate()
  @transient protected lazy val targetDeltaLog: DeltaLog = targetFileIndex.deltaLog

  /**
   * Map to get target output attributes by name.
   * The case sensitivity of the map is set accordingly to Spark configuration.
   */
  @transient private lazy val targetOutputAttributesMap: Map[String, Attribute] = {
    val attrMap: Map[String, Attribute] = target
      .outputSet.view
      .map(attr => attr.name -> attr).toMap
    if (conf.caseSensitiveAnalysis) {
      attrMap
    } else {
      CaseInsensitiveMap(attrMap)
    }
  }

  /** Whether this merge statement has only MATCHED clauses. */
  protected def isMatchedOnly: Boolean = notMatchedClauses.isEmpty && matchedClauses.nonEmpty &&
    notMatchedBySourceClauses.isEmpty

  /** Whether this merge statement only has only insert (NOT MATCHED) clauses. */
  protected def isInsertOnly: Boolean = matchedClauses.isEmpty && notMatchedClauses.nonEmpty &&
    notMatchedBySourceClauses.isEmpty

  import SQLMetrics._

  override lazy val metrics: Map[String, SQLMetric] = baseMetrics

  lazy val baseMetrics: Map[String, SQLMetric] = Map(
    "numSourceRows" -> createMetric(sc, "number of source rows"),
    "numSourceRowsInSecondScan" ->
      createMetric(sc, "number of source rows (during repeated scan)"),
    "numTargetRowsCopied" -> createMetric(sc, "number of target rows rewritten unmodified"),
    "numTargetRowsInserted" -> createMetric(sc, "number of inserted rows"),
    "numTargetRowsUpdated" -> createMetric(sc, "number of updated rows"),
    "numTargetRowsMatchedUpdated" -> createMetric(sc, "number of rows updated by a matched clause"),
    "numTargetRowsNotMatchedBySourceUpdated" ->
      createMetric(sc, "number of rows updated by a not matched by source clause"),
    "numTargetRowsDeleted" -> createMetric(sc, "number of deleted rows"),
    "numTargetRowsMatchedDeleted" -> createMetric(sc, "number of rows deleted by a matched clause"),
    "numTargetRowsNotMatchedBySourceDeleted" ->
      createMetric(sc, "number of rows deleted by a not matched by source clause"),
    "numTargetFilesBeforeSkipping" -> createMetric(sc, "number of target files before skipping"),
    "numTargetFilesAfterSkipping" -> createMetric(sc, "number of target files after skipping"),
    "numTargetFilesRemoved" -> createMetric(sc, "number of files removed to target"),
    "numTargetFilesAdded" -> createMetric(sc, "number of files added to target"),
    "numTargetChangeFilesAdded" ->
      createMetric(sc, "number of change data capture files generated"),
    "numTargetChangeFileBytes" ->
      createMetric(sc, "total size of change data capture files generated"),
    "numTargetBytesBeforeSkipping" -> createMetric(sc, "number of target bytes before skipping"),
    "numTargetBytesAfterSkipping" -> createMetric(sc, "number of target bytes after skipping"),
    "numTargetBytesRemoved" -> createMetric(sc, "number of target bytes removed"),
    "numTargetBytesAdded" -> createMetric(sc, "number of target bytes added"),
    "numTargetPartitionsAfterSkipping" ->
      createMetric(sc, "number of target partitions after skipping"),
    "numTargetPartitionsRemovedFrom" ->
      createMetric(sc, "number of target partitions from which files were removed"),
    "numTargetPartitionsAddedTo" ->
      createMetric(sc, "number of target partitions to which files were added"),
    "executionTimeMs" ->
      createTimingMetric(sc, "time taken to execute the entire operation"),
    "scanTimeMs" ->
      createTimingMetric(sc, "time taken to scan the files for matches"),
    "rewriteTimeMs" ->
      createTimingMetric(sc, "time taken to rewrite the matched files")
  )

  protected def collectMergeStats(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction,
      startTime: Long,
      mergeActions: Seq[Action],
      materializeSourceReason: MergeIntoMaterializeSourceReason.MergeIntoMaterializeSourceReason,
      tags: Map[String, String] = Map.empty)
    : MergeStats = {
    val finalActions = createSetTransaction(spark, targetDeltaLog).toSeq ++ mergeActions
    // Metrics should be recorded before commit (where they are written to delta logs).
    metrics("executionTimeMs").set((System.nanoTime() - startTime) / 1000 / 1000)
    deltaTxn.registerSQLMetrics(spark, metrics)

    // We only detect changes in the number of source rows. This is a best-effort detection; a
    // more comprehensive solution would be to checksum the values for the columns that we read
    // in both jobs.
    // If numSourceRowsInSecondScan is < 0 then it hasn't run, e.g. for insert-only merges.
    // In that case we have only read the source table once.
    if (metrics("numSourceRowsInSecondScan").value >= 0 &&
        metrics("numSourceRows").value != metrics("numSourceRowsInSecondScan").value) {
      log.warn(s"Merge source has ${metrics("numSourceRows")} rows in initial scan but " +
        s"${metrics("numSourceRowsInSecondScan")} rows in second scan")
      if (conf.getConf(DeltaSQLConf.MERGE_FAIL_IF_SOURCE_CHANGED)) {
        throw DeltaErrors.sourceNotDeterministicInMergeException(spark)
      }
    }

    deltaTxn.commitIfNeeded(
      finalActions,
      DeltaOperations.Merge(
        Option(condition),
        matchedPredicates = matchedClauses.map(DeltaOperations.MergePredicate(_)),
        notMatchedPredicates = notMatchedClauses.map(DeltaOperations.MergePredicate(_)),
        notMatchedBySourcePredicates =
          notMatchedBySourceClauses.map(DeltaOperations.MergePredicate(_))
      ),
      tags)

    // Record metrics.
    val stats = MergeStats.fromMergeSQLMetrics(
      metrics,
      condition,
      matchedClauses,
      notMatchedClauses,
      notMatchedBySourceClauses,
      isPartitioned = deltaTxn.metadata.partitionColumns.nonEmpty)
    stats.copy(
      materializeSourceReason = Some(materializeSourceReason.toString),
      materializeSourceAttempts = Some(attempt))
  }

  protected def shouldOptimizeMatchedOnlyMerge(spark: SparkSession): Boolean = {
    isMatchedOnly && spark.conf.get(DeltaSQLConf.MERGE_MATCHED_ONLY_ENABLED)
  }
  /**
   * Write the output data to files, repartitioning the output DataFrame by the partition columns
   * if table is partitioned and `merge.repartitionBeforeWrite.enabled` is set to true.
   */
  protected def writeFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      outputDF: DataFrame): Seq[FileAction] = {
    val partitionColumns = txn.metadata.partitionColumns
    if (partitionColumns.nonEmpty && spark.conf.get(DeltaSQLConf.MERGE_REPARTITION_BEFORE_WRITE)) {
      txn.writeFiles(outputDF.repartition(partitionColumns.map(col): _*))
    } else {
      txn.writeFiles(outputDF)
    }
  }

  /**
   * Build a new logical plan to read the given `files` instead of the whole target table.
   * The plan returned has the same output columns (exprIds) as the `target` logical plan, so that
   * existing update/insert expressions can be applied on this new plan. Unneeded non-partition
   * columns may be dropped.
   */
  protected def buildTargetPlanWithFiles(
      spark: SparkSession,
      deltaTxn: OptimisticTransaction,
      files: Seq[AddFile],
      columnsToDrop: Seq[String]): LogicalPlan = {
    // Action type "batch" is a historical artifact; the original implementation used it.
    val fileIndex = new TahoeBatchFileIndex(
      spark,
      actionType = "batch",
      files,
      deltaTxn.deltaLog,
      targetFileIndex.path,
      deltaTxn.snapshot)

    buildTargetPlanWithIndex(
      spark,
      deltaTxn,
      fileIndex,
      columnsToDrop
    )
  }

  /**
   * Build a new logical plan to read the target table using the given `fileIndex`.
   * The plan returned has the same output columns (exprIds) as the `target` logical plan, so that
   * existing update/insert expressions can be applied on this new plan. Unneeded non-partition
   * columns may be dropped.
   */
  protected def buildTargetPlanWithIndex(
    spark: SparkSession,
    deltaTxn: OptimisticTransaction,
    fileIndex: TahoeFileIndex,
    columnsToDrop: Seq[String]): LogicalPlan = {

    val targetOutputCols = getTargetOutputCols(deltaTxn)

    val plan = {

      // In case of schema evolution & column mapping, we need to rebuild the file format
      // because under column mapping, the reference schema within DeltaParquetFileFormat
      // that is used to populate metadata needs to be updated.
      //
      // WARNING: We must do this before replacing the file index, or we risk invalidating the
      // metadata column expression ids that replaceFileIndex might inject into the plan.
      val planWithReplacedFileFormat = if (deltaTxn.metadata.columnMappingMode != NoMapping) {
        val updatedFileFormat = deltaTxn.deltaLog.fileFormat(deltaTxn.protocol, deltaTxn.metadata)
        DeltaTableUtils.replaceFileFormat(target, updatedFileFormat)
      } else {
        target
      }

      // We have to do surgery to use the attributes from `targetOutputCols` to scan the table.
      // In cases of schema evolution, they may not be the same type as the original attributes.
      // We can ignore the new columns which aren't yet AttributeReferences.
      val newReadCols = targetOutputCols.collect { case a: AttributeReference => a }
      DeltaTableUtils.replaceFileIndex(
        spark,
        planWithReplacedFileFormat,
        fileIndex,
        columnsToDrop,
        newOutput = Some(newReadCols))
    }

    // Add back the null expression aliases for columns that are new to the target schema
    // and don't exist in the input snapshot.
    // These have been added in `getTargetOutputCols` but have been removed in `newReadCols` above
    // and are thus not in `plan.output`.
    val newColumnsWithNulls = targetOutputCols.filter(_.isInstanceOf[Alias])
    Project(plan.output ++ newColumnsWithNulls, plan)
  }

  /**
   * Get the expression references for the output columns of the target table relative to
   * the transaction. Due to schema evolution, there are two kinds of expressions here:
   *  * References to columns in the target dataframe. Note that these references may have a
   *    different data type than they originally did due to schema evolution, but the exprId
   *    will be the same. These references will be marked as nullable if `makeNullable` is set
   *    to true.
   *  * Literal nulls, for new columns which are being added to the target table as part of
   *    this transaction, since new columns will have a value of null for all existing rows.
   */
  protected def getTargetOutputCols(
      txn: OptimisticTransaction, makeNullable: Boolean = false): Seq[NamedExpression] = {
    txn.metadata.schema.map { col =>
      targetOutputAttributesMap
        .get(col.name)
        .map { a =>
          AttributeReference(col.name, col.dataType, makeNullable || col.nullable)(a.exprId)
        }
        .getOrElse(Alias(Literal(null), col.name)())
    }
  }

  /** Expressions to increment SQL metrics */
  protected def incrementMetricAndReturnBool(name: String, valueToReturn: Boolean): Expression =
    IncrementMetric(Literal(valueToReturn), metrics(name))

  protected def getTargetOnlyPredicates(spark: SparkSession): Seq[Expression] = {
    val targetOnlyPredicatesOnCondition =
      splitConjunctivePredicates(condition).filter(_.references.subsetOf(target.outputSet))

    if (!isMatchedOnly) {
      targetOnlyPredicatesOnCondition
    } else {
      val targetOnlyMatchedPredicate = matchedClauses
        .map(clause => clause.condition.getOrElse(Literal(true)))
        .map { condition =>
          splitConjunctivePredicates(condition)
            .filter(_.references.subsetOf(target.outputSet))
            .reduceOption(And)
            .getOrElse(Literal(true))
        }
        .reduceOption(Or)
      targetOnlyPredicatesOnCondition ++ targetOnlyMatchedPredicate
    }
  }

  protected def seqToString(exprs: Seq[Expression]): String = exprs.map(_.sql).mkString("\n\t")

  /**
   * Execute the given `thunk` and return its result while recording the time taken to do it
   * and setting additional local properties for better UI visibility.
   *
   * @param extraOpType extra operation name recorded in the logs
   * @param status human readable status string describing what the thunk is doing
   * @param sqlMetricName name of SQL metric to update with the time taken by the thunk
   * @param thunk the code to execute
   */
  protected def recordMergeOperation[A](
      extraOpType: String = "",
      status: String = null,
      sqlMetricName: String = null)(
      thunk: => A): A = {
    val changedOpType = if (extraOpType == "") {
      "delta.dml.merge"
    } else {
      s"delta.dml.merge.$extraOpType"
    }

    val prevDesc = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val newDesc = Option(status).map { s =>
      // Append the status to existing description if any
      val prefix = Option(prevDesc).filter(_.nonEmpty).map(_ + " - ").getOrElse("")
      prefix + s
    }

    def executeThunk(): A = {
      try {
        val startTimeNs = System.nanoTime()
        newDesc.foreach { d => sc.setJobDescription(d) }
        val r = thunk
        val timeTakenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNs)
        if (sqlMetricName != null && timeTakenMs > 0) {
          metrics(sqlMetricName) += timeTakenMs
        }
        r
      } finally {
        if (newDesc.isDefined) {
          sc.setJobDescription(prevDesc)
        }
      }
    }

    recordDeltaOperation(targetDeltaLog, changedOpType) {
      if (status == null) {
        executeThunk()
      } else {
        withStatusCode("DELTA", status) { executeThunk() }
      }
    }
  }
}

object MergeIntoCommandBase {
  val ROW_DROPPED_COL = "_row_dropped_"
  val PRECOMPUTED_CONDITION_COL = "_condition_"

  /** Count the number of distinct partition values among the AddFiles in the given set. */
  def totalBytesAndDistinctPartitionValues(files: Seq[FileAction]): (Long, Int) = {
    val distinctValues = new mutable.HashSet[Map[String, String]]()
    var bytes = 0L
    files.collect { case file: AddFile =>
      distinctValues += file.partitionValues
      bytes += file.size
    }.toList
    // If the only distinct value map is an empty map, then it must be an unpartitioned table.
    // Return 0 in that case.
    val numDistinctValues =
      if (distinctValues.size == 1 && distinctValues.head.isEmpty) 0 else distinctValues.size
    (bytes, numDistinctValues)
  }
}
