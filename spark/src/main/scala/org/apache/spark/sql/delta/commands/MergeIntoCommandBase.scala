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

import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSource, MergeIntoMaterializeSourceReason, MergeStats}
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeIntoMatchedClause, DeltaMergeIntoNotMatchedBySourceClause, DeltaMergeIntoNotMatchedClause, LogicalPlan}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.StructType

abstract class MergeIntoCommandBase extends LeafRunnableCommand
  with DeltaCommand
  with DeltaLogging
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
  /** Expressions to increment SQL metrics */
  protected def incrementMetricAndReturnBool(name: String, valueToReturn: Boolean): Expression =
    IncrementMetric(Literal(valueToReturn), metrics(name))
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

