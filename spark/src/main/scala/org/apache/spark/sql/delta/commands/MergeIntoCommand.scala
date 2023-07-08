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
import org.apache.spark.sql.delta.commands.merge.{ClassicMergeExecutor, InsertOnlyMergeExecutor}
import org.apache.spark.sql.delta.files._
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.{LongType, StructType}

/**
 * Performs a merge of a source query/table into a Delta table.
 *
 * Issues an error message when the ON search_condition of the MERGE statement can match
 * a single row from the target table with multiple rows of the source table-reference.
 *
 * Algorithm:
 *
 * Phase 1: Find the input files in target that are touched by the rows that satisfy
 *    the condition and verify that no two source rows match with the same target row.
 *    This is implemented as an inner-join using the given condition. See [[ClassicMergeExecutor]]
 *    for more details.
 *
 * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
 *
 * Phase 3: Use the Delta protocol to atomically remove the touched files and add the new files.
 *
 * @param source                     Source data to merge from
 * @param target                     Target table to merge into
 * @param targetFileIndex            TahoeFileIndex of the target table
 * @param condition                  Condition for a source row to match with a target row
 * @param matchedClauses             All info related to matched clauses.
 * @param notMatchedClauses          All info related to not matched clauses.
 * @param notMatchedBySourceClauses  All info related to not matched by source clauses.
 * @param migratedSchema             The final schema of the target - may be changed by schema
 *                                   evolution.
 */
case class MergeIntoCommand(
    @transient source: LogicalPlan,
    @transient target: LogicalPlan,
    @transient targetFileIndex: TahoeFileIndex,
    condition: Expression,
    matchedClauses: Seq[DeltaMergeIntoMatchedClause],
    notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
    notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
    migratedSchema: Option[StructType]) extends MergeIntoCommandBase
  with InsertOnlyMergeExecutor
  with ClassicMergeExecutor {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("num_affected_rows", LongType)(),
    AttributeReference("num_updated_rows", LongType)(),
    AttributeReference("num_deleted_rows", LongType)(),
    AttributeReference("num_inserted_rows", LongType)())

  protected def runMerge(spark: SparkSession): Seq[Row] = {
    recordDeltaOperation(targetDeltaLog, "delta.dml.merge") {
      val startTime = System.nanoTime()
      targetDeltaLog.withNewTransaction { deltaTxn =>
        if (hasBeenExecuted(deltaTxn, spark)) {
          sendDriverMetrics(spark, metrics)
          return Seq.empty
        }
        if (target.schema.size != deltaTxn.metadata.schema.size) {
          throw DeltaErrors.schemaChangedSinceAnalysis(
            atAnalysis = target.schema, latestSchema = deltaTxn.metadata.schema)
        }

        if (canMergeSchema) {
          updateMetadata(
            spark, deltaTxn, migratedSchema.getOrElse(target.schema),
            deltaTxn.metadata.partitionColumns, deltaTxn.metadata.configuration,
            isOverwriteMode = false, rearrangeOnly = false)
        }

        // If materialized, prepare the DF reading the materialize source
        // Otherwise, prepare a regular DF from source plan.
        val materializeSourceReason = prepareSourceDFAndReturnMaterializeReason(
          spark,
          source,
          condition,
          matchedClauses,
          notMatchedClauses,
          isInsertOnly)

        val deltaActions = {
          if (isInsertOnly && spark.conf.get(DeltaSQLConf.MERGE_INSERT_ONLY_ENABLED)) {
            // This is a single-job execution so there is no WriteChanges.
            metrics("numSourceRowsInSecondScan").set(-1)
            writeOnlyInserts(
              spark, deltaTxn, filterMatchedRows = true, numSourceRowsMetric = "numSourceRows")
          } else {
            val (filesToRewrite, deduplicateCDFDeletes) = findTouchedFiles(spark, deltaTxn)
            if (filesToRewrite.nonEmpty) {
              val newWrittenFiles = withStatusCode("DELTA", "Writing merged data") {
                writeAllChanges(spark, deltaTxn, filesToRewrite, deduplicateCDFDeletes)
              }
              filesToRewrite.map(_.remove) ++ newWrittenFiles
            } else {
              // Run an insert-only job instead of WriteChanges
              writeOnlyInserts(
                spark,
                deltaTxn,
                filterMatchedRows = false,
                numSourceRowsMetric = "numSourceRowsInSecondScan")
            }
          }
        }
        val stats = collectMergeStats(
          spark,
          deltaTxn,
          startTime,
          deltaActions,
          materializeSourceReason)

        recordDeltaEvent(targetFileIndex.deltaLog, "delta.dml.merge.stats", data = stats)

      }
      spark.sharedState.cacheManager.recacheByPlan(spark, target)
    }
    sendDriverMetrics(spark, metrics)
    Seq(Row(metrics("numTargetRowsUpdated").value + metrics("numTargetRowsDeleted").value +
            metrics("numTargetRowsInserted").value, metrics("numTargetRowsUpdated").value,
            metrics("numTargetRowsDeleted").value, metrics("numTargetRowsInserted").value))
  }
}
