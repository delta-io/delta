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

package org.apache.spark.sql.delta.commands.merge

import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{DeltaMergeIntoClause, DeltaMergeIntoMatchedClause, DeltaMergeIntoNotMatchedBySourceClause, DeltaMergeIntoNotMatchedClause}
import org.apache.spark.sql.execution.metric.SQLMetric

case class MergeDataSizes(
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  rows: Option[Long] = None,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  files: Option[Long] = None,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  bytes: Option[Long] = None,
  @JsonDeserialize(contentAs = classOf[java.lang.Long])
  partitions: Option[Long] = None)

/**
 * Represents the state of a single merge clause:
 * - merge clause's (optional) predicate
 * - action type (insert, update, delete)
 * - action's expressions
 */
case class MergeClauseStats(
    condition: Option[String],
    actionType: String,
    actionExpr: Seq[String])

object MergeClauseStats {
  def apply(mergeClause: DeltaMergeIntoClause): MergeClauseStats = {
    MergeClauseStats(
      condition = mergeClause.condition.map(_.sql),
      mergeClause.clauseType.toLowerCase(),
      actionExpr = mergeClause.actions.map(_.sql))
  }
}

/** State for a merge operation */
case class MergeStats(
    // Merge condition expression
    conditionExpr: String,

    // Expressions used in old MERGE stats, now always Null
    updateConditionExpr: String,
    updateExprs: Seq[String],
    insertConditionExpr: String,
    insertExprs: Seq[String],
    deleteConditionExpr: String,

    // Newer expressions used in MERGE with any number of MATCHED/NOT MATCHED/NOT MATCHED BY SOURCE
    matchedStats: Seq[MergeClauseStats],
    notMatchedStats: Seq[MergeClauseStats],
    notMatchedBySourceStats: Seq[MergeClauseStats],

    // Timings
    executionTimeMs: Long,
    scanTimeMs: Long,
    rewriteTimeMs: Long,

    // Data sizes of source and target at different stages of processing
    source: MergeDataSizes,
    targetBeforeSkipping: MergeDataSizes,
    targetAfterSkipping: MergeDataSizes,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    sourceRowsInSecondScan: Option[Long],

    // Data change sizes
    targetFilesRemoved: Long,
    targetFilesAdded: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetChangeFilesAdded: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetChangeFileBytes: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetBytesRemoved: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetBytesAdded: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetPartitionsRemovedFrom: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    targetPartitionsAddedTo: Option[Long],
    targetRowsCopied: Long,
    targetRowsUpdated: Long,
    targetRowsMatchedUpdated: Long,
    targetRowsNotMatchedBySourceUpdated: Long,
    targetRowsInserted: Long,
    targetRowsDeleted: Long,
    targetRowsMatchedDeleted: Long,
    targetRowsNotMatchedBySourceDeleted: Long,

    // MergeMaterializeSource stats
    materializeSourceReason: Option[String] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    materializeSourceAttempts: Option[Long] = None
)

object MergeStats {

  def fromMergeSQLMetrics(
      metrics: Map[String, SQLMetric],
      condition: Expression,
      matchedClauses: Seq[DeltaMergeIntoMatchedClause],
      notMatchedClauses: Seq[DeltaMergeIntoNotMatchedClause],
      notMatchedBySourceClauses: Seq[DeltaMergeIntoNotMatchedBySourceClause],
      isPartitioned: Boolean): MergeStats = {

    def metricValueIfPartitioned(metricName: String): Option[Long] = {
      if (isPartitioned) Some(metrics(metricName).value) else None
    }

    MergeStats(
      // Merge condition expression
      conditionExpr = condition.sql,

      // Newer expressions used in MERGE with any number of MATCHED/NOT MATCHED/
      // NOT MATCHED BY SOURCE
      matchedStats = matchedClauses.map(MergeClauseStats(_)),
      notMatchedStats = notMatchedClauses.map(MergeClauseStats(_)),
      notMatchedBySourceStats = notMatchedBySourceClauses.map(MergeClauseStats(_)),

      // Timings
      executionTimeMs = metrics("executionTimeMs").value,
      scanTimeMs = metrics("scanTimeMs").value,
      rewriteTimeMs = metrics("rewriteTimeMs").value,

      // Data sizes of source and target at different stages of processing
      source = MergeDataSizes(rows = Some(metrics("numSourceRows").value)),
      targetBeforeSkipping =
        MergeDataSizes(
          files = Some(metrics("numTargetFilesBeforeSkipping").value),
          bytes = Some(metrics("numTargetBytesBeforeSkipping").value)),
      targetAfterSkipping =
        MergeDataSizes(
          files = Some(metrics("numTargetFilesAfterSkipping").value),
          bytes = Some(metrics("numTargetBytesAfterSkipping").value),
          partitions = metricValueIfPartitioned("numTargetPartitionsAfterSkipping")),
      sourceRowsInSecondScan =
        metrics.get("numSourceRowsInSecondScan").map(_.value).filter(_ >= 0),

      // Data change sizes
      targetFilesAdded = metrics("numTargetFilesAdded").value,
      targetChangeFilesAdded = metrics.get("numTargetChangeFilesAdded").map(_.value),
      targetChangeFileBytes = metrics.get("numTargetChangeFileBytes").map(_.value),
      targetFilesRemoved = metrics("numTargetFilesRemoved").value,
      targetBytesAdded = Some(metrics("numTargetBytesAdded").value),
      targetBytesRemoved = Some(metrics("numTargetBytesRemoved").value),
      targetPartitionsRemovedFrom = metricValueIfPartitioned("numTargetPartitionsRemovedFrom"),
      targetPartitionsAddedTo = metricValueIfPartitioned("numTargetPartitionsAddedTo"),
      targetRowsCopied = metrics("numTargetRowsCopied").value,
      targetRowsUpdated = metrics("numTargetRowsUpdated").value,
      targetRowsMatchedUpdated = metrics("numTargetRowsMatchedUpdated").value,
      targetRowsNotMatchedBySourceUpdated = metrics("numTargetRowsNotMatchedBySourceUpdated").value,
      targetRowsInserted = metrics("numTargetRowsInserted").value,
      targetRowsDeleted = metrics("numTargetRowsDeleted").value,
      targetRowsMatchedDeleted = metrics("numTargetRowsMatchedDeleted").value,
      targetRowsNotMatchedBySourceDeleted = metrics("numTargetRowsNotMatchedBySourceDeleted").value,

      // Deprecated fields
      updateConditionExpr = null,
      updateExprs = null,
      insertConditionExpr = null,
      insertExprs = null,
      deleteConditionExpr = null)
  }
}
