/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.hooks

import com.fasterxml.jackson.annotation.JsonProperty

import org.apache.spark.sql.delta.actions.Action

/**
 * Top-level request body for POST /api/2.1/unity-catalog/delta/preview/metrics.
 *
 * @param tableId UUID of the UC-managed Delta table
 * @param report  The commit metrics, wrapped in a typed report envelope
 */
case class ReportDeltaMetricsRequest(
    @JsonProperty("table_id") tableId: String,
    @JsonProperty("report") report: CommitReportEnvelope)

/**
 * Envelope that matches the server's @JsonSubTypes WRAPPER_OBJECT format.
 * Serializes as: { "commit_report": { ... } }
 */
case class CommitReportEnvelope(
    @JsonProperty("commit_report") commitReport: CommitReport)

/**
 * Skeleton payload for transport wiring validation. The full metrics payload
 * is added in a follow-up change.
 */
case class CommitReport()

/**
 * Builds the PO metrics request payload from committed transaction data.
 *
 * Package-private for direct testing without Delta infrastructure.
 */
object ReportDeltaMetrics {

  /**
   * Builds a skeleton request for transport wiring validation.
   * Full metrics extraction is added in a follow-up change.
   */
  private[hooks] def buildRequest(
      tableId: String,
      _committedActions: Seq[Action],
      _committedVersion: Long): ReportDeltaMetricsRequest = {
    ReportDeltaMetricsRequest(
      tableId = tableId,
      report = CommitReportEnvelope(CommitReport())
    )
  }
}
