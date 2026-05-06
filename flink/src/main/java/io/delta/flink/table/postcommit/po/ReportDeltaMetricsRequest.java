/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.table.postcommit.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Top-level request body for {@code POST /api/2.1/unity-catalog/delta/preview/metrics}.
 *
 * <p>Serializes to:
 *
 * <pre>{@code
 * {
 *   "table_id": "<uuid>",
 *   "report": {
 *     "commit_report": { ... }
 *   }
 * }
 * }</pre>
 *
 * <p>The {@code report} field uses Jackson {@code WRAPPER_OBJECT} polymorphism via {@link
 * CommitReport}, so the concrete type name (e.g. {@code "commit_report"}) becomes the JSON wrapper
 * key automatically.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReportDeltaMetricsRequest {

  @JsonProperty("table_id")
  private String tableId;

  @JsonProperty("report")
  private CommitReport report;

  public ReportDeltaMetricsRequest() {}

  public ReportDeltaMetricsRequest(String tableId, CommitReport report) {
    this.tableId = tableId;
    this.report = report;
  }

  public String getTableId() {
    return tableId;
  }

  public ReportDeltaMetricsRequest tableId(String tableId) {
    this.tableId = tableId;
    return this;
  }

  public CommitReport getReport() {
    return report;
  }

  public ReportDeltaMetricsRequest report(CommitReport report) {
    this.report = report;
    return this;
  }

  /**
   * Convenience factory for building a request from a table UUID and a {@link CommitReport}.
   *
   * @param tableId the UC table UUID string
   * @param commitReport the commit metrics payload
   */
  public static ReportDeltaMetricsRequest of(String tableId, CommitReport commitReport) {
    return new ReportDeltaMetricsRequest(tableId, commitReport);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ReportDeltaMetricsRequest)) return false;
    ReportDeltaMetricsRequest that = (ReportDeltaMetricsRequest) o;
    return Objects.equals(tableId, that.tableId) && Objects.equals(report, that.report);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, report);
  }

  @Override
  public String toString() {
    return "ReportDeltaMetricsRequest{" + "tableId='" + tableId + '\'' + ", report=" + report + '}';
  }
}
