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

package io.delta.unity.metrics;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.internal.metrics.MetricsReportSerializer;
import io.delta.kernel.internal.metrics.Timer;
import java.util.Optional;

/**
 * Telemetry framework for Unity Catalog commit operations.
 *
 * <p>Collects timing metrics for commit operations and generates reports for successful and failed
 * commits.
 */
public class UcCommitTelemetry {

  private final String ucTableId;
  private final String ucTablePath;
  private final CommitMetadata commitMetadata;
  private final MetricsCollector metricsCollector;

  public UcCommitTelemetry(String ucTableId, String ucTablePath, CommitMetadata commitMetadata) {
    this.ucTableId = ucTableId;
    this.ucTablePath = ucTablePath;
    this.commitMetadata = commitMetadata;
    this.metricsCollector = new MetricsCollector();
  }

  public MetricsCollector getMetricsCollector() {
    return metricsCollector;
  }

  public Report createSuccessReport() {
    return new Report(metricsCollector.capture(), Optional.empty());
  }

  public Report createFailureReport(Exception error) {
    return new Report(metricsCollector.capture(), Optional.of(error));
  }

  /** Mutable collector for gathering metrics during commit. */
  public static class MetricsCollector {
    public final Timer totalCommitTimer = new Timer();
    public final Timer writeCommitFileTimer = new Timer();
    public final Timer commitToUcServerTimer = new Timer();

    public MetricsResult capture() {
      return new MetricsResult(this);
    }
  }

  /** Immutable snapshot of collected metric results. */
  @JsonPropertyOrder({
    "totalCommitDurationNs",
    "writeCommitFileDurationNs",
    "commitToUcServerDurationNs"
  })
  public static class MetricsResult {
    public final long totalCommitDurationNs;
    public final long writeCommitFileDurationNs;
    public final long commitToUcServerDurationNs;

    MetricsResult(MetricsCollector collector) {
      this.totalCommitDurationNs = collector.totalCommitTimer.totalDurationNs();
      this.writeCommitFileDurationNs = collector.writeCommitFileTimer.totalDurationNs();
      this.commitToUcServerDurationNs = collector.commitToUcServerTimer.totalDurationNs();
    }
  }

  /** Complete UC commit report with metadata and metrics. */
  @JsonPropertyOrder({
    "operationType",
    "reportUUID",
    "ucTableId",
    "ucTablePath",
    "commitVersion",
    "commitType",
    "metrics",
    "exception"
  })
  public class Report implements io.delta.kernel.metrics.MetricsReport {
    public final String operationType = "UcCommit";
    public final String reportUUID = java.util.UUID.randomUUID().toString();
    public final String ucTableId = UcCommitTelemetry.this.ucTableId;
    public final String ucTablePath = UcCommitTelemetry.this.ucTablePath;
    public final long commitVersion = commitMetadata.getVersion();
    public final CommitMetadata.CommitType commitType = commitMetadata.getCommitType();
    public final MetricsResult metrics;
    public final Optional<String> exception;

    public Report(MetricsResult metrics, Optional<Exception> exception) {
      this.metrics = metrics;
      this.exception = exception.map(Exception::toString);
    }

    @Override
    public String toJson() throws JsonProcessingException {
      return MetricsReportSerializer.OBJECT_MAPPER.writeValueAsString(this);
    }
  }
}
