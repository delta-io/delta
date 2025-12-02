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

package io.delta.kernel.unitycatalog.metrics;

import io.delta.kernel.internal.metrics.MetricsReportSerializer;
import io.delta.kernel.internal.metrics.Timer;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.shaded.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.delta.kernel.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Optional;

/**
 * Telemetry framework for Unity Catalog snapshot loading operations.
 *
 * <p>Collects timing metrics for snapshot loading and generates reports for successful and failed
 * loads.
 */
public class UcLoadSnapshotTelemetry {

  private final String ucTableId;
  private final String ucTablePath;
  private final Optional<Long> versionOpt;
  private final Optional<Long> timestampOpt;
  private final MetricsCollector metricsCollector;

  public UcLoadSnapshotTelemetry(
      String ucTableId,
      String ucTablePath,
      Optional<Long> versionOpt,
      Optional<Long> timestampOpt) {
    this.ucTableId = ucTableId;
    this.ucTablePath = ucTablePath;
    this.versionOpt = versionOpt;
    this.timestampOpt = timestampOpt;
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

  /** Mutable collector for gathering metrics during snapshot loading. */
  public static class MetricsCollector {
    public final Timer totalSnapshotLoadTimer = new Timer();
    public final Timer getCommitsTimer = new Timer();
    public final Timer kernelSnapshotBuildTimer = new Timer();
    public final Timer loadLatestSnapshotForTimestampTimeTravelTimer = new Timer();
    private int numCatalogCommits = -1;
    private long resolvedSnapshotVersion = -1;

    public void setNumCatalogCommits(int count) {
      this.numCatalogCommits = count;
    }

    public void setResolvedSnapshotVersion(long version) {
      this.resolvedSnapshotVersion = version;
    }

    public MetricsResult capture() {
      return new MetricsResult(this);
    }
  }

  /** Immutable snapshot of collected metric results. */
  @JsonPropertyOrder({
    "totalLoadSnapshotDurationNs",
    "getCommitsDurationNs",
    "numCatalogCommits",
    "kernelSnapshotBuildDurationNs",
    "loadLatestSnapshotForTimestampTimeTravelDurationNs",
    "resolvedSnapshotVersion"
  })
  public static class MetricsResult {
    public final long totalLoadSnapshotDurationNs;
    public final long getCommitsDurationNs;
    public final int numCatalogCommits;
    public final long kernelSnapshotBuildDurationNs;
    public final long loadLatestSnapshotForTimestampTimeTravelDurationNs;
    public final long resolvedSnapshotVersion;

    MetricsResult(MetricsCollector collector) {
      this.totalLoadSnapshotDurationNs = collector.totalSnapshotLoadTimer.totalDurationNs();
      this.getCommitsDurationNs = collector.getCommitsTimer.totalDurationNs();
      this.numCatalogCommits = collector.numCatalogCommits;
      this.kernelSnapshotBuildDurationNs = collector.kernelSnapshotBuildTimer.totalDurationNs();
      this.loadLatestSnapshotForTimestampTimeTravelDurationNs =
          collector.loadLatestSnapshotForTimestampTimeTravelTimer.totalDurationNs();
      this.resolvedSnapshotVersion = collector.resolvedSnapshotVersion;
    }
  }

  /** Complete UC snapshot loading report with metadata and metrics. */
  @JsonPropertyOrder({
    "operationType",
    "reportUUID",
    "ucTableId",
    "ucTablePath",
    "versionOpt",
    "timestampOpt",
    "metrics",
    "exception"
  })
  public class Report implements MetricsReport {
    public final String operationType = "UcLoadSnapshot";
    public final String reportUUID = java.util.UUID.randomUUID().toString();
    public final String ucTableId = UcLoadSnapshotTelemetry.this.ucTableId;
    public final String ucTablePath = UcLoadSnapshotTelemetry.this.ucTablePath;
    public final Optional<Long> versionOpt = UcLoadSnapshotTelemetry.this.versionOpt;
    public final Optional<Long> timestampOpt = UcLoadSnapshotTelemetry.this.timestampOpt;
    public final MetricsResult metrics;
    public final Optional<Exception> exception;

    public Report(MetricsResult metrics, Optional<Exception> exception) {
      this.metrics = metrics;
      this.exception = exception;
    }

    @Override
    public String toJson() throws JsonProcessingException {
      return MetricsReportSerializer.OBJECT_MAPPER.writeValueAsString(this);
    }
  }
}
