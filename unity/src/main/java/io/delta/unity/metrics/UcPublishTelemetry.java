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

import io.delta.kernel.internal.metrics.MetricsReportSerializer;
import io.delta.kernel.internal.metrics.Timer;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.shaded.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.delta.kernel.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Optional;

/**
 * Telemetry framework for Unity Catalog publish operations.
 *
 * <p>Collects timing and counter metrics for publish operations and generates reports for
 * successful and failed publishes.
 */
public class UcPublishTelemetry {

  private final String ucTableId;
  private final String ucTablePath;
  private final long snapshotVersion;
  private final int numCommitsToPublish;
  private final MetricsCollector metricsCollector;

  public UcPublishTelemetry(
      String ucTableId, String ucTablePath, long snapshotVersion, int numCommitsToPublish) {
    this.ucTableId = ucTableId;
    this.ucTablePath = ucTablePath;
    this.snapshotVersion = snapshotVersion;
    this.numCommitsToPublish = numCommitsToPublish;
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

  /** Mutable collector for gathering metrics during publish. */
  public static class MetricsCollector {
    public final Timer totalPublishTimer = new Timer();
    private int commitsPublished = 0;
    private int commitsAlreadyPublished = 0;

    public void incrementCommitsPublished() {
      commitsPublished++;
    }

    /**
     * Increments the counter for commits already published by another process. Called when
     * FileAlreadyExistsException indicates the commit was previously published.
     */
    public void incrementCommitsAlreadyPublished() {
      commitsAlreadyPublished++;
    }

    /** @return number of commits published */
    public int getCommitsPublished() {
      return commitsPublished;
    }

    /** @return number of commits already published by another process */
    public int getCommitsAlreadyPublished() {
      return commitsAlreadyPublished;
    }

    public MetricsResult capture() {
      return new MetricsResult(this);
    }
  }

  /** Immutable snapshot of collected metric results. */
  @JsonPropertyOrder({
    "totalPublishDurationNs",
    "numCommitsPublished",
    "numCommitsAlreadyPublished"
  })
  public static class MetricsResult {
    public final long totalPublishDurationNs;
    public final int numCommitsPublished;
    public final int numCommitsAlreadyPublished;

    MetricsResult(MetricsCollector collector) {
      this.totalPublishDurationNs = collector.totalPublishTimer.totalDurationNs();
      this.numCommitsPublished = collector.commitsPublished;
      this.numCommitsAlreadyPublished = collector.commitsAlreadyPublished;
    }
  }

  /** Complete UC publish report with metadata and metrics. */
  @JsonPropertyOrder({
    "operationType",
    "reportUUID",
    "ucTableId",
    "ucTablePath",
    "snapshotVersion",
    "numCommitsToPublish",
    "metrics",
    "exception"
  })
  public class Report implements MetricsReport {
    public final String operationType = "UcPublish";
    public final String reportUUID = java.util.UUID.randomUUID().toString();
    public final String ucTableId = UcPublishTelemetry.this.ucTableId;
    public final String ucTablePath = UcPublishTelemetry.this.ucTablePath;
    public final long snapshotVersion = UcPublishTelemetry.this.snapshotVersion;
    public final int numCommitsToPublish = UcPublishTelemetry.this.numCommitsToPublish;
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
