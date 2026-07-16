/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metrics;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.MetricsReporter;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Emits the bounded preflight and final reports for one log replay. */
public final class LogReplayTelemetry {
  private static final Logger logger = LoggerFactory.getLogger(LogReplayTelemetry.class);

  private final Engine engine;
  private final LogReplayReport.Builder reportBuilder;
  private boolean finalReportEmitted;

  public LogReplayTelemetry(
      Engine engine,
      ActionsIterator.SelectedReplayFiles selectedTopLevelFiles,
      Optional<Long> checkpointVersion,
      String tablePath,
      long tableVersion,
      UUID snapshotReportUUID) {
    this.engine = engine;
    this.reportBuilder =
        new LogReplayReport.Builder(
            selectedTopLevelFiles,
            checkpointVersion,
            UUID.randomUUID(),
            tablePath,
            tableVersion,
            snapshotReportUUID);
  }

  /**
   * Best-effort preflight reporting before an action iterator is constructed.
   *
   * <p>This report is emitted before replay begins, but neither it nor a final report is guaranteed
   * when replay or reporting encounters an {@link Error}, including an {@link OutOfMemoryError}.
   */
  public void reportPreflight() {
    report(LogReplayReport.Phase.PREFLIGHT, null);
  }

  /**
   * Records the canonical file status created for a V2 manifest sidecar.
   *
   * <p>The status is built from the manifest metadata after pagination filtering, so recording it
   * does not require an additional filesystem lookup.
   */
  public void recordSidecar(FileStatus sidecarFileStatus, long checkpointVersion) {
    reportBuilder.recordSidecar(sidecarFileStatus, checkpointVersion);
  }

  /** Emits at most one final report after normal completion or an ordinary exception. */
  public void reportFinal(LogReplayReport.Outcome outcome) {
    if (finalReportEmitted) {
      return;
    }
    finalReportEmitted = true;
    report(LogReplayReport.Phase.FINAL, outcome);
  }

  private void report(LogReplayReport.Phase phase, LogReplayReport.Outcome outcome) {
    LogReplayReport report = reportBuilder.build(phase, outcome);
    final List<MetricsReporter> reporters;
    try {
      reporters = engine.getMetricsReporters();
    } catch (Exception e) {
      // Reporting is diagnostic only. Do not log the exception: it can contain table paths or
      // payload data.
      logger.warn("Log replay telemetry reporters unavailable");
      return;
    }
    reporters.forEach(
        reporter -> {
          try {
            reporter.report(report);
          } catch (Exception e) {
            // Reporting is diagnostic only. A faulty reporter must not affect replay or
            // prevent later reporters from receiving the same event.
            // Do not log the exception or report: either can contain table paths or payload
            // data.
            logger.warn("Log replay telemetry reporter failed");
          }
        });
  }
}
