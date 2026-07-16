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

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.replay.DeltaLogFile;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.metrics.MetricsReport;
import io.delta.kernel.utils.FileStatus;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Bounded telemetry for a transaction-log replay.
 *
 * <p>A preflight report includes only the already-listed top-level artifacts selected for replay. A
 * final report uses the same top-level set and additionally includes V2 checkpoint sidecars
 * observed while parsing manifests. Preflight reporting is best effort before iterator
 * construction; final reporting is best effort and only occurs after successful completion or an
 * ordinary exception. Errors, including {@link OutOfMemoryError}, produce no final report.
 */
@JsonPropertyOrder({
  "operationType",
  "reportUUID",
  "logReplayId",
  "tablePath",
  "tableVersion",
  "snapshotReportUUID",
  "phase",
  "outcome",
  "selectedCheckpointVersion",
  "v2CheckpointManifestFormat",
  "deltaArtifacts",
  "compactionArtifacts",
  "checkpointArtifacts",
  "sidecarArtifacts"
})
public final class LogReplayReport implements MetricsReport {
  public enum Phase {
    PREFLIGHT,
    FINAL
  }

  public enum Outcome {
    SUCCESS,
    FAILURE
  }

  /** Bounded size statistics. Size aggregates exclude entries whose size is unknown. */
  public static final class ArtifactMetrics {
    private final long count;
    private final long knownSizeCount;
    private final long unknownSizeCount;
    private final long totalKnownSizeInBytes;
    private final Long minKnownSizeInBytes;
    private final Long maxKnownSizeInBytes;
    private final Double sizeStandardDeviationInBytes;

    private ArtifactMetrics(SizeAccumulator accumulator) {
      count = accumulator.count;
      knownSizeCount = accumulator.knownSizeCount;
      unknownSizeCount = accumulator.count - accumulator.knownSizeCount;
      totalKnownSizeInBytes = accumulator.totalKnownSizeInBytes;
      minKnownSizeInBytes =
          accumulator.knownSizeCount == 0 ? null : accumulator.minKnownSizeInBytes;
      maxKnownSizeInBytes =
          accumulator.knownSizeCount == 0 ? null : accumulator.maxKnownSizeInBytes;
      sizeStandardDeviationInBytes =
          accumulator.knownSizeCount == 0
              ? null
              : Math.sqrt(accumulator.sumSquaredDifferences / accumulator.knownSizeCount);
    }

    public long getCount() {
      return count;
    }

    public long getKnownSizeCount() {
      return knownSizeCount;
    }

    public long getUnknownSizeCount() {
      return unknownSizeCount;
    }

    public long getTotalKnownSizeInBytes() {
      return totalKnownSizeInBytes;
    }

    public Long getMinKnownSizeInBytes() {
      return minKnownSizeInBytes;
    }

    public Long getMaxKnownSizeInBytes() {
      return maxKnownSizeInBytes;
    }

    public Double getSizeStandardDeviationInBytes() {
      return sizeStandardDeviationInBytes;
    }
  }

  private final UUID logReplayId;
  private final UUID reportUUID;
  private final String tablePath;
  private final long tableVersion;
  private final UUID snapshotReportUUID;
  private final Phase phase;
  private final Outcome outcome;
  private final Optional<Long> selectedCheckpointVersion;
  private final Optional<String> v2CheckpointManifestFormat;
  private final ArtifactMetrics deltaArtifacts;
  private final ArtifactMetrics compactionArtifacts;
  private final ArtifactMetrics checkpointArtifacts;
  private final ArtifactMetrics sidecarArtifacts;

  LogReplayReport(
      UUID logReplayId,
      UUID reportUUID,
      String tablePath,
      long tableVersion,
      UUID snapshotReportUUID,
      Phase phase,
      Outcome outcome,
      Optional<Long> selectedCheckpointVersion,
      Optional<String> v2CheckpointManifestFormat,
      SizeAccumulator deltas,
      SizeAccumulator compactions,
      SizeAccumulator checkpoints,
      SizeAccumulator sidecars) {
    this.logReplayId = logReplayId;
    this.reportUUID = reportUUID;
    this.tablePath = tablePath;
    this.tableVersion = tableVersion;
    this.snapshotReportUUID = snapshotReportUUID;
    this.phase = phase;
    this.outcome = outcome;
    this.selectedCheckpointVersion = selectedCheckpointVersion;
    this.v2CheckpointManifestFormat = v2CheckpointManifestFormat;
    this.deltaArtifacts = new ArtifactMetrics(deltas);
    this.compactionArtifacts = new ArtifactMetrics(compactions);
    this.checkpointArtifacts = new ArtifactMetrics(checkpoints);
    this.sidecarArtifacts = new ArtifactMetrics(sidecars);
  }

  public UUID getLogReplayId() {
    return logReplayId;
  }

  public String getOperationType() {
    return "LogReplay";
  }

  public UUID getReportUUID() {
    return reportUUID;
  }

  public String getTablePath() {
    return tablePath;
  }

  public long getTableVersion() {
    return tableVersion;
  }

  public UUID getSnapshotReportUUID() {
    return snapshotReportUUID;
  }

  public Phase getPhase() {
    return phase;
  }

  /** Returns null for preflight because replay has not yet started. */
  public Outcome getOutcome() {
    return outcome;
  }

  public Optional<Long> getSelectedCheckpointVersion() {
    return selectedCheckpointVersion;
  }

  /** Returns the V2 checkpoint manifest format when the selected replay includes one. */
  public Optional<String> getV2CheckpointManifestFormat() {
    return v2CheckpointManifestFormat;
  }

  public ArtifactMetrics getDeltaArtifacts() {
    return deltaArtifacts;
  }

  public ArtifactMetrics getCompactionArtifacts() {
    return compactionArtifacts;
  }

  public ArtifactMetrics getCheckpointArtifacts() {
    return checkpointArtifacts;
  }

  /** Sidecars are absent from preflight because they are discovered from replayed manifests. */
  public ArtifactMetrics getSidecarArtifacts() {
    return sidecarArtifacts;
  }

  @Override
  public String toJson() throws JsonProcessingException {
    return MetricsReportSerializer.OBJECT_MAPPER.writeValueAsString(this);
  }

  static final class SizeAccumulator {
    private long count;
    private long knownSizeCount;
    private long totalKnownSizeInBytes;
    private long minKnownSizeInBytes = Long.MAX_VALUE;
    private long maxKnownSizeInBytes;
    private double mean;
    private double sumSquaredDifferences;

    void add(FileStatus fileStatus) {
      count++;
      if (!fileStatus.isSizeKnown()) {
        return;
      }
      addKnownSize(fileStatus.getSize());
    }

    void addKnownSize(long sizeInBytes) {
      knownSizeCount++;
      totalKnownSizeInBytes += sizeInBytes;
      minKnownSizeInBytes = Math.min(minKnownSizeInBytes, sizeInBytes);
      maxKnownSizeInBytes = Math.max(maxKnownSizeInBytes, sizeInBytes);
      double delta = sizeInBytes - mean;
      mean += delta / knownSizeCount;
      sumSquaredDifferences += delta * (sizeInBytes - mean);
    }
  }

  static final class Builder {
    private final SizeAccumulator deltas = new SizeAccumulator();
    private final SizeAccumulator compactions = new SizeAccumulator();
    private final SizeAccumulator checkpoints = new SizeAccumulator();
    private final SizeAccumulator sidecars = new SizeAccumulator();
    private Optional<Long> selectedCheckpointVersion;
    private Optional<String> v2CheckpointManifestFormat = Optional.empty();
    private final UUID logReplayId;
    private final String tablePath;
    private final long tableVersion;
    private final UUID snapshotReportUUID;

    Builder(List<FileStatus> selectedTopLevelFiles, Optional<Long> selectedCheckpointVersion) {
      this(
          selectedTopLevelFiles,
          selectedCheckpointVersion,
          UUID.randomUUID(),
          "",
          -1L,
          UUID.randomUUID());
    }

    Builder(
        List<FileStatus> selectedTopLevelFiles,
        Optional<Long> selectedCheckpointVersion,
        UUID logReplayId,
        String tablePath,
        long tableVersion,
        UUID snapshotReportUUID) {
      this.selectedCheckpointVersion = selectedCheckpointVersion;
      this.logReplayId = logReplayId;
      this.tablePath = tablePath;
      this.tableVersion = tableVersion;
      this.snapshotReportUUID = snapshotReportUUID;
      for (FileStatus fileStatus : selectedTopLevelFiles) {
        recordTopLevelArtifact(fileStatus);
      }
    }

    Builder(
        ActionsIterator.SelectedReplayFiles selectedTopLevelFiles,
        Optional<Long> selectedCheckpointVersion,
        UUID logReplayId,
        String tablePath,
        long tableVersion,
        UUID snapshotReportUUID) {
      this.selectedCheckpointVersion = selectedCheckpointVersion;
      this.logReplayId = logReplayId;
      this.tablePath = tablePath;
      this.tableVersion = tableVersion;
      this.snapshotReportUUID = snapshotReportUUID;
      for (DeltaLogFile file : selectedTopLevelFiles.getFiles()) {
        recordTopLevelArtifact(file);
      }
    }

    void recordSidecar(String path, long version, long sizeInBytes) {
      sidecars.addKnownSize(sizeInBytes);
    }

    void recordSidecar(FileStatus fileStatus, long version) {
      sidecars.add(fileStatus);
    }

    LogReplayReport build(Phase phase, Outcome outcome) {
      return new LogReplayReport(
          logReplayId,
          UUID.randomUUID(),
          tablePath,
          tableVersion,
          snapshotReportUUID,
          phase,
          outcome,
          selectedCheckpointVersion,
          v2CheckpointManifestFormat,
          deltas,
          compactions,
          checkpoints,
          sidecars);
    }

    private void recordTopLevelArtifact(FileStatus fileStatus) {
      Path path = new Path(fileStatus.getPath());
      String filename = path.getName();
      if (FileNames.isCommitFile(filename)) {
        deltas.add(fileStatus);
      } else if (FileNames.isLogCompactionFile(filename)) {
        compactions.add(fileStatus);
      } else {
        checkpoints.add(fileStatus);
        if (FileNames.isV2CheckpointFile(filename)) {
          v2CheckpointManifestFormat =
              Optional.of(filename.endsWith(".parquet") ? "parquet" : "json");
        }
      }
    }

    private void recordTopLevelArtifact(DeltaLogFile file) {
      switch (file.getLogType()) {
        case COMMIT:
          deltas.add(file.getFile());
          break;
        case LOG_COMPACTION:
          compactions.add(file.getFile());
          break;
        case CHECKPOINT_CLASSIC:
        case MULTIPART_CHECKPOINT:
          checkpoints.add(file.getFile());
          break;
        case V2_CHECKPOINT_MANIFEST:
          checkpoints.add(file.getFile());
          String filename = new Path(file.getFile().getPath()).getName();
          v2CheckpointManifestFormat =
              Optional.of(filename.endsWith(".parquet") ? "parquet" : "json");
          break;
        case SIDECAR:
          throw new IllegalArgumentException("Sidecars are not top-level replay artifacts");
      }
    }
  }
}
