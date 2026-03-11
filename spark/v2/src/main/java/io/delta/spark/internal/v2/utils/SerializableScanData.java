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
package io.delta.spark.internal.v2.utils;

import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.replay.LogReplay;
import java.io.Serializable;
import java.util.List;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Fully serializable bundle of everything needed to reconstruct a {@link LogReplay} and call {@code
 * getAddFilesAsColumnarBatches}. Extracted from an existing {@link SnapshotImpl} on the driver with
 * zero additional I/O.
 */
public class SerializableScanData implements Serializable {
  private static final long serialVersionUID = 1L;

  final String dataPath;
  final String logPath;
  final long version;
  final List<SerializableFileStatus> deltas;
  final List<SerializableFileStatus> compactions;
  final List<SerializableFileStatus> checkpoints;
  final SerializableFileStatus deltaAtEndVersion;
  final SerializableFileStatus lastSeenChecksum; // nullable
  final Long maxPublishedDeltaVersion; // nullable
  final SerializableConfiguration hadoopConf;

  public SerializableScanData(
      String dataPath,
      String logPath,
      long version,
      List<SerializableFileStatus> deltas,
      List<SerializableFileStatus> compactions,
      List<SerializableFileStatus> checkpoints,
      SerializableFileStatus deltaAtEndVersion,
      SerializableFileStatus lastSeenChecksum,
      Long maxPublishedDeltaVersion,
      SerializableConfiguration hadoopConf) {
    this.dataPath = dataPath;
    this.logPath = logPath;
    this.version = version;
    this.deltas = deltas;
    this.compactions = compactions;
    this.checkpoints = checkpoints;
    this.deltaAtEndVersion = deltaAtEndVersion;
    this.lastSeenChecksum = lastSeenChecksum;
    this.maxPublishedDeltaVersion = maxPublishedDeltaVersion;
    this.hadoopConf = hadoopConf;
  }
}
