/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;

/**
 * Internal wrapper class holding information needed to perform log replay. Represents either a
 * Delta commit file, classic checkpoint, a multipart checkpoint, a V2 checkpoint, or a sidecar
 * checkpoint.
 */
public class DeltaLogFile {
  public enum LogType {
    COMMIT,
    CHECKPOINT_CLASSIC,
    MULTIPART_CHECKPOINT,
    V2_CHECKPOINT_MANIFEST,
    SIDECAR
  }

  public static DeltaLogFile forCommitOrCheckpoint(FileStatus file) {
    String filePath = file.getPath();
    String fileName = new Path(file.getPath()).getName();
    LogType logType;
    long version = -1;
    if (FileNames.isCommitFile(filePath)) {
      logType = LogType.COMMIT;
      version = FileNames.deltaVersion(filePath);
    } else if (FileNames.isClassicCheckpointFile(fileName)) {
      logType = LogType.CHECKPOINT_CLASSIC;
      version = FileNames.checkpointVersion(filePath);
    } else if (FileNames.isMultiPartCheckpointFile(fileName)) {
      logType = LogType.MULTIPART_CHECKPOINT;
      version = FileNames.checkpointVersion(fileName);
    } else if (FileNames.isV2CheckpointFile(fileName)) {
      logType = LogType.V2_CHECKPOINT_MANIFEST;
      version = FileNames.checkpointVersion(fileName);
    } else {
      throw new IllegalArgumentException(
          "File is not a commit or checkpoint file: " + file.getPath());
    }
    return new DeltaLogFile(file, logType, version);
  }

  public static DeltaLogFile ofSideCar(FileStatus file, long version) {
    return new DeltaLogFile(file, LogType.SIDECAR, version);
  }

  private final FileStatus file;
  private final LogType logType;
  private final long version;

  private DeltaLogFile(FileStatus file, LogType logType, long version) {
    this.file = file;
    this.logType = logType;
    this.version = version;
  }

  public FileStatus getFile() {
    return file;
  }

  public LogType getLogType() {
    return logType;
  }

  public long getVersion() {
    return version;
  }
}
