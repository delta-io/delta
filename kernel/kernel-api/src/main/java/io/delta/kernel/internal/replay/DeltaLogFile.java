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
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;

/**
 * Internal wrapper class holding information needed to perform log replay. Represents either a
 * Delta commit file, classic checkpoint, a multipart checkpoint, a V2 checkpoint, or a sidecar
 * checkpoint.
 */
public class DeltaLogFile {

  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public enum Category {
    COMMIT,
    LOG_COMPACTION,
    CHECKPOINT,
    CHECKSUM,
    NOT_APPLICABLE
  }

  public enum LogType {
    COMMIT(Category.COMMIT),
    LOG_COMPACTION(Category.LOG_COMPACTION),
    CHECKPOINT_CLASSIC(Category.CHECKPOINT),
    MULTIPART_CHECKPOINT(Category.CHECKPOINT),
    V2_CHECKPOINT_MANIFEST(Category.CHECKPOINT),
    SIDECAR(Category.NOT_APPLICABLE);

    private final Category category;

    LogType(Category category) {
      this.category = category;
    }

    public Category getCategory() {
      return category;
    }
  }

  public static DeltaLogFile forFileStatus(FileStatus file) {
    final String fileName = new Path(file.getPath()).getName();
    final LogType logType;
    final long version;
    if (FileNames.isCommitFile(fileName)) {
      logType = LogType.COMMIT;
      version = FileNames.deltaVersion(fileName);
    } else if (FileNames.isLogCompactionFile(fileName)) {
      return CompactionFile.forFileStatus(file);
    } else if (FileNames.isClassicCheckpointFile(fileName)) {
      logType = LogType.CHECKPOINT_CLASSIC;
      version = FileNames.checkpointVersion(fileName);
    } else if (FileNames.isMultiPartCheckpointFile(fileName)) {
      logType = LogType.MULTIPART_CHECKPOINT;
      version = FileNames.checkpointVersion(fileName);
    } else if (FileNames.isV2CheckpointFile(fileName)) {
      logType = LogType.V2_CHECKPOINT_MANIFEST;
      version = FileNames.checkpointVersion(fileName);
    } else {
      throw new IllegalArgumentException(
          "File is not a recognized delta log type: " + file.getPath());
    }
    return new DeltaLogFile(file, logType, version);
  }

  public static DeltaLogFile ofSideCar(FileStatus file, long version) {
    return new DeltaLogFile(file, LogType.SIDECAR, version);
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  private final FileStatus fileStatus;
  private final LogType logType;
  private final long version;

  private DeltaLogFile(FileStatus fileStatus, LogType logType, long version) {
    this.fileStatus = fileStatus;
    this.logType = logType;
    this.version = version;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public LogType getLogType() {
    return logType;
  }

  public Category getCategory() {
    return logType.getCategory();
  }

  /**
   * Get the version for this log file. Note that for LOG_COMPACTION files this returns the end
   * version, similar to a checkpoint
   */
  public long getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "DeltaLogFile{" +
        "file=" + fileStatus +
        ", logType=" + logType +
        ", version=" + version +
        '}';
  }

  ////////////////////
  // Static classes //
  ////////////////////

  public static class CompactionFile extends DeltaLogFile {
    public static CompactionFile forFileStatus(FileStatus file) {
      final Tuple2<Long, Long> versions = FileNames.logCompactionVersions(file.getPath());
      return new CompactionFile(file, versions._1, versions._2);
    }

    private final long startVersion;
    private final long endVersion;

    private CompactionFile(FileStatus file, long startVersion, long endVersion) {
      super(file, LogType.LOG_COMPACTION, endVersion);
      this.startVersion = startVersion;
      this.endVersion = endVersion;
    }

    public long getStartVersion() {
      return startVersion;
    }

    public long getEndVersion() {
      return endVersion;
    }
  }

}
