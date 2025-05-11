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

package io.delta.kernel.internal.files;

import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.Objects;

public abstract class ParsedLogFile {

  ///////////////////////////////////////
  // Static enums, fields, and methods //
  ///////////////////////////////////////

  public enum Category {
    DELTA,
    CHECKPOINT,
    LOG_COMPACTION,
    CHECKSUM
  }

  public static ParsedLogFile forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isCommitFile(path)) {
      // TODO: is publishedDeltaFile, isStagedCommitFile
      return PublishedDeltaFile.create(fileStatus);
    } else if (FileNames.isClassicCheckpointFile(path)) {
      return ClassicCheckpointFile.create(fileStatus);
    } else if (FileNames.isMultiPartCheckpointFile(path)) {
      return MultipartCheckpointFile.create(fileStatus);
    } else if (FileNames.isV2CheckpointFile(path)) {
      return V2CheckpointFile.create(fileStatus);
    } else if (FileNames.isLogCompactionFile(path)) {
      return LogCompactionFile.create(fileStatus);
    } else if (FileNames.isChecksumFile(path)) {
      return ChecksumFile.create(fileStatus);
    } else {
      throw new IllegalArgumentException("File is not a recognized delta log type: " + path);
    }
  }

  ///////////////////////////////
  // Member fields and methods //
  ///////////////////////////////

  private final FileStatus fileStatus;
  private final long version;
  private final Category category;

  protected ParsedLogFile(FileStatus fileStatus, long version, Category category) {
    this.fileStatus = fileStatus;
    this.version = version;
    this.category = category;
  }

  public FileStatus getFileStatus() {
    return fileStatus;
  }

  public long getVersion() {
    return version;
  }

  public Category getCategory() {
    return category;
  }

  /** NOTE: Children only need to override this if they have additional fields. */
  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ParsedLogFile that = (ParsedLogFile) o;
    return version == that.version
        && Objects.equals(fileStatus, that.fileStatus)
        && category == that.category;
  }

  /** NOTE: Children only need to override this if they have additional fields. */
  @Override
  public int hashCode() {
    return Objects.hash(fileStatus, version, category);
  }

  ////////////
  // Deltas //
  ////////////

  public abstract static class DeltaFile extends ParsedLogFile {
    protected DeltaFile(FileStatus fileStatus, long version) {
      super(fileStatus, version, Category.DELTA);
    }
  }

  public static class PublishedDeltaFile extends DeltaFile {
    private static PublishedDeltaFile create(FileStatus fileStatus) {
      return new PublishedDeltaFile(fileStatus, FileNames.deltaVersion(fileStatus.getPath()));
    }

    private PublishedDeltaFile(FileStatus fileStatus, long version) {
      super(fileStatus, version);
    }
  }

  // TODO: StagedCommitFile

  /////////////////
  // Checkpoints //
  /////////////////

  public abstract static class CheckpointFile extends ParsedLogFile {
    protected CheckpointFile(FileStatus fileStatus, long version) {
      super(fileStatus, version, Category.CHECKPOINT);
    }

    public abstract boolean mayContainSidecarFiles();
  }

  public static class ClassicCheckpointFile extends CheckpointFile {
    private static ClassicCheckpointFile create(FileStatus fileStatus) {
      long version = FileNames.checkpointVersion(fileStatus.getPath());
      return new ClassicCheckpointFile(fileStatus, version);
    }

    private ClassicCheckpointFile(FileStatus fileStatus, long version) {
      super(fileStatus, version);
    }

    @Override
    public boolean mayContainSidecarFiles() {
      return true;
    }
  }

  public static class MultipartCheckpointFile extends CheckpointFile {
    private static MultipartCheckpointFile create(FileStatus fileStatus) {
      final long version = FileNames.checkpointVersion(fileStatus.getPath());
      final Tuple2<Integer, Integer> partAndNumParts =
          FileNames.multiPartCheckpointPartAndNumParts(fileStatus.getPath());
      return new MultipartCheckpointFile(
          fileStatus, version, partAndNumParts._1, partAndNumParts._2);
    }

    private final int part;
    private final int numParts;

    private MultipartCheckpointFile(FileStatus fileStatus, long version, int part, int numParts) {
      super(fileStatus, version);
      this.part = part;
      this.numParts = numParts;
    }

    public int getPart() {
      return part;
    }

    public int getNumParts() {
      return numParts;
    }

    @Override
    public boolean mayContainSidecarFiles() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      final MultipartCheckpointFile that = (MultipartCheckpointFile) o;
      return part == that.part && numParts == that.numParts;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), part, numParts);
    }
  }

  public static class V2CheckpointFile extends CheckpointFile {
    private static V2CheckpointFile create(FileStatus fileStatus) {
      long version = FileNames.checkpointVersion(fileStatus.getPath());
      return new V2CheckpointFile(fileStatus, version);
    }

    private V2CheckpointFile(FileStatus fileStatus, long version) {
      super(fileStatus, version);
    }

    @Override
    public boolean mayContainSidecarFiles() {
      return true;
    }
  }

  ///////////
  // Other //
  ///////////

  public static class LogCompactionFile extends ParsedLogFile {
    private static LogCompactionFile create(FileStatus fileStatus) {
      final Tuple2<Long, Long> startEnd = FileNames.logCompactionVersions(fileStatus.getPath());
      return new LogCompactionFile(fileStatus, startEnd._1, startEnd._2);
    }

    private final long startVersion;
    private final long endVersion;

    private LogCompactionFile(FileStatus fileStatus, long startVersion, long endVersion) {
      super(fileStatus, endVersion, Category.LOG_COMPACTION);
      this.startVersion = startVersion;
      this.endVersion = endVersion;
    }

    public long getStartVersion() {
      return startVersion;
    }

    public long getEndVersion() {
      return endVersion;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      final LogCompactionFile that = (LogCompactionFile) o;
      return startVersion == that.startVersion && endVersion == that.endVersion;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), startVersion, endVersion);
    }
  }

  public static class ChecksumFile extends ParsedLogFile {
    private static ChecksumFile create(FileStatus fileStatus) {
      return new ChecksumFile(fileStatus, FileNames.checksumVersion(fileStatus.getPath()));
    }

    private ChecksumFile(FileStatus fileStatus, long version) {
      super(fileStatus, version, Category.CHECKSUM);
    }
  }
}
