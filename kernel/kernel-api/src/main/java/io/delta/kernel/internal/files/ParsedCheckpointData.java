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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.Optional;

public class ParsedCheckpointData extends ParsedLogData
    implements Comparable<ParsedCheckpointData> {

  public static ParsedCheckpointData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isMultiPartCheckpointFile(path)) {
      return ParsedMultiPartCheckpointData.forFileStatus(fileStatus);
    }

    final long version;
    final ParsedLogType type;

    if (FileNames.isClassicCheckpointFile(path)) {
      version = FileNames.checkpointVersion(path);
      type = ParsedLogType.CLASSIC_CHECKPOINT;
    } else if (FileNames.isV2CheckpointFile(path)) {
      version = FileNames.checkpointVersion(path);
      type = ParsedLogType.V2_CHECKPOINT;
    } else {
      throw new IllegalArgumentException("File is not a recognized checkpoint type: " + path);
    }

    return new ParsedCheckpointData(version, type, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedCheckpointData forInlineData(
      long version, ParsedLogType type, ColumnarBatch inlineData) {
    if (type == ParsedLogType.MULTIPART_CHECKPOINT) {
      throw new IllegalArgumentException(
          "For MULTIPART_CHECKPOINT, use ParsedMultiPartCheckpointData.forInlineData() instead");
    }

    return new ParsedCheckpointData(version, type, Optional.empty(), Optional.of(inlineData));
  }

  protected ParsedCheckpointData(
      long version,
      ParsedLogType type,
      Optional<FileStatus> fileStatusOpt,
      Optional<ColumnarBatch> inlineDataOpt) {
    super(version, type, fileStatusOpt, inlineDataOpt);
    checkArgument(type.category == ParsedLogCategory.CHECKPOINT, "Must be a checkpoint");
  }

  @Override
  public int compareTo(ParsedCheckpointData that) {
    // Compare versions.
    if (version != that.version) {
      return Long.compare(version, that.version);
    }

    // Compare types.
    if (type != that.type) {
      return Integer.compare(type.ordinal(), that.type.ordinal());
    }

    // Use type-specific tiebreakers if versions and types are the same.
    switch (type) {
      case CLASSIC_CHECKPOINT:
      case V2_CHECKPOINT:
        return getTieBreaker(that);
      case MULTIPART_CHECKPOINT:
        final ParsedMultiPartCheckpointData thisCasted = (ParsedMultiPartCheckpointData) this;
        final ParsedMultiPartCheckpointData thatCasted = (ParsedMultiPartCheckpointData) that;
        final int numPartsComparison = Long.compare(thisCasted.numParts, thatCasted.numParts);
        if (numPartsComparison != 0) {
          return numPartsComparison;
        } else {
          return getTieBreaker(that);
        }
      default:
        throw new IllegalStateException("Unexpected type: " + type);
    }
  }

  /**
   * Here, we prefer inline data -- if the data is already stored in memory, we should read that
   * instead of going to the cloud store to read a file status.
   */
  private int getTieBreaker(ParsedCheckpointData that) {
    if (this.isInline() && that.isMaterialized()) {
      return 1; // Prefer this
    } else if (this.isMaterialized() && that.isInline()) {
      return -1; // Prefer that
    } else if (this.isMaterialized() && that.isMaterialized()) {
      return this.getFileStatus().getPath().compareTo(that.getFileStatus().getPath());
    } else {
      return 0;
    }
  }
}
