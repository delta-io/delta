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
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.FileStatus;
import java.util.Objects;
import java.util.Optional;

/**
 * Log compaction file containing compacted delta entries across a version range.
 *
 * <p>These files compact multiple delta files into a single JSON file to reduce the number of files
 * readers need to process. Example: {@code
 * 00000000000000000001.00000000000000000009.compacted.json} represents compacted entries from
 * version 1 to 9.
 */
// TODO: Add the comparable logic from CheckpointInstance.
public final class ParsedLogCompactionData extends ParsedLogData {
  public static ParsedLogCompactionData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isLogCompactionFile(fileStatus.getPath()),
        "Expected a log compaction file but got %s",
        fileStatus.getPath());

    final Tuple2<Long, Long> startEnd = FileNames.logCompactionVersions(fileStatus.getPath());
    return new ParsedLogCompactionData(
        startEnd._1, startEnd._2, Optional.of(fileStatus), Optional.empty());
  }

  public final long startVersion;
  public final long endVersion;

  private ParsedLogCompactionData(
      long startVersion,
      long endVersion,
      Optional<FileStatus> fileStatusOpt,
      Optional<ColumnarBatch> inlineDataOpt) {
    super(endVersion, fileStatusOpt, inlineDataOpt);
    checkArgument(
        startVersion >= 0 && endVersion >= 0, "startVersion and endVersion must be non-negative");
    checkArgument(startVersion < endVersion, "startVersion must be less than endVersion");
    this.startVersion = startVersion;
    this.endVersion = endVersion;
  }

  @Override
  public Class<? extends ParsedLogData> getGroupByCategoryClass() {
    return ParsedLogCompactionData.class;
  }

  @Override
  protected void appendAdditionalToStringFields(StringBuilder sb) {
    sb.append(", startVersion=").append(startVersion);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ParsedLogCompactionData that = (ParsedLogCompactionData) o;
    return startVersion == that.startVersion && endVersion == that.endVersion;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), startVersion, endVersion);
  }
}
