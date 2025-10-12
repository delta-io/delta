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

/**
 * Classic checkpoint stored as a single Parquet file.
 *
 * <p>Example: {@code 00000000000000000001.checkpoint.parquet}
 */
public final class ParsedClassicCheckpointData extends ParsedCheckpointData {

  public static ParsedClassicCheckpointData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isClassicCheckpointFile(fileStatus.getPath()),
        "Expected a classic checkpoint file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.checkpointVersion(path);
    return new ParsedClassicCheckpointData(version, Optional.of(fileStatus), Optional.empty());
  }

  private ParsedClassicCheckpointData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  protected CheckpointTypePriority getCheckpointTypePriority() {
    return CheckpointTypePriority.CLASSIC;
  }

  @Override
  protected int compareToSameType(ParsedCheckpointData that) {
    return compareByDataSource(that);
  }
}
