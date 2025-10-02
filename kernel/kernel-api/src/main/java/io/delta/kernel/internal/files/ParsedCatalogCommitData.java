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
 * A catalog commit Delta file represents an atomic change to a table.
 *
 * <p>Can be staged and written to a staged commit file, like so: {@code
 * _delta_log/_staged_commits/00000000000000000001.uuid-1234.json}.
 *
 * <p>Can also be inline.
 */
public final class ParsedCatalogCommitData extends ParsedDeltaData {

  public static ParsedCatalogCommitData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isStagedDeltaFile(fileStatus.getPath()),
        "Expected a staged commit file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.deltaVersion(path);
    return new ParsedCatalogCommitData(version, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedCatalogCommitData forInlineData(long version, ColumnarBatch inlineData) {
    return new ParsedCatalogCommitData(version, Optional.empty(), Optional.of(inlineData));
  }

  private ParsedCatalogCommitData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  public Class<? extends ParsedLogData> getGroupByCategoryClass() {
    return ParsedCatalogCommitData.class;
  }
}
