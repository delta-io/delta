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
 * Delta commit file represent atomic changes to a table.
 *
 * <p>There are different types of Delta files:
 *
 * <ul>
 *   <li><b>Published commits</b>: Normal delta files in {@code _delta_log/} directory like {@code
 *       00000000000000000001.json}
 *   <li><b>Staged commits</b>: Files in {@code _delta_log/_staged_commits/} with UUID naming like
 *       {@code 00000000000000000001.uuid-1234.json}
 *   <li><b>Inline commits</b>: Content stored directly by the catalog, not as files
 * </ul>
 */
public final class ParsedDeltaData extends ParsedLogData {

  public static ParsedDeltaData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isCommitFile(fileStatus.getPath()),
        "Expected a Delta file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.deltaVersion(path);
    return new ParsedDeltaData(version, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedDeltaData forInlineData(long version, ColumnarBatch inlineData) {
    return new ParsedDeltaData(version, Optional.empty(), Optional.of(inlineData));
  }

  /* Stores whether this delta is a ratified commit. Stored to avoid repeated Path parsing */
  private final boolean isRatifiedCommit;

  private ParsedDeltaData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
    // Compute and store whether this is a ratified commit
    if (isFile()) {
      isRatifiedCommit = FileNames.isStagedDeltaFile(getFileStatus().getPath());
    } else { // isInline
      // TODO: revisit whether we can have inline data of a published delta file
      isRatifiedCommit = true;
    }
  }

  /** Whether this ParsedDeltaData is a ratified commit. False if it's published. */
  public boolean isRatifiedCommit() {
    return isRatifiedCommit;
  }

  @Override
  public String getParentCategoryName() {
    return "Delta";
  }

  @Override
  public Class<? extends ParsedLogData> getParentCategoryClass() {
    return ParsedDeltaData.class;
  }
}
