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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.FileStatus;
import java.util.Optional;

/** Base class for Delta types that represent atomic changes to a table. */
public abstract class ParsedDeltaData extends ParsedLogData {

  public static ParsedDeltaData forFileStatus(FileStatus fileStatus) {
    final String path = fileStatus.getPath();

    if (FileNames.isPublishedDeltaFile(path)) {
      return ParsedPublishedDeltaData.forFileStatus(fileStatus);
    } else if (FileNames.isStagedDeltaFile(path)) {
      return ParsedCatalogCommitData.forFileStatus(fileStatus);
    } else {
      throw new IllegalArgumentException("Unknown delta file type: " + path);
    }
  }

  protected ParsedDeltaData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }
}
