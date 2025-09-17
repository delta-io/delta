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
 * Version checksum file containing table state information for integrity validation.
 *
 * <p>These auxiliary files contain important metadata about the table state at a specific version
 * to enable detection of non-compliant modifications to Delta files. Contains information like
 * table size, file counts, and metadata. Example: {@code 00000000000000000001.crc}
 */
public final class ParsedChecksumData extends ParsedLogData {

  public static ParsedChecksumData forFileStatus(FileStatus fileStatus) {
    checkArgument(
        FileNames.isChecksumFile(fileStatus.getPath()),
        "Expected a checksum file but got %s",
        fileStatus.getPath());

    final String path = fileStatus.getPath();
    final long version = FileNames.checksumVersion(path);
    return new ParsedChecksumData(version, Optional.of(fileStatus), Optional.empty());
  }

  public static ParsedChecksumData forInlineData(long version, ColumnarBatch inlineData) {
    return new ParsedChecksumData(version, Optional.empty(), Optional.of(inlineData));
  }

  private ParsedChecksumData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  @Override
  public String getParentCategoryName() {
    return "Checksum";
  }

  @Override
  public Class<? extends ParsedLogData> getParentCategoryClass() {
    return ParsedChecksumData.class;
  }
}
