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
import io.delta.kernel.utils.FileStatus;
import java.util.Optional;

public abstract class ParsedCheckpointData extends ParsedLogData
    implements Comparable<ParsedCheckpointData> {

  protected ParsedCheckpointData(
      long version, Optional<FileStatus> fileStatusOpt, Optional<ColumnarBatch> inlineDataOpt) {
    super(version, fileStatusOpt, inlineDataOpt);
  }

  protected abstract int getCheckpointTypePriority();

  @Override
  public Class<? extends ParsedLogData> getParentCategoryClass() {
    return ParsedCheckpointData.class;
  }

  /** Here, returning 1 means that `this` is preferred over (i.e. greater than) `that`. */
  @Override
  public int compareTo(ParsedCheckpointData that) {
    // Compare versions.
    if (version != that.version) {
      return Long.compare(version, that.version);
    }

    // Compare types by priority.
    int thisTypePriority = this.getCheckpointTypePriority();
    int thatTypePriority = that.getCheckpointTypePriority();
    if (thisTypePriority != thatTypePriority) {
      return Integer.compare(thisTypePriority, thatTypePriority);
    }

    // Use type-specific tiebreakers if versions and types are the same.
    if (this instanceof ParsedMultiPartCheckpointData
        && that instanceof ParsedMultiPartCheckpointData) {
      final ParsedMultiPartCheckpointData thisCasted = (ParsedMultiPartCheckpointData) this;
      final ParsedMultiPartCheckpointData thatCasted = (ParsedMultiPartCheckpointData) that;
      return thisCasted.compareToMultiPart(thatCasted);
    } else {
      return getTieBreaker(that);
    }
  }

  /**
   * Here, we prefer inline data -- if the data is already stored in memory, we should read that
   * instead of going to the cloud store to read a file status.
   */
  protected int getTieBreaker(ParsedCheckpointData that) {
    if (this.isInline() && that.isFile()) {
      return 1; // Prefer this
    } else if (this.isFile() && that.isInline()) {
      return -1; // Prefer that
    } else if (this.isFile() && that.isFile()) {
      return this.getFileStatus().getPath().compareTo(that.getFileStatus().getPath());
    } else {
      return 0;
    }
  }
}
