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
package io.delta.kernel.spark.utils;

import io.delta.kernel.data.Row;
import javax.annotation.Nullable;
import org.apache.spark.annotation.Experimental;

/**
 * Represents a file action from a Delta commit with its version and index information.
 *
 * <p>This is similar to IndexedFile in DeltaSource but simplified for Kernel API usage.
 *
 * <p>Either addFileRow or removeFileRow may be null (for sentinel values or single action type),
 * but at most one should be non-null for actual file actions.
 */
@Experimental
public class IndexedFileAction {
  private final long version;
  private final long index;
  @Nullable private final Row addFileRow;
  @Nullable private final Row removeFileRow;

  public IndexedFileAction(
      long version, long index, @Nullable Row addFileRow, @Nullable Row removeFileRow) {
    this.version = version;
    this.index = index;
    this.addFileRow = addFileRow;
    this.removeFileRow = removeFileRow;
  }

  public long getVersion() {
    return version;
  }

  public long getIndex() {
    return index;
  }

  @Nullable
  public Row getAddFile() {
    return addFileRow;
  }

  @Nullable
  public Row getRemoveFile() {
    return removeFileRow;
  }

  public boolean hasFileAction() {
    return addFileRow != null || removeFileRow != null;
  }
}
