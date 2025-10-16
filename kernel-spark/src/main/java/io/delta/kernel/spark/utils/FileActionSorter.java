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

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.spark.annotation.Experimental;

/**
 * Interface for sorting file actions (AddFile/RemoveFile) by modificationTime and path.
 *
 * <p>This matches DeltaSource behavior: snapshot.allFiles.sort("modificationTime", "path")
 *
 * <p>Different implementations can provide different sorting strategies (e.g., in-memory sorting,
 * Spark DataFrame sorting, etc.).
 */
@Experimental
public interface FileActionSorter {

  /**
   * Sorts scan files (AddFile rows) by modificationTime (ascending) and path (ascending).
   *
   * <p>The returned iterator should yield AddFile Row objects in sorted order.
   *
   * @param scanFilesIter iterator of FilteredColumnarBatch containing AddFile rows (schema:
   *     struct(add: AddFile, tableRoot: string, ...);)
   * @return iterator of sorted AddFile Row objects
   */
  CloseableIterator<Row> sortScanFiles(CloseableIterator<FilteredColumnarBatch> scanFilesIter);
}
