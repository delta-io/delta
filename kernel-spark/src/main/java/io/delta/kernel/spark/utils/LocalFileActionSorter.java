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
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;
import org.apache.spark.annotation.Experimental;

/**
 * In-memory implementation of {@link FileActionSorter}.
 *
 * <p>This implementation materializes all AddFile rows in memory and sorts them using Java's
 * Collections.sort. This is acceptable for initial snapshots as DeltaSource does the same.
 */
@Experimental
public class LocalFileActionSorter implements FileActionSorter {

  /** Singleton instance for convenience */
  public static final LocalFileActionSorter INSTANCE = new LocalFileActionSorter();

  @Override
  public CloseableIterator<Row> sortScanFiles(
      CloseableIterator<FilteredColumnarBatch> scanFilesIter) {
    List<Row> allAddFiles =
        Utils.intoRows(scanFilesIter)
            .map(
                scanFileRow -> {
                  // scanFileRow schema: struct<add: AddFile, tableRoot: string, ...>
                  int addIdx = scanFileRow.getSchema().indexOf("add");
                  if (addIdx < 0 || scanFileRow.isNullAt(addIdx)) {
                    throw new IllegalStateException(
                        "Expected scan file row to contain a non-null 'add' field");
                  }
                  return scanFileRow.getStruct(addIdx);
                })
            .toInMemoryList();

    allAddFiles.sort(
        (row1, row2) -> {
          // Compare modificationTime first
          int modTimeIdx1 = row1.getSchema().indexOf("modificationTime");
          int modTimeIdx2 = row2.getSchema().indexOf("modificationTime");
          long modTime1 = row1.getLong(modTimeIdx1);
          long modTime2 = row2.getLong(modTimeIdx2);

          int timeCompare = Long.compare(modTime1, modTime2);
          if (timeCompare != 0) {
            return timeCompare;
          }

          // If modificationTime is equal, compare path
          int pathIdx1 = row1.getSchema().indexOf("path");
          int pathIdx2 = row2.getSchema().indexOf("path");
          String path1 = row1.getString(pathIdx1);
          String path2 = row2.getString(pathIdx2);
          return path1.compareTo(path2);
        });

    return Utils.toCloseableIterator(allAddFiles.iterator());
  }
}
