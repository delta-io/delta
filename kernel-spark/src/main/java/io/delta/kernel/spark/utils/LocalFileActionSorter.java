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
import java.util.ArrayList;
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

    // Materialize all AddFile rows
    List<Row> allAddFiles = extractAddFilesToInMemoryList(scanFilesIter);

    // Sort by modificationTime, then path (matching DeltaSource)
    sortAddFilesByModificationTime(allAddFiles);

    // Return iterator over sorted list
    return toCloseableIterator(allAddFiles);
  }

  /**
   * Extracts all AddFile rows from scan files iterator into an in-memory list.
   *
   * @param scanFilesIter iterator of FilteredColumnarBatch
   * @return list of AddFile Row objects
   */
  private static List<Row> extractAddFilesToInMemoryList(
      CloseableIterator<FilteredColumnarBatch> scanFilesIter) {
    List<Row> allAddFiles = new ArrayList<>();

    // Use Kernel's Utils.intoRows() to simplify batch-to-row conversion
    CloseableIterator<Row> allRows = io.delta.kernel.internal.util.Utils.intoRows(scanFilesIter);

    try {
      while (allRows.hasNext()) {
        Row scanFileRow = allRows.next();
        // scanFileRow schema: struct<add: AddFile, tableRoot: string, ...>
        int addIdx = scanFileRow.getSchema().indexOf("add");
        if (addIdx >= 0 && !scanFileRow.isNullAt(addIdx)) {
          Row addFileRow = scanFileRow.getStruct(addIdx);
          allAddFiles.add(addFileRow);
        }
      }
    } finally {
      try {
        allRows.close();
      } catch (Exception e) {
        throw new RuntimeException("Error closing rows iterator", e);
      }
    }
    return allAddFiles;
  }

  /**
   * Converts a list to a CloseableIterator.
   *
   * @param list the list to iterate over
   * @return CloseableIterator over the list
   */
  private static CloseableIterator<Row> toCloseableIterator(List<Row> list) {
    return new CloseableIterator<Row>() {
      private int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex < list.size();
      }

      @Override
      public Row next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException();
        }
        return list.get(currentIndex++);
      }

      @Override
      public void close() {
        // No resources to close
      }
    };
  }

  /**
   * Sorts a list of AddFile rows by modificationTime (ascending), then path (ascending).
   *
   * <p>This matches DeltaSource behavior.
   *
   * @param addFiles list of AddFile Row objects to sort in-place
   */
  private static void sortAddFilesByModificationTime(List<Row> addFiles) {
    addFiles.sort(
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
  }
}
