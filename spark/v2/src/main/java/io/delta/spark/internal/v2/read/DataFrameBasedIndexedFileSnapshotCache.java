/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.SparkRowToKernelRow;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.functions;

/**
 * Distributed implementation of {@link IndexedFileSnapshotCache} backed by a persisted Spark
 * DataFrame of sorted, indexed AddFile rows.
 *
 * <p>Pushes {@code fromIndex} filtering to executors via SQL predicate push-down, so only
 * unprocessed files are streamed to the driver.
 *
 * <p>Callers synchronize externally (Spark's MicroBatchExecution loop is single-threaded).
 */
public class DataFrameBasedIndexedFileSnapshotCache
    implements IndexedFileSnapshotCache, AutoCloseable {

  static final String FILE_IDX_COL = "_file_idx";

  private final long version;
  private final long commitTimestamp;
  private Dataset<Row> sortedAddFiles;

  public DataFrameBasedIndexedFileSnapshotCache(
      long version, long commitTimestamp, Dataset<Row> sortedAddFiles) {
    this.version = version;
    this.commitTimestamp = commitTimestamp;
    this.sortedAddFiles = sortedAddFiles;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  public Dataset<Row> getSortedAddFiles() {
    return sortedAddFiles;
  }

  @Override
  public CloseableIterator<IndexedFile> getFiles(long fromIndex) {
    Dataset<Row> df = sortedAddFiles;

    if (fromIndex > DeltaSourceOffset.BASE_INDEX()) {
      df = df.where(functions.col(FILE_IDX_COL).gt(fromIndex));
    }

    return dataFrameToIndexedFiles(df, version);
  }

  @Override
  public void close() {
    Dataset<Row> df = sortedAddFiles;
    if (df != null) {
      df.unpersist();
      sortedAddFiles = null;
    }
  }

  /**
   * Converts an indexed DataFrame of AddFile rows into a lazy CloseableIterator of IndexedFiles,
   * wrapped with BEGIN/END sentinels. Uses toLocalIterator() to stream rows from executors to the
   * driver one at a time, avoiding pulling all data into driver memory.
   *
   * <p>The DataFrame must contain a {@link #FILE_IDX_COL} column (appended during cache creation)
   * that holds a deterministic, monotonically increasing index for each file. This index is read
   * directly from each row, so the caller can pre-filter the DataFrame (e.g. {@code WHERE _file_idx
   * > lastProcessedIndex}) to skip already-processed files without re-scanning them on the driver.
   */
  static CloseableIterator<IndexedFile> dataFrameToIndexedFiles(Dataset<Row> df, long version) {
    int fileIdxOrdinal = df.schema().fieldIndex(FILE_IDX_COL);
    java.util.Iterator<Row> localIter = df.toLocalIterator();

    return new CloseableIterator<IndexedFile>() {
      private boolean sentBegin = false;
      private boolean sentEnd = false;

      @Override
      public boolean hasNext() {
        return !sentEnd;
      }

      @Override
      public IndexedFile next() {
        if (!sentBegin) {
          sentBegin = true;
          return IndexedFile.sentinel(version, DeltaSourceOffset.BASE_INDEX());
        }

        if (localIter.hasNext()) {
          Row sparkRow = localIter.next();
          long fileIdx = sparkRow.getLong(fileIdxOrdinal);
          io.delta.kernel.data.Row kernelRow =
              new SparkRowToKernelRow(sparkRow, AddFile.SCHEMA_WITHOUT_STATS);
          return IndexedFile.addFile(version, fileIdx, new AddFile(kernelRow));
        }

        sentEnd = true;
        return IndexedFile.sentinel(version, DeltaSourceOffset.END_INDEX());
      }

      @Override
      public void close() throws IOException {
        // toLocalIterator() resources are managed by Spark
      }
    };
  }
}
