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
package io.delta.spark.dsv2.scan.batch;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.dsv2.scan.utils.KernelToSparkRowAdapter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads Delta Lake data files using the Kernel API and converts to Spark InternalRows.
 *
 * <p><strong>Thread Safety:</strong> This class is NOT thread-safe. Each instance should be used by
 * a single thread only. Spark's partition readers are typically used in a single-threaded context
 * per partition.
 */
public class KernelSparkPartitionReader implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(KernelSparkPartitionReader.class);

  /**
   * Scan state row with schema {@link ScanStateRow#SCHEMA} containing schema, configuration and
   * table metadata for the read operation
   */
  private final Row scanState;

  /**
   * Scan file row with schema {@link InternalScanFileUtils#SCAN_FILE_SCHEMA} containing file
   * metadata (path, size, etc.) for the file being read.
   */
  private final Row scanFileRow;

  private final Engine engine;
  private CloseableIterator<InternalRow> dataIterator;
  private boolean initialized = false;

  public KernelSparkPartitionReader(Engine engine, Row scanState, Row scanFileRow) {
    this.scanState = requireNonNull(scanState, "scanState is null");
    this.scanFileRow = requireNonNull(scanFileRow, "scanFileRow is null");
    this.engine = requireNonNull(engine, "engine is null");
  }

  /**
   * Advances to the next row, performing lazy initialization on first call.
   *
   * @return true if a row is available
   */
  @Override
  public boolean next() {
    initialize();
    return dataIterator != null && dataIterator.hasNext();
  }

  /**
   * Returns the current row. Call only after {@link #next()} returns true. Note: Returned row may
   * be reused; use {@code row.copy()} if needed for later access.
   */
  @Override
  public InternalRow get() {
    if (!initialized || dataIterator == null) {
      throw new IllegalStateException("Reader not initialized. Call next() first.");
    }
    if (!dataIterator.hasNext()) {
      throw new IllegalStateException("No more data available.");
    }
    return dataIterator.next();
  }

  private void initialize() {
    if (initialized) {
      return;
    }

    try {
      CloseableIterator<ColumnarBatch> physicalDataIter =
          engine
              .getParquetHandler()
              .readParquetFiles(
                  Utils.singletonCloseableIterator(
                      InternalScanFileUtils.getAddFileStatus(scanFileRow)),
                  ScanStateRow.getPhysicalDataReadSchema(scanState),
                  // TODO: parquet push down
                  Optional.empty())
              .map(FileReadResult::getData);
      CloseableIterator<FilteredColumnarBatch> logicalDataIter =
          Scan.transformPhysicalData(engine, scanState, scanFileRow, physicalDataIter);
      dataIterator = new RowIteratorAdapter(logicalDataIter);
      initialized = true;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to initialize partition reader", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (dataIterator != null) {
      dataIterator.close();
    }
  }

  /** Adapter that converts FilteredColumnarBatch Iterator to InternalRow Iterator. */
  private static class RowIteratorAdapter implements CloseableIterator<InternalRow> {
    private final CloseableIterator<FilteredColumnarBatch> batchIterator;
    private CloseableIterator<Row> currentBatchRows;

    RowIteratorAdapter(CloseableIterator<FilteredColumnarBatch> batchIterator) {
      this.batchIterator = requireNonNull(batchIterator, "batchIterator is null");
    }

    @Override
    public boolean hasNext() {
      if (currentBatchRows != null && currentBatchRows.hasNext()) {
        return true;
      }

      // Try to get next batch
      while (batchIterator.hasNext()) {
        FilteredColumnarBatch batch = batchIterator.next();
        if (currentBatchRows != null) {
          try {
            currentBatchRows.close();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        currentBatchRows = batch.getRows();
        if (currentBatchRows.hasNext()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public InternalRow next() {
      if (!hasNext()) {
        throw new java.util.NoSuchElementException("No more rows available");
      }
      return new KernelToSparkRowAdapter(currentBatchRows.next());
    }

    @Override
    public void close() throws IOException {
      if (currentBatchRows != null) {
        currentBatchRows.close();
      }
      batchIterator.close();
    }
  }
}
