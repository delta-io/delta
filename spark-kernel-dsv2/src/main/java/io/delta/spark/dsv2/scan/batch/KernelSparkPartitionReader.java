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
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.json.JsonUtils;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileReadResult;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Partition reader that reads data from Delta Lake files using the Delta Kernel API. */
public class KernelSparkPartitionReader implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(KernelSparkPartitionReader.class);

  private final String serializedScanState;
  private final String serializedScanFileRow;
  private CloseableIterator<InternalRow> dataIterator;
  private boolean initialized = false;
  private final Engine engine;

  /**
   * Creates a new partition reader with the given serialized scan information.
   *
   * <p>The reader is created in an uninitialized state and will perform lazy initialization when
   * {@link #next()} is first called. This includes deserializing the scan state and file row
   * information, setting up the Kernel engine, and preparing the data reading pipeline.
   *
   * @param serializedScanState JSON-serialized string containing the Kernel scan state. This
   *     includes global scan configuration such as schema information, table metadata, and any
   *     applied filters. Must not be null.
   * @param serializedScanFileRow JSON-serialized string containing the Kernel scan file row. This
   *     includes file-specific metadata such as file path, size, and partition values for the file
   *     to be read. Must not be null.
   * @throws NullPointerException if either parameter is null
   */
  public KernelSparkPartitionReader(String serializedScanState, String serializedScanFileRow) {
    this.serializedScanState = requireNonNull(serializedScanState, "serializedScanState is null");
    this.serializedScanFileRow =
        requireNonNull(serializedScanFileRow, "serializedScanFileRow is null");
    this.engine = DefaultEngine.create(new Configuration());
  }

  /**
   * Advances the reader to the next row and returns whether a row is available.
   *
   * <p>This method performs lazy initialization on the first call, which includes:
   *
   * <ul>
   *   <li>Deserializing the scan state and file row from JSON
   *   <li>Setting up the Kernel engine and data reading pipeline
   *   <li>Reading and transforming the physical data
   * </ul>
   *
   * <p>Subsequent calls simply advance the internal iterator through the available data.
   *
   * @return {@code true} if a row is available and can be retrieved with {@link #get()}, {@code
   *     false} if no more rows are available
   * @throws IOException if an error occurs during data reading or processing
   * @throws UncheckedIOException if a Kernel API operation fails
   */
  @Override
  public boolean next() throws IOException {
    initialize();
    return dataIterator.hasNext();
  }

  /**
   * Returns the current row.
   *
   * <p>This method should only be called after a successful call to {@link #next()} that returned
   * {@code true}. The returned row is a Spark {@link InternalRow} containing the converted data
   * from the Delta Lake file.
   *
   * <p><strong>Important:</strong> The returned row object may be reused by the reader for
   * efficiency. If you need to store the row beyond the next call to {@link #next()} or {@link
   * #get()}, you should create a copy using {@code row.copy()}.
   *
   * @return the current row as a Spark {@link InternalRow}
   * @throws IllegalStateException if called before {@link #next()} or after {@link #next()}
   *     returned {@code false}
   */
  @Override
  public InternalRow get() {
    if (!initialized) {
      throw new IllegalStateException("Reader not initialized. Call next() first.");
    }
    return dataIterator.next();
  }

  private void initialize() {
    if (initialized) {
      return;
    }

    try {
      // 1. Deserialize scan state using proper schema
      Row scanStateRow = JsonUtils.rowFromJson(serializedScanState, ScanStateRow.SCHEMA);

      // 2. Deserialize file row using SCAN_FILE_SCHEMA
      StructType scanFileSchema = InternalScanFileUtils.SCAN_FILE_SCHEMA;
      Row scanFileRow = JsonUtils.rowFromJson(serializedScanFileRow, scanFileSchema);

      // 3. Get physical data read schema and file status
      StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(scanStateRow);
      FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);

      // 4. Read physical data from Parquet file
      CloseableIterator<FileReadResult> fileReadResultIter =
          engine
              .getParquetHandler()
              .readParquetFiles(
                  Utils.singletonCloseableIterator(fileStatus),
                  physicalReadSchema,
                  Optional.empty());

      // Convert FileReadResult to ColumnarBatch
      CloseableIterator<ColumnarBatch> physicalDataIter =
          new CloseableIterator<ColumnarBatch>() {
            @Override
            public boolean hasNext() {
              return fileReadResultIter.hasNext();
            }

            @Override
            public ColumnarBatch next() {
              FileReadResult result = fileReadResultIter.next();
              return result.getData();
            }

            @Override
            public void close() throws IOException {
              fileReadResultIter.close();
            }
          };

      // 5. Transform physical data to logical data using scan state
      CloseableIterator<FilteredColumnarBatch> logicalDataIter =
          Scan.transformPhysicalData(engine, scanStateRow, scanFileRow, physicalDataIter);

      // 6. Convert to Spark InternalRows
      dataIterator = convertToInternalRows(logicalDataIter);
      initialized = true;

    } catch (IOException e) {
      LOG.error("Failed to initialize partition reader", e);
      throw new UncheckedIOException("Failed to initialize partition reader", e);
    } catch (Exception e) {
      LOG.error("Unexpected error during partition reader initialization", e);
      throw new RuntimeException("Failed to initialize partition reader", e);
    }
  }

  /**
   * Closes the partition reader and releases any associated resources.
   *
   * <p>This method should be called when the reader is no longer needed to ensure proper cleanup of
   * resources such as file handles, memory buffers, and iterators. It is safe to call this method
   * multiple times.
   *
   * <p>After calling this method, any subsequent calls to {@link #next()} or {@link #get()} may
   * result in undefined behavior.
   *
   * @throws IOException if an error occurs while closing underlying resources
   */
  @Override
  public void close() throws IOException {
    if (dataIterator != null) {
      dataIterator.close();
    }
  }

  /** Convert Kernel FilteredColumnarBatch iterator to Spark InternalRow iterator. */
  private CloseableIterator<InternalRow> convertToInternalRows(
      CloseableIterator<FilteredColumnarBatch> batchIterator) {
    return new CloseableIterator<InternalRow>() {
      private CloseableIterator<InternalRow> currentBatchRows;

      @Override
      public boolean hasNext() {
        // Check if current batch has more rows
        if (currentBatchRows != null && currentBatchRows.hasNext()) {
          return true;
        }

        // Try to get next batch
        while (batchIterator.hasNext()) {
          FilteredColumnarBatch batch = batchIterator.next();
          try (CloseableIterator<Row> rows = batch.getRows()) {
            // Convert this batch to InternalRows
            currentBatchRows =
                convertBatchToInternalRows(batch.getData(), batch.getSelectionVector());
            if (currentBatchRows.hasNext()) {
              return true;
            }
          } catch (IOException e) {
            throw new UncheckedIOException("Failed to process batch", e);
          }
        }

        return false;
      }

      @Override
      public InternalRow next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException("No more rows available");
        }
        return currentBatchRows.next();
      }

      @Override
      public void close() throws IOException {
        if (currentBatchRows != null) {
          currentBatchRows.close();
        }
        batchIterator.close();
      }
    };
  }

  /** Convert a single ColumnarBatch to InternalRow iterator. */
  private CloseableIterator<InternalRow> convertBatchToInternalRows(
      ColumnarBatch batch, Optional<ColumnVector> selectionVector) {

    StructType schema = batch.getSchema();
    int numRows = batch.getSize();

    return new CloseableIterator<InternalRow>() {
      private int currentRowIndex = 0;

      @Override
      public boolean hasNext() {
        // Skip rows not selected by the selection vector
        while (currentRowIndex < numRows) {
          if (!selectionVector.isPresent() || selectionVector.get().getBoolean(currentRowIndex)) {
            return true;
          }
          currentRowIndex++;
        }
        return false;
      }

      @Override
      public InternalRow next() {
        if (!hasNext()) {
          throw new java.util.NoSuchElementException("No more rows available");
        }

        Object[] values = new Object[schema.length()];
        for (int i = 0; i < schema.length(); i++) {
          ColumnVector columnVector = batch.getColumnVector(i);
          DataType dataType = schema.at(i).getDataType();
          values[i] = convertValue(columnVector, currentRowIndex, dataType);
        }

        currentRowIndex++;
        return new GenericInternalRow(values);
      }

      @Override
      public void close() throws IOException {
        // Nothing to close for this iterator
      }
    };
  }

  /** Convert a single value from Kernel ColumnVector to Spark-compatible object. */
  private Object convertValue(ColumnVector columnVector, int rowIndex, DataType dataType) {
    if (columnVector.isNullAt(rowIndex)) {
      return null;
    }

    if (dataType instanceof io.delta.kernel.types.BooleanType) {
      return columnVector.getBoolean(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.ByteType) {
      return columnVector.getByte(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.ShortType) {
      return columnVector.getShort(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.IntegerType) {
      return columnVector.getInt(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.LongType) {
      return columnVector.getLong(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.FloatType) {
      return columnVector.getFloat(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.DoubleType) {
      return columnVector.getDouble(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.StringType) {
      return UTF8String.fromString(columnVector.getString(rowIndex));
    } else if (dataType instanceof io.delta.kernel.types.DateType) {
      return columnVector.getInt(rowIndex); // Date as days since epoch
    } else if (dataType instanceof io.delta.kernel.types.TimestampType) {
      return columnVector.getLong(rowIndex); // Timestamp as microseconds since epoch
    } else if (dataType instanceof io.delta.kernel.types.DecimalType) {
      return columnVector.getDecimal(rowIndex);
    } else if (dataType instanceof io.delta.kernel.types.BinaryType) {
      return columnVector.getBinary(rowIndex);
    } else {
      LOG.warn("Unsupported data type: {}, returning string representation", dataType);
      return UTF8String.fromString(columnVector.getString(rowIndex));
    }
  }
}
