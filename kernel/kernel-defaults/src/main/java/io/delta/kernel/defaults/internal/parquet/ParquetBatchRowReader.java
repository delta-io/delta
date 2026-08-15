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
package io.delta.kernel.defaults.internal.parquet;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.stream.LongStream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterates rows from an already-opened {@link ParquetFileReader}, using public parquet-mr APIs
 * ({@code readNextFilteredRowGroup} + {@link ColumnIOFactory}) instead of package-private {@code
 * InternalParquetRecordReader}.
 *
 * <p>Callers own footer reuse by opening the file reader with {@link
 * ParquetFileReader#open(org.apache.parquet.io.InputFile,
 * org.apache.parquet.hadoop.metadata.ParquetMetadata, ParquetReadOptions,
 * org.apache.parquet.io.SeekableInputStream)} before calling {@link #open}.
 *
 * <p>Ownership of {@code fileReader} transfers to this class; {@link #close()} closes it.
 */
final class ParquetBatchRowReader implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(ParquetBatchRowReader.class);
  private static final String STRICT_TYPE_CHECKING = "parquet.strict.typing";
  private final ParquetFileReader fileReader;
  private final ColumnIOFactory columnIOFactory;
  private final MessageType fileSchema;
  private final MessageType requestedSchema;
  private final RecordMaterializer<Object> materializer;
  private final boolean strictTypeChecking;
  private final boolean filterRecords;
  private final FilterCompat.Filter recordFilter;
  private final long totalRows;
  private long rowsConsumed = 0;
  private long rowsLoadedFromGroups = 0;
  private PageReadStore currentRowGroup;
  private RecordReader<Object> recordReader;
  private long currentRowIndex = -1;
  private PrimitiveIterator.OfLong rowIndexInFile;

  private ParquetBatchRowReader(
      ParquetFileReader fileReader,
      ColumnIOFactory columnIOFactory,
      MessageType fileSchema,
      MessageType requestedSchema,
      RecordMaterializer<Object> materializer,
      boolean strictTypeChecking,
      boolean filterRecords,
      FilterCompat.Filter recordFilter,
      long totalRows) {
    this.fileReader = fileReader;
    this.columnIOFactory = columnIOFactory;
    this.fileSchema = fileSchema;
    this.requestedSchema = requestedSchema;
    this.materializer = materializer;
    this.strictTypeChecking = strictTypeChecking;
    this.filterRecords = filterRecords;
    this.recordFilter = recordFilter;
    this.totalRows = totalRows;
  }

  /**
   * Takes ownership of {@code fileReader}. Projects columns via {@code readSupport}, applies the
   * row-group filter already configured on {@code fileReader}'s options, and prepares to iterate
   * rows.
   */
  static ParquetBatchRowReader open(
      ParquetFileReader fileReader, ParquetReadOptions options, ReadSupport<Object> readSupport) {
    requireNonNull(fileReader, "fileReader");
    requireNonNull(options, "options");
    requireNonNull(readSupport, "readSupport");
    // Mirror InternalParquetRecordReader: copy option properties onto the conf used by ReadSupport.
    ParquetConfiguration conf = requireNonNull(options.getConfiguration(), "options.configuration");
    for (String property : options.getPropertyNames()) {
      conf.set(property, options.getProperty(property));
    }
    FileMetaData parquetFileMetadata = fileReader.getFooter().getFileMetaData();
    MessageType fileSchema = parquetFileMetadata.getSchema();
    Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
    ReadSupport.ReadContext readContext =
        readSupport.init(new InitContext(conf, toSetMultiMap(fileMetadata), fileSchema));
    ColumnIOFactory columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
    MessageType requestedSchema = readContext.getRequestedSchema();
    // Setting the projection schema before running any filtering (e.g. getting filtered record
    // count) because projection impacts filtering
    fileReader.setRequestedSchema(requestedSchema);
    RecordMaterializer<Object> materializer =
        readSupport.prepareForRead(conf, fileMetadata, fileSchema, readContext);
    return new ParquetBatchRowReader(
        fileReader,
        columnIOFactory,
        fileSchema,
        requestedSchema,
        materializer,
        options.isEnabled(STRICT_TYPE_CHECKING, true),
        options.useRecordFilter(),
        options.getRecordFilter() == null ? FilterCompat.NOOP : options.getRecordFilter(),
        fileReader.getFilteredRecordCount());
  }

  /**
   * Advances to the next row. Returns {@code true} if a row is available; {@link
   * #currentRowIndex()} is then valid until the next call.
   *
   * <p>Materialized values are written into the {@link ReadSupport}'s {@link RecordMaterializer}
   * (Kernel's {@code BatchReadSupport} / {@code RowRecordCollector}). The returned boolean is the
   * only signal the iterator needs — there is no {@code getCurrentValue()}.
   */
  boolean next() throws IOException {
    while (rowsConsumed < totalRows) {
      loadNextRowGroupIfNeeded();
      rowsConsumed++;
      try {
        Object value = recordReader.read();
        advanceRowIndex();
        if (recordReader.shouldSkipCurrentRecord()) {
          // this record is being filtered via the filter2 package
          continue;
        }
        if (value == null) {
          // only happens with FilteredRecordReader at end of block
          rowsConsumed = rowsLoadedFromGroups;
          continue;
        }
        return true;
      } catch (RuntimeException e) {
        throw new ParquetDecodingException(
            String.format(
                "Can not read value at %d in file %s", rowsConsumed, fileReader.getFile()),
            e);
      }
    }
    return false;
  }

  /** File-level row index of the row made current by the last successful {@link #next()}. */
  long currentRowIndex() {
    return currentRowIndex;
  }

  @Override
  public void close() throws IOException {
    try {
      if (currentRowGroup != null) {
        currentRowGroup.close();
        currentRowGroup = null;
      }
    } finally {
      fileReader.close();
    }
  }

  private void loadNextRowGroupIfNeeded() throws IOException {
    if (rowsConsumed != rowsLoadedFromGroups) {
      return;
    }
    if (currentRowGroup != null) {
      currentRowGroup.close();
      currentRowGroup = null;
    }
    logger.info("at row " + rowsConsumed + ". reading next block");
    // Applies stats / dictionary / bloom / column-index filters from ParquetReadOptions.
    // (Plain readNextRowGroup() would ignore those.)
    currentRowGroup = fileReader.readNextFilteredRowGroup();
    if (currentRowGroup == null) {
      throw new IOException(
          "expecting more rows but reached last block. Read "
              + rowsConsumed
              + " out of "
              + totalRows);
    }
    resetRowIndexIterator(currentRowGroup);
    MessageColumnIO columnIO =
        columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
    recordReader =
        columnIO.getRecordReader(
            currentRowGroup, materializer, filterRecords ? recordFilter : FilterCompat.NOOP);
    rowsLoadedFromGroups += currentRowGroup.getRowCount();
  }

  /** Resets the row index based on the current processed row group. */
  private void resetRowIndexIterator(PageReadStore pages) {
    Optional<Long> rowGroupOffset = pages.getRowIndexOffset();
    if (!rowGroupOffset.isPresent()) {
      rowIndexInFile = null;
      currentRowIndex = -1;
      return;
    }
    currentRowIndex = -1;
    final PrimitiveIterator.OfLong rowIdxInRowGroup;
    if (pages.getRowIndexes().isPresent()) {
      rowIdxInRowGroup = pages.getRowIndexes().get();
    } else {
      rowIdxInRowGroup = LongStream.range(0, pages.getRowCount()).iterator();
    }
    final long offset = rowGroupOffset.get();
    rowIndexInFile =
        new PrimitiveIterator.OfLong() {
          @Override
          public long nextLong() {
            return offset + rowIdxInRowGroup.nextLong();
          }

          @Override
          public boolean hasNext() {
            return rowIdxInRowGroup.hasNext();
          }
        };
  }

  private void advanceRowIndex() {
    if (rowIndexInFile != null && rowIndexInFile.hasNext()) {
      currentRowIndex = rowIndexInFile.nextLong();
    } else {
      currentRowIndex = -1;
    }
  }

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> out = new HashMap<>();
    for (Map.Entry<K, V> e : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(e.getValue());
      out.put(e.getKey(), set);
    }
    return out;
  }
}
