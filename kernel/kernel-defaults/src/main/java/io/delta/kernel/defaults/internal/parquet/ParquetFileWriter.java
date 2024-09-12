/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import static io.delta.kernel.defaults.internal.DefaultKernelUtils.getDataType;
import static io.delta.kernel.defaults.internal.parquet.ParquetStatsReader.readDataFileStatistics;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.ParquetOutputFormat.*;

import com.sun.corba.se.impl.io.TypeMismatchException;
import io.delta.kernel.Meta;
import io.delta.kernel.data.*;
import io.delta.kernel.defaults.internal.parquet.ParquetColumnWriters.ColumnWriter;
import io.delta.kernel.expressions.CollationIdentifier;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.*;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/**
 * Implements writing data given as {@link FilteredColumnarBatch} to Parquet files.
 *
 * <p>It makes use of the `parquet-mr` library to write the data in Parquet format. The main class
 * used is {@link ParquetWriter} which is used to write the data row by row to the Parquet file.
 * Supporting interface for this writer is {@link WriteSupport} (in this writer implementation, it
 * is {@link BatchWriteSupport}). {@link BatchWriteSupport}, on call back from {@link
 * ParquetWriter}, reads the contents of {@link ColumnarBatch} and passes the contents to {@link
 * ParquetWriter} through {@link RecordConsumer}.
 */
public class ParquetFileWriter {
  public static final String TARGET_FILE_SIZE_CONF =
      "delta.kernel.default.parquet.writer.targetMaxFileSize";
  public static final long DEFAULT_TARGET_FILE_SIZE = 128 * 1024 * 1024; // 128MB

  private final Configuration configuration;
  private final boolean writeAsSingleFile;
  private final Path location;
  private final long targetMaxFileSize;
  private final List<Column> statsColumns;

  private long currentFileNumber; // used to generate the unique file names.

  /**
   * Create writer to write data into one or more files depending upon the {@code
   * delta.kernel.default.parquet.writer.targetMaxFileSize} value and the given data.
   */
  public ParquetFileWriter(Configuration configuration, Path location, List<Column> statsColumns) {
    this.configuration = requireNonNull(configuration, "configuration is null");
    this.location = requireNonNull(location, "directory is null");
    // Default target file size is 128 MB.
    this.targetMaxFileSize = configuration.getLong(TARGET_FILE_SIZE_CONF, DEFAULT_TARGET_FILE_SIZE);
    checkArgument(targetMaxFileSize > 0, "Invalid target Parquet file size: " + targetMaxFileSize);
    this.statsColumns = requireNonNull(statsColumns, "statsColumns is null");
    this.writeAsSingleFile = false;
  }

  /** Create writer to write the data exactly into one file. */
  public ParquetFileWriter(Configuration configuration, Path destPath) {
    this.configuration = requireNonNull(configuration, "configuration is null");
    this.writeAsSingleFile = true;
    this.location = requireNonNull(destPath, "destPath is null");
    this.targetMaxFileSize = Long.MAX_VALUE;
    this.statsColumns = Collections.emptyList();
  }

  /**
   * Write the given data to Parquet files.
   *
   * @param dataIter Iterator of data to write.
   * @return an iterator of {@link DataFileStatus} where each entry contains the metadata of the
   *     data file written. It is the responsibility of the caller to close the iterator.
   */
  public CloseableIterator<DataFileStatus> write(
      CloseableIterator<FilteredColumnarBatch> dataIter) {
    return new CloseableIterator<DataFileStatus>() {
      // Last written file output.
      private Optional<DataFileStatus> lastWrittenFileOutput = Optional.empty();

      // Current batch of data that is being written, updated in {@link #hasNextRow()}.
      private FilteredColumnarBatch currentBatch = null;

      // Which record in the `currentBatch` is being written,
      // initialized in {@link #hasNextRow()} and updated in {@link #consumeNextRow}.
      private int currentBatchCursor = 0;

      // BatchWriteSupport is initialized when the first batch is read and reused for
      // subsequent batches with the same schema. `ParquetWriter` can use this write support
      // to consume data from `ColumnarBatch` and write it to Parquet files.
      private BatchWriteSupport batchWriteSupport = null;

      private StructType dataSchema = null;

      @Override
      public void close() {
        Utils.closeCloseables(dataIter);
      }

      @Override
      public boolean hasNext() {
        if (lastWrittenFileOutput.isPresent()) {
          return true;
        }
        lastWrittenFileOutput = writeNextFile();
        return lastWrittenFileOutput.isPresent();
      }

      @Override
      public DataFileStatus next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        DataFileStatus toReturn = lastWrittenFileOutput.get();
        lastWrittenFileOutput = Optional.empty();
        return toReturn;
      }

      private Optional<DataFileStatus> writeNextFile() {
        if (!hasNextRow()) {
          return Optional.empty();
        }

        Path filePath = generateNextFilePath();
        assert batchWriteSupport != null : "batchWriteSupport is not initialized";
        long currentFileRowCount = 0; // tracks the number of rows written to the current file
        try (ParquetWriter<Integer> writer = createWriter(filePath, batchWriteSupport)) {
          boolean maxFileSizeReached;
          do {
            if (consumeNextRow(writer)) {
              // If the row was written, increment the row count
              currentFileRowCount++;
            }
            // If we are writing a single file, then don't need to check for the current
            // file size. Otherwise see if the current file size reached the target file
            // size.
            maxFileSizeReached = !writeAsSingleFile && writer.getDataSize() >= targetMaxFileSize;
            // Keep writing until max file is reached or no more data to write
          } while (!maxFileSizeReached && hasNextRow());
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to write the Parquet file: " + filePath, e);
        }

        return Optional.of(
            constructDataFileStatus(filePath.toString(), dataSchema, currentFileRowCount, batchWriteSupport));
      }

      /**
       * Returns true if there is data to write.
       *
       * <p>Internally it traverses the rows in one batch after the other. Whenever a batch is fully
       * consumed, moves to the next input batch and updates the column writers in
       * `batchWriteSupport`.
       */
      boolean hasNextRow() {
        boolean hasNextRowInCurrentBatch =
            currentBatch != null
                &&
                // Is current batch is fully read?
                currentBatchCursor < currentBatch.getData().getSize();

        if (hasNextRowInCurrentBatch) {
          return true;
        }

        // loop until we find a non-empty batch or there are no more batches
        do {
          if (!dataIter.hasNext()) {
            return false;
          }
          currentBatch = dataIter.next();
          currentBatchCursor = 0;
        } while (currentBatch.getData().getSize() == 0); // skip empty batches

        // Initialize the batch support and create writers for each column
        ColumnarBatch inputBatch = currentBatch.getData();
        dataSchema = inputBatch.getSchema();
        BatchWriteSupport writeSupport = createOrGetWriteSupport(dataSchema);

        ColumnWriter[] columnWriters = ParquetColumnWriters.createColumnVectorWriters(inputBatch);

        writeSupport.setColumnVectorWriters(columnWriters);

        return true;
      }

      /**
       * Consume the next row of data to write. If the row is selected, write it. Otherwise, skip
       * it. At the end move the cursor to the next row.
       *
       * @return true if the row was written, false if it was skipped
       */
      boolean consumeNextRow(ParquetWriter<Integer> writer) throws IOException {
        Optional<ColumnVector> selectionVector = currentBatch.getSelectionVector();
        boolean isRowSelected =
            !selectionVector.isPresent()
                || (!selectionVector.get().isNullAt(currentBatchCursor)
                    && selectionVector.get().getBoolean(currentBatchCursor));

        if (isRowSelected) {
          writer.write(currentBatchCursor);
        }
        currentBatchCursor++;
        return isRowSelected;
      }

      /**
       * Create a {@link BatchWriteSupport} if it does not exist or return the existing one for
       * given schema.
       */
      BatchWriteSupport createOrGetWriteSupport(StructType inputSchema) {
        if (batchWriteSupport == null) {
          MessageType parquetSchema = ParquetSchemaUtils.toParquetSchema(inputSchema);
          batchWriteSupport = new BatchWriteSupport(inputSchema, parquetSchema);
          return batchWriteSupport;
        }
        // Ensure the new input schema matches the one used to create the write support
        if (!batchWriteSupport.inputSchema.equals(inputSchema)) {
          throw new IllegalArgumentException(
              "Input data has columnar batches with "
                  + "different schemas:\n schema 1: "
                  + batchWriteSupport.inputSchema
                  + "\n schema 2: "
                  + inputSchema);
        }
        return batchWriteSupport;
      }
    };
  }

  /**
   * Implementation of {@link WriteSupport} to write the {@link ColumnarBatch} to Parquet files.
   * {@link ParquetWriter} makes use of this interface to consume the data row by row and write to
   * the Parquet file. Call backs from the {@link ParquetWriter} includes: - {@link
   * #init(Configuration)}: Called once to init and get {@link WriteContext} which includes the
   * schema and extra properties. - {@link #prepareForWrite(RecordConsumer)}: Called once to prepare
   * for writing the data. {@link RecordConsumer} is a way for this batch support to write data for
   * each column in the current row. - {@link #write(Integer)}: Called for each row to write the
   * data. In this method, column values are passed to the {@link RecordConsumer} through series of
   * calls.
   */
  private static class BatchWriteSupport extends WriteSupport<Integer> {
    final StructType inputSchema;
    final MessageType parquetSchema;

    private ColumnWriter[] columnWriters;
    private RecordConsumer recordConsumer;

    BatchWriteSupport(
        StructType inputSchema, // WriteSupport created for this specific schema
        MessageType parquetSchema) { // Parquet equivalent schema
      this.inputSchema = requireNonNull(inputSchema, "inputSchema is null");
      this.parquetSchema = requireNonNull(parquetSchema, "parquetSchema is null");
    }

    void setColumnVectorWriters(ColumnWriter[] columnWriters) {
      this.columnWriters = requireNonNull(columnWriters, "columnVectorWriters is null");
    }

    @Override
    public String getName() {
      return "delta-kernel-default-parquet-writer";
    }

    @Override
    public WriteContext init(Configuration configuration) {
      Map<String, String> extraProps =
          Collections.singletonMap(
              "io.delta.kernel.default-parquet-writer", "Kernel-Defaults-" + Meta.KERNEL_VERSION);
      return new WriteContext(parquetSchema, extraProps);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
      this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Integer rowId) {
      // Use java asserts which are disabled in prod to reduce the overhead
      // and enabled in tests with `-ea` argument.
      assert (recordConsumer != null) : "Parquet record consumer is null";
      assert (columnWriters != null) : "Column writers are not set";
      recordConsumer.startMessage();
      for (int i = 0; i < columnWriters.length; i++) {
        columnWriters[i].writeRowValue(recordConsumer, rowId);
      }
      recordConsumer.endMessage();
    }

    public ColumnWriter[] getColumnWriters() {
      return columnWriters;
    }
  }

  /** Generate the next file path to write the data. */
  private Path generateNextFilePath() {
    if (writeAsSingleFile) {
      checkArgument(currentFileNumber++ == 0, "expected to write just one file");
      return location;
    }
    String fileName = String.format("%s-%03d.parquet", UUID.randomUUID(), currentFileNumber++);
    return new Path(location, fileName);
  }

  /**
   * Helper method to create {@link ParquetWriter} for given file path and write support. It makes
   * use of configuration options in `configuration` to configure the writer. Different available
   * configuration options are defined in {@link ParquetOutputFormat}.
   */
  private ParquetWriter<Integer> createWriter(Path filePath, WriteSupport<Integer> writeSupport)
      throws IOException {
    return new ParquetRowDataBuilder(filePath, writeSupport)
        .withCompressionCodec(
            CompressionCodecName.fromConf(
                configuration.get(
                    ParquetOutputFormat.COMPRESSION, CompressionCodecName.SNAPPY.name())))
        .withRowGroupSize(getLongBlockSize(configuration))
        .withPageSize(getPageSize(configuration))
        .withDictionaryPageSize(getDictionaryPageSize(configuration))
        .withMaxPaddingSize(
            configuration.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
        .withDictionaryEncoding(getEnableDictionary(configuration))
        .withValidation(getValidation(configuration))
        .withWriterVersion(getWriterVersion(configuration))
        .withConf(configuration)
        .build();
  }

  private static class ParquetRowDataBuilder
      extends ParquetWriter.Builder<Integer, ParquetRowDataBuilder> {
    private final WriteSupport<Integer> writeSupport;

    protected ParquetRowDataBuilder(Path path, WriteSupport<Integer> writeSupport) {
      super(path);
      this.writeSupport = requireNonNull(writeSupport, "writeSupport is null");
    }

    @Override
    protected ParquetRowDataBuilder self() {
      return this;
    }

    @Override
    protected WriteSupport<Integer> getWriteSupport(Configuration conf) {
      return writeSupport;
    }
  }

  /**
   * Construct the {@link DataFileStatus} for the given file path. It reads the file status and
   * Parquet footer to compute the statistics for the file.
   *
   * <p>Potential improvement in future to directly compute the statistics while writing the file if
   * this becomes a sufficiently large part of the write operation time.
   *
   * @param path the path of the file
   * @param dataSchema the schema of the data in the file
   * @param numRows the number of rows in the file. If no column stats are required, this is used to
   *     construct the {@link DataFileStatistics}. Otherwise, the stats are read from the file.
   * @return the {@link DataFileStatus} for the file
   */
  private DataFileStatus constructDataFileStatus(String path, StructType dataSchema, long numRows, BatchWriteSupport batchWriteSupport) {
    try {
      // Get the FileStatus to figure out the file size and modification time
      Path hadoopPath = new Path(path);
      FileStatus fileStatus = hadoopPath.getFileSystem(configuration).getFileStatus(hadoopPath);
      Path resolvedPath = fileStatus.getPath();

      DataFileStatistics stats;
      if (statsColumns.isEmpty()) {
        stats =
            new DataFileStatistics(
                numRows,
                emptyMap() /* minValues */,
                emptyMap() /* maxValues */,
                emptyMap() /* nullCounts */);
      } else {
        stats = appendCollatedStatistics(
                batchWriteSupport,
                dataSchema,
                readDataFileStatistics(resolvedPath, configuration, dataSchema, statsColumns));
      }

      return new DataFileStatus(
          resolvedPath.toString(),
          fileStatus.getLen(),
          fileStatus.getModificationTime(),
          Optional.ofNullable(stats));
    } catch (IOException ioe) {
      throw new UncheckedIOException("Failed to read the stats for: " + path, ioe);
    }
  }

  private DataFileStatistics appendCollatedStatistics(BatchWriteSupport batchWriteSupport,
                                                      StructType dataSchema,
                                                      DataFileStatistics stats) {
    Map<CollationIdentifier, Map<Column, Literal>> collatedMinValues = new HashMap<>();
    Map<CollationIdentifier, Map<Column, Literal>> collatedMaxValues = new HashMap<>();
    for (Column column : statsColumns) {
      DataType dataType = getDataType(dataSchema, column);
      if (dataType instanceof StringType &&
              ((StringType) dataType).getCollationIdentifier().equals(CollationIdentifier.DEFAULT_COLLATION_IDENTIFIER)) {
        StringType stringType = (StringType) dataType;

        Tuple2<Literal, Literal> collatedMinMax = collectCollatedMinMax(column, batchWriteSupport);
        collatedMinValues.getOrDefault(stringType.getCollationIdentifier(), emptyMap()).put(column, collatedMinMax._1);
        collatedMaxValues.getOrDefault(stringType.getCollationIdentifier(), emptyMap()).put(column, collatedMinMax._2);
      }
    }

    return new DataFileStatistics(
          stats.getNumRecords(),
          stats.getMinValues(),
          stats.getMaxValues(),
          stats.getNullCounts(),
          collatedMinValues,
          collatedMaxValues);
  }

  private Tuple2<Literal, Literal> collectCollatedMinMax(Column column, BatchWriteSupport batchWriteSupport) {
    String[] parts = column.getNames();
    ColumnWriter[] columnWriters = batchWriteSupport.getColumnWriters();
    ParquetColumnWriters.CollatedStringWriter collatedStringWriter = getNestedCollatedStringWriter(columnWriters, parts);
    CollationIdentifier collationIdentifier = collatedStringWriter.getCollationIdentifier();

    return new Tuple2<>(Literal.ofString(collatedStringWriter.getMinValue(), collationIdentifier),
            Literal.ofString(collatedStringWriter.getMaxValue(), collationIdentifier));
  }

  private ParquetColumnWriters.CollatedStringWriter getNestedCollatedStringWriter(ColumnWriter[] columnWriters, String[] parts) {
    ColumnWriter[] startingColumnWriters = Arrays.stream(columnWriters).
            filter(columnWriter -> columnWriter.colName.equals(parts[0]))
            .toArray(ColumnWriter[]::new);
    if (startingColumnWriters.length != 1) {
      throw new IllegalArgumentException(String.format("Invalid column name: %s.", parts[0]));
    }

    ColumnWriter columnWriter = startingColumnWriters[0];
    for (int i = 1; i < parts.length; i++) {
      columnWriter = columnWriter.getNestedColumnWriter(parts[i]);
    }

    if (!(columnWriter instanceof ParquetColumnWriters.CollatedStringWriter)) {
      throw new TypeMismatchException(String.format(
              "Invalid column writer type: %s. CollatedStringWriter expected. ", columnWriter.getClass().getName()));
    }
    return (ParquetColumnWriters.CollatedStringWriter) columnWriter;
  }
}
