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
package io.delta.kernel.parquet;

import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;
import static java.util.Objects.requireNonNull;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import io.delta.kernel.DefaultKernelUtils;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

public class ParquetBatchReader
{
    private final Configuration configuration;
    private final int maxBatchSize;

    public ParquetBatchReader(Configuration configuration)
    {
        this.configuration = requireNonNull(configuration, "configuration is null");
        this.maxBatchSize =
            configuration.getInt("delta.kernel.default.parquet.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid Parquet reader batch size: " + maxBatchSize);
    }

    public CloseableIterator<ColumnarBatch> read(String path, StructType schema)
    {
        BatchReadSupport batchReadSupport = new BatchReadSupport(maxBatchSize, schema);
        ParquetRecordReader<Object> reader = new ParquetRecordReader<>(batchReadSupport);

        Path filePath = new Path(URI.create(path));
        try {
            FileSystem fs = filePath.getFileSystem(configuration);
            FileStatus fileStatus = fs.getFileStatus(filePath);
            reader.initialize(
                new FileSplit(filePath, 0, fileStatus.getLen(), new String[0]),
                configuration,
                Reporter.NULL
            );
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new CloseableIterator<ColumnarBatch>()
        {
            private boolean hasNotConsumedNextElement;

            @Override
            public void close()
                throws IOException
            {
                reader.close();
            }

            @Override
            public boolean hasNext()
            {
                try {
                    if (hasNotConsumedNextElement) {
                        return true;
                    }
                    hasNotConsumedNextElement = reader.nextKeyValue();
                    return hasNotConsumedNextElement;
                }
                catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public ColumnarBatch next()
            {
                if (!hasNotConsumedNextElement) {
                    throw new NoSuchElementException();
                }
                int batchSize = 0;
                do {
                    hasNotConsumedNextElement = false;
                    // hasNext reads to row to confirm there is a next element.
                    try {
                        batchReadSupport.moveToNextRow(reader.getCurrentRowIndex());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    batchSize++;
                }
                while (batchSize < maxBatchSize && hasNext());

                return batchReadSupport.getDataAsColumnarBatch(batchSize);
            }
        };
    }

    /**
     * Implement a {@link ReadSupport} that will collect the data for each row and return
     * as a {@link ColumnarBatch}.
     */
    public static class BatchReadSupport
        extends ReadSupport<Object>
    {
        private final int maxBatchSize;
        private final StructType readSchema;
        private RowRecordCollector rowRecordCollector;

        public BatchReadSupport(int maxBatchSize, StructType readSchema)
        {
            this.maxBatchSize = maxBatchSize;
            this.readSchema = requireNonNull(readSchema, "readSchema is not null");
        }

        @Override
        public ReadContext init(InitContext context)
        {
            return new ReadContext(
                DefaultKernelUtils.pruneSchema(context.getFileSchema(), readSchema));
        }

        @Override
        public RecordMaterializer<Object> prepareForRead(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema,
            ReadContext readContext)
        {
            rowRecordCollector = new RowRecordCollector(maxBatchSize, readSchema, fileSchema);
            return rowRecordCollector;
        }

        public ColumnarBatch getDataAsColumnarBatch(int batchSize)
        {
            return rowRecordCollector.getDataAsColumnarBatch(batchSize);
        }

        /**
         * @param fileRowIndex the file row index of the row just processed
         */
        public void moveToNextRow(long fileRowIndex)
        {
            rowRecordCollector.moveToNextRow(fileRowIndex);
        }
    }

    /**
     * Collects the records given by the Parquet reader as columnar data. Parquet reader allows
     * reading data row by row, but {@link ParquetBatchReader} wants to expose the data as a
     * columnar batch. Parquet reader takes an implementation of {@link RecordMaterializer}
     * to which it gives data for each column one row a time. This {@link RecordMaterializer}
     * implementation collects the column values for multiple rows and returns a
     * {@link ColumnarBatch} at the end.
     */
    public static class RowRecordCollector
        extends RecordMaterializer<Object>
    {
        private static final Object FAKE_ROW_RECORD = new Object();
        private final RowConverter rowRecordGroupConverter;

        public RowRecordCollector(int maxBatchSize, StructType readSchema, MessageType fileSchema)
        {
            this.rowRecordGroupConverter =
                new RowConverter(maxBatchSize, readSchema, fileSchema);
        }

        @Override
        public void skipCurrentRecord()
        {
            super.skipCurrentRecord();
        }

        /**
         * Return a fake object. This is not used by {@link ParquetBatchReader}, instead
         * {@link #getDataAsColumnarBatch}} once a sufficient number of rows are collected.
         */
        @Override
        public Object getCurrentRecord()
        {
            return FAKE_ROW_RECORD;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return rowRecordGroupConverter;
        }

        /**
         * Return the data collected so far as a {@link ColumnarBatch}.
         */
        public ColumnarBatch getDataAsColumnarBatch(int batchSize)
        {
            return rowRecordGroupConverter.getDataAsColumnarBatch(batchSize);
        }

        /**
         * @param fileRowIndex the file row index of the row just processed
         */
        public void moveToNextRow(long fileRowIndex)
        {
            rowRecordGroupConverter.moveToNextRow(fileRowIndex);
        }
    }
}
