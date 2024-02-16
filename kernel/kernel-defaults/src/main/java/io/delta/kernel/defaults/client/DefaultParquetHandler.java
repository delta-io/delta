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
package io.delta.kernel.defaults.client;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.data.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.*;

import io.delta.kernel.internal.util.Utils;

import io.delta.kernel.defaults.internal.parquet.ParquetBatchReader;
import io.delta.kernel.defaults.internal.parquet.ParquetFileWriter;

/**
 * Default implementation of {@link ParquetHandler} based on Hadoop APIs.
 */
public class DefaultParquetHandler implements ParquetHandler {
    private final Configuration hadoopConf;

    /**
     * Create an instance of default {@link ParquetHandler} implementation.
     *
     * @param hadoopConf Hadoop configuration to use.
     */
    public DefaultParquetHandler(Configuration hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> fileIter,
            StructType physicalSchema,
            Optional<Predicate> predicate) throws IOException {
        return new CloseableIterator<ColumnarBatch>() {
            private final ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
            private FileStatus currentFile;
            private CloseableIterator<ColumnarBatch> currentFileReader;

            @Override
            public void close() throws IOException {
                Utils.closeCloseables(currentFileReader, fileIter);
            }

            @Override
            public boolean hasNext() {
                if (currentFileReader != null && currentFileReader.hasNext()) {
                    return true;
                } else {
                    // There is no file in reading or the current file being read has no more data.
                    // Initialize the next file reader or return false if there are no more files to
                    // read.
                    Utils.closeCloseables(currentFileReader);
                    currentFileReader = null;
                    if (fileIter.hasNext()) {
                        currentFile = fileIter.next();
                        currentFileReader = batchReader.read(currentFile.getPath(), physicalSchema);
                        return hasNext(); // recurse since it's possible the loaded file is empty
                    } else {
                        return false;
                    }
                }
            }

            @Override
            public ColumnarBatch next() {
                return currentFileReader.next();
            }
        };
    }

    @Override
    public CloseableIterator<DataFileStatus> writeParquetFiles(
            String directoryPath,
            CloseableIterator<FilteredColumnarBatch> dataIter,
            long maxFileSize,
            List<Column> statsColumns) throws IOException {
        ParquetFileWriter batchWriter =
            new ParquetFileWriter(hadoopConf, new Path(directoryPath), maxFileSize, statsColumns);
        return batchWriter.write(dataIter);
    }
}
