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
package io.delta.kernel.defaults.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import static java.lang.String.format;

import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.*;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.Utils;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.defaults.internal.logstore.LogStoreProvider;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
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
            private final ParquetFileReader batchReader = new ParquetFileReader(hadoopConf);
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
                        String nextFile = fileIter.next().getPath();
                        currentFileReader = batchReader.read(nextFile, physicalSchema, predicate);
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
            List<Column> statsColumns) throws IOException {
        ParquetFileWriter batchWriter =
            new ParquetFileWriter(hadoopConf, new Path(directoryPath), statsColumns);
        return batchWriter.write(dataIter);
    }

    /**
     * Makes use of {@link LogStore} implementations in `delta-storage` to atomically write the data
     * to a file depending upon the destination filesystem.
     *
     * @param filePath Fully qualified destination file path
     * @param data     Iterator of {@link FilteredColumnarBatch}
     * @throws IOException
     */
    @Override
    public void writeParquetFileAtomically(
            String filePath,
            CloseableIterator<FilteredColumnarBatch> data) throws IOException {
        try {
            Path targetPath = new Path(filePath);
            LogStore logStore =
                    LogStoreProvider.getLogStore(hadoopConf, targetPath.toUri().getScheme());

            boolean useRename = logStore.isPartialWriteVisible(targetPath, hadoopConf);

            Path writePath = targetPath;
            if (useRename) {
                // In order to atomically write the file, write to a temp file and rename
                // to target path
                String tempFileName = format(".%s.%s.tmp", targetPath.getName(), UUID.randomUUID());
                writePath = new Path(targetPath.getParent(), tempFileName);
            }
            ParquetFileWriter fileWriter = new ParquetFileWriter(hadoopConf, writePath);

            Optional<DataFileStatus> writtenFile;

            try (CloseableIterator<DataFileStatus> statuses = fileWriter.write(data)) {
                writtenFile = InternalUtils.getSingularElement(statuses);
            } catch (UncheckedIOException uio) {
                throw uio.getCause();
            }

            checkState(writtenFile.isPresent(), "expected to write one output file");
            if (useRename) {
                FileSystem fs = targetPath.getFileSystem(hadoopConf);
                boolean renameDone = false;
                try {
                    renameDone = fs.rename(writePath, targetPath);
                    if (!renameDone) {
                        if (fs.exists(targetPath)) {
                            throw new java.nio.file.FileAlreadyExistsException(
                                    "target file already exists: " + targetPath);
                        }
                        throw new IOException("Failed to rename the file");
                    }
                } finally {
                    if (!renameDone) {
                        fs.delete(writePath, false /* recursive */);
                    }
                }
            }
        } finally {
            Utils.closeCloseables(data);
        }
    }
}
