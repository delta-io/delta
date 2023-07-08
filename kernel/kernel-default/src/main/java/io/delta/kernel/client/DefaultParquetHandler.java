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
package io.delta.kernel.client;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.parquet.ParquetBatchReader;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

public class DefaultParquetHandler extends DefaultFileHandler implements ParquetHandler
{
    private final Configuration hadoopConf;

    public DefaultParquetHandler(Configuration hadoopConf)
    {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public CloseableIterator<FileDataReadResult> readParquetFiles(
        CloseableIterator<FileReadContext> fileIter, StructType physicalSchema) throws IOException
    {
        return new CloseableIterator<FileDataReadResult>()
        {
            private final ParquetBatchReader batchReader = new ParquetBatchReader(hadoopConf);
            private FileReadContext currentFile;
            private CloseableIterator<ColumnarBatch> currentFileReader;

            @Override
            public void close() throws IOException
            {
                Utils.closeCloseables(currentFileReader, fileIter);
            }

            @Override
            public boolean hasNext()
            {
                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                if (currentFileReader == null || !currentFileReader.hasNext()) {
                    Utils.closeCloseables(currentFileReader);
                    currentFileReader = null;
                    if (fileIter.hasNext()) {
                        currentFile = fileIter.next();
                        FileStatus fileStatus = Utils.getFileStatus(currentFile.getScanFileRow());
                        currentFileReader = batchReader.read(fileStatus.getPath(), physicalSchema);
                    }
                    else {
                        return false;
                    }
                }

                return currentFileReader.hasNext();
            }

            @Override
            public FileDataReadResult next()
            {
                final ColumnarBatch data = currentFileReader.next();
                return new FileDataReadResult()
                {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return data;
                    }

                    @Override
                    public Row getScanFileRow()
                    {
                        return currentFile.getScanFileRow();
                    }
                };
            }
        };
    }
}
