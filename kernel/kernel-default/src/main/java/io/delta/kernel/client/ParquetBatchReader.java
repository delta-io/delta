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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetRecordReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ParquetBatchReader
{
    private final Configuration configuration;

    public ParquetBatchReader(Configuration configuration)
    {
        this.configuration = requireNonNull(configuration, "configuration is null");
    }

    public CloseableIterator<ColumnarBatch> read(String path, StructType schema) {
        ParquetRecordReader<Row> reader =
                new ParquetRecordReader<>(
                        new ParquetRowReader.RowReadSupport(schema),
                        FilterCompat.NOOP);

        Path filePath = new Path(path);
        try {
            FileSystem fs = filePath.getFileSystem(configuration);
            FileStatus fileStatus = fs.getFileStatus(filePath);
            reader.initialize(
                    new FileSplit(filePath, 0, fileStatus.getLen(), new String[0]),
                    configuration,
                    Reporter.NULL
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return new CloseableIterator<ColumnarBatch>() {
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
                    return reader.nextKeyValue();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public ColumnarBatch next()
            {
                try {
                    List<Row> rows = new ArrayList<>();
                    for (int i = 0; i < 1024; i++) {
                        rows.add(reader.getCurrentValue());
                        if (!hasNext()) {
                            break;
                        }
                    }
                    return new DefaultColumnarBatch(schema, rows);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
