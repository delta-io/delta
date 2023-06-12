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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.JsonRow;
import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DefaultJsonHandler
    implements JsonHandler
{
    private final ObjectMapper objectMapper;
    private final Configuration hadoopConf;

    public DefaultJsonHandler(Configuration hadoopConf)
    {
        this.objectMapper = new ObjectMapper();
        this.hadoopConf = hadoopConf;
    }

    @Override
    public CloseableIterator<FileReadContext> contextualizeFileReads(
            CloseableIterator<Row> fileIter,
            Expression predicate)
    {
        return new CloseableIterator<FileReadContext>() {
            @Override
            public void close()
                    throws IOException
            {
                fileIter.close();
            }

            @Override
            public boolean hasNext()
            {
                return fileIter.hasNext();
            }

            @Override
            public FileReadContext next()
            {
                return () -> fileIter.next();
            }
        };
    }

    @Override
    public ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema)
    {
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < jsonStringVector.getSize(); i++) {
            rows.add(parseJson(jsonStringVector.getString(i), outputSchema));
        }
        return new DefaultRowBasedColumnarBatch(outputSchema, rows);
    }

    @Override
    public CloseableIterator<FileDataReadResult> readJsonFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema) throws IOException
    {
        return new CloseableIterator<FileDataReadResult>() {
            private FileReadContext currentFile;
            private BufferedReader currentFileReader;
            private String nextLine;

            @Override
            public void close()
                    throws IOException
            {
                if (currentFileReader != null) {
                    currentFileReader.close();
                }

                fileIter.close();
                // TODO: implement safe close of multiple closeables.
            }

            @Override
            public boolean hasNext()
            {
                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    if (currentFileReader == null ||
                            (nextLine = currentFileReader.readLine()) == null) {
                        if (fileIter.hasNext()) {
                            currentFile = fileIter.next();
                            FileStatus fileStatus = io.delta.kernel.utils.Utils.getFileStatus(
                                    currentFile.getScanFileRow());
                            Path filePath = new Path(fileStatus.getPath());
                            FileSystem fs = filePath.getFileSystem(hadoopConf);
                            FSDataInputStream stream = fs.open(filePath);
                            currentFileReader = new BufferedReader(
                                    new InputStreamReader(stream, StandardCharsets.UTF_8));
                            nextLine = currentFileReader.readLine();
                        } else {
                            return false;
                        }
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                return nextLine != null;
            }

            @Override
            public FileDataReadResult next()
            {
                List<Row> rows = new ArrayList<>();
                int i = 0;
                do {
                    // hasNext already reads the next one and keeps it in member variable `nextLine`
                    rows.add(parseJson(nextLine, physicalSchema));
                    try {
                        nextLine = currentFileReader.readLine();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                    // TODO: decide on the batch size
                } while(i < 1024 && nextLine != null);
                ColumnarBatch nextBatch = new DefaultRowBasedColumnarBatch(physicalSchema, rows);

                return new FileDataReadResult() {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return nextBatch;
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

    private Row parseJson(String json, StructType readSchema)
    {
        try {
            final JsonNode jsonNode = objectMapper.readTree(json);
            return new JsonRow((ObjectNode) jsonNode, readSchema);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }
}
