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
import static io.delta.kernel.DefaultKernelUtils.checkArgument;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DefaultJsonRow;
import io.delta.kernel.data.DefaultRowBasedColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;

public class DefaultJsonHandler
    extends DefaultFileHandler
    implements JsonHandler
{
    private final ObjectMapper objectMapper;
    private final Configuration hadoopConf;
    private final int maxBatchSize;

    public DefaultJsonHandler(Configuration hadoopConf)
    {
        this.objectMapper = new ObjectMapper();
        this.hadoopConf = hadoopConf;
        this.maxBatchSize =
            hadoopConf.getInt("delta.kernel.default.json.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid JSON reader batch size: " + maxBatchSize);
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
        return new CloseableIterator<FileDataReadResult>()
        {
            private FileReadContext currentFile;
            private BufferedReader currentFileReader;
            private String nextLine;

            @Override
            public void close()
                throws IOException
            {
                Utils.closeCloseables(currentFileReader, fileIter);
            }

            @Override
            public boolean hasNext()
            {
                if (nextLine != null) {
                    return true; // we have un-consumed last read line
                }

                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    if (currentFileReader == null ||
                        (nextLine = currentFileReader.readLine()) == null) {

                        tryOpenNextFile();
                        if (currentFileReader != null) {
                            nextLine = currentFileReader.readLine();
                        }
                    }
                }
                catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                return nextLine != null;
            }

            @Override
            public FileDataReadResult next()
            {
                if (nextLine == null) {
                    throw new NoSuchElementException();
                }

                List<Row> rows = new ArrayList<>();
                int currentBatchSize = 0;
                do {
                    // hasNext already reads the next one and keeps it in member variable `nextLine`
                    rows.add(parseJson(nextLine, physicalSchema));
                    nextLine = null;
                    currentBatchSize++;
                }
                while (currentBatchSize < maxBatchSize && hasNext());

                ColumnarBatch batch = new DefaultRowBasedColumnarBatch(physicalSchema, rows);
                Row scanFileRow = currentFile.getScanFileRow();
                return new FileDataReadResult()
                {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return batch;
                    }

                    @Override
                    public Row getScanFileRow()
                    {
                        return scanFileRow;
                    }
                };
            }

            private void tryOpenNextFile()
                throws IOException
            {
                Utils.closeCloseables(currentFileReader); // close the current opened file
                currentFileReader = null;

                if (fileIter.hasNext()) {
                    currentFile = fileIter.next();
                    FileStatus fileStatus =
                        Utils.getFileStatus(currentFile.getScanFileRow());
                    Path filePath = new Path(fileStatus.getPath());
                    FileSystem fs = filePath.getFileSystem(hadoopConf);
                    FSDataInputStream stream = null;
                    try {
                        stream = fs.open(filePath);
                        currentFileReader = new BufferedReader(
                            new InputStreamReader(stream, StandardCharsets.UTF_8));
                    }
                    catch (Exception e) {
                        Utils.closeCloseablesSilently(stream); // close it avoid leaking resources
                        throw e;
                    }
                }
            }
        };
    }

    private Row parseJson(String json, StructType readSchema)
    {
        try {
            final JsonNode jsonNode = objectMapper.readTree(json);
            return new DefaultJsonRow((ObjectNode) jsonNode, readSchema);
        }
        catch (JsonProcessingException ex) {
            throw new RuntimeException(String.format("Could not parse JSON: %s", json), ex);
        }
    }
}
