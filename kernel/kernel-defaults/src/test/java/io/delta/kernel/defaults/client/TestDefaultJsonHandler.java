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

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;
import static io.delta.kernel.expressions.AlwaysTrue.ALWAYS_TRUE;

import io.delta.kernel.internal.InternalScanFile;

import io.delta.kernel.defaults.utils.DefaultKernelTestUtils;

public class TestDefaultJsonHandler {
    private static final JsonHandler JSON_HANDLER = new DefaultJsonHandler(new Configuration() {
        {
            set("delta.kernel.default.json.reader.batch-size", "1");
        }
    });
    private static final FileSystemClient FS_CLIENT =
        new DefaultFileSystemClient(new Configuration());

    @Test
    public void contextualizeFiles()
        throws Exception {
        try (CloseableIterator<Row> inputScanFiles = testFiles();
            CloseableIterator<FileReadContext> fileReadContexts =
                JSON_HANDLER.contextualizeFileReads(testFiles(), ALWAYS_TRUE)) {
            while (inputScanFiles.hasNext() || fileReadContexts.hasNext()) {
                assertEquals(inputScanFiles.hasNext(), fileReadContexts.hasNext());
                Row inputScanFile = inputScanFiles.next();
                FileReadContext outputScanContext = fileReadContexts.next();
                compareScanFileRows(inputScanFile, outputScanContext.getScanFileRow());
            }
        }
    }

    @Test
    public void readJsonFiles()
        throws Exception {
        try (
            CloseableIterator<FileDataReadResult> data =
                JSON_HANDLER.readJsonFiles(
                    JSON_HANDLER.contextualizeFileReads(testFiles(), ALWAYS_TRUE),
                    new StructType()
                        .add("path", StringType.INSTANCE)
                        .add("size", LongType.INSTANCE)
                        .add("dataChange", BooleanType.INSTANCE))) {

            List<String> actPaths = new ArrayList<>();
            List<Long> actSizes = new ArrayList<>();
            List<Boolean> actDataChanges = new ArrayList<>();
            while (data.hasNext() && data.hasNext()) {
                ColumnarBatch dataBatch = data.next().getData();
                try (CloseableIterator<Row> dataBatchRows = dataBatch.getRows()) {
                    while (dataBatchRows.hasNext()) {
                        Row row = dataBatchRows.next();
                        actPaths.add(row.getString(0));
                        actSizes.add(row.getLong(1));
                        actDataChanges.add(row.getBoolean(2));
                    }
                }
            }

            List<String> expPaths = Arrays.asList(
                "part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet",
                "part-00000-cb078bc1-0aeb-46ed-9cf8-74a843b32c8c-c000.snappy.parquet",
                "part-00001-9bf4b8f8-1b95-411b-bf10-28dc03aa9d2f-c000.snappy.parquet",
                "part-00000-0441e99a-c421-400e-83a1-212aa6c84c73-c000.snappy.parquet",
                "part-00001-34c8c673-3f44-4fa7-b94e-07357ec28a7d-c000.snappy.parquet",
                "part-00000-842017c2-3e02-44b5-a3d6-5b9ae1745045-c000.snappy.parquet",
                "part-00001-e62ca5a1-923c-4ee6-998b-c61d1cfb0b1c-c000.snappy.parquet"
            );
            List<Long> expSizes = Arrays.asList(348L, 687L, 705L, 650L, 650L, 649L, 649L);
            List<Boolean> expDataChanges = Arrays.asList(true, true, true, true, true, true, true);

            assertEquals(expPaths, actPaths);
            assertEquals(expSizes, actSizes);
            assertEquals(actDataChanges, expDataChanges);
        }
    }

    @Test
    public void parseJsonContent()
        throws Exception {
        String input =
            "{" +
                "   \"path\":" +
                "\"part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet\", " +
                "   \"partitionValues\":{\"p1\" : \"0\", \"p2\" : \"str\"}," +
                "   \"size\":348," +
                "   \"modificationTime\":1603723974000, " +
                "   \"dataChange\":true" +
                "   }";
        StructType readSchema = new StructType()
            .add("path", StringType.INSTANCE)
            .add("partitionValues",
                new MapType(StringType.INSTANCE, StringType.INSTANCE, false))
            .add("size", LongType.INSTANCE)
            .add("dataChange", BooleanType.INSTANCE);

        ColumnarBatch batch =
            JSON_HANDLER.parseJson(Utils.singletonColumnVector(input), readSchema);
        assertEquals(1, batch.getSize());

        try (CloseableIterator<Row> rows = batch.getRows()) {
            Row row = rows.next();
            assertEquals(
                "part-00000-d83dafd8-c344-49f0-ab1c-acd944e32493-c000.snappy.parquet",
                row.getString(0)
            );

            Map<String, String> expPartitionValues = new HashMap<String, String>() {
                {
                    put("p1", "0");
                    put("p2", "str");
                }
            };
            assertEquals(expPartitionValues, row.getMap(1));
            assertEquals(348L, row.getLong(2));
            assertEquals(true, row.getBoolean(3));
        }
    }

    private static CloseableIterator<Row> testFiles()
        throws Exception {
        String listFrom = DefaultKernelTestUtils.getTestResourceFilePath("json-files/1.json");
        CloseableIterator<FileStatus> list = FS_CLIENT.listFrom(listFrom);
        return list.map(fileStatus -> InternalScanFile.generateScanFileRow(fileStatus));
    }

    private static void compareScanFileRows(Row expected, Row actual) {
        // basically compare the paths
        assertEquals(expected.getStruct(0).getString(0), actual.getStruct(0).getString(0));
    }
}
