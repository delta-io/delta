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
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.delta.kernel.client.FileSystemClient;
import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.util.VectorUtils;
import static io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector;

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
    public void readJsonFiles()
        throws Exception {
        try (
            CloseableIterator<ColumnarBatch> data =
                JSON_HANDLER.readJsonFiles(
                    testFiles(),
                    new StructType()
                        .add("path", StringType.STRING)
                        .add("size", LongType.LONG)
                        .add("dataChange", BooleanType.BOOLEAN),
                    Optional.empty())) {

            List<String> actPaths = new ArrayList<>();
            List<Long> actSizes = new ArrayList<>();
            List<Boolean> actDataChanges = new ArrayList<>();
            while (data.hasNext() && data.hasNext()) {
                ColumnarBatch dataBatch = data.next();
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
            .add("path", StringType.STRING)
            .add("partitionValues",
                new MapType(StringType.STRING, StringType.STRING, false))
            .add("size", LongType.LONG)
            .add("dataChange", BooleanType.BOOLEAN);

        ColumnarBatch batch =
            JSON_HANDLER.parseJson(singletonStringColumnVector(input), readSchema);
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
            Map<String, String> actualPartitionValues = VectorUtils.toJavaMap(row.getMap(1));
            assertEquals(expPartitionValues, actualPartitionValues);
            assertEquals(348L, row.getLong(2));
            assertEquals(true, row.getBoolean(3));
        }
    }

    @Test
    public void parseNestedComplexTypes() throws IOException {
        String json = "{" +
                "  \"array\": [0, 1, null]," +
                "  \"nested_array\": [[\"a\", \"b\"], [\"c\"], []]," +
                "  \"map\": {\"a\":  true, \"b\":  false},\n" +
                "  \"nested_map\": {\"a\":  {\"one\":  [], \"two\":  [1, 2, 3]}, \"b\":  {}},\n" +
                "  \"array_of_struct\": " +
                "[{\"field1\": \"foo\", \"field2\": 3}, {\"field1\": null}]\n" +
                "}";
        StructType schema = new StructType()
                .add("array", new ArrayType(IntegerType.INTEGER, true))
                .add("nested_array", new ArrayType(new ArrayType(StringType.STRING, true), true))
                .add("map", new MapType(StringType.STRING, BooleanType.BOOLEAN, true))
                .add("nested_map",
                        new MapType(
                                StringType.STRING,
                                new MapType(
                                        StringType.STRING,
                                        new ArrayType(IntegerType.INTEGER, true),
                                        true
                                ),
                                true
                        )
                ).add("array_of_struct",
                        new ArrayType(
                                new StructType()
                                        .add("field1", StringType.STRING, true)
                                        .add("field2", IntegerType.INTEGER, true),
                                true
                        )
                );
        ColumnarBatch batch = JSON_HANDLER.parseJson(singletonStringColumnVector(json), schema);

        try (CloseableIterator<Row> rows = batch.getRows()) {
            Row result = rows.next();
            List<Integer> exp0 = Arrays.asList(0, 1, null);
            assertEquals(exp0, VectorUtils.toJavaList(result.getArray(0)));
            List<List<String>> exp1 = Arrays.asList(Arrays.asList("a", "b"), Arrays.asList("c"),
                    Collections.emptyList());
            assertEquals(exp1, VectorUtils.toJavaList(result.getArray(1)));
            Map<String, Boolean> exp2 = new HashMap<String, Boolean>() {
                {
                    put("a", true);
                    put("b", false);
                }
            };
            assertEquals(exp2, VectorUtils.toJavaMap(result.getMap(2)));
            Map<String, List<Integer>> nestedMap = new HashMap<String, List<Integer>>() {
                {
                    put("one", Collections.emptyList());
                    put("two", Arrays.asList(1, 2, 3));
                }
            };
            Map<String, Map<String, List<Integer>>> exp3 =
                    new HashMap<String, Map<String, List<Integer>>>()  {
                        {
                            put("a", nestedMap);
                            put("b", Collections.emptyMap());
                        }
                    };
            assertEquals(exp3, VectorUtils.toJavaMap(result.getMap(3)));
            ArrayValue arrayOfStruct = result.getArray(4);
            assertEquals(arrayOfStruct.getSize(), 2);
            assertEquals("foo", arrayOfStruct.getElements().getChild(0).getString(0));
            assertEquals(3, arrayOfStruct.getElements().getChild(1).getInt(0));
            assertTrue(arrayOfStruct.getElements().getChild(0).isNullAt(1));
            assertTrue(arrayOfStruct.getElements().getChild(1).isNullAt(1));
        }
    }

    private static CloseableIterator<Row> testFiles()
        throws Exception {
        String listFrom = DefaultKernelTestUtils.getTestResourceFilePath("json-files/1.json");
        CloseableIterator<FileStatus> list = FS_CLIENT.listFrom(listFrom);
        return list.map(fileStatus -> InternalScanFileUtils.generateScanFileRow(fileStatus));
    }

    private static void compareScanFileRows(Row expected, Row actual) {
        // basically compare the paths
        assertEquals(expected.getStruct(0).getString(0), actual.getStruct(0).getString(0));
    }
}
