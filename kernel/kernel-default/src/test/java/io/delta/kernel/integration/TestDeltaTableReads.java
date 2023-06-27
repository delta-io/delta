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
package io.delta.kernel.integration;

import static io.delta.kernel.DefaultKernelUtils.daysSinceEpoch;
import static io.delta.kernel.integration.DataBuilderUtils.row;
import static io.delta.kernel.utils.DefaultKernelTestUtils.getTestResourceFilePath;
import static io.delta.kernel.utils.DefaultKernelTestUtils.goldenTablePath;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.delta.kernel.Snapshot;
import io.delta.kernel.client.DefaultTableClient;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.integration.DataBuilderUtils.TestColumnBatchBuilder;
import io.delta.kernel.types.*;

/**
 * Test reading Delta lake tables end to end using the Kernel APIs and default {@link TableClient}
 * implementation ({@link DefaultTableClient})
 * <p>
 * It uses golden tables generated using the source code here:
 * https://github.com/delta-io/delta/blob/master/connectors/golden-tables/src/test/scala/io/delta/golden/GoldenTables.scala
 */
public class TestDeltaTableReads
    extends BaseIntegration
{
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void tablePrimitives()
        throws Exception
    {
        String tablePath = goldenTablePath("data-reader-primitives");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = removeUnsupportedType(snapshot.getSchema(tableClient));

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);

        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema)
            .addAllNullsRow();

        for (int i = 0; i < 10; i++) {
            builder = builder.addRow(
                i,
                (long) i,
                (byte) i,
                (short) i,
                i % 2 == 0,
                (float) i,
                (double) i,
                String.valueOf(i),
                new byte[] {(byte) i, (byte) i}
            );
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void partitionedTable()
        throws Exception
    {
        String tablePath = goldenTablePath("data-reader-partition-values");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = removeUnsupportedType(snapshot.getSchema(tableClient));

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);

        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema);

        for (int i = 0; i < 2; i++) {
            builder = builder.addRow(
                i,
                (long) i,
                (byte) i,
                (short) i,
                i % 2 == 0,
                (float) i,
                (double) i,
                String.valueOf(i),
                "null",
                daysSinceEpoch(Date.valueOf("2021-09-08")),
                Arrays.asList(
                    row(arrayElemStructTypeOf(readSchema, "as_list_of_records"), i),
                    row(arrayElemStructTypeOf(readSchema, "as_list_of_records"), i),
                    row(arrayElemStructTypeOf(readSchema, "as_list_of_records"), i)
                ),
                row(
                    structTypeOf(readSchema, "as_nested_struct"),
                    String.valueOf(i),
                    String.valueOf(i),
                    row(
                        structTypeOf(
                            structTypeOf(readSchema, "as_nested_struct"),
                            "ac"
                        ),
                        i,
                        (long) i
                    )
                ),
                String.valueOf(i)
            );
        }

        builder = builder.addRow(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            Arrays.asList(
                row(arrayElemStructTypeOf(readSchema, "as_list_of_records"), 2),
                row(arrayElemStructTypeOf(readSchema, "as_list_of_records"), 2),
                row(arrayElemStructTypeOf(readSchema, "as_list_of_records"), 2)
            ),
            row(
                structTypeOf(readSchema, "as_nested_struct"),
                "2",
                "2",
                row(
                    structTypeOf(
                        structTypeOf(readSchema, "as_nested_struct"),
                        "ac"
                    ),
                    2,
                    2L
                )
            ),
            "2"
        );

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void tableWithComplexArrayTypes()
        throws Exception
    {
        String tablePath = goldenTablePath("data-reader-array-complex-objects");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = removeUnsupportedType(snapshot.getSchema(tableClient));

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);

        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema);

        for (int i = 0; i < 10; i++) {
            final int index = i;
            builder.addRow(
                i,
                Arrays.asList(
                    Arrays.asList(
                        Arrays.asList(i, i, i),
                        Arrays.asList(i, i, i)
                    ),
                    Arrays.asList(
                        Arrays.asList(i, i, i),
                        Arrays.asList(i, i, i)
                    )
                ),
                Arrays.asList(
                    Arrays.asList(
                        Arrays.asList(
                            Arrays.asList(i, i, i),
                            Arrays.asList(i, i, i)),
                        Arrays.asList(
                            Arrays.asList(i, i, i),
                            Arrays.asList(i, i, i))
                    ),
                    Arrays.asList(
                        Arrays.asList(
                            Arrays.asList(i, i, i),
                            Arrays.asList(i, i, i)),
                        Arrays.asList(
                            Arrays.asList(i, i, i),
                            Arrays.asList(i, i, i)))
                ),
                Arrays.asList(
                    new HashMap()
                    {{
                        put(String.valueOf(index), (long) index);
                    }},
                    new HashMap()
                    {{
                        put(String.valueOf(index), (long) index);
                    }}
                ),
                Arrays.asList(
                    row(arrayElemStructTypeOf(readSchema, "list_of_records"), i),
                    row(arrayElemStructTypeOf(readSchema, "list_of_records"), i),
                    row(arrayElemStructTypeOf(readSchema, "list_of_records"), i)
                )
            );
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void tableWithComplexMapTypes()
        throws Exception
    {
        String tablePath = goldenTablePath("data-reader-map");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = new StructType()
            .add("i", IntegerType.INSTANCE)
            .add("a", new MapType(IntegerType.INSTANCE, IntegerType.INSTANCE, true))
            .add("b", new MapType(LongType.INSTANCE, ByteType.INSTANCE, true))
            .add("c", new MapType(ShortType.INSTANCE, BooleanType.INSTANCE, true))
            .add("d", new MapType(FloatType.INSTANCE, DoubleType.INSTANCE, true))
            .add("f", new MapType(
                IntegerType.INSTANCE,
                new ArrayType(new StructType().add("val", IntegerType.INSTANCE), true),
                true)
            );

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);

        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema);

        for (int i = 0; i < 10; i++) {
            final int index = i;
            builder.addRow(
                i,
                new HashMap()
                {{
                    put(index, index);
                }},
                new HashMap()
                {{
                    put((long) index, (byte) index);
                }},
                new HashMap()
                {{
                    put((short) index, index % 2 == 0);
                }},
                new HashMap()
                {{
                    put((float) index, (double) index);
                }},
                new HashMap()
                {{
                    StructType elemType = new StructType().add("val", IntegerType.INSTANCE);
                    put(
                        index,
                        Arrays.asList(
                            row(elemType, index),
                            row(elemType, index),
                            row(elemType, index)
                        )
                    );
                }}
            );
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void tableWithCheckpoint()
        throws Exception
    {
        String tablePath = getTestResourceFilePath("basic-with-checkpoint");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = snapshot.getSchema(tableClient);

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);
        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema);
        for (int i = 0; i < 150; i++) {
            builder = builder.addRow((long) i);
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void tableWithNameColumnMappingMode()
        throws Exception
    {
        String tablePath = getTestResourceFilePath("data-reader-primitives-column-mapping-name");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = removeUnsupportedType(snapshot.getSchema(tableClient));

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);

        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema)
            .addAllNullsRow();

        for (int i = 0; i < 10; i++) {
            builder = builder.addRow(
                i,
                (long) i,
                (byte) i,
                (short) i,
                i % 2 == 0,
                (float) i,
                (double) i,
                String.valueOf(i),
                new byte[] {(byte) i, (byte) i}
            );
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void partitionedTableWithColumnMapping()
        throws Exception
    {
        String tablePath =
            getTestResourceFilePath("data-reader-partition-values-column-mapping-name");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = new StructType()
            // partition fields
            .add("as_int", IntegerType.INSTANCE)
            .add("as_double", DoubleType.INSTANCE)
            // data fields
            .add("value", StringType.INSTANCE);

        List<ColumnarBatch> actualData = readSnapshot(readSchema, snapshot);

        TestColumnBatchBuilder builder = DataBuilderUtils.builder(readSchema);

        for (int i = 0; i < 2; i++) {
            builder = builder.addRow(i, (double) i, String.valueOf(i));
        }

        builder = builder.addRow(null, null, "2");

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void columnMappingIdModeThrowsError()
        throws Exception
    {
        expectedEx.expect(UnsupportedOperationException.class);
        expectedEx.expectMessage("Unsupported column mapping mode: id");

        String tablePath = getTestResourceFilePath("column-mapping-id");
        Snapshot snapshot = snapshot(tablePath);
        readSnapshot(snapshot.getSchema(tableClient), snapshot);
    }

    private StructType structTypeOf(StructType structType, String colName)
    {
        return (StructType) structType.get(colName).getDataType();
    }

    private StructType arrayElemStructTypeOf(StructType structType, String colName)
    {
        return (StructType) ((ArrayType) structType.get(colName).getDataType()).getElementType();
    }
}
