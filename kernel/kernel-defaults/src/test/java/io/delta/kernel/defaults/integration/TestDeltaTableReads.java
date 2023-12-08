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
package io.delta.kernel.defaults.integration;

import java.math.BigDecimal;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import static io.delta.golden.GoldenTableUtils.goldenTablePath;

import io.delta.kernel.Snapshot;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.types.*;

import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.defaults.integration.DataBuilderUtils.TestColumnBatchBuilder;
import static io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getTestResourceFilePath;

/**
 * Test reading Delta lake tables end to end using the Kernel APIs and default {@link TableClient}
 * implementation ({@link DefaultTableClient})
 * <p>
 * It uses golden tables generated using the source code here:
 * https://github.com/delta-io/delta/blob/master/connectors/golden-tables/src/test/scala/io/delta
 * /golden/GoldenTables.scala
 */
public class TestDeltaTableReads
    extends BaseIntegration {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void tablePrimitives()
        throws Exception {
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
                new byte[] {(byte) i, (byte) i,},
                new BigDecimal(i)
            );
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void tableWithCheckpoint()
        throws Exception {
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
        throws Exception {
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
                new byte[] {(byte) i, (byte) i},
                new BigDecimal(i)
            );
        }

        ColumnarBatch expData = builder.build();
        compareEqualUnorderd(expData, actualData);
    }

    @Test
    public void partitionedTableWithColumnMapping()
        throws Exception {
        String tablePath =
            getTestResourceFilePath("data-reader-partition-values-column-mapping-name");
        Snapshot snapshot = snapshot(tablePath);
        StructType readSchema = new StructType()
            // partition fields
            .add("as_int", IntegerType.INTEGER)
            .add("as_double", DoubleType.DOUBLE)
            // data fields
            .add("value", StringType.STRING);

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
        throws Exception {
        expectedEx.expect(UnsupportedOperationException.class);
        expectedEx.expectMessage("Unsupported column mapping mode: id");

        String tablePath = getTestResourceFilePath("column-mapping-id");
        Snapshot snapshot = snapshot(tablePath);
        readSnapshot(snapshot.getSchema(tableClient), snapshot);
    }
}
