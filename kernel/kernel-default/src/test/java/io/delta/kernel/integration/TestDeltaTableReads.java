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

import static io.delta.kernel.utils.DefaultKernelTestUtils.goldenTablePath;
import java.util.List;
import org.junit.Test;

import io.delta.kernel.Snapshot;
import io.delta.kernel.client.DefaultTableClient;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.integration.DataBuilderUtils.TestColumnBatchBuilder;
import io.delta.kernel.types.StructType;

/**
 * Test reading Delta lake tables end to end using the Kernel APIs and default {@link TableClient}
 * implementation ({@link DefaultTableClient})
 */
public class TestDeltaTableReads
    extends BaseIntegration
{
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
            builder.addRow(
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
    public void partitionedTableWithoutCheckpoint()
        throws Exception
    {

    }

    @Test
    public void tableWithCheckpoint()
        throws Exception
    {

    }

    @Test
    public void partitionedTableWithCheckpoint()
        throws Exception
    {

    }

    @Test
    public void tableWithNameColumnMappingMode()
        throws Exception
    {

    }

    @Test
    public void tableWithDeletionVectors()
        throws Exception
    {

    }
}
