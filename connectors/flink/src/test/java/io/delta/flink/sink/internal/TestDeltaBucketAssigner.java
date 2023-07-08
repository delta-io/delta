/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink.internal;

import java.util.Arrays;
import java.util.LinkedHashMap;
import javax.annotation.Nullable;

import io.delta.flink.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestDeltaBucketAssigner {

    @Test
    public void testNoPartition() {
        // GIVEN
        TestContext context = new TestContext();
        DeltaBucketAssigner<RowData> partitionAssigner =
            new DeltaBucketAssigner<>(new RootPathAssigner());
        RowData testRowData = DeltaSinkTestUtils.getTestRowData(1).get(0);

        // WHEN
        String partitionsPath = partitionAssigner.getBucketId(testRowData, context);

        // THEN
        assertEquals("", partitionsPath);
    }

    @Test
    public void testOnePartitioningColumn() {
        // GIVEN
        TestContext context = new TestContext();
        DeltaBucketAssigner<Integer> partitionAssigner =
            new DeltaBucketAssigner<>(new OnePartitioningColumnComputer("value"));
        Integer testEvent = 5;

        // WHEN
        String partitionsPath = partitionAssigner.getBucketId(testEvent, context);

        // THEN
        String expectedPartitionsPath = "value=5/";
        assertEquals(expectedPartitionsPath, partitionsPath);
    }

    @Test
    public void testMultiplePartitioningColumns() {
        // GIVEN
        TestContext context = new TestContext();
        DeltaBucketAssigner<RowData> partitionAssigner =
            new DeltaBucketAssigner<>(new MultiplePartitioningColumnComputer());
        RowData testEvent = DeltaSinkTestUtils.getTestRowDataEvent("a", "b", 3);

        // WHEN
        String partitionsPath = partitionAssigner.getBucketId(testEvent, context);

        // THEN
        String expectedPartitionsPath = "name=a/age=3/";
        assertEquals(expectedPartitionsPath, partitionsPath);
    }

    @Test
    public void testRowDataPartitionComputer() {
        // GIVEN
        TestContext context = new TestContext();
        RowType testRowType = new RowType(Arrays.asList(
            new RowType.RowField("partition_col1", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("partition_col2", new IntType()),
            new RowType.RowField("partition_col3", new BigIntType()),
            new RowType.RowField("partition_col4", new SmallIntType()),
            new RowType.RowField("partition_col5", new TinyIntType()),
            new RowType.RowField("col5", new VarCharType()),
            new RowType.RowField("col6", new IntType())
        ));
        DataFormatConverters.DataFormatConverter<RowData, Row> converter =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(testRowType)
            );
        String[] partitionCols = {"partition_col1", "partition_col2", "partition_col3",
            "partition_col4", "partition_col5"};

        DeltaPartitionComputer<RowData> partitionComputer =
            new DeltaPartitionComputer.DeltaRowDataPartitionComputer(testRowType, partitionCols);

        RowData record = converter.toInternal(
            Row.of("1", Integer.MAX_VALUE, Long.MAX_VALUE, Short.MAX_VALUE, Byte.MAX_VALUE,
                "some_val", 2));

        // WHEN
        LinkedHashMap<String, String> partitionValues =
            partitionComputer.generatePartitionValues(record, context);

        // THEN
        LinkedHashMap<String, String> expected = new LinkedHashMap<String, String>() {{
                put("partition_col1", "1");
                put("partition_col2", String.valueOf(Integer.MAX_VALUE));
                put("partition_col3", String.valueOf(Long.MAX_VALUE));
                put("partition_col4", String.valueOf(Short.MAX_VALUE));
                put("partition_col5", String.valueOf(Byte.MAX_VALUE));
            }};

        assertEquals(expected, partitionValues);
    }

    @Test
    public void testRowDataPartitionComputerWithStaticPartitionValues() {
        // GIVEN
        TestContext context = new TestContext();
        RowType testRowType = new RowType(Arrays.asList(
            new RowType.RowField("partition_col1", new VarCharType(VarCharType.MAX_LENGTH)),
            new RowType.RowField("partition_col2", new IntType()),
            new RowType.RowField("col5", new VarCharType()),
            new RowType.RowField("col6", new IntType())
        ));
        DataFormatConverters.DataFormatConverter<RowData, Row> converter =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(testRowType)
            );
        String[] partitionCols = {"partition_col1", "partition_col2"};
        int staticPartCol2Value = 555;
        LinkedHashMap<String, String> staticPartitionValues = new LinkedHashMap<String, String>() {{
                put("partition_col2", String.valueOf(staticPartCol2Value));
            }};

        DeltaPartitionComputer<RowData> partitionComputer =
            new DeltaPartitionComputer.DeltaRowDataPartitionComputer(
                testRowType, partitionCols, staticPartitionValues);

        RowData record = converter.toInternal(Row.of("1", 2, "some_val", 2));

        // WHEN
        LinkedHashMap<String, String> partitionValues =
            partitionComputer.generatePartitionValues(record, context);

        // THEN
        LinkedHashMap<String, String> expected = new LinkedHashMap<String, String>() {{
                put("partition_col1", "1");
                put("partition_col2", String.valueOf(staticPartCol2Value));
            }};

        assertEquals(expected, partitionValues);
    }

    @Test(expected = RuntimeException.class)
    public void testRowDataPartitionComputerNotAllowedType() {
        // GIVEN
        TestContext context = new TestContext();
        RowType testRowType = new RowType(Arrays.asList(
            new RowType.RowField("partition_col1", new DoubleType()),
            new RowType.RowField("col5", new VarCharType()),
            new RowType.RowField("col6", new IntType())
        ));
        DataFormatConverters.DataFormatConverter<RowData, Row> converter =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(testRowType)
            );
        String[] partitionCols = {"partition_col1"};

        DeltaPartitionComputer<RowData> partitionComputer =
            new DeltaPartitionComputer.DeltaRowDataPartitionComputer(testRowType, partitionCols);

        RowData record = converter.toInternal(Row.of(Double.MAX_VALUE, "some_val", 2));

        // WHEN
        // below should fail
        partitionComputer.generatePartitionValues(record, context);
    }

    ///////////////////////////////////////////////////////////////////////////
    // Test Classes
    ///////////////////////////////////////////////////////////////////////////

    private static final class TestContext implements BucketAssigner.Context {

        @Override
        public long currentProcessingTime() {
            return 0;
        }

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Nullable
        @Override
        public Long timestamp() {
            return null;
        }
    }

    static class RootPathAssigner implements DeltaPartitionComputer<RowData> {
        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
            RowData element,
            BucketAssigner.Context context) {
            return new LinkedHashMap<>();
        }
    }

    static class OnePartitioningColumnComputer implements DeltaPartitionComputer<Integer> {

        public final String partitionName;

        OnePartitioningColumnComputer(String partitionName) {
            this.partitionName = partitionName;
        }

        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
            Integer element,
            BucketAssigner.Context context) {
            LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
            partitionSpec.put(partitionName, element.toString());
            return partitionSpec;
        }
    }

    static class MultiplePartitioningColumnComputer implements DeltaPartitionComputer<RowData> {

        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
            RowData element, BucketAssigner.Context context) {
            String name = element.getString(0).toString();
            int age = element.getInt(2);
            LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
            partitionSpec.put("name", name);
            partitionSpec.put("age", Integer.toString(age));
            return partitionSpec;
        }
    }
}
