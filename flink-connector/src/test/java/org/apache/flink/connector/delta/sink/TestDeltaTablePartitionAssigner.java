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

package org.apache.flink.connector.delta.sink;

import java.util.LinkedHashMap;
import javax.annotation.Nullable;

import org.apache.flink.connector.delta.sink.utils.DeltaSinkTestUtils;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.table.data.RowData;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestDeltaTablePartitionAssigner {

    @Test
    public void testNoPartition() {
        // GIVEN
        TestContext context = new TestContext();
        DeltaTablePartitionAssigner<RowData> partitionAssigner =
            new DeltaTablePartitionAssigner<>(new RootPathAssigner());
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
        DeltaTablePartitionAssigner<Integer> partitionAssigner =
            new DeltaTablePartitionAssigner<>(new OnePartitioningColumnComputer("value"));
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
        DeltaTablePartitionAssigner<RowData> partitionAssigner =
            new DeltaTablePartitionAssigner<>(new MultiplePartitioningColumnComputer());
        RowData testEvent = DeltaSinkTestUtils.getTestRowDataEvent("a", "b", 3);

        // WHEN
        String partitionsPath = partitionAssigner.getBucketId(testEvent, context);

        // THEN
        String expectedPartitionsPath = "name=a/age=3/";
        assertEquals(expectedPartitionsPath, partitionsPath);
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

    static class RootPathAssigner
        implements DeltaTablePartitionAssigner.DeltaPartitionComputer<RowData> {
        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
            RowData element,
            BucketAssigner.Context context) {
            return new LinkedHashMap<>();
        }
    }

    static class OnePartitioningColumnComputer
        implements DeltaTablePartitionAssigner.DeltaPartitionComputer<Integer> {

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

    static class MultiplePartitioningColumnComputer implements
        DeltaTablePartitionAssigner.DeltaPartitionComputer<RowData> {

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
