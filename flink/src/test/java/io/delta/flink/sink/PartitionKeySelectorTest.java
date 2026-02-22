/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

public class PartitionKeySelectorTest implements FlinkTypeTests {

  @FlinkTypeTests.TestAllFlinkTypes
  public void testNullValue(LogicalType primitiveType) throws Exception {
    RowType singleFieldType = RowType.of(new LogicalType[] {primitiveType}, new String[] {"id"});
    RowData withNull = GenericRowData.of(new Object[] {null});

    assertEquals(3355, new PartitionKeySelector(singleFieldType, List.of("id")).getKey(withNull));

    RowType doubleFieldType =
        RowType.of(new LogicalType[] {new IntType(), primitiveType}, new String[] {"a", "b"});
    RowData withNull1 = GenericRowData.of(24, null);
    RowData withNull2 = GenericRowData.of(24, null);
    PartitionKeySelector selector = new PartitionKeySelector(doubleFieldType, List.of("a", "b"));
    assertEquals(selector.getKey(withNull1), selector.getKey(withNull2));
  }
}
