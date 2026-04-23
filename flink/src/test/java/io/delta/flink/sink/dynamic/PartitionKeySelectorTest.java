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

package io.delta.flink.sink.dynamic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.flink.table.data.GenericRowData;
import org.junit.jupiter.api.Test;

public class PartitionKeySelectorTest {

  private final PartitionKeySelector selector = new PartitionKeySelector();

  @Test
  void testSameTableAndSamePartitionValuesYieldSameKey() throws Exception {
    DynamicRow a = row("catalog.schema.t1", new String[] {"2024", "us"});
    DynamicRow b = row("catalog.schema.t1", new String[] {"2024", "us"});
    assertEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testSameTableAndEqualPartitionValuesInDifferentArrayInstancesYieldSameKey()
      throws Exception {
    DynamicRow a = row("t", new String[] {"x", "y"});
    DynamicRow b = row("t", new String[] {"x", "y"});
    assertEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testSameTableAndDifferentPartitionValuesYieldDifferentKeys() throws Exception {
    DynamicRow a = row("catalog.schema.t1", new String[] {"2024", "us"});
    DynamicRow b = row("catalog.schema.t1", new String[] {"2024", "eu"});
    assertNotEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testDifferentTableAndSamePartitionValuesYieldDifferentKeys() throws Exception {
    DynamicRow a = row("catalog.schema.t1", new String[] {"2024"});
    DynamicRow b = row("catalog.schema.t2", new String[] {"2024"});
    assertNotEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testNullPartitionValuesDoNotThrowAndAreStable() throws Exception {
    DynamicRow a = row("t", null);
    DynamicRow b = row("t", null);
    assertEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testNullPartitionValuesEquivalentToEmptyPartitionArray() throws Exception {
    assertEquals(selector.getKey(row("t", null)), selector.getKey(row("t", new String[0])));
  }

  private static DynamicRow row(String tableName, String[] partitionValues) {
    return new DynamicRow() {
      @Override
      public String getTableName() {
        return tableName;
      }

      @Override
      public String getSchemaStr() {
        return "{}";
      }

      @Override
      public GenericRowData getRow() {
        return GenericRowData.of();
      }

      @Override
      public String[] getPartitionColumns() {
        return new String[0];
      }

      @Override
      public String[] getPartitionValues() {
        return partitionValues;
      }
    };
  }
}
