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

import io.delta.flink.sink.WriterResultContext;
import java.util.ArrayList;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.junit.jupiter.api.Test;

/** JUnit tests for {@link TableKeySelector}. */
class TableKeySelectorTest {

  private final TableKeySelector selector = new TableKeySelector();

  @Test
  void testSameTableNameYieldsSameKey() throws Exception {
    CommittableWithLineage<DeltaDynamicWriterResult> a = lineage("warehouse.t1", 1L, 0);
    CommittableWithLineage<DeltaDynamicWriterResult> b = lineage("warehouse.t1", 2L, 3);
    assertEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testDifferentTableNamesYieldDifferentKeys() throws Exception {
    CommittableWithLineage<DeltaDynamicWriterResult> a = lineage("cat.sch.t1", 0L, 0);
    CommittableWithLineage<DeltaDynamicWriterResult> b = lineage("cat.sch.t2", 0L, 0);
    assertNotEquals(selector.getKey(a), selector.getKey(b));
  }

  @Test
  void testKeyIsTableName() throws Exception {
    String name = "unity.my_catalog.schema.my_table";
    assertEquals(name, selector.getKey(lineage(name, 0L, 0)));
  }

  private static CommittableWithLineage<DeltaDynamicWriterResult> lineage(
      String tableName, long checkpointId, int subtaskId) {
    return new CommittableWithLineage<>(
        new DeltaDynamicWriterResult(tableName, new ArrayList<>(), new WriterResultContext()),
        checkpointId,
        subtaskId);
  }
}
