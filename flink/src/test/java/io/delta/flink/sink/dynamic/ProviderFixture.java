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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.StructType;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test helper that supplies a {@link DeltaTableProvider} counting {@code getOrCreate} calls and
 * retaining the latest {@link StubDeltaTable} per logical table name.
 */
final class ProviderFixture {

  final AtomicInteger creates = new AtomicInteger();
  private final StructType canonicalSchema;
  private final ConcurrentHashMap<String, StubDeltaTable> stubs = new ConcurrentHashMap<>();

  ProviderFixture(StructType canonicalSchema) {
    this.canonicalSchema = canonicalSchema;
  }

  StubDeltaTable stubFor(String tableName) {
    StubDeltaTable s = stubs.get(tableName);
    assertNotNull(s, "no stub for " + tableName);
    return s;
  }

  DeltaTableProvider provider() {
    return (tableName, st, partitionCols) -> {
      creates.incrementAndGet();
      StructType schemaForStub = st != null ? st : canonicalSchema;
      if (st != null) {
        assertTrue(st.equivalent(canonicalSchema));
      }
      StubDeltaTable t = new StubDeltaTable(tableName, schemaForStub, partitionCols);
      stubs.put(tableName, t);
      return t;
    };
  }
}
