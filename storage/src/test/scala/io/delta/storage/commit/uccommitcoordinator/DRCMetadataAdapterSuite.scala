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

package io.delta.storage.commit.uccommitcoordinator

import java.util.Collections

import io.unitycatalog.client.delta.model.{PrimitiveType, StructField}

import org.scalatest.funsuite.AnyFunSuite

class DRCMetadataAdapterSuite extends AnyFunSuite {

  private def newAdapter(): DRCMetadataAdapter = {
    val p = new PrimitiveType(); p.setType("long")
    val f = new StructField()
    f.setName("id"); f.setType(p); f.setNullable(false); f.setMetadata(Collections.emptyMap())
    new DRCMetadataAdapter(
      /* id */ "uuid-1",
      /* name */ "t",
      /* description */ null,
      java.util.Collections.singletonList(f),
      Collections.emptyList(),
      Collections.emptyMap(),
      /* createdTime */ 0L)
  }

  test("getSchemaString returns a deterministic empty-struct stub, not an exception") {
    // Load-bearing: the DRC read path's real schema accessor is getDRCColumns(), but framework
    // defensive calls (logging, toString in debuggers, metric tags) must not crash. The stub is
    // stable so callers that DO consume it get predictable behavior.
    val a = newAdapter()
    val s1 = a.getSchemaString
    assert(s1 == "{\"type\":\"struct\",\"fields\":[]}")
    // Stable across repeated calls.
    assert(a.getSchemaString == s1)
    assert(a.getSchemaString == s1)
  }

  test("getDRCColumns is the real schema accessor and returns the underlying POJO list") {
    val a = newAdapter()
    assert(a.getDRCColumns.size == 1)
    assert(a.getDRCColumns.get(0).getName == "id")
  }

  test("partition columns and configuration are returned as unmodifiable views") {
    val a = new DRCMetadataAdapter("id", "n", null,
      Collections.emptyList(),
      java.util.Arrays.asList("p1"),
      Collections.singletonMap("k", "v"),
      0L)
    intercept[UnsupportedOperationException] { a.getPartitionColumns.add("p2") }
    intercept[UnsupportedOperationException] { a.getConfiguration.put("k2", "v2") }
  }
}
