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

import java.util.{ArrayList, Arrays, Collections, HashMap}

import io.unitycatalog.client.delta.model.{PrimitiveType, StructField}

import org.scalatest.funsuite.AnyFunSuite

class DRCMetadataAdapterSuite extends AnyFunSuite {

  private def primField(name: String): StructField = {
    val t = new PrimitiveType(); t.setType("string")
    val f = new StructField(); f.setName(name); f.setType(t); f.setNullable(true)
    f.setMetadata(Collections.emptyMap()); f
  }

  private def newAdapter(
      id: String = "id-1",
      name: String = "t",
      description: String = null,
      columns: java.util.List[StructField] = Arrays.asList(primField("a")),
      partitionColumns: java.util.List[String] = Collections.emptyList(),
      configuration: java.util.Map[String, String] = Collections.emptyMap(),
      createdTime: java.lang.Long = 0L): DRCMetadataAdapter =
    new DRCMetadataAdapter(
      id, name, description, columns, partitionColumns, configuration, createdTime)

  test("getDRCColumns returns UC POJOs verbatim, ready for the converter") {
    val cols = Arrays.asList(primField("a"), primField("b"))
    val adapter = newAdapter(columns = cols)
    assert(adapter.getDRCColumns.size == 2)
    assert(adapter.getDRCColumns.get(0).getName == "a")
    assert(adapter.getDRCColumns.get(1).getName == "b")
  }

  test("getDRCColumns list is unmodifiable") {
    val adapter = newAdapter()
    intercept[UnsupportedOperationException] {
      adapter.getDRCColumns.add(primField("x"))
    }
  }

  test("defensive copy: mutations on the source list do not leak into the adapter") {
    val mutable = new ArrayList[StructField]()
    mutable.add(primField("a"))
    val adapter = newAdapter(columns = mutable)
    mutable.add(primField("sneaky"))
    // The adapter snapshotted at construction -- post-hoc additions are invisible.
    assert(adapter.getDRCColumns.size == 1)
    assert(adapter.getDRCColumns.get(0).getName == "a")
  }

  test("configuration map is defensively copied and unmodifiable") {
    val mutable = new HashMap[String, String]()
    mutable.put("k", "v")
    val adapter = newAdapter(configuration = mutable)
    mutable.put("sneaky", "bypass")
    assert(!adapter.getConfiguration.containsKey("sneaky"))
    intercept[UnsupportedOperationException] {
      adapter.getConfiguration.put("x", "y")
    }
  }

  test("rejects null id / columns / partitionColumns / configuration") {
    intercept[NullPointerException] {
      new DRCMetadataAdapter(
        null, "t", null, Collections.emptyList(), Collections.emptyList(),
        Collections.emptyMap(), 0L)
    }
    intercept[NullPointerException] {
      new DRCMetadataAdapter(
        "id", "t", null, null, Collections.emptyList(),
        Collections.emptyMap(), 0L)
    }
    intercept[NullPointerException] {
      new DRCMetadataAdapter(
        "id", "t", null, Collections.emptyList(), null,
        Collections.emptyMap(), 0L)
    }
    intercept[NullPointerException] {
      new DRCMetadataAdapter(
        "id", "t", null, Collections.emptyList(), Collections.emptyList(),
        null, 0L)
    }
  }

  test("getSchemaString returns a deterministic empty-struct stub (fallback path)") {
    val adapter = newAdapter()
    val s1 = adapter.getSchemaString
    val s2 = adapter.getSchemaString
    assert(s1 == """{"type":"struct","fields":[]}""")
    assert(s2 == s1)
  }

  test("getProvider is always 'delta' regardless of constructor input") {
    assert(newAdapter().getProvider == "delta")
  }
}
