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
package io.delta.kernel.clustering

import java.util.Locale

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Column
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the public static helper
 * [[ClusteringColumnInfo#resolveAllFromDomainJson(StructType, String)]]. Each case takes a raw
 * `delta.clustering` domain JSON string (the form a connector would read out of the snapshot's
 * domain metadata) and asserts on the resolved descriptors or the propagated `KernelException`.
 */
class ClusteringColumnInfoSuite extends AnyFunSuite {

  private val schema = new StructType()
    .add("part1", IntegerType.INTEGER)
    .add("part2", IntegerType.INTEGER)

  test("resolveAllFromDomainJson returns the resolved descriptors when domain is populated") {
    val json = """{"clusteringColumns":[["part1"],["part2"]]}"""
    val infos = ClusteringColumnInfo.resolveAllFromDomainJson(schema, json).asScala
    assert(infos.size == 2)
    assert(infos(0).getLogicalColumn == new Column("part1"))
    assert(infos(0).getPhysicalColumn == new Column("part1"))
    assert(infos(0).getDataType == IntegerType.INTEGER)
    assert(infos(1).getLogicalColumn == new Column("part2"))
    assert(infos(1).getPhysicalColumn == new Column("part2"))
    assert(infos(1).getDataType == IntegerType.INTEGER)
  }

  test("resolveAllFromDomainJson resolves nested-field (multi-part) clustering columns") {
    val nestedSchema = new StructType()
      .add(
        "user",
        new StructType()
          .add(
            "address",
            new StructType()
              .add("city", StringType.STRING)))
    val json = """{"clusteringColumns":[["user","address","city"]]}"""
    val infos = ClusteringColumnInfo.resolveAllFromDomainJson(nestedSchema, json).asScala
    assert(infos.size == 1)
    assert(infos.head.getLogicalColumn == new Column(Array("user", "address", "city")))
    assert(infos.head.getPhysicalColumn == new Column(Array("user", "address", "city")))
    assert(infos.head.getDataType == StringType.STRING)
  }

  test("resolveAllFromDomainJson returns an empty list when the domain has no columns") {
    val json = """{"clusteringColumns":[]}"""
    val infos = ClusteringColumnInfo.resolveAllFromDomainJson(schema, json)
    assert(infos.isEmpty)
  }

  test("resolveAllFromDomainJson throws KernelException when clusteringColumns field is " +
    "missing or null") {
    // A clustering domain whose JSON parses but lacks a `clusteringColumns` field is incomplete
    // and surfaced as a KernelException rather than silently coalesced to an empty list, so
    // writer bugs are not masked. `[]` is the right way to spell a clustered-but-empty table.
    Seq("{}", """{"clusteringColumns":null}""").foreach { json =>
      val ex = intercept[KernelException] {
        ClusteringColumnInfo.resolveAllFromDomainJson(schema, json)
      }
      assert(
        ex.getMessage.toLowerCase(Locale.ROOT).contains("clustering"),
        s"json=$json: unexpected exception message: ${ex.getMessage}")
    }
  }

  test("resolveAllFromDomainJson propagates KernelException for malformed (unparseable) JSON") {
    val ex = intercept[KernelException] {
      ClusteringColumnInfo.resolveAllFromDomainJson(schema, "not valid json at all")
    }
    assert(
      ex.getMessage.toLowerCase(Locale.ROOT).contains("clustering"),
      s"unexpected exception message: ${ex.getMessage}")
  }

  test("resolveAllFromDomainJson propagates KernelException for an unresolvable physical column") {
    val json = """{"clusteringColumns":[["does_not_exist"]]}"""
    val ex = intercept[KernelException] {
      ClusteringColumnInfo.resolveAllFromDomainJson(schema, json)
    }
    assert(
      ex.getMessage.contains("Column 'column(`does_not_exist`)' was not found"),
      s"unexpected exception message: ${ex.getMessage}")
  }
}
