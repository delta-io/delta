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

import java.util.{Arrays => JArr, Collections => JColl, Optional}

import scala.collection.JavaConverters._

import io.unitycatalog.client.delta.model.{
  DeltaProtocol,
  PrimitiveType => UCPrimitiveType,
  RemovePropertiesUpdate,
  SetPartitionColumnsUpdate,
  SetPropertiesUpdate,
  SetProtocolUpdate,
  SetSchemaUpdate,
  StructField => UCStructField,
  StructType => UCStructType
}
import org.scalatest.funsuite.AnyFunSuite

class DeltaRestMetadataDiffSuite extends AnyFunSuite {

  // ---- propertyUpdates ----

  test("propertyUpdates: identical maps produce no updates") {
    val m = Map("k" -> "v", "a" -> "b").asJava
    val out = DeltaRestMetadataDiff.propertyUpdates(m, m).asScala
    assert(out.isEmpty)
  }

  test("propertyUpdates: null inputs behave as empty maps") {
    assert(DeltaRestMetadataDiff.propertyUpdates(null, null).isEmpty)
    val m = JColl.singletonMap("k", "v")
    val addedOnly = DeltaRestMetadataDiff.propertyUpdates(null, m).asScala
    assert(addedOnly.length == 1)
    assert(addedOnly.head.isInstanceOf[SetPropertiesUpdate])
    val removedOnly = DeltaRestMetadataDiff.propertyUpdates(m, null).asScala
    assert(removedOnly.length == 1)
    assert(removedOnly.head.isInstanceOf[RemovePropertiesUpdate])
  }

  test("propertyUpdates emits SetPropertiesUpdate with only changed keys") {
    val oldProps = Map("a" -> "1", "b" -> "2").asJava
    val newProps = Map("a" -> "1", "b" -> "99", "c" -> "3").asJava
    val out = DeltaRestMetadataDiff.propertyUpdates(oldProps, newProps).asScala
    assert(out.length == 1)
    val sp = out.head.asInstanceOf[SetPropertiesUpdate]
    val upserts = sp.getUpdates.asScala.toMap
    assert(upserts == Map("b" -> "99", "c" -> "3"))
    assert(!upserts.contains("a"))
  }

  test("propertyUpdates emits RemovePropertiesUpdate with only removed keys") {
    val oldProps = Map("keep" -> "1", "drop1" -> "2", "drop2" -> "3").asJava
    val newProps = Map("keep" -> "1").asJava
    val out = DeltaRestMetadataDiff.propertyUpdates(oldProps, newProps).asScala
    assert(out.length == 1)
    val rp = out.head.asInstanceOf[RemovePropertiesUpdate]
    val removed = rp.getRemovals.asScala.toSet
    assert(removed == Set("drop1", "drop2"))
  }

  test("propertyUpdates emits both Set and Remove when both apply, Set first") {
    val oldProps = Map("a" -> "1", "b" -> "2").asJava
    val newProps = Map("a" -> "CHANGED", "c" -> "3").asJava
    val out = DeltaRestMetadataDiff.propertyUpdates(oldProps, newProps).asScala
    assert(out.length == 2)
    assert(out(0).isInstanceOf[SetPropertiesUpdate])
    assert(out(1).isInstanceOf[RemovePropertiesUpdate])
    val sp = out(0).asInstanceOf[SetPropertiesUpdate]
    assert(sp.getUpdates.asScala.toMap == Map("a" -> "CHANGED", "c" -> "3"))
    val rp = out(1).asInstanceOf[RemovePropertiesUpdate]
    assert(rp.getRemovals.asScala.toSet == Set("b"))
  }

  test("propertyUpdates: null value vs absent key are distinct") {
    val oldProps = Map.empty[String, String].asJava
    val withNull = new java.util.HashMap[String, String]()
    withNull.put("k", null)
    val out = DeltaRestMetadataDiff.propertyUpdates(oldProps, withNull).asScala
    // A null new value is still a "different" value relative to absent -- containsKey check
    // distinguishes the two.
    assert(out.length == 1)
    val sp = out.head.asInstanceOf[SetPropertiesUpdate]
    assert(sp.getUpdates.containsKey("k"))
    assert(sp.getUpdates.get("k") === null)
  }

  // ---- protocolUpdate ----

  test("protocolUpdate: returns empty when old == new") {
    val p = new DeltaProtocol().minReaderVersion(1).minWriterVersion(2)
    val result = DeltaRestMetadataDiff.protocolUpdate(Optional.of(p), p)
    assert(!result.isPresent)
  }

  test("protocolUpdate: returns SetProtocolUpdate when versions differ") {
    val oldP = new DeltaProtocol().minReaderVersion(1).minWriterVersion(2)
    val newP = new DeltaProtocol().minReaderVersion(3).minWriterVersion(7)
    val result = DeltaRestMetadataDiff.protocolUpdate(Optional.of(oldP), newP)
    assert(result.isPresent)
    val sp = result.get.asInstanceOf[SetProtocolUpdate]
    assert(sp.getProtocol.getMinReaderVersion == 3)
    assert(sp.getProtocol.getMinWriterVersion == 7)
  }

  test("protocolUpdate: returns SetProtocolUpdate when old is absent (initial commit)") {
    val newP = new DeltaProtocol().minReaderVersion(3).minWriterVersion(7)
    val result = DeltaRestMetadataDiff.protocolUpdate(Optional.empty(), newP)
    assert(result.isPresent)
    assert(result.get.isInstanceOf[SetProtocolUpdate])
  }

  test("protocolUpdate rejects null newProtocol") {
    intercept[NullPointerException] {
      DeltaRestMetadataDiff.protocolUpdate(Optional.empty(), null)
    }
  }

  // ---- columnsUpdate ----

  test("columnsUpdate: returns empty when old == new") {
    val s = struct("c" -> "long")
    val result = DeltaRestMetadataDiff.columnsUpdate(Optional.of(s), s)
    assert(!result.isPresent)
  }

  test("columnsUpdate: returns SetSchemaUpdate when schema differs") {
    val oldS = struct("a" -> "long")
    val newS = struct("a" -> "long", "b" -> "string")
    val result = DeltaRestMetadataDiff.columnsUpdate(Optional.of(oldS), newS)
    assert(result.isPresent)
    val su = result.get.asInstanceOf[SetSchemaUpdate]
    assert(su.getColumns.getFields.size() == 2)
    assert(su.getColumns.getFields.get(1).getName == "b")
  }

  test("columnsUpdate: initial commit with no old schema returns a full SetSchemaUpdate") {
    val newS = struct("c" -> "long")
    val result = DeltaRestMetadataDiff.columnsUpdate(Optional.empty(), newS)
    assert(result.isPresent)
    assert(result.get.isInstanceOf[SetSchemaUpdate])
  }

  test("columnsUpdate rejects null newColumns") {
    intercept[NullPointerException] {
      DeltaRestMetadataDiff.columnsUpdate(Optional.empty(), null)
    }
  }

  // ---- partitionColumnsUpdate ----

  test("partitionColumnsUpdate: returns empty when old == new (in order)") {
    val same = JArr.asList("year", "month")
    val result = DeltaRestMetadataDiff.partitionColumnsUpdate(Optional.of(same), same)
    assert(!result.isPresent)
  }

  test("partitionColumnsUpdate: order change yields a new full-replacement update") {
    val oldP = JArr.asList("year", "month")
    val newP = JArr.asList("month", "year")
    val result = DeltaRestMetadataDiff.partitionColumnsUpdate(Optional.of(oldP), newP)
    assert(result.isPresent)
    val spc = result.get.asInstanceOf[SetPartitionColumnsUpdate]
    assert(spc.getPartitionColumns.asScala.toSeq === Seq("month", "year"))
  }

  test("partitionColumnsUpdate: adding partitioning to a non-partitioned table") {
    val result = DeltaRestMetadataDiff.partitionColumnsUpdate(
      Optional.of(JColl.emptyList[String]()),
      JArr.asList("year"))
    assert(result.isPresent)
  }

  test("partitionColumnsUpdate: initial commit (oldOpt empty) always emits") {
    val result = DeltaRestMetadataDiff.partitionColumnsUpdate(
      Optional.empty(),
      JArr.asList("year"))
    assert(result.isPresent)
  }

  test("partitionColumnsUpdate: null new list treated as empty") {
    val result = DeltaRestMetadataDiff.partitionColumnsUpdate(
      Optional.of(JArr.asList("y")), null)
    assert(result.isPresent)
    val spc = result.get.asInstanceOf[SetPartitionColumnsUpdate]
    assert(spc.getPartitionColumns.isEmpty)
  }

  // ---- helpers ----

  private def struct(fields: (String, String)*): UCStructType = {
    val arr = new java.util.ArrayList[UCStructField]()
    fields.foreach { case (name, primName) =>
      val p = new UCPrimitiveType()
      p.setType(primName)
      val f = new UCStructField().name(name).nullable(true)
      f.setType(p)
      arr.add(f)
    }
    val s = new UCStructType()
    s.setFields(arr)
    s.setType("struct")
    s
  }
}
