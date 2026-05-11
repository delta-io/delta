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

import java.util.{Collections, Locale, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.{Operation, ScanBuilder, Snapshot}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Column
import io.delta.kernel.statistics.SnapshotStatistics
import io.delta.kernel.transaction.UpdateTableTransactionBuilder
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

/**
 * Exercises the default body of [[Snapshot#getClusteringColumnInfos()]]. The in-tree
 * [[io.delta.kernel.internal.SnapshotImpl]] always overrides this method, so the kernel-defaults
 * integration suites only test the override. This suite uses a minimal stub `Snapshot` to pin the
 * default-body contract directly so connectors that implement `Snapshot` themselves don't ship on
 * an untested code path.
 */
class SnapshotGetClusteringColumnInfosDefaultBodySuite extends AnyFunSuite {

  private val schema = new StructType()
    .add("part1", IntegerType.INTEGER)
    .add("part2", IntegerType.INTEGER)

  private def stub(domainJson: Option[String]): Snapshot = new StubSnapshot(schema, domainJson)

  test("default body returns Optional.empty when delta.clustering domain is absent") {
    val result = stub(None).getClusteringColumnInfos
    assert(!result.isPresent, "absent domain must yield Optional.empty")
  }

  test("default body returns the resolved descriptors when domain is populated") {
    val json = """{"clusteringColumns":[["part1"],["part2"]]}"""
    val result = stub(Some(json)).getClusteringColumnInfos
    assert(result.isPresent)
    val infos = result.get().asScala
    assert(infos.size == 2)
    assert(infos(0).getLogicalColumn == new Column("part1"))
    assert(infos(0).getPhysicalColumn == new Column("part1"))
    assert(infos(0).getDataType == IntegerType.INTEGER)
    assert(infos(1).getLogicalColumn == new Column("part2"))
    assert(infos(1).getPhysicalColumn == new Column("part2"))
    assert(infos(1).getDataType == IntegerType.INTEGER)
  }

  test("default body returns Optional.of(empty list) when domain has no columns") {
    val json = """{"clusteringColumns":[]}"""
    val result = stub(Some(json)).getClusteringColumnInfos
    assert(result.isPresent)
    assert(result.get().isEmpty)
  }

  test("default body throws KernelException when clusteringColumns field is missing or null") {
    // A clustering domain whose JSON parses but lacks a `clusteringColumns` field is incomplete
    // and surfaced as a KernelException rather than silently coalesced to an empty list, so
    // writer bugs are not masked. `[]` is the right way to spell a clustered-but-empty table.
    Seq("{}", """{"clusteringColumns":null}""").foreach { json =>
      val ex = intercept[KernelException] {
        stub(Some(json)).getClusteringColumnInfos
      }
      assert(
        ex.getMessage.toLowerCase(Locale.ROOT).contains("clustering"),
        s"json=$json: unexpected exception message: ${ex.getMessage}")
    }
  }

  test("default body propagates KernelException for malformed (unparseable) JSON") {
    val ex = intercept[KernelException] {
      stub(Some("not valid json at all")).getClusteringColumnInfos
    }
    assert(
      ex.getMessage.toLowerCase(Locale.ROOT).contains("clustering"),
      s"unexpected exception message: ${ex.getMessage}")
  }

  test("default body propagates KernelException for an unresolvable physical column") {
    val json = """{"clusteringColumns":[["does_not_exist"]]}"""
    val ex = intercept[KernelException] {
      stub(Some(json)).getClusteringColumnInfos
    }
    assert(
      ex.getMessage.contains("Column 'column(`does_not_exist`)' was not found"),
      s"unexpected exception message: ${ex.getMessage}")
  }
}

/**
 * Minimal `Snapshot` implementation that overrides only the two methods the default
 * `getClusteringColumnInfos` body calls. All other interface methods throw
 * `UnsupportedOperationException` -- they're not exercised by these tests.
 */
private class StubSnapshot(
    schemaToReturn: StructType,
    clusteringDomainJson: Option[String]) extends Snapshot {

  override def getSchema: StructType = schemaToReturn

  override def getDomainMetadata(domain: String): Optional[String] = {
    if (domain == "delta.clustering") {
      clusteringDomainJson.map(Optional.of[String]).getOrElse(Optional.empty[String]())
    } else {
      Optional.empty[String]()
    }
  }

  // Methods not exercised by these tests -- throw if called so a misuse fails loudly.
  private def unsupported(): Nothing =
    throw new UnsupportedOperationException("not supported by StubSnapshot")

  override def getPath: String = unsupported()
  override def getVersion: Long = unsupported()
  override def getPartitionColumnNames: java.util.List[String] = unsupported()
  override def getTimestamp(engine: Engine): Long = unsupported()
  override def getTableProperties: java.util.Map[String, String] = Collections.emptyMap()
  override def getStatistics: SnapshotStatistics = unsupported()
  override def getScanBuilder: ScanBuilder = unsupported()
  override def buildUpdateTableTransaction(
      engineInfo: String,
      operation: Operation): UpdateTableTransactionBuilder = unsupported()
  override def publish(engine: Engine): Snapshot = unsupported()
  override def writeChecksum(engine: Engine, mode: ChecksumWriteMode): Unit = unsupported()
  override def writeCheckpoint(engine: Engine): Unit = unsupported()
}
