/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.unitycatalog

import scala.jdk.CollectionConverters._

import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.MockSnapshotUtils
import io.delta.kernel.transaction.DataLayoutSpec
import io.delta.kernel.types.{IntegerType, StringType, StructType}
import io.delta.kernel.utils.CloseableIterable

import org.scalatest.funsuite.AnyFunSuite

class UnityCatalogUtilsSuite
    extends AnyFunSuite
    with UCCatalogManagedTestUtils
    with MockSnapshotUtils {

  private val testUcTableId = "testUcTableId"

  test("getPropertiesForCreate: throws when snapshot is not version 0") {
    val mockSnapshotV1 = getMockSnapshot(new Path("/fake/table/path"), latestVersion = 1)

    val exMsg = intercept[IllegalArgumentException] {
      UnityCatalogUtils.getPropertiesForCreate(defaultEngine, mockSnapshotV1)
    }.getMessage

    assert(exMsg.contains("Expected a snapshot at version 0, but got a snapshot at version 1"))
  }

  test("getPropertiesForCreate: handles all cases together") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)

      val testSchema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add(
          "address",
          new StructType()
            .add("city", StringType.STRING)
            .add("state", StringType.STRING))
        .add("data", StringType.STRING)

      val clusteringColumns = List(new Column("id"), new Column(Array("address", "city")))

      val snapshot = ucCatalogManagedClient
        .buildCreateTableTransaction(testUcTableId, tablePath, testSchema, "test-engine")
        .withTableProperties(
          Map(
            "foo" -> "bar",
            "delta.enableRowTracking" -> "true",
            "delta.columnMapping.mode" -> "name").asJava)
        .withDataLayoutSpec(DataLayoutSpec.clustered(clusteringColumns.asJava))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
        .getPostCommitSnapshot
        .get()
        .asInstanceOf[SnapshotImpl]

      val snapshotTimestamp = snapshot.getTimestamp(engine)

      val actualProps = UnityCatalogUtils.getPropertiesForCreate(engine, snapshot).asScala

      val expectedProps = Map(
        // Case 0: Properties we expect to be injected by the UC-CatalogManaged-Client (and are
        //         stored in the metadata.configuration)
        "io.unitycatalog.tableId" -> testUcTableId,

        // Case 1: Table properties from metadata.configuration
        "foo" -> "bar",
        "delta.enableRowTracking" -> "true",
        "delta.columnMapping.mode" -> "name",

        // Case 2: Protocol-derived properties
        "delta.minReaderVersion" -> "3",
        "delta.minWriterVersion" -> "7",
        "delta.feature.catalogManaged" -> "supported",
        "delta.feature.rowTracking" -> "supported",
        "delta.feature.columnMapping" -> "supported",
        "delta.feature.inCommitTimestamp" -> "supported",

        // Case 3: UC metastore properties
        "delta.lastUpdateVersion" -> "0",
        "delta.lastCommitTimestamp" -> s"$snapshotTimestamp",

        // Case 4: Clustering properties - these should be the LOGICAL names not the PHYSICAL names
        "clusteringColumns" -> """[["id"],["address","city"]]""")

      val failures = expectedProps.collect {
        case (k, v) if !actualProps.contains(k) => s"$k: MISSING (expected: $v)"
        case (k, v) if actualProps(k) != v => s"$k: expected '$v', got '${actualProps(k)}'"
      }

      assert(failures.isEmpty, failures.mkString("Property mismatches:\n", "\n", ""))
    }
  }

  test("getPropertiesForCreate: clustered table with empty clustering columns") {
    withTempDirAndEngine { case (tablePathUnresolved, engine) =>
      val ucClient = new InMemoryUCClient("ucMetastoreId")
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val tablePath = engine.getFileSystemClient.resolvePath(tablePathUnresolved)

      val snapshot = ucCatalogManagedClient
        .buildCreateTableTransaction(testUcTableId, tablePath, testSchema, "test-engine")
        .withDataLayoutSpec(DataLayoutSpec.clustered(List.empty.asJava))
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable())
        .getPostCommitSnapshot
        .get()
        .asInstanceOf[SnapshotImpl]

      val props = UnityCatalogUtils.getPropertiesForCreate(engine, snapshot).asScala
      assert(props("clusteringColumns") == "[]")
    }
  }
}
