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
package io.delta.kernel.internal

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.test.MockSnapshotUtils
import io.delta.kernel.types.{IntegerType, StringType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class TransactionMetadataFactorySuite extends AnyFunSuite {

  // Test schema for metadata creation
  val testSchema: StructType = new StructType()
    .add("id", IntegerType.INTEGER)
    .add("name", StringType.STRING)
    .add("value", IntegerType.INTEGER)

  val testTablePath = "/test/table/path"

  // =====================================================================
  // buildCreateTableMetadata tests
  // =====================================================================

  test("buildCreateTableMetadata - basic schema without partitions or clustering") {
    val tableProperties = Map("key1" -> "value1", "key2" -> "value2").asJava
    val output = TransactionMetadataFactory.buildCreateTableMetadata(
      testTablePath,
      testSchema,
      tableProperties,
      Optional.empty(), // no partition columns
      Optional.empty() // no clustering columns
    )

    // Verify both metadata and protocol are present for create table
    assert(output.newMetadata.isPresent, "New metadata should be present for create table")
    assert(output.newProtocol.isPresent, "New protocol should be present for create table")
    assert(
      !output.physicalNewClusteringColumns.isPresent,
      "No clustering columns should be present")

    val metadata = output.newMetadata.get()
    assert(metadata.getSchema === testSchema)
    assert(metadata.getConfiguration.asScala === tableProperties.asScala)
  }

  test("buildCreateTableMetadata - with partition columns") {
    val tableProperties = Map("delta.autoOptimize" -> "true").asJava
    val partitionCols = Optional.of(List("name").asJava)

    val output = TransactionMetadataFactory.buildCreateTableMetadata(
      testTablePath,
      testSchema,
      tableProperties,
      partitionCols,
      Optional.empty())

    assert(output.newMetadata.isPresent)
    assert(output.newProtocol.isPresent)
    assert(!output.physicalNewClusteringColumns.isPresent)

    val metadata = output.newMetadata.get()
    assert(metadata.getSchema === testSchema)

    // Verify partition columns are set correctly
    val partitionColumns = metadata.getPartitionColumns
    assert(partitionColumns.getSize === 1)
  }

  test("buildCreateTableMetadata - with clustering columns") {
    val tableProperties = Map("delta.feature.clustering" -> "supported").asJava
    val clusteringCols = Optional.of(List(new Column("name")).asJava)

    val output = TransactionMetadataFactory.buildCreateTableMetadata(
      testTablePath,
      testSchema,
      tableProperties,
      Optional.empty(), // no partition columns
      clusteringCols)

    assert(output.newMetadata.isPresent)
    assert(output.newProtocol.isPresent)
    assert(output.physicalNewClusteringColumns.isPresent &&
      output.physicalNewClusteringColumns.get.size == 1)
  }

  test("buildCreateTableMetadata - should reject both partition and clustering columns") {
    val tableProperties = Map.empty[String, String].asJava
    val partitionCols = Optional.of(List("name").asJava)
    val clusteringCols = Optional.of(List(new Column("value")).asJava)

    assertThrows[IllegalArgumentException] {
      TransactionMetadataFactory.buildCreateTableMetadata(
        testTablePath,
        testSchema,
        tableProperties,
        partitionCols,
        clusteringCols)
    }
  }

  // =====================================================================
  // buildReplaceTableMetadata tests
  // =====================================================================

  test("buildReplaceTableMetadata - basic replacement") {
    val newTableProperties = Map("newKey" -> "newValue").asJava

    // Create a mock snapshot for the existing table
    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 1L)

    val output = TransactionMetadataFactory.buildReplaceTableMetadata(
      testTablePath,
      mockSnapshot,
      testSchema,
      newTableProperties,
      Optional.empty(), // no partition columns
      Optional.empty() // no clustering columns
    )

    assert(output.newMetadata.isPresent, "New metadata should be present for replace table")
    assert(output.newProtocol.isPresent, "New protocol should be present for replace table")
    assert(!output.physicalNewClusteringColumns.isPresent)

    val metadata = output.newMetadata.get()
    assert(metadata.getSchema === testSchema)
  }

  test("buildReplaceTableMetadata - with partition columns") {
    val newTableProperties = Map("delta.autoOptimize" -> "false").asJava
    val partitionCols = Optional.of(List("id").asJava)

    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 2L)

    val output = TransactionMetadataFactory.buildReplaceTableMetadata(
      testTablePath,
      mockSnapshot,
      testSchema,
      newTableProperties,
      partitionCols,
      Optional.empty())

    assert(output.newMetadata.isPresent)
    assert(output.newProtocol.isPresent)

    val metadata = output.newMetadata.get()
    assert(metadata.getSchema === testSchema)
  }

  test("buildReplaceTableMetadata - with clustering columns") {
    val newTableProperties = Map("delta.feature.clustering" -> "supported").asJava
    val clusteringCols = Optional.of(List(new Column("name"), new Column("value")).asJava)

    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 3L)

    val output = TransactionMetadataFactory.buildReplaceTableMetadata(
      testTablePath,
      mockSnapshot,
      testSchema,
      newTableProperties,
      Optional.empty(), // no partition columns
      clusteringCols)

    assert(output.newMetadata.isPresent, "New metadata should be present for replace table")
    assert(output.newProtocol.isPresent, "New protocol should be present for replace table")
    assert(output.physicalNewClusteringColumns.isPresent, "Clustering columns should be resolved")

    val metadata = output.newMetadata.get()
    assert(metadata.getSchema === testSchema)

    // Verify clustering columns are resolved
    val clusteringColumns = output.physicalNewClusteringColumns.get()
    assert(clusteringColumns.size() === 2, "Should have 2 clustering columns")
  }

  test("buildReplaceTableMetadata - should reject both partition and clustering columns") {
    val newTableProperties = Map.empty[String, String].asJava
    val partitionCols = Optional.of(List("name").asJava)
    val clusteringCols = Optional.of(List(new Column("value")).asJava)

    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 1L)

    assertThrows[IllegalArgumentException] {
      TransactionMetadataFactory.buildReplaceTableMetadata(
        testTablePath,
        mockSnapshot,
        testSchema,
        newTableProperties,
        partitionCols,
        clusteringCols)
    }
  }

  // =====================================================================
  // buildUpdateTableMetadata tests
  // =====================================================================

  test("buildUpdateTableMetadata - no changes") {
    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 3L)

    val output = TransactionMetadataFactory.buildUpdateTableMetadata(
      testTablePath,
      mockSnapshot,
      Optional.empty(), // no properties added
      Optional.empty(), // no properties removed
      Optional.empty(), // no schema change
      Optional.empty() // no clustering columns
    )

    // With no changes, no new metadata or protocol should be present
    assert(!output.newMetadata.isPresent, "No new metadata should be present when no changes")
    assert(!output.newProtocol.isPresent, "No new protocol should be present when no changes")
    assert(!output.physicalNewClusteringColumns.isPresent)
  }

  test("buildUpdateTableMetadata - add table properties") {
    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 4L)

    val newProperties = Map("newKey" -> "newValue", "anotherKey" -> "anotherValue").asJava
    val output = TransactionMetadataFactory.buildUpdateTableMetadata(
      testTablePath,
      mockSnapshot,
      Optional.of(newProperties),
      Optional.empty(),
      Optional.empty(),
      Optional.empty())

    // Properties change should trigger new metadata
    assert(output.newMetadata.isPresent, "New metadata should be present when properties change")

    val metadata = output.newMetadata.get()
    // Verify the new properties are merged
    assert(metadata.getConfiguration.containsKey("newKey"))
    assert(metadata.getConfiguration.get("newKey") === "newValue")
  }

  test("buildUpdateTableMetadata - with clustering columns") {
    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 9L)

    val clusteringColumns = Optional.of(List(new Column("name")).asJava)
    val output = TransactionMetadataFactory.buildUpdateTableMetadata(
      testTablePath,
      mockSnapshot,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      clusteringColumns)

    assert(output.physicalNewClusteringColumns.isPresent &&
      output.physicalNewClusteringColumns.get.size == 1)
  }

  test("buildUpdateTableMetadata - overlapping set and unset properties should fail") {
    val mockSnapshot = MockSnapshotUtils.getMockSnapshot(
      new Path(testTablePath),
      latestVersion = 7L)

    val propertiesToAdd = Map("conflictKey" -> "newValue").asJava
    val propertyKeysToRemove = Set("conflictKey").asJava

    assertThrows[Exception] {
      TransactionMetadataFactory.buildUpdateTableMetadata(
        testTablePath,
        mockSnapshot,
        Optional.of(propertiesToAdd),
        Optional.of(propertyKeysToRemove),
        Optional.empty(),
        Optional.empty())
    }
  }
}
