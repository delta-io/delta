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
package io.delta.kernel.transaction

import scala.collection.JavaConverters._

import io.delta.kernel.expressions.Column

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for [[DataLayoutSpec]].
 */
class DataLayoutSpecSuite extends AnyFunSuite {

  // Helper methods for creating test columns
  private def cols(names: String*): java.util.List[Column] =
    names.map(new Column(_)).asJava

  test("noDataLayout creates spec with no special layout") {
    val spec = DataLayoutSpec.noDataLayout()

    assert(spec.hasNoDataLayoutSpec())
    assert(!spec.hasPartitioning())
    assert(!spec.hasClustering())
  }

  test("partitioned creates spec with partition columns") {
    val partitionCols = cols("year", "month", "day")
    val spec = DataLayoutSpec.partitioned(partitionCols)

    assert(!spec.hasNoDataLayoutSpec())
    assert(spec.hasPartitioning())
    assert(!spec.hasClustering())
    assert(spec.getPartitionColumns() == partitionCols)
    assert(spec.getPartitionColumnsAsStrings().asScala == Seq("year", "month", "day"))
  }

  test("partitioned throws exception for null columns") {
    val exception = intercept[IllegalArgumentException] {
      DataLayoutSpec.partitioned(null)
    }
    assert(exception.getMessage.contains("Partition columns cannot be null or empty"))
  }

  test("partitioned throws exception for empty columns") {
    val exception = intercept[IllegalArgumentException] {
      DataLayoutSpec.partitioned(List.empty[Column].asJava)
    }
    assert(exception.getMessage.contains("Partition columns cannot be null or empty"))
  }

  test("partitioned throws exception for nested columns") {
    val nestedCol = new Column(Array("struct_col", "nested_field"))
    val exception = intercept[IllegalArgumentException] {
      DataLayoutSpec.partitioned(List(nestedCol).asJava)
    }
    assert(exception.getMessage.contains("Partition columns must be only top-level columns"))
  }

  test("clustered creates spec with clustering columns") {
    val clusteringCols = cols("user_id", "timestamp")
    val spec = DataLayoutSpec.clustered(clusteringCols)

    assert(!spec.hasNoDataLayoutSpec())
    assert(!spec.hasPartitioning())
    assert(spec.hasClustering())
    assert(spec.getClusteringColumns() == clusteringCols)
  }

  test("clustered with empty columns list") {
    val spec = DataLayoutSpec.clustered(List.empty[Column].asJava)

    assert(!spec.hasNoDataLayoutSpec())
    assert(!spec.hasPartitioning())
    assert(spec.hasClustering())
    assert(spec.getClusteringColumns().isEmpty)
  }

  test("clustered throws exception for null columns") {
    val exception = intercept[IllegalArgumentException] {
      DataLayoutSpec.clustered(null)
    }
    assert(exception.getMessage.contains("Clustering columns cannot be null (but can be empty)"))
  }

  test("getPartitionColumns throws exception when partitioning not enabled") {
    val spec = DataLayoutSpec.noDataLayout()
    val exception = intercept[IllegalStateException] {
      spec.getPartitionColumns()
    }
    assert(exception.getMessage.contains(
      "Cannot get partition columns: partitioning is not enabled on this layout"))
  }

  test("getPartitionColumnsAsStrings throws exception when partitioning not enabled") {
    val spec = DataLayoutSpec.noDataLayout()
    val exception = intercept[IllegalStateException] {
      spec.getPartitionColumnsAsStrings()
    }
    assert(exception.getMessage.contains(
      "Cannot get partition columns: partitioning is not enabled on this layout"))
  }

  test("getClusteringColumns throws exception when clustering not enabled") {
    val spec = DataLayoutSpec.noDataLayout()
    val exception = intercept[IllegalStateException] {
      spec.getClusteringColumns()
    }
    assert(exception.getMessage.contains(
      "Cannot get clustering columns: clustering is not enabled on this layout"))
  }
}
