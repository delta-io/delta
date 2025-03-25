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
package io.delta.kernel.internal.clustering

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.util.{ColumnMapping, ColumnMappingSuiteBase, VectorUtils}
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ClusteringMetadataDomainSuite
    extends AnyFunSuite
    with ColumnMappingSuiteBase {

  private def createMetadata(schema: StructType): Metadata = new Metadata(
    UUID.randomUUID.toString,
    Optional.empty(),
    Optional.empty(),
    new Format,
    schema.toJson,
    schema,
    VectorUtils.buildArrayValue(Collections.emptyList(), StringType.STRING),
    Optional.empty(),
    VectorUtils.stringStringMapValue(Collections.emptyMap()))

  test("Successfully get DomainMetadata for non-nested columns") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add("name", IntegerType.INTEGER, true)
      .add("age", IntegerType.INTEGER, true)

    val clusterColumns = List(new Column("name"), new Column("age"))
    val clusteringMetadataDomain =
      new ClusteringMetadataDomain(
        clusterColumns.asJava,
        schema)

    val clusteringDomainMetadata = clusteringMetadataDomain.toDomainMetadata
    assert(clusteringMetadataDomain.getClusteringColumns == clusterColumns.asJava)
    assert(clusteringDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringDomainMetadata.getConfiguration ==
      "{\"clusteringColumns\":[[\"name\"],[\"age\"]]}")
  }

  test("Successfully get DomainMetadata for nested columns") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add(
        "user",
        new StructType()
          .add(
            "address",
            new StructType()
              .add("city", StringType.STRING, true)))

    val clusteringMetadataDomain =
      new ClusteringMetadataDomain(
        List(new Column(Array("user", "address", "city"))).asJava,
        schema)

    val clusteringDomainMetadata = clusteringMetadataDomain.toDomainMetadata
    assert(clusteringMetadataDomain.getClusteringColumns ==
      List(new Column(Array("user", "address", "city"))).asJava)
    assert(clusteringDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringDomainMetadata.getConfiguration ==
      "{\"clusteringColumns\":[[\"user\",\"address\",\"city\"]]}")
  }

  test("Correctly maps logical column names to physical column names") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add("name", IntegerType.INTEGER, true)

    val metadata =
      ColumnMapping.updateColumnMappingMetadataIfNeeded(
        createMetadata(schema).withColumnMappingEnabled("id"),
        true)

    val clusteringMetadata =
      new ClusteringMetadataDomain(List(new Column("name")).asJava, metadata.get.getSchema)

    assert(clusteringMetadata.toDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringMetadata.getClusteringColumns.asScala.size == 1)
    assert(clusteringMetadata.getClusteringColumns.get(0).getNames()(0).startsWith("col-"))
  }
}
