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

  test("Correctly maps logical column names to physical column names") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add("name", IntegerType.INTEGER, true)

    val metadata =
      ColumnMapping.updateColumnMappingMetadataIfNeeded(
        createMetadata(schema).withColumnMappingEnabled("id"),
        true)

    val clusteringMetadata =
      new ClusteringMetadataDomain(List("name").asJava, metadata.get.getSchema)

    assert(clusteringMetadata.toDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringMetadata.getClusteringColumns.asScala == List("name"))
  }

  test("Correctly maps nested logical column names to physical column names") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add(
        "user",
        new StructType()
          .add(
            "address",
            new StructType()
              .add("city", StringType.STRING, true)))

    val clusteringMetadata =
      new ClusteringMetadataDomain(List("user.address.city").asJava, schema)

    assert(clusteringMetadata.toDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringMetadata.getClusteringColumns ==
      List(List("user", "address", "city").asJava).asJava)
    assert(clusteringMetadata.toDomainMetadata.getConfiguration ==
      "{\"clusteringColumns\":[[\"user\",\"address\",\"city\"]]}")
  }
}
