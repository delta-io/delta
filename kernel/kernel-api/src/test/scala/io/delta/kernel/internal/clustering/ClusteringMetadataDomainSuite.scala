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

  private def convertToPhysicalColumn(
      logicalColumns: List[Column],
      schema: StructType): List[Column] = {
    logicalColumns.map { column =>
      ColumnMapping.getPhysicalColumnNameAndDataType(schema, column)._1
    }
  }

  test("ClusteringDomainMetadata could be serialized") {
    val clusteringColumns =
      List(new Column(Array("col1", "`col2,col3`", "`col4.col5`,col6")))
    val clusteringMetadataDomain = ClusteringMetadataDomain.fromPhysicalColumns(
      clusteringColumns.asJava)
    val serializedString = clusteringMetadataDomain.toDomainMetadata.toString
    assert(serializedString ===
      """|DomainMetadata{domain='delta.clustering', configuration=
         |'{"clusteringColumns":[["col1","`col2,col3`","`col4.col5`,col6"]]}',
         | removed='false'}""".stripMargin.replace("\n", ""))
  }

  test("ClusteringDomainMetadata could be deserialized") {
    val configJson = """{"clusteringColumns":[["col1","`col2,col3`","`col4.col5`,col6"]]}"""
    val clusteringMD = ClusteringMetadataDomain.fromJsonConfiguration(configJson)

    assert(clusteringMD.getClusteringColumnsAsColumnList === List(new Column(Array(
      "col1",
      "`col2,col3`",
      "`col4.col5`,col6"))).asJava)
  }

  test("Successfully get DomainMetadata for non-nested columns") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add("name", IntegerType.INTEGER, true)
      .add("age", IntegerType.INTEGER, true)

    val clusterColumns = List(new Column("name"), new Column("age"))
    val physicalColumns = convertToPhysicalColumn(clusterColumns, schema)

    val clusteringMetadataDomain =
      ClusteringMetadataDomain.fromPhysicalColumns(
        physicalColumns.asJava)

    val clusteringDomainMetadata = clusteringMetadataDomain.toDomainMetadata
    assert(clusteringMetadataDomain.getClusteringColumnsAsColumnList == clusterColumns.asJava)
    assert(clusteringDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringDomainMetadata.getConfiguration ==
      """{"clusteringColumns":[["name"],["age"]]}""")
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

    val clusterColumns = List(new Column(Array("user", "address", "city")))
    val physicalColumns = convertToPhysicalColumn(clusterColumns, schema)

    val clusteringMetadataDomain = ClusteringMetadataDomain.fromPhysicalColumns(
      physicalColumns.asJava)

    val clusteringDomainMetadata = clusteringMetadataDomain.toDomainMetadata
    assert(clusteringMetadataDomain.getClusteringColumnsAsColumnList ==
      List(new Column(Array("user", "address", "city"))).asJava)
    assert(clusteringDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringDomainMetadata.getConfiguration ==
      """{"clusteringColumns":[["user","address","city"]]}""")
  }

  test("Correctly maps logical column names to physical column names") {
    val schema = new StructType()
      .add("id", IntegerType.INTEGER, true)
      .add("name", IntegerType.INTEGER, true)

    val metadata =
      ColumnMapping.updateColumnMappingMetadataIfNeeded(
        createMetadata(schema).withColumnMappingEnabled("id"),
        true)

    val clusterColumns = List(new Column("name"))
    val physicalColumns = convertToPhysicalColumn(clusterColumns, metadata.get.getSchema)

    val clusteringMetadataDomain = ClusteringMetadataDomain.fromPhysicalColumns(
      physicalColumns.asJava)

    assert(clusteringMetadataDomain.toDomainMetadata.getDomain == "delta.clustering")
    assert(clusteringMetadataDomain.getClusteringColumnsAsColumnList.asScala.size == 1)
    assert(clusteringMetadataDomain.getClusteringColumnsAsColumnList.get(
      0).getNames()(0).startsWith("col-"))
  }
}
