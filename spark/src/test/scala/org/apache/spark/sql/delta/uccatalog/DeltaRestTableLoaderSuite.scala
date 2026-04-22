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

package org.apache.spark.sql.delta.uccatalog

import java.util.{Arrays => JArr, Collections => JColl, UUID}

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient
import io.unitycatalog.client.delta.model.{
  LoadTableResponse,
  PrimitiveType => UCPrimitiveType,
  StructField => UCStructField,
  StructType => UCStructType,
  TableMetadata,
  TableType => UCTableType
}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

class DeltaRestTableLoaderSuite extends AnyFunSuite {

  test("load() builds V1Table with schema, location, tableType, and DRC properties") {
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000abc")
    val response = buildResponse(
      etag = "etag-123",
      tableUuid = uuid,
      tableType = UCTableType.MANAGED,
      location = "s3://bucket/path/to/table",
      columns = Seq(
        field("id", "long"),
        field("name", "string")),
      properties = Map("delta.enableChangeDataFeed" -> "true"),
      partitionColumns = Seq("id"),
      latestVersion = 17L)

    val client = new StubClient(response, "unity", "default", "t")
    val ident = Identifier.of(Array("default"), "t")

    val v1 = DeltaRestTableLoader.load("unity", ident, client)
    val ct = v1.catalogTable

    assert(ct.identifier.catalog.contains("unity"))
    assert(ct.identifier.database.contains("default"))
    assert(ct.identifier.table === "t")

    assert(ct.tableType === CatalogTableType.MANAGED)
    assert(ct.storage.locationUri.map(_.toString).contains("s3://bucket/path/to/table"))
    assert(ct.schema.fields.length === 2)
    assert(ct.schema.fields(0).dataType === LongType)
    assert(ct.schema.fields(1).dataType === StringType)
    assert(ct.partitionColumnNames === Seq("id"))
    assert(ct.provider.contains("delta"))

    assert(ct.properties("delta.enableChangeDataFeed") === "true")
    assert(ct.properties(DeltaRestTableLoader.PROP_DRC_ETAG) === "etag-123")
    assert(ct.properties(DeltaRestTableLoader.PROP_DRC_TABLE_ID) === uuid.toString)
    assert(ct.properties(DeltaRestTableLoader.PROP_DRC_LATEST_VERSION) === "17")

    assert(client.calls === Seq(("unity", "default", "t")))
  }

  test("load() maps non-MANAGED table type to EXTERNAL") {
    val response = buildResponse(
      tableType = UCTableType.EXTERNAL,
      location = "s3://bucket/p",
      columns = Seq(field("c", "int")),
      properties = Map.empty,
      partitionColumns = Nil)
    val client = new StubClient(response, "cat", "sch", "tbl")
    val v1 = DeltaRestTableLoader.load("cat", Identifier.of(Array("sch"), "tbl"), client)
    assert(v1.catalogTable.tableType === CatalogTableType.EXTERNAL)
    assert(v1.catalogTable.schema.fields.head.dataType === IntegerType)
  }

  test("load() rejects multi-level namespaces (DRC only supports single-level)") {
    val response = buildResponse(columns = Seq(field("c", "int")))
    val client = new StubClient(response, "cat", "s1", "t")
    val ex = intercept[IllegalArgumentException] {
      DeltaRestTableLoader.load("cat", Identifier.of(Array("s1", "s2"), "t"), client)
    }
    assert(ex.getMessage.contains("single-level"))
  }

  test("load() rejects null args") {
    val response = buildResponse()
    val client = new StubClient(response, "c", "s", "t")
    intercept[IllegalArgumentException] {
      DeltaRestTableLoader.load("", Identifier.of(Array("s"), "t"), client)
    }
    intercept[IllegalArgumentException] {
      DeltaRestTableLoader.load("c", null, client)
    }
    intercept[IllegalArgumentException] {
      DeltaRestTableLoader.load("c", Identifier.of(Array("s"), "t"), null)
    }
  }

  test("buildV1Table handles null columns/properties/partition list gracefully") {
    val md = new TableMetadata()
      .etag("e")
      .tableType(UCTableType.MANAGED)
      .location("s3://b/p")
    // columns, properties, partitionColumns left null
    val response = new LoadTableResponse().metadata(md)

    val v1 = DeltaRestTableLoader
      .buildV1Table("cat", Identifier.of(Array("s"), "t"), response)
    assert(v1.catalogTable.schema.fields.isEmpty)
    assert(v1.catalogTable.partitionColumnNames.isEmpty)
    assert(v1.catalogTable.properties(DeltaRestTableLoader.PROP_DRC_ETAG) === "e")
  }

  test("buildV1Table fails loudly when metadata block is absent") {
    val response = new LoadTableResponse()
    val ex = intercept[IllegalStateException] {
      DeltaRestTableLoader
        .buildV1Table("c", Identifier.of(Array("s"), "t"), response)
    }
    assert(ex.getMessage.contains("missing metadata"))
  }

  // Helpers
  private def field(name: String, typeName: String): UCStructField = {
    val f = new UCStructField().name(name).nullable(true)
    val p = new UCPrimitiveType()
    p.setType(typeName)
    f.setType(p)
    f
  }

  private def buildResponse(
      etag: String = "etag-default",
      tableUuid: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555"),
      tableType: UCTableType = UCTableType.MANAGED,
      location: String = "s3://b/p",
      columns: Seq[UCStructField] = Nil,
      properties: Map[String, String] = Map.empty,
      partitionColumns: Seq[String] = Nil,
      latestVersion: Long = 0L): LoadTableResponse = {
    val columnsHolder = new UCStructType()
    columnsHolder.setType("struct")
    columnsHolder.setFields(JArr.asList(columns: _*))

    val md = new TableMetadata()
      .etag(etag)
      .tableUuid(tableUuid)
      .tableType(tableType)
      .location(location)
      .columns(columnsHolder)
      .properties(properties.asJava)
    md.setPartitionColumns(JArr.asList(partitionColumns: _*))

    new LoadTableResponse()
      .metadata(md)
      .commits(JColl.emptyList())
      .latestTableVersion(latestVersion)
  }
}

/** Hand-rolled UCDeltaClient stub -- verifies call shape and returns a canned response. */
private class StubClient(
    response: LoadTableResponse,
    expectedCatalog: String,
    expectedSchema: String,
    expectedTable: String) extends UCDeltaClient {
  val calls = scala.collection.mutable.Buffer.empty[(String, String, String)]
  override def loadTable(catalog: String, schema: String, table: String): LoadTableResponse = {
    calls += ((catalog, schema, table))
    assert(catalog == expectedCatalog)
    assert(schema == expectedSchema)
    assert(table == expectedTable)
    response
  }
}
