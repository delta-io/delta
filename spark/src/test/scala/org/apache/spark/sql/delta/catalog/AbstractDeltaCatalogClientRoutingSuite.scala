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

package org.apache.spark.sql.delta.catalog

import java.util

import io.delta.storage.commit.{TableIdentifier => StorageTableIdentifier}
import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCDeltaModels}
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.{StagingTableInfo, TableInfo}
import io.delta.storage.commit.actions.AbstractMetadata

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{Identifier, V1Table}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Unit tests for the Delta REST API client wiring on [[AbstractDeltaCatalog]]. These verify
 * only the catalog-option / initialize plumbing and the loadTable dispatch decision; they do
 * not require a UC server.
 */
class AbstractDeltaCatalogClientRoutingSuite extends QueryTest with DeltaSQLCommandTest {

  private def options(kv: (String, String)*): CaseInsensitiveStringMap = {
    val m = new util.HashMap[String, String]()
    kv.foreach { case (k, v) => m.put(k, v) }
    new CaseInsensitiveStringMap(m)
  }

  test("deltaRestApi.enabled=false leaves deltaCatalogClient null") {
    val catalog = new AbstractDeltaCatalog
    catalog.initialize("test_cat", options())
    assert(catalog.deltaCatalogClient == null,
      "Delta REST API client should not be constructed when the catalog opts out")
  }

  test("deltaRestApi.enabled=true requires uri") {
    val catalog = new AbstractDeltaCatalog
    val e = intercept[IllegalArgumentException] {
      catalog.initialize("test_cat", options("deltaRestApi.enabled" -> "true"))
    }
    assert(e.getMessage.contains("'uri' is required"))
  }

  test("deltaRestApi.enabled=true requires an auth configuration") {
    val catalog = new AbstractDeltaCatalog
    val e = intercept[IllegalArgumentException] {
      catalog.initialize("test_cat",
        options("deltaRestApi.enabled" -> "true", "uri" -> "http://uc"))
    }
    assert(e.getMessage.contains("auth configuration is required"))
  }

  test("auth.* options are passed through to TokenProvider (new format)") {
    val catalog = new AbstractDeltaCatalog
    catalog.initialize("test_cat",
      options(
        "deltaRestApi.enabled" -> "true",
        "uri" -> "http://uc",
        "auth.type" -> "static",
        "auth.token" -> "tok"))
    assert(catalog.deltaCatalogClient != null)
  }

  test("deltaRestApi.enabled=true with uri+token constructs the Delta REST API client") {
    val catalog = new AbstractDeltaCatalog
    catalog.initialize("test_cat",
      options("deltaRestApi.enabled" -> "true", "uri" -> "http://uc", "token" -> "tok"))
    assert(catalog.deltaCatalogClient != null,
      "Delta REST API client should be constructed when the catalog opts in")
    assert(catalog.deltaCatalogClient.isInstanceOf[UCDeltaCatalogClientImpl],
      s"Delta REST API client should be UCDeltaCatalogClientImpl, " +
        s"was ${catalog.deltaCatalogClient.getClass}")
  }

  test("fromCatalogOptionsIfEnabled returns null when the flag is off") {
    val result = UCDeltaCatalogClientImpl.fromCatalogOptionsIfEnabled("test_cat", options())
    assert(result == null)
  }

  test("fromCatalogOptionsIfEnabled returns non-null when the flag is on") {
    val result = UCDeltaCatalogClientImpl.fromCatalogOptionsIfEnabled(
      "test_cat",
      options("deltaRestApi.enabled" -> "true", "uri" -> "http://uc", "token" -> "tok"))
    assert(result != null)
  }

  test("loadTable converts TableInfo to V1Table with catalog-supplied fields") {
    val metadata = new AbstractMetadata {
      override def getId: String = null
      override def getName: String = "tbl"
      override def getDescription: String = "a test table"
      override def getProvider: String = "DELTA"
      override def getFormatOptions: java.util.Map[String, String] =
        java.util.Collections.emptyMap()
      override def getSchemaString: String =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"""
      override def getPartitionColumns: java.util.List[String] = java.util.Collections.emptyList()
      override def getConfiguration: java.util.Map[String, String] =
        java.util.Map.of("ucTableId", "uuid-1", "delta.feature.x", "supported")
      override def getCreatedTime: java.lang.Long = 42L
    }
    val info = new TableInfo(
      "uuid-1",
      UCDeltaModels.TableType.EXTERNAL,
      "s3://bucket/table",
      metadata,
      java.util.Map.of("fs.s3a.access.key", "key"))

    val client = new UCDeltaCatalogClientImpl(
      catalogName = "main",
      ucClient = new StubUCDeltaClient(info))

    val table = client.loadTable(Identifier.of(Array("sch"), "tbl"))
    val v1 = table.asInstanceOf[V1Table].catalogTable
    assert(v1.identifier.table === "tbl")
    assert(v1.identifier.database === Some("sch"))
    assert(v1.identifier.catalog === Some("main"))
    assert(v1.tableType === CatalogTableType.EXTERNAL)
    assert(v1.storage.locationUri.map(_.toString) === Some("s3://bucket/table"))
    assert(v1.provider === Some("delta"))
    assert(v1.comment === Some("a test table"))
    assert(v1.createTime === 42L)
    assert(!v1.tracksPartitionsInCatalog)
    assert(v1.schema.fieldNames.toSeq === Seq("id"))
    val merged = v1.storage.properties
    assert(merged.get("ucTableId") === Some("uuid-1"))
    assert(merged.get("fs.s3a.access.key") === Some("key"))
  }
}

/** Returns a fixed [[TableInfo]] from {@code loadTable}; throws elsewhere. */
private class StubUCDeltaClient(info: TableInfo) extends UCDeltaClient {
  override def getMetastoreId(): String = throw new UnsupportedOperationException
  override def loadTable(tableIdentifier: StorageTableIdentifier): TableInfo = info
  override def createStagingTable(
      catalog: String, schema: String, table: String): StagingTableInfo =
    throw new UnsupportedOperationException
  override def createTable(
      catalog: String,
      schema: String,
      name: String,
      location: String,
      tableType: UCDeltaModels.TableType,
      comment: String,
      partitionColumns: java.util.List[String],
      protocol: UCDeltaModels.DeltaProtocol,
      properties: java.util.Map[String, String]): AbstractMetadata =
    throw new UnsupportedOperationException
  override def commit(
      tableId: String,
      tableUri: java.net.URI,
      tableIdentifier: io.delta.storage.commit.TableIdentifier,
      commit: java.util.Optional[io.delta.storage.commit.Commit],
      lastKnownBackfilledVersion: java.util.Optional[java.lang.Long],
      oldMetadata: java.util.Optional[AbstractMetadata],
      newMetadata: java.util.Optional[AbstractMetadata],
      oldProtocol: java.util.Optional[io.delta.storage.commit.actions.AbstractProtocol],
      newProtocol: java.util.Optional[io.delta.storage.commit.actions.AbstractProtocol],
      uniform: java.util.Optional[io.delta.storage.commit.uniform.UniformMetadata]): Unit =
    throw new UnsupportedOperationException
  override def getCommits(
      tableId: String,
      tableUri: java.net.URI,
      startVersion: java.util.Optional[java.lang.Long],
      endVersion: java.util.Optional[java.lang.Long]): io.delta.storage.commit.GetCommitsResponse =
    throw new UnsupportedOperationException
  override def finalizeCreate(
      tableName: String,
      catalogName: String,
      schemaName: String,
      storageLocation: String,
      columns: java.util.List[io.delta.storage.commit.uccommitcoordinator.UCClient.ColumnDef],
      properties: java.util.Map[String, String]): Unit =
    throw new UnsupportedOperationException
  override def close(): Unit = ()
}
