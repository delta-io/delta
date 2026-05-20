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

import java.net.URI
import java.util
import java.util.{Collections, Optional, UUID}

import io.delta.storage.commit.{Commit, GetCommitsResponse, TableIdentifier => StorageTableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCDeltaClient, UCDeltaModels}
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.{DeltaProtocol, StagingTableInfo, TableInfo, TableType => UcTableType}
import io.delta.storage.commit.uccommitcoordinator.exceptions.CredentialFetchFailedException
import io.delta.storage.commit.uniform.UniformMetadata

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{Identifier, Table, V1Table}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
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

  test("deltaRestApi.enabled=false leaves deltaCatalogClient empty") {
    val catalog = new AbstractDeltaCatalog
    catalog.initialize("test_cat", options())
    assert(catalog.deltaCatalogClient.isEmpty,
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
    assert(catalog.deltaCatalogClient.isDefined)
  }

  test("deltaRestApi.enabled=true with uri+token constructs the Delta REST API client") {
    val catalog = new AbstractDeltaCatalog
    catalog.initialize("test_cat",
      options("deltaRestApi.enabled" -> "true", "uri" -> "http://uc", "token" -> "tok"))
    val client = catalog.deltaCatalogClient.getOrElse(
      fail("Delta REST API client should be constructed when the catalog opts in"))
    assert(client.isInstanceOf[UCDeltaCatalogClientImpl],
      s"Delta REST API client should be UCDeltaCatalogClientImpl, was ${client.getClass}")
  }

  test("AbstractDeltaCatalogClient.fromCatalogOptionsIfEnabled returns None when flag is off") {
    val result = AbstractDeltaCatalogClient.fromCatalogOptionsIfEnabled(
      "test_cat", options(), noFallback)
    assert(result.isEmpty)
  }

  test("AbstractDeltaCatalogClient.fromCatalogOptionsIfEnabled returns Some when flag is on") {
    val result = AbstractDeltaCatalogClient.fromCatalogOptionsIfEnabled(
      "test_cat",
      options("deltaRestApi.enabled" -> "true", "uri" -> "http://uc", "token" -> "tok"),
      noFallback)
    assert(result.isDefined)
  }

  private val noFallback: Identifier => Table =
    _ => throw new UnsupportedOperationException("fallback not expected in this test")

  test("loadTable converts TableInfo to V1Table with catalog-supplied fields") {
    val tableId = UUID.randomUUID()
    val metadata = new AbstractMetadata {
      override def getId: String = null
      override def getName: String = "tbl"
      override def getDescription: String = "a test table"
      override def getProvider: String = "DELTA"
      override def getFormatOptions: util.Map[String, String] = Collections.emptyMap()
      override def getSchemaString: String =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"""
      override def getPartitionColumns: util.List[String] = Collections.emptyList()
      override def getConfiguration: util.Map[String, String] =
        util.Map.of("ucTableId", tableId.toString, "delta.feature.x", "supported")
      override def getCreatedTime: java.lang.Long = 42L
    }
    val info = new TableInfo(
      tableId,
      UCDeltaModels.TableType.EXTERNAL,
      "s3://bucket/table",
      metadata,
      util.Map.of("fs.s3a.access.key", "key"))

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
    assert(merged.get("ucTableId") === Some(tableId.toString))
    assert(merged.get("fs.s3a.access.key") === Some("key"))
  }

  test("loadTable falls back to SSP on CredentialFetchFailedException when SSP is enabled") {
    val tableId = UUID.randomUUID()
    val metadata = new AbstractMetadata {
      override def getId: String = null
      override def getName: String = "tbl"
      override def getDescription: String = null
      override def getProvider: String = "DELTA"
      override def getFormatOptions: util.Map[String, String] = Collections.emptyMap()
      override def getSchemaString: String =
        """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}"""
      override def getPartitionColumns: util.List[String] = Collections.emptyList()
      // No credential properties; this is the "without credentials" TableInfo.
      override def getConfiguration: util.Map[String, String] = Collections.emptyMap()
      override def getCreatedTime: java.lang.Long = 0L
    }
    val tableInfoNoCreds = new TableInfo(
      tableId,
      UCDeltaModels.TableType.EXTERNAL,
      "s3://bucket/no-creds-table",
      metadata,
      Collections.emptyMap()) // no storage properties either
    val credEx = new CredentialFetchFailedException(
      "creds exhausted", new RuntimeException("simulated"), tableInfoNoCreds)

    val client = new UCDeltaCatalogClientImpl(
      catalogName = "main",
      ucClient = new StubUCDeltaClient(throw credEx),
      serverSidePlanningEnabled = true)

    // Capture and restore the SSP conf so this test doesn't leak into others.
    val sspKey = DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key
    val originalSsp = spark.conf.getOption(sspKey)
    spark.conf.unset(sspKey)
    try {
      val table = client.loadTable(Identifier.of(Array("sch"), "tbl"))
      val v1 = table.asInstanceOf[V1Table].catalogTable
      assert(v1.identifier.table === "tbl")
      assert(v1.storage.locationUri.map(_.toString) === Some("s3://bucket/no-creds-table"))
      assert(v1.storage.properties.isEmpty,
        s"no credentials should be set on the V1Table; got ${v1.storage.properties}")
      // The fallback path must have flipped SSP on.
      assert(spark.conf.get(sspKey) === "true",
        "Server-side planning conf should be set after CredentialFetchFailedException fallback")
    } finally {
      originalSsp match {
        case Some(value) => spark.conf.set(sspKey, value)
        case None => spark.conf.unset(sspKey)
      }
    }
  }

  test("loadTable without serverSidePlanningEnabled rethrows CredentialFetchFailedException") {
    val ex = new CredentialFetchFailedException(
      "creds exhausted", new RuntimeException("simulated"), null)
    val client = new UCDeltaCatalogClientImpl(
      catalogName = "main",
      ucClient = new StubUCDeltaClient(throw ex),
      serverSidePlanningEnabled = false)
    val thrown = intercept[CredentialFetchFailedException] {
      client.loadTable(Identifier.of(Array("sch"), "tbl"))
    }
    assert(thrown eq ex)
  }
}

/**
 * Returns the result of {@code loadTableResult} (a by-name parameter) from
 * {@code loadTable}; throws on every other method. Pass a [[TableInfo]] to get a successful
 * load, or {@code throw new ...} to simulate UC-side failures.
 *
 * <p>Because {@code loadTableResult} is by-name, the body re-evaluates on every
 * {@code loadTable} invocation: a {@code throw} expression re-throws each time; a
 * {@link TableInfo} reference is rebound (cheap). For tests that need to vary the result
 * across calls, replace this with a {@code Supplier}-shaped constructor.
 */
private class StubUCDeltaClient(loadTableResult: => TableInfo) extends UCDeltaClient {
  override def getMetastoreId(): String = throw new UnsupportedOperationException
  override def loadTable(tableIdentifier: StorageTableIdentifier): TableInfo = loadTableResult
  override def createStagingTable(
      catalog: String, schema: String, table: String): StagingTableInfo =
    throw new UnsupportedOperationException
  override def createTable(
      catalog: String,
      schema: String,
      name: String,
      location: String,
      tableType: UcTableType,
      comment: String,
      partitionColumns: util.List[String],
      protocol: DeltaProtocol,
      properties: util.Map[String, String]): AbstractMetadata =
    throw new UnsupportedOperationException
  override def commit(
      tableId: String,
      tableUri: URI,
      tableIdentifier: StorageTableIdentifier,
      commit: Optional[Commit],
      lastKnownBackfilledVersion: Optional[java.lang.Long],
      oldMetadata: Optional[AbstractMetadata],
      newMetadata: Optional[AbstractMetadata],
      oldProtocol: Optional[AbstractProtocol],
      newProtocol: Optional[AbstractProtocol],
      uniform: Optional[UniformMetadata]): Unit =
    throw new UnsupportedOperationException
  override def getCommits(
      tableId: String,
      tableUri: URI,
      tableIdentifier: StorageTableIdentifier,
      startVersion: Optional[java.lang.Long],
      endVersion: Optional[java.lang.Long]): GetCommitsResponse =
    throw new UnsupportedOperationException
  override def finalizeCreate(
      tableName: String,
      catalogName: String,
      schemaName: String,
      storageLocation: String,
      columns: util.List[UCClient.ColumnDef],
      properties: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException
  override def close(): Unit = ()
}
