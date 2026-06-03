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

import scala.jdk.CollectionConverters._

import io.delta.storage.commit.{Commit, GetCommitsResponse, TableIdentifier => StorageTableIdentifier}
import io.delta.storage.commit.actions.{AbstractDomainMetadata, AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCDeltaClient, UCDeltaModels}
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.{DeltaProtocol, StagingTableInfo, TableInfo, TableType => UcTableType}
import io.delta.storage.commit.uccommitcoordinator.exceptions.{CredentialFetchFailedException, NoSuchTableException => StorageNoSuchTableException}
import io.delta.storage.commit.uniform.{IcebergMetadata, UniformMetadata}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.delta.IcebergConstants
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Unit tests for the UC Delta API client wiring on [[AbstractDeltaCatalog]]. These verify
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
      "UC Delta API client should not be constructed when the catalog opts out")
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

  test("deltaRestApi.enabled=true with uri+token constructs the UC Delta API client") {
    val catalog = new AbstractDeltaCatalog
    catalog.initialize("test_cat",
      options("deltaRestApi.enabled" -> "true", "uri" -> "http://uc", "token" -> "tok"))
    val client = catalog.deltaCatalogClient.getOrElse(
      fail("UC Delta API client should be constructed when the catalog opts in"))
    assert(client.isInstanceOf[UCDeltaCatalogClientImpl],
      s"UC Delta API client should be UCDeltaCatalogClientImpl, was ${client.getClass}")
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

  /**
   * createStagingTable contract tests. The contract: only fresh Delta CREATE / CTAS requests
   * with raw caller-supplied properties may reach this client; upstream routing filters out
   * external / REPLACE-existing / path-based requests. If any "already-prepared" marker
   * (LOCATION / IS_MANAGED_LOCATION / EXTERNAL / path-based ident) is seen here, the impl
   * fails loudly as defense-in-depth.
   */
  private def newRecordingClient(
      requiredProtocol: UCDeltaModels.DeltaProtocol = null,
      suggestedProtocol: UCDeltaModels.DeltaProtocol = null,
      requiredProperties: util.Map[String, String] = java.util.Collections.emptyMap(),
      suggestedProperties: util.Map[String, String] = java.util.Collections.emptyMap()
  ): (UCDeltaCatalogClientImpl, RecordingStubUCDeltaClient) = {
    val stub = new RecordingStubUCDeltaClient(
      new StagingTableInfo(
        /* tableId = */ UUID.fromString("00000000-0000-0000-0000-000000000001"),
        /* tableType = */ UCDeltaModels.TableType.MANAGED,
        /* location = */ "s3://bucket/staging/tbl",
        requiredProtocol,
        suggestedProtocol,
        requiredProperties,
        suggestedProperties,
        /* storageProperties = */ java.util.Map.of("fs.s3a.access.key", "stage-key")))
    val client = new UCDeltaCatalogClientImpl(catalogName = "main", ucClient = stub)
    (client, stub)
  }

  private def protocol(
      minReader: Int = 0,
      minWriter: Int = 0,
      readerFeatures: Seq[String] = Nil,
      writerFeatures: Seq[String] = Nil): UCDeltaModels.DeltaProtocol = {
    val p = new UCDeltaModels.DeltaProtocol()
    if (minReader > 0) p.minReaderVersion(minReader)
    if (minWriter > 0) p.minWriterVersion(minWriter)
    if (readerFeatures.nonEmpty) p.readerFeatures(readerFeatures.asJava)
    if (writerFeatures.nonEmpty) p.writerFeatures(writerFeatures.asJava)
    p
  }

  private def javaMap(kv: (String, String)*): util.Map[String, String] = {
    val m = new util.HashMap[String, String]()
    kv.foreach { case (k, v) => m.put(k, v) }
    m
  }

  private def stageProps(kv: (String, String)*): util.Map[String, String] = {
    val m = new util.HashMap[String, String]()
    kv.foreach { case (k, v) => m.put(k, v) }
    m
  }

  test("createStagingTable: managed Delta create on the new path stages + augments props") {
    val (client, stub) = newRecordingClient()
    val ident = Identifier.of(Array("sch"), "tbl")
    val out = client.createStagingTable(ident, stageProps("delta.catalogManaged" -> "supported"))
    assert(stub.stagedRequests.size === 1, "should call UC createStagingTable exactly once")
    assert(stub.stagedRequests.head === ("main", "sch", "tbl"))
    assert(out.get(TableCatalog.PROP_LOCATION) === "s3://bucket/staging/tbl")
    assert(out.get(TableCatalog.PROP_IS_MANAGED_LOCATION) === "true")
    assert(out.get("fs.s3a.access.key") === "stage-key",
      "credential properties must be copied onto the bare key")
    assert(out.get(TableCatalog.OPTION_PREFIX + "fs.s3a.access.key") === "stage-key",
      "credential properties must also be copied onto the `option.`-prefixed key so they " +
        "are picked up as writeOptions downstream")
  }

  test("createStagingTable: PROP_LOCATION already set throws") {
    val (client, stub) = newRecordingClient()
    val ident = Identifier.of(Array("sch"), "tbl")
    val in = stageProps(
      "delta.catalogManaged" -> "supported",
      TableCatalog.PROP_LOCATION -> "s3://already/staged")
    val ex = intercept[IllegalStateException](client.createStagingTable(ident, in))
    assert(ex.getMessage.contains(TableCatalog.PROP_LOCATION))
    assert(stub.stagedRequests.isEmpty, "must not stage when failing the invariant check")
  }

  test("createStagingTable: PROP_IS_MANAGED_LOCATION already set throws") {
    val (client, stub) = newRecordingClient()
    val ident = Identifier.of(Array("sch"), "tbl")
    val in = stageProps(
      "delta.catalogManaged" -> "supported",
      TableCatalog.PROP_IS_MANAGED_LOCATION -> "true")
    val ex = intercept[IllegalStateException](client.createStagingTable(ident, in))
    assert(ex.getMessage.contains(TableCatalog.PROP_IS_MANAGED_LOCATION))
    assert(stub.stagedRequests.isEmpty)
  }

  test("createStagingTable: PROP_EXTERNAL set throws") {
    val (client, stub) = newRecordingClient()
    val ident = Identifier.of(Array("sch"), "tbl")
    val in = stageProps(TableCatalog.PROP_EXTERNAL -> "true")
    val ex = intercept[IllegalStateException](client.createStagingTable(ident, in))
    assert(ex.getMessage.contains(TableCatalog.PROP_EXTERNAL))
    assert(stub.stagedRequests.isEmpty)
  }

  // -------------------------------------------------------------------------
  // createStagingTable: UC-returned required/suggested protocol + properties merging
  // -------------------------------------------------------------------------

  test("createStagingTable: required properties are merged into augmented props") {
    val (client, _) = newRecordingClient(
      requiredProperties = javaMap("delta.enableInCommitTimestamps" -> "true"))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.catalogManaged" -> "supported"))
    assert(out.get("delta.enableInCommitTimestamps") === "true")
    assert(out.get("delta.catalogManaged") === "supported", "user props must survive merging")
  }

  test("createStagingTable: required property matching user value is fine (no throw)") {
    val (client, _) = newRecordingClient(
      requiredProperties = javaMap("delta.enableInCommitTimestamps" -> "true"))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps(
        "delta.catalogManaged" -> "supported",
        "delta.enableInCommitTimestamps" -> "true"))
    assert(out.get("delta.enableInCommitTimestamps") === "true")
  }

  test("createStagingTable: required property conflicting with user value throws") {
    val (client, _) = newRecordingClient(
      requiredProperties = javaMap("delta.enableInCommitTimestamps" -> "true"))
    val ident = Identifier.of(Array("sch"), "tbl")
    val e = intercept[IllegalArgumentException] {
      client.createStagingTable(ident, stageProps(
        "delta.catalogManaged" -> "supported",
        "delta.enableInCommitTimestamps" -> "false"))
    }
    assert(e.getMessage.contains("'delta.enableInCommitTimestamps'='true'"),
      "error must name the required key and value")
    assert(e.getMessage.contains("'delta.enableInCommitTimestamps'='false'"),
      "error must name the caller-supplied conflicting value")
  }

  test("createStagingTable: suggested property is applied when caller hasn't set it") {
    val (client, _) = newRecordingClient(
      suggestedProperties = javaMap("delta.someSuggested" -> "ucs-val"))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.catalogManaged" -> "supported"))
    assert(out.get("delta.someSuggested") === "ucs-val")
  }

  test("createStagingTable: suggested property defers to caller when both set") {
    val (client, _) = newRecordingClient(
      suggestedProperties = javaMap("delta.someSuggested" -> "ucs-val"))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps(
        "delta.catalogManaged" -> "supported",
        "delta.someSuggested" -> "caller-val"))
    assert(out.get("delta.someSuggested") === "caller-val",
      "user TBLPROPERTIES must win over a suggested value")
  }

  test("createStagingTable: required protocol features are encoded as delta.feature.*") {
    val (client, _) = newRecordingClient(
      requiredProtocol = protocol(
        minReader = 3, minWriter = 7,
        readerFeatures = Seq("deletionVectors"),
        writerFeatures = Seq("catalogManaged", "rowTracking")))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.catalogManaged" -> "supported"))
    assert(out.get("delta.feature.deletionVectors") === "supported")
    assert(out.get("delta.feature.catalogManaged") === "supported")
    assert(out.get("delta.feature.rowTracking") === "supported")
    // Min reader/writer versions are intentionally not emitted -- Delta derives them from
    // the feature list. Pinning them here would tell Delta we're explicitly in table-
    // features mode and suppress legacy writer features (appendOnly, invariants) in the
    // resulting protocol.
    assert(!out.containsKey("delta.minReaderVersion"))
    assert(!out.containsKey("delta.minWriterVersion"))
  }

  test("createStagingTable: suggested protocol features known to Delta are applied") {
    val (client, _) = newRecordingClient(
      suggestedProtocol = protocol(
        readerFeatures = Seq("columnMapping"),
        writerFeatures = Seq("domainMetadata")))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.catalogManaged" -> "supported"))
    assert(out.get("delta.feature.columnMapping") === "supported")
    assert(out.get("delta.feature.domainMetadata") === "supported")
  }

  test("createStagingTable: suggested protocol feature unknown to Delta is silently skipped") {
    // Capability negotiation: UC may suggest a feature this Delta version hasn't shipped.
    // We just don't apply it -- the suggestion is opt-in, so skipping is graceful.
    val (client, _) = newRecordingClient(
      suggestedProtocol = protocol(writerFeatures = Seq("someFutureUnknownFeature")))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.catalogManaged" -> "supported"))
    assert(!out.containsKey("delta.feature.someFutureUnknownFeature"))
  }

  test("createStagingTable: required protocol feature unknown to Delta throws") {
    val (client, _) = newRecordingClient(
      requiredProtocol = protocol(writerFeatures = Seq("someFutureUnknownFeature")))
    val e = intercept[IllegalArgumentException] {
      client.createStagingTable(
        Identifier.of(Array("sch"), "tbl"),
        stageProps("delta.catalogManaged" -> "supported"))
    }
    assert(e.getMessage.contains("someFutureUnknownFeature"))
    assert(e.getMessage.contains("Delta version does not support"),
      "error must explain that the local Delta lacks the feature")
  }

  test("createStagingTable: suggested protocol feature defers to caller's existing value") {
    val (client, _) = newRecordingClient(
      suggestedProtocol = protocol(writerFeatures = Seq("rowTracking")))
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps(
        "delta.catalogManaged" -> "supported",
        "delta.feature.rowTracking" -> "disabled"))
    assert(out.get("delta.feature.rowTracking") === "disabled",
      "a caller-set delta.feature.* must win over a suggestion")
  }

  test("createStagingTable: required protocol feature conflicting with caller value throws") {
    val (client, _) = newRecordingClient(
      requiredProtocol = protocol(writerFeatures = Seq("catalogManaged")))
    val e = intercept[IllegalArgumentException] {
      client.createStagingTable(
        Identifier.of(Array("sch"), "tbl"),
        stageProps(
          "delta.catalogManaged" -> "supported",
          "delta.feature.catalogManaged" -> "disabled"))
    }
    assert(e.getMessage.contains("'delta.feature.catalogManaged'='supported'"))
    assert(e.getMessage.contains("'delta.feature.catalogManaged'='disabled'"))
  }

  test("createStagingTable: null-valued required/suggested entries are skipped") {
    // UC's contract: null on the wire means "engine substitutes at commit time" (e.g. the
    // row-tracking materialized column names). They must not leak into stage-time TBLPROPERTIES
    // -- Delta would reject them as unknown configs. The required-property branch is defensive
    // against future engine-generated required keys; UC's current contract has none.
    val req = new util.HashMap[String, String]()
    req.put("delta.checkpointPolicy", "v2")                                   // non-null -> applied
    req.put("delta.engineGeneratedPlaceholder", null)                         // null -> skipped
    val sug = new util.HashMap[String, String]()
    sug.put("delta.enableRowTracking", "true")                                // non-null -> applied
    sug.put("delta.rowTracking.materializedRowIdColumnName", null)            // null -> skipped
    val (client, _) = newRecordingClient(requiredProperties = req, suggestedProperties = sug)
    val out = client.createStagingTable(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.catalogManaged" -> "supported"))
    assert(out.get("delta.checkpointPolicy") === "v2")
    assert(out.get("delta.enableRowTracking") === "true")
    assert(!out.containsKey("delta.engineGeneratedPlaceholder"),
      "null-valued required entry must be skipped (engine fills it at commit time)")
    assert(!out.containsKey("delta.rowTracking.materializedRowIdColumnName"),
      "null-valued suggested entry must be skipped")
  }

  test("createStagingTable: path-based identifier throws") {
    val (client, stub) = newRecordingClient()
    // `delta`.`/tmp/foo` style: single-component namespace + absolute filesystem path as name
    val ident = Identifier.of(Array("delta"), "/tmp/foo")
    val ex = intercept[IllegalStateException](
      client.createStagingTable(ident, stageProps("delta.catalogManaged" -> "supported")))
    assert(ex.getMessage.contains("path-based"))
    assert(stub.stagedRequests.isEmpty)
  }

  // -------------------------------------------------------------------------
  // loadTableAndBuildReplaceProps: validation + augmentation for REPLACE / RTAS /
  // CREATE OR REPLACE on an existing catalog-managed Delta table.
  // -------------------------------------------------------------------------

  /**
   * Builds a TableInfo that, by default, satisfies all catalog-managed-Delta preconditions
   * (tableType=MANAGED, provider=delta, `delta.feature.catalogManaged=supported`). Tests
   * override individual fields to exercise the rejection branches.
   */
  private def existingDeltaTableInfo(
      tableType: UCDeltaModels.TableType = UCDeltaModels.TableType.MANAGED,
      provider: String = "DELTA",
      catalogManaged: Boolean = true,
      storageProperties: util.Map[String, String] =
        util.Map.of("fs.s3a.access.key", "existing-key")): TableInfo = {
    val config = new util.HashMap[String, String]()
    if (catalogManaged) config.put("delta.feature.catalogManaged", "supported")
    val metadata = new TestMetadata(provider = provider, configuration = config)
    new TableInfo(
      UUID.fromString("00000000-0000-0000-0000-000000000002"),
      tableType,
      "s3://bucket/existing/tbl",
      metadata,
      storageProperties,
      Optional.empty())
  }

  private def replaceClient(loadResult: => TableInfo): UCDeltaCatalogClientImpl =
    new UCDeltaCatalogClientImpl(catalogName = "main", ucClient = new StubUCDeltaClient(loadResult))

  test(
      "loadTableAndBuildReplaceProps: happy path augments props with provider + " +
        "is_managed_location + credentials, and does NOT set PROP_LOCATION") {
    val client = replaceClient(existingDeltaTableInfo())
    val out = client.loadTableAndBuildReplaceProps(
      Identifier.of(Array("sch"), "tbl"),
      stageProps("delta.feature.catalogManaged" -> "supported"))
    assert(out.get(TableCatalog.PROP_PROVIDER) === "delta",
      "existing provider must be re-emitted")
    assert(out.get(TableCatalog.PROP_IS_MANAGED_LOCATION) === "true")
    assert(!out.containsKey(TableCatalog.PROP_LOCATION),
      "PROP_LOCATION must NOT be set; downstream resolves location from existing snapshot")
    assert(out.get("fs.s3a.access.key") === "existing-key",
      "fs.* storage credentials must be mirrored bare")
    assert(out.get(TableCatalog.OPTION_PREFIX + "fs.s3a.access.key") === "existing-key",
      "fs.* storage credentials must also be mirrored under the option.-prefixed key")
  }

  test("loadTableAndBuildReplaceProps: caller-supplied UC_TABLE_ID_KEY throws") {
    val client = replaceClient(existingDeltaTableInfo())
    val e = intercept[IllegalArgumentException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps(
          "delta.feature.catalogManaged" -> "supported",
          "io.unitycatalog.tableId" -> "user-supplied"))
    }
    assert(e.getMessage.contains("io.unitycatalog.tableId"))
  }

  test("loadTableAndBuildReplaceProps: caller-supplied catalogManaged != supported throws") {
    val client = replaceClient(existingDeltaTableInfo())
    val e = intercept[IllegalArgumentException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps("delta.feature.catalogManaged" -> "disabled"))
    }
    assert(e.getMessage.contains("catalogManaged"))
    assert(e.getMessage.contains("disabled"))
  }

  test("loadTableAndBuildReplaceProps: caller-supplied PROP_LOCATION throws") {
    val client = replaceClient(existingDeltaTableInfo())
    val e = intercept[UnsupportedOperationException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps(
          "delta.feature.catalogManaged" -> "supported",
          TableCatalog.PROP_LOCATION -> "s3://other/location"))
    }
    assert(e.getMessage.contains(TableCatalog.PROP_LOCATION))
  }

  test("loadTableAndBuildReplaceProps: existing EXTERNAL table is rejected") {
    val client = replaceClient(
      existingDeltaTableInfo(tableType = UCDeltaModels.TableType.EXTERNAL))
    val e = intercept[UnsupportedOperationException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps("delta.feature.catalogManaged" -> "supported"))
    }
    assert(e.getMessage.contains("catalog-managed"))
  }

  test(
      "loadTableAndBuildReplaceProps: existing non-Delta provider throws " +
        "notADeltaTableException") {
    val client = replaceClient(
      existingDeltaTableInfo(provider = "PARQUET", catalogManaged = false))
    val e = intercept[org.apache.spark.sql.delta.DeltaAnalysisException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps("delta.feature.catalogManaged" -> "supported"))
    }
    assert(e.getMessage.contains("not a Delta table"))
  }

  test("loadTableAndBuildReplaceProps: existing table missing catalogManaged config is rejected") {
    val client = replaceClient(existingDeltaTableInfo(catalogManaged = false))
    val e = intercept[UnsupportedOperationException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps("delta.feature.catalogManaged" -> "supported"))
    }
    assert(e.getMessage.contains("catalog-managed"))
  }

  test(
      "loadTableAndBuildReplaceProps: caller PROP_PROVIDER different from existing throws " +
        "cannotChangeProvider") {
    val client = replaceClient(existingDeltaTableInfo())
    val e = intercept[org.apache.spark.sql.delta.DeltaAnalysisException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps(
          "delta.feature.catalogManaged" -> "supported",
          TableCatalog.PROP_PROVIDER -> "parquet"))
    }
    assert(e.getMessage.contains("provider"))
  }

  test(
      "loadTableAndBuildReplaceProps: StorageNoSuchTableException is wrapped as " +
        "NoSuchTableException") {
    val client = replaceClient(throw new StorageNoSuchTableException("no such table"))
    intercept[org.apache.spark.sql.catalyst.analysis.NoSuchTableException] {
      client.loadTableAndBuildReplaceProps(
        Identifier.of(Array("sch"), "tbl"),
        stageProps("delta.feature.catalogManaged" -> "supported"))
    }
  }

  test("loadTable converts TableInfo to V1Table with catalog-supplied fields") {
    val tableId = UUID.randomUUID()
    val metadata = new TestMetadata(
      description = "a test table",
      configuration = util.Map.of("ucTableId", tableId.toString, "delta.feature.x", "supported"),
      createdTime = 42L)
    val info = new TableInfo(
      tableId,
      UCDeltaModels.TableType.EXTERNAL,
      "s3://bucket/table",
      metadata,
      util.Map.of("fs.s3a.access.key", "key"),
      Optional.empty())

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
    val metadata = new TestMetadata() // no credential properties
    val tableInfoNoCreds = new TableInfo(
      tableId,
      UCDeltaModels.TableType.EXTERNAL,
      "s3://bucket/no-creds-table",
      metadata,
      Collections.emptyMap(), // no storage properties either
      Optional.empty())
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

  test("toV1Table propagates UniForm Iceberg metadata into storage.properties") {
    val tableId = UUID.randomUUID()
    val metadata = new TestMetadata()
    val iceberg =
      new IcebergMetadata("s3://bucket/metadata/v1.metadata.json", 42L, "2025-01-04T03:13:11.423")
    val uniform = new UniformMetadata(iceberg)
    val info = new TableInfo(
      tableId,
      UCDeltaModels.TableType.MANAGED,
      "s3://bucket/table",
      metadata,
      Collections.emptyMap(),
      Optional.of(uniform))

    val client = new UCDeltaCatalogClientImpl(
      catalogName = "main",
      ucClient = new StubUCDeltaClient(info))

    val v1 = client.loadTable(Identifier.of(Array("sch"), "tbl")).asInstanceOf[V1Table].catalogTable
    val props = v1.storage.properties
    assert(props.get(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP) ===
      Some("s3://bucket/metadata/v1.metadata.json"))
    assert(props.get(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP) ===
      Some("42"))
    assert(props.get(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP) ===
      Some("2025-01-04T03:13:11.423"))
    // uniform keys must NOT appear in catalogTable.properties (only in storage.properties)
    assert(!v1.properties.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP))
    assert(!v1.properties.contains(
      IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP)
    )
    assert(!v1.properties.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP))
  }

  test("toV1Table with no UniForm metadata leaves storage.properties without iceberg keys") {
    val tableId = UUID.randomUUID()
    val metadata = new TestMetadata()
    val info = new TableInfo(
      tableId,
      UCDeltaModels.TableType.MANAGED,
      "s3://bucket/table",
      metadata,
      Collections.emptyMap(),
      Optional.empty())

    val client = new UCDeltaCatalogClientImpl(
      catalogName = "main",
      ucClient = new StubUCDeltaClient(info))

    val v1 = client.loadTable(Identifier.of(Array("sch"), "tbl")).asInstanceOf[V1Table].catalogTable
    val props = v1.storage.properties
    assert(!props.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP))
    assert(!props.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP))
    assert(!props.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP))
    assert(!v1.properties.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP))
    assert(!v1.properties.contains(
      IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP)
    )
    assert(!v1.properties.contains(IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP))
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
 * Concrete [[AbstractMetadata]] for tests. All fields default to sensible no-op values so
 * callers only override what they care about.
 */
private class TestMetadata(
    id: String = null,
    name: String = "tbl",
    description: String = null,
    provider: String = "DELTA",
    schemaString: String =
      """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}}]}""",
    configuration: util.Map[String, String] = Collections.emptyMap(),
    createdTime: java.lang.Long = 0L
) extends AbstractMetadata {
  override def getId: String = id
  override def getName: String = name
  override def getDescription: String = description
  override def getProvider: String = provider
  override def getFormatOptions: util.Map[String, String] = Collections.emptyMap()
  override def getSchemaString: String = schemaString
  override def getPartitionColumns: util.List[String] = Collections.emptyList()
  override def getConfiguration: util.Map[String, String] = configuration
  override def getCreatedTime: java.lang.Long = createdTime
}

/**
 * Test base that throws [[UnsupportedOperationException]] from every [[UCDeltaClient]] method.
 * Subclasses override only the methods exercised by their suite, so tests fail loud (instead
 * of silently doing nothing) if the code under test calls a UC operation we didn't intend to
 * cover. `close()` no-ops because tests close clients indiscriminately in teardown.
 */
private abstract class ThrowingUCDeltaClient extends UCDeltaClient {
  override def getMetastoreId(): String = throw new UnsupportedOperationException
  override def loadTable(tableIdentifier: StorageTableIdentifier): TableInfo =
    throw new UnsupportedOperationException
  override def createStagingTable(
      tableIdentifier: StorageTableIdentifier): StagingTableInfo =
    throw new UnsupportedOperationException
  override def createTable(
      tableUri: URI,
      tableIdentifier: StorageTableIdentifier,
      tableType: UcTableType,
      metadata: AbstractMetadata,
      protocol: AbstractProtocol,
      domainMetadata: util.List[AbstractDomainMetadata],
      lastCommitTimestampMs: Long): TableInfo =
    throw new UnsupportedOperationException
  // scalastyle:off argcount
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
      domainMetadataToCommit: util.List[AbstractDomainMetadata],
      uniform: Optional[UniformMetadata]): Unit =
    throw new UnsupportedOperationException
  // scalastyle:on argcount
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

/**
 * Returns the result of {@code loadTableResult} (a by-name parameter) from
 * {@code loadTable}; inherits throwing implementations for every other method. Pass a
 * [[TableInfo]] to get a successful load, or {@code throw new ...} to simulate UC-side
 * failures.
 *
 * <p>Because {@code loadTableResult} is by-name, the body re-evaluates on every
 * {@code loadTable} invocation: a {@code throw} expression re-throws each time; a
 * {@link TableInfo} reference is rebound (cheap). For tests that need to vary the result
 * across calls, replace this with a {@code Supplier}-shaped constructor.
 */
private class StubUCDeltaClient(loadTableResult: => TableInfo) extends ThrowingUCDeltaClient {
  override def loadTable(tableIdentifier: StorageTableIdentifier): TableInfo = loadTableResult
}

/**
 * Returns a fixed [[StagingTableInfo]] from {@code createStagingTable} and records every
 * call. Used by the {@code createStagingTable} contract tests to assert when staging is /
 * isn't dispatched to UC.
 */
private class RecordingStubUCDeltaClient(info: StagingTableInfo) extends ThrowingUCDeltaClient {
  val stagedRequests: scala.collection.mutable.ArrayBuffer[(String, String, String)] =
    scala.collection.mutable.ArrayBuffer.empty
  override def createStagingTable(
      tableIdentifier: StorageTableIdentifier): StagingTableInfo = {
    val ns = tableIdentifier.getNamespace
    stagedRequests += ((ns(0), ns(1), tableIdentifier.getName))
    info
  }
}
