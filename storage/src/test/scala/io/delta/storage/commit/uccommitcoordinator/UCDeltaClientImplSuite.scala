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

package io.delta.storage.commit.uccommitcoordinator

import java.io.{FileNotFoundException, IOException}
import java.util.{Collections, Optional, UUID}
import java.util.concurrent.atomic.AtomicReference

import io.unitycatalog.client.{ApiClient, ApiException}
import io.unitycatalog.client.delta.DeltaRestClientProvider
import io.unitycatalog.client.delta.api.TablesApi
import io.unitycatalog.client.delta.model.{
  LoadTableResponse,
  PrimitiveType,
  StructField,
  StructType,
  TableMetadata
}

import org.scalatest.funsuite.AnyFunSuite

class UCDeltaClientImplSuite extends AnyFunSuite {

  // --- fixtures -------------------------------------------------------------

  private def primField(name: String, ptype: String, nullable: Boolean): StructField = {
    val p = new PrimitiveType(); p.setType(ptype)
    val f = new StructField(); f.setName(name); f.setType(p); f.setNullable(nullable)
    f.setMetadata(Collections.emptyMap()); f
  }

  private def okResponse(): LoadTableResponse = {
    val st = new StructType()
    st.setFields(java.util.Arrays.asList(
      primField("id", "long", nullable = false),
      primField("name", "string", nullable = true)))
    val meta = new TableMetadata()
    meta.setTableUuid(UUID.fromString("00000000-0000-0000-0000-000000000001"))
    meta.setColumns(st)
    meta.setEtag("etag-1")
    meta.setProperties(new java.util.HashMap[String, String]() {{
      put("delta.minReaderVersion", "1")
      put("delta.minWriterVersion", "2")
      put("delta.feature.catalogManaged", "supported")
    }})
    meta.setPartitionColumns(Collections.emptyList())
    meta.setCreatedTime(12345L)
    val resp = new LoadTableResponse()
    resp.setMetadata(meta); resp.setLatestTableVersion(42L)
    resp.setCommits(Collections.emptyList()); resp
  }

  private def provider(api: Option[TablesApi]): DeltaRestClientProvider =
    new DeltaRestClientProvider {
      override def getDeltaTablesApi: Optional[TablesApi] = api match {
        case Some(a) => Optional.of(a)
        case None => Optional.empty()
      }
      override def getApiClient: ApiClient =
        throw new UnsupportedOperationException("unused in unit tests")
    }

  private class StubApi(onLoad: () => LoadTableResponse) extends TablesApi(new ApiClient()) {
    override def loadTable(catalog: String, schema: String, table: String): LoadTableResponse =
      onLoad.apply()
  }

  private class FailingApi(ex: ApiException) extends TablesApi(new ApiClient()) {
    override def loadTable(c: String, s: String, t: String): LoadTableResponse = throw ex
  }

  // --- happy path -----------------------------------------------------------

  test("DRC happy path: response becomes a DRCMetadataAdapter with derived protocol") {
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => okResponse()))))
    assert(client.isDRCEnabled)
    val resp = client.loadTable("cat", "sch", "tbl")
    assert(resp.getLatestTableVersion == 42L)
    assert(resp.getEtag == Optional.of("etag-1"))
    val adapter = resp.getMetadata.asInstanceOf[DRCMetadataAdapter]
    assert(adapter.getDRCColumns.size == 2)
    assert(adapter.getDRCColumns.get(0).getName == "id")
    val proto = resp.getProtocol
    assert(proto.getMinReaderVersion == 1)
    assert(proto.getMinWriterVersion == 2)
    assert(proto.getWriterFeatures.contains("catalogManaged"))
    // Reader features sourced from Delta log via SnapshotManagement, not from flat
    // delta.feature.* props here. Populating them would silently force reader-feature
    // support for writer-only features (appendOnly, invariants, etc.).
    assert(proto.getReaderFeatures.isEmpty)
  }

  test("path-taken counters increment on each call") {
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => okResponse()))))
    client.loadTable("c", "s", "t")
    client.loadTable("c", "s", "t")
    assert(client.getDRCLoadTableCount == 2)
    assert(client.getLegacyLoadTableCount == 0)
  }

  // --- silent-failure guards ------------------------------------------------

  test("writer-only features do NOT appear in readerFeatures (silent-mis-read guard)") {
    val r = okResponse()
    r.getMetadata.getProperties.put("delta.feature.appendOnly", "supported")
    r.getMetadata.getProperties.put("delta.feature.invariants", "supported")
    val resp = new UCDeltaClientImpl(provider(Some(new StubApi(() => r))))
      .loadTable("c", "s", "t")
    assert(resp.getProtocol.getReaderFeatures.isEmpty)
    assert(resp.getProtocol.getWriterFeatures.contains("appendOnly"))
    assert(resp.getProtocol.getWriterFeatures.contains("invariants"))
  }

  test("unknown delta.feature status fails loud (no silent downgrade)") {
    val r = okResponse()
    r.getMetadata.getProperties.put("delta.feature.myFeature", "deprecated")
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => r))))
    val ex = intercept[IllegalStateException] { client.loadTable("c", "s", "t") }
    assert(ex.getMessage.contains("Unknown delta.feature status"))
    assert(ex.getMessage.contains("deprecated"))
  }

  test("unbackfilled commits fail loud (commit translation not yet implemented)") {
    val r = okResponse()
    r.setCommits(java.util.Arrays.asList(
      new io.unitycatalog.client.delta.model.DeltaCommit()))
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => r))))
    val ex = intercept[IllegalStateException] { client.loadTable("c", "s", "t") }
    assert(ex.getMessage.contains("unbackfilled commits"))
  }

  test("null required field (metadata.columns) fails loud") {
    val badResp: LoadTableResponse = {
      val r = okResponse(); r.getMetadata.setColumns(null); r
    }
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => badResp))))
    val ex = intercept[IllegalStateException] { client.loadTable("c", "s", "t") }
    assert(ex.getMessage.contains("metadata.columns"))
  }

  test("null required field (latest-table-version) fails loud") {
    val badResp = { val r = okResponse(); r.setLatestTableVersion(null); r }
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => badResp))))
    val ex = intercept[IllegalStateException] { client.loadTable("c", "s", "t") }
    assert(ex.getMessage.contains("latest-table-version"))
  }

  test("null required field (metadata.table-uuid) fails loud") {
    val badResp = { val r = okResponse(); r.getMetadata.setTableUuid(null); r }
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => badResp))))
    val ex = intercept[IllegalStateException] { client.loadTable("c", "s", "t") }
    assert(ex.getMessage.contains("table-uuid"))
  }

  test("etag-absent response round-trips Optional.empty") {
    val r = okResponse(); r.getMetadata.setEtag(null)
    val resp = new UCDeltaClientImpl(provider(Some(new StubApi(() => r))))
      .loadTable("c", "s", "t")
    assert(!resp.getEtag.isPresent)
  }

  // --- routing --------------------------------------------------------------

  test("flag off: loadTable throws UnsupportedOperation, isDRCEnabled is false") {
    val client = new UCDeltaClientImpl(provider(None))
    assert(!client.isDRCEnabled)
    val ex = intercept[UnsupportedOperationException] { client.loadTable("c", "s", "t") }
    assert(ex.getMessage.contains("DRC-only"))
    // Legacy counter increments even when we throw -- we still took the legacy path.
    assert(client.getLegacyLoadTableCount == 1)
    assert(client.getDRCLoadTableCount == 0)
  }

  test("flag flip mid-session re-routes on the next call (no caching)") {
    // Load-bearing: UCDeltaClientImpl is constructed once per catalog and lives across
    // many loadTable calls. A cached isDRCEnabled would pin the first-seen flag value.
    val apiRef = new AtomicReference[Optional[TablesApi]](Optional.empty())
    val flippingProvider = new DeltaRestClientProvider {
      override def getDeltaTablesApi: Optional[TablesApi] = apiRef.get()
      override def getApiClient: ApiClient =
        throw new UnsupportedOperationException("unused")
    }
    val client = new UCDeltaClientImpl(flippingProvider)
    assert(!client.isDRCEnabled)
    apiRef.set(Optional.of(new StubApi(() => okResponse())))
    assert(client.isDRCEnabled)
    val resp = client.loadTable("c", "s", "t")
    assert(resp.getTableUuid == "00000000-0000-0000-0000-000000000001")
    assert(client.getDRCLoadTableCount == 1)
  }

  // --- error translation ----------------------------------------------------

  test("404 translates to FileNotFoundException with FQN in message") {
    val api = new FailingApi(new ApiException(
      404, "not found", null.asInstanceOf[java.net.http.HttpHeaders], "nope"))
    val ex = intercept[FileNotFoundException] {
      new UCDeltaClientImpl(provider(Some(api))).loadTable("c", "s", "t")
    }
    assert(ex.getMessage.contains("c.s.t"))
    assert(ex.getMessage.contains("nope"))
  }

  test("500 translates to IOException (not FileNotFoundException) preserving HTTP code") {
    val api = new FailingApi(new ApiException(
      500, "boom", null.asInstanceOf[java.net.http.HttpHeaders], "boom"))
    val ex = intercept[IOException] {
      new UCDeltaClientImpl(provider(Some(api))).loadTable("c", "s", "t")
    }
    assert(!ex.isInstanceOf[FileNotFoundException])
    assert(ex.getMessage.contains("HTTP 500"))
  }

  test("409 translates to IOException with HTTP code (typed mapping deferred)") {
    val api = new FailingApi(new ApiException(
      409, "conflict", null.asInstanceOf[java.net.http.HttpHeaders], ""))
    val ex = intercept[IOException] {
      new UCDeltaClientImpl(provider(Some(api))).loadTable("c", "s", "t")
    }
    assert(ex.getMessage.contains("HTTP 409"))
  }

  test("429 translates to IOException with HTTP code (typed mapping deferred)") {
    val api = new FailingApi(new ApiException(
      429, "limit", null.asInstanceOf[java.net.http.HttpHeaders], ""))
    val ex = intercept[IOException] {
      new UCDeltaClientImpl(provider(Some(api))).loadTable("c", "s", "t")
    }
    assert(ex.getMessage.contains("HTTP 429"))
  }

  // --- input validation -----------------------------------------------------

  test("empty / null catalog, schema, or table rejected") {
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => okResponse()))))
    intercept[NullPointerException] { client.loadTable(null, "s", "t") }
    intercept[IllegalArgumentException] { client.loadTable("", "s", "t") }
    intercept[NullPointerException] { client.loadTable("c", null, "t") }
    intercept[IllegalArgumentException] { client.loadTable("c", "", "t") }
    intercept[NullPointerException] { client.loadTable("c", "s", null) }
    intercept[IllegalArgumentException] { client.loadTable("c", "s", "") }
  }

  // --- stubs for not-yet-implemented methods -------------------------------

  test("unimplemented methods throw UnsupportedOperation with PR-pointer") {
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => okResponse()))))
    val methods: Seq[() => Any] = Seq(
      () => client.listTables("c", "s", Optional.empty(), Optional.empty()),
      () => client.createStagingTable("c", "s", "n"),
      () => client.dropTable("c", "s", "t"),
      () => client.renameTable("c", "s", "o", "n"),
      () => client.getTableCredentials("c", "s", "t", UCCredentialOperation.READ),
      () => client.getStagingTableCredentials("tid"),
      () => client.getPathCredentials("s3://x", UCCredentialOperation.READ))
    methods.foreach { m =>
      val ex = intercept[UnsupportedOperationException] { m() }
      assert(ex.getMessage.contains("not yet implemented"))
    }
  }

  // --- Legacy UCClient delegation ------------------------------------------

  /** [[UCClient]] stub that records each call so tests can assert delegation. */
  private class RecordingLegacy extends UCClient {
    var metastoreIdCalls = 0
    var commitCalls = 0
    var getCommitsCalls = 0
    var finalizeCreateCalls = 0
    var closeCalls = 0

    override def getMetastoreId(): String = { metastoreIdCalls += 1; "metastore-1" }
    override def commit(
        tableId: String, tableUri: java.net.URI,
        commit: Optional[io.delta.storage.commit.Commit],
        lastKnownBackfilledVersion: Optional[java.lang.Long],
        disown: Boolean,
        newMetadata: Optional[io.delta.storage.commit.actions.AbstractMetadata],
        newProtocol: Optional[io.delta.storage.commit.actions.AbstractProtocol],
        uniform: Optional[io.delta.storage.commit.uniform.UniformMetadata]): Unit =
      { commitCalls += 1 }
    override def getCommits(
        tableId: String, tableUri: java.net.URI,
        startVersion: Optional[java.lang.Long],
        endVersion: Optional[java.lang.Long]): io.delta.storage.commit.GetCommitsResponse = {
      getCommitsCalls += 1
      new io.delta.storage.commit.GetCommitsResponse(Collections.emptyList(), -1L)
    }
    override def finalizeCreate(
        tableName: String, catalogName: String, schemaName: String, storageLocation: String,
        columns: java.util.List[UCClient.ColumnDef],
        properties: java.util.Map[String, String]): Unit = { finalizeCreateCalls += 1 }
    override def close(): Unit = { closeCalls += 1 }
  }

  test("inherited legacy methods delegate to the injected UCClient") {
    val legacy = new RecordingLegacy
    val client = new UCDeltaClientImpl(provider(None), legacy)
    assert(client.getMetastoreId == "metastore-1")
    client.commit("tid", new java.net.URI("s3://b/t"),
      Optional.empty(), Optional.empty(), false,
      Optional.empty(), Optional.empty(), Optional.empty())
    client.getCommits("tid", new java.net.URI("s3://b/t"),
      Optional.empty(), Optional.empty())
    client.finalizeCreate(
      "t", "c", "s", "s3://b/t",
      Collections.emptyList(), Collections.emptyMap())
    assert(legacy.metastoreIdCalls == 1)
    assert(legacy.commitCalls == 1)
    assert(legacy.getCommitsCalls == 1)
    assert(legacy.finalizeCreateCalls == 1)
  }

  test("inherited legacy methods fail loud when no legacy UCClient is injected") {
    val client = new UCDeltaClientImpl(provider(Some(new StubApi(() => okResponse()))))
    intercept[UnsupportedOperationException] { client.getMetastoreId }
    intercept[UnsupportedOperationException] {
      client.commit("tid", new java.net.URI("s3://b/t"),
        Optional.empty(), Optional.empty(), false,
        Optional.empty(), Optional.empty(), Optional.empty())
    }
    intercept[UnsupportedOperationException] {
      client.getCommits("tid", new java.net.URI("s3://b/t"),
        Optional.empty(), Optional.empty())
    }
    intercept[UnsupportedOperationException] {
      client.finalizeCreate(
        "t", "c", "s", "s3://b/t",
        Collections.emptyList(), Collections.emptyMap())
    }
  }

  test("close delegates to the injected legacy UCClient") {
    val legacy = new RecordingLegacy
    val client = new UCDeltaClientImpl(provider(None), legacy)
    client.close()
    assert(legacy.closeCalls == 1)
  }

  test("close is a no-op when no legacy UCClient is injected") {
    new UCDeltaClientImpl(provider(None)).close() // should not throw
  }

  test("DRC loadTable works alongside an injected legacy client without cross-talk") {
    val legacy = new RecordingLegacy
    val client = new UCDeltaClientImpl(
      provider(Some(new StubApi(() => okResponse()))), legacy)
    val resp = client.loadTable("c", "s", "t")
    assert(resp.getLatestTableVersion == 42L)
    assert(legacy.commitCalls == 0)
  }
}
