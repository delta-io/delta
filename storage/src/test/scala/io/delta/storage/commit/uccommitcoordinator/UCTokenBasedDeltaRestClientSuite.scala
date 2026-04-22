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
import java.net.URI
import java.util.{Collections, Optional, UUID}

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

class UCTokenBasedDeltaRestClientSuite extends AnyFunSuite {

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
    meta.setColumns(st); meta.setEtag("etag-1")
    meta.setLocation("s3://bucket/path")
    meta.setProperties(new java.util.HashMap[String, String]() {{
      put("delta.minReaderVersion", "1"); put("delta.minWriterVersion", "2")
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
        case Some(a) => Optional.of(a); case None => Optional.empty()
      }
      override def getApiClient: ApiClient = throw new UnsupportedOperationException("unused")
    }

  private class StubApi(onLoad: () => LoadTableResponse) extends TablesApi(new ApiClient()) {
    override def loadTable(catalog: String, schema: String, table: String): LoadTableResponse =
      onLoad.apply()
  }
  private class FailingApi(ex: ApiException) extends TablesApi(new ApiClient()) {
    override def loadTable(c: String, s: String, t: String): LoadTableResponse = throw ex
  }

  private def client(api: Option[TablesApi]): UCTokenBasedDeltaRestClient =
    new UCTokenBasedDeltaRestClient(new ApiClient(), provider(api))

  private val anyUri = URI.create("s3://bucket/path")

  test("DRC path: response is translated to DRCMetadataAdapter") {
    val c = client(Some(new StubApi(() => okResponse())))
    assert(c.isDRCEnabled)
    val resp = c.loadTable(
      "cat", "sch", "tbl",
      /* ucTableId */ null, anyUri, Optional.empty(), Optional.empty())
    assert(resp.getCommitsResponse.getLatestTableVersion == 42L)
    assert(resp.getEtag == "etag-1")
    assert(resp.getLocation == "s3://bucket/path")
    val adapter = resp.getMetadata.asInstanceOf[DRCMetadataAdapter]
    assert(adapter.getDRCColumns.size == 2)
    assert(adapter.getDRCColumns.get(0).getName == "id")
    // Protocol is not populated from loadTable response in v1 -- SnapshotManagement derives it
    // from the commit log. Populating here would silently force reader-feature support for
    // writer-only features.
    assert(resp.getProtocol == null)
  }

  test("flag off: loadTable throws UnsupportedOperation, isDRCEnabled is false") {
    val c = client(None)
    assert(!c.isDRCEnabled)
    intercept[UnsupportedOperationException] {
      c.loadTable("c", "s", "t", null, anyUri, Optional.empty(), Optional.empty())
    }
  }

  test("null required field (metadata.columns) fails loud") {
    val badResp: LoadTableResponse = {
      val r = okResponse(); r.getMetadata.setColumns(null); r
    }
    val c = client(Some(new StubApi(() => badResp)))
    val ex = intercept[IllegalStateException] {
      c.loadTable("c", "s", "t", null, anyUri, Optional.empty(), Optional.empty())
    }
    assert(ex.getMessage.contains("metadata.columns"))
  }

  test("unbackfilled commits in response fail loud (translation not yet wired)") {
    val r = okResponse()
    r.setCommits(java.util.Arrays.asList(
      new io.unitycatalog.client.delta.model.DeltaCommit()))
    val c = client(Some(new StubApi(() => r)))
    val ex = intercept[IllegalStateException] {
      c.loadTable("c", "s", "t", null, anyUri, Optional.empty(), Optional.empty())
    }
    assert(ex.getMessage.contains("unbackfilled commits"))
  }

  test("404 translates to FileNotFoundException") {
    val api = new FailingApi(
      new ApiException(404, "not found", null.asInstanceOf[java.net.http.HttpHeaders], "not found"))
    val ex = intercept[FileNotFoundException] {
      client(Some(api)).loadTable("c", "s", "t", null, anyUri, Optional.empty(), Optional.empty())
    }
    assert(ex.getMessage.contains("not found"))
  }

  test("500 translates to IOException (not FileNotFoundException)") {
    val api = new FailingApi(
      new ApiException(500, "boom", null.asInstanceOf[java.net.http.HttpHeaders], "boom"))
    val ex = intercept[IOException] {
      client(Some(api)).loadTable("c", "s", "t", null, anyUri, Optional.empty(), Optional.empty())
    }
    assert(!ex.isInstanceOf[FileNotFoundException])
    assert(ex.getMessage.contains("HTTP 500"))
  }

  test("flag flip mid-session re-routes on the next call (no caching)") {
    val apiRef = new java.util.concurrent.atomic.AtomicReference[Optional[TablesApi]](
      Optional.empty())
    val flippingProvider = new DeltaRestClientProvider {
      override def getDeltaTablesApi: Optional[TablesApi] = apiRef.get()
      override def getApiClient: ApiClient = throw new UnsupportedOperationException("unused")
    }
    val c = new UCTokenBasedDeltaRestClient(new ApiClient(), flippingProvider)
    assert(!c.isDRCEnabled)
    apiRef.set(Optional.of(new StubApi(() => okResponse())))
    assert(c.isDRCEnabled)
    val resp = c.loadTable("c", "s", "t", null, anyUri, Optional.empty(), Optional.empty())
    assert(resp.getTableId == "00000000-0000-0000-0000-000000000001")
  }

  test("createTable and typed commits throw UnsupportedOperation until later PRs") {
    val c = client(Some(new StubApi(() => okResponse())))
    intercept[UnsupportedOperationException] {
      c.createTable("cat", "sch", "tbl", "s3://x",
        null.asInstanceOf[io.delta.storage.commit.actions.AbstractMetadata],
        null.asInstanceOf[io.delta.storage.commit.actions.AbstractProtocol],
        true, "delta", Collections.emptyList())
    }
    intercept[UnsupportedOperationException] {
      c.commit(null.asInstanceOf[io.delta.storage.commit.TableDescriptor],
        null.asInstanceOf[org.apache.hadoop.fs.Path],
        Optional.empty(), Optional.empty(), false,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty())
    }
    intercept[UnsupportedOperationException] {
      c.commit("cat", "sch", "tbl", "uuid", anyUri,
        Optional.empty(), Optional.empty(), false,
        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(),
        Optional.empty(), Optional.empty())
    }
  }
}
