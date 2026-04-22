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

import java.util.{Arrays => JArr, Collections => JColl, Optional => JOpt, UUID}

import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient
import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.delta.DeltaRestClientProvider
import io.unitycatalog.client.delta.api.TablesApi
import io.unitycatalog.client.delta.model.{
  CredentialOperation,
  CredentialsResponse,
  LoadTableResponse,
  PrimitiveType => UCPrimitiveType,
  StructField => UCStructField,
  StructType => UCStructType,
  TableMetadata,
  TableType => UCTableType
}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, DelegatingCatalogExtension, Identifier}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class DeltaRestReadPathSuite extends AnyFunSuite {
  import DeltaRestReadPathSuite._

  private val ident = Identifier.of(Array("default"), "t")

  test("tryLoad returns None and does not invoke client factory when no provider on chain") {
    val plainCatalog = new NonProviderCatalog
    val factory = new RecordingFactory(dummyResponse)
    val result = DeltaRestReadPath.tryLoad(plainCatalog, "unity", ident, factory.build)
    assert(result.isEmpty)
    assert(factory.invocations === 0,
      "flag-off path must not construct a UCDeltaClient " +
        "(regression guard for flatMap/map swaps and eager side effects)")
  }

  test("tryLoad returns None and does not invoke client factory when flag is off") {
    val provider = new FakeProvider(flagOn = false)
    val factory = new RecordingFactory(dummyResponse)
    val result = DeltaRestReadPath.tryLoad(provider, "unity", ident, factory.build)
    assert(result.isEmpty)
    assert(factory.invocations === 0,
      "flag-off path must not construct a UCDeltaClient")
  }

  test("tryLoad returns Some(V1Table) and invokes factory exactly once when flag is on") {
    val provider = new FakeProvider(flagOn = true)
    val factory = new RecordingFactory(dummyResponse)
    val result = DeltaRestReadPath.tryLoad(provider, "unity", ident, factory.build)
    assert(result.isDefined)
    assert(factory.invocations === 1)
    assert(result.get.catalogTable.identifier.catalog.contains("unity"))
  }

  test("tryLoad walks past wrappers to find the provider") {
    val provider = new FakeProvider(flagOn = true)
    val outer = new WrappingCatalog(provider)
    val factory = new RecordingFactory(dummyResponse)
    val result = DeltaRestReadPath.tryLoad(outer, "unity", ident, factory.build)
    assert(result.isDefined)
    assert(factory.invocations === 1)
  }

  test("tryLoad exceptions from the client propagate (no silent fallback)") {
    val provider = new FakeProvider(flagOn = true)
    val failingFactory: ApiClient => UCDeltaClient = _ => new FailingClient
    val ex = intercept[RuntimeException] {
      DeltaRestReadPath.tryLoad(provider, "unity", ident, failingFactory)
    }
    assert(ex.getMessage.contains("forced failure"))
  }
}

object DeltaRestReadPathSuite {

  private def dummyResponse: LoadTableResponse = {
    val col = new UCStructField().name("id").nullable(true)
    val p = new UCPrimitiveType()
    p.setType("long")
    col.setType(p)
    val columns = new UCStructType()
    columns.setType("struct")
    columns.setFields(JArr.asList(col))
    val md = new TableMetadata()
      .etag("e")
      .tableUuid(UUID.fromString("00000000-0000-0000-0000-000000000abc"))
      .tableType(UCTableType.MANAGED)
      .location("s3://b/p")
      .columns(columns)
    new LoadTableResponse().metadata(md).commits(JColl.emptyList()).latestTableVersion(0L)
  }

  class FakeProvider(flagOn: Boolean) extends CatalogPlugin with DeltaRestClientProvider {
    override def name(): String = "fake-provider"
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
    override def getDeltaTablesApi: JOpt[TablesApi] = {
      if (flagOn) JOpt.of(new TablesApi()) else JOpt.empty()
    }
    override def getApiClient: ApiClient = new ApiClient()
  }

  class NonProviderCatalog extends CatalogPlugin {
    override def name(): String = "plain"
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  }

  class WrappingCatalog(innerDelegate: CatalogPlugin) extends DelegatingCatalogExtension {
    setDelegateCatalog(innerDelegate)
    override def name(): String = "wrapper"
  }

  class RecordingFactory(response: LoadTableResponse) {
    var invocations = 0
    val build: ApiClient => UCDeltaClient = _ => {
      invocations += 1
      new CannedClient(response)
    }
  }

  class CannedClient(response: LoadTableResponse) extends UCDeltaClient {
    override def loadTable(c: String, s: String, t: String): LoadTableResponse = response
    override def getTableCredentials(
        c: String, s: String, t: String, op: CredentialOperation): CredentialsResponse =
      new CredentialsResponse()
  }

  class FailingClient extends UCDeltaClient {
    override def loadTable(c: String, s: String, t: String): LoadTableResponse = {
      throw new RuntimeException("forced failure")
    }
    override def getTableCredentials(
        c: String, s: String, t: String, op: CredentialOperation): CredentialsResponse =
      new CredentialsResponse()
  }
}
