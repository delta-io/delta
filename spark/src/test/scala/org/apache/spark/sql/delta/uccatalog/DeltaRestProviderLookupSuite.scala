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

import java.util.{Optional => JOpt}

import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.delta.DeltaRestClientProvider
import io.unitycatalog.client.delta.api.TablesApi
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, DelegatingCatalogExtension, Identifier, Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

class DeltaRestProviderLookupSuite extends AnyFunSuite {
  import DeltaRestProviderLookupSuite._

  test("null catalog returns None") {
    assert(DeltaRestProviderLookup.findProvider(null).isEmpty)
  }

  test("catalog that is itself a provider returns itself") {
    val provider = new StubProviderCatalog(flagOn = true)
    val result = DeltaRestProviderLookup.findProvider(provider)
    assert(result.contains(provider))
  }

  test("catalog -> delegate (provider) returns the inner provider") {
    val provider = new StubProviderCatalog(flagOn = true)
    val wrapper = new StubDelegatingCatalog(provider)
    val result = DeltaRestProviderLookup.findProvider(wrapper)
    assert(result.contains(provider))
  }

  test("catalog -> delegate -> delegate (provider) walks through wrappers") {
    val provider = new StubProviderCatalog(flagOn = false)
    val mid = new StubDelegatingCatalog(provider)
    val outer = new StubDelegatingCatalog(mid)
    val result = DeltaRestProviderLookup.findProvider(outer)
    assert(result.contains(provider))
  }

  test("catalog without a provider in the chain returns None") {
    val nonProvider = new StubPlainCatalog
    val wrapper = new StubDelegatingCatalog(nonProvider)
    assert(DeltaRestProviderLookup.findProvider(wrapper).isEmpty)
  }

  test("catalog with null delegate returns None") {
    val wrapper = new StubDelegatingCatalog(null)
    assert(DeltaRestProviderLookup.findProvider(wrapper).isEmpty)
  }

  test("defensive: chain deeper than MAX_CHAIN_DEPTH bails out with None") {
    // Build a chain of non-provider delegating catalogs exceeding MAX_CHAIN_DEPTH.
    var current: CatalogPlugin = new StubPlainCatalog
    for (_ <- 0 until DeltaRestProviderLookup.MAX_CHAIN_DEPTH + 2) {
      current = new StubDelegatingCatalog(current)
    }
    assert(DeltaRestProviderLookup.findProvider(current).isEmpty)
  }
}

object DeltaRestProviderLookupSuite {

  /** Non-delegating catalog that is itself a DeltaRestClientProvider. */
  class StubProviderCatalog(flagOn: Boolean) extends CatalogPlugin with DeltaRestClientProvider {
    private var catalogName: String = "stub-provider"
    override def name(): String = catalogName
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
      this.catalogName = name
    }
    override def getDeltaTablesApi: JOpt[TablesApi] = {
      if (flagOn) JOpt.of(new TablesApi()) else JOpt.empty()
    }
    override def getApiClient: ApiClient = new ApiClient()
  }

  /** Non-delegating plain CatalogPlugin (neither provider nor delegating). */
  class StubPlainCatalog extends CatalogPlugin {
    private var catalogName: String = "stub-plain"
    override def name(): String = catalogName
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
      this.catalogName = name
    }
  }

  /**
   * A DelegatingCatalogExtension whose `delegate` field we set via setDelegateCatalog so the
   * reflection walk in DeltaRestProviderLookup finds it.
   */
  class StubDelegatingCatalog(innerDelegate: CatalogPlugin) extends DelegatingCatalogExtension {
    // DelegatingCatalogExtension stores the delegate via setDelegateCatalog; call it here so
    // reflection picks it up. The cast to TableCatalog is safe because both StubProviderCatalog
    // and StubDelegatingCatalog implement enough of the surface for the chain walk's purposes
    // (the chain walk only reads the field, it never calls table ops).
    if (innerDelegate != null) {
      // DelegatingCatalogExtension.setDelegateCatalog takes a CatalogPlugin in Spark 4.x.
      setDelegateCatalog(innerDelegate.asInstanceOf[
        org.apache.spark.sql.connector.catalog.CatalogPlugin])
    }

    // DelegatingCatalogExtension does not override initialize/name when no delegate is set,
    // so we bolt on trivial implementations to avoid NPEs.
    override def name(): String = "stub-delegating"
  }
}
