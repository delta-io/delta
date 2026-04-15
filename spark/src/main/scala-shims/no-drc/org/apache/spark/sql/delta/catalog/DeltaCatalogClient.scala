/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCTokenBasedDeltaRestClient}

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog, V1Table}

/**
 * No-DRC stub. Delegates catalog ops to the delegate (UCProxy) directly.
 * Released UC doesn't expose getApiClient(), so we can't create
 * UCTokenBasedDeltaRestClient here.
 */
class DeltaCatalogClient private (
    val ucDeltaClient: UCDeltaClient,
    delegate: TableCatalog,
    catalogName: String) {

  def loadTable(ident: Identifier): Table = {
    delegate.loadTable(ident)
  }

  def createTable(
      ident: Identifier,
      v1Table: CatalogTable,
      postCommitSnapshot: Snapshot): Unit = {
    val t = V1Table(v1Table)
    delegate.createTable(ident, t.columns(), t.partitioning, t.properties)
  }
}

object DeltaCatalogClient {
  def apply(
      delegate: CatalogPlugin,
      drcEnabled: Boolean,
      catalogName: String): DeltaCatalogClient = {
    // No UCDeltaClient available for catalog ops -- delegate handles them.
    // ucDeltaClient is null; the commit path (UCCommitCoordinatorBuilder)
    // creates its own client separately.
    new DeltaCatalogClient(null, delegate.asInstanceOf[TableCatalog], catalogName)
  }
}
