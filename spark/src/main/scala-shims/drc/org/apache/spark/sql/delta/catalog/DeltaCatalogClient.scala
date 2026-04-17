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

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.{DRCMetadataAdapter, UCDeltaClient, UCTokenBasedDeltaRestClient}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY
import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.delta.DeltaRestClientProvider

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * DRC-enabled Spark catalog client. Delegates all UC API calls to
 * UCDeltaClient (Java, storage module). This layer only translates
 * between storage-level types and Spark types. No UC SDK imports
 * beyond ApiClient for client construction.
 *
 * When DRC is disabled (config flag off), createTable falls back to the
 * catalog delegate (UCProxy) which handles columns via the legacy API.
 */
class DeltaCatalogClient private (
    val ucDeltaClient: UCDeltaClient,
    delegate: TableCatalog,
    drcEnabled: Boolean,
    catalogName: String) {

  /**
   * Load table from UC via UCDeltaClient. DRC path returns protocol/metadata
   * in the response; no-DRC path returns null (caller reads from delta log).
   */
  def loadTable(ident: Identifier): Table = {
    require(ident.namespace().length >= 1,
      s"Expected at least one namespace element for $ident")
    val schemaName = ident.namespace()(0)
    val tableName = ident.name()

    val resp = ucDeltaClient.loadTable(
      catalogName, schemaName, tableName,
      null, null,
      java.util.Optional.empty(), java.util.Optional.empty())

    val tableType = if (resp.getTableType == "EXTERNAL") {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    // Schema: DRC path uses direct UC model -> Spark conversion (no JSON roundtrip).
    // Fallback: JSON parse or empty (delta log provides it).
    val schema = Option(resp.getMetadata) match {
      case Some(drc: DRCMetadataAdapter) if drc.getDRCColumns != null =>
        DeltaRestSchemaConverter.toSparkStructType(drc.getDRCColumns)
      case Some(m) if m.getSchemaString != null =>
        DataType.fromJson(m.getSchemaString).asInstanceOf[StructType]
      case _ =>
        new StructType()
    }

    val credProps = resp.getCredentials.asScala.toMap
    val tableProps = Option(resp.getProperties).map(_.asScala.toMap)
      .getOrElse(Map.empty) +
      (UC_TABLE_ID_KEY -> resp.getTableId) ++
      Option(resp.getEtag).map("io.unitycatalog.etag" -> _)

    V1Table(CatalogTable(
      identifier = TableIdentifier(tableName, Some(schemaName), Some(catalogName)),
      tableType = tableType,
      storage = DataSource.buildStorageFormatFromOptions(Map.empty)
        .copy(
          locationUri = Option(resp.getLocation).map(new java.net.URI(_)),
          properties = credProps),
      schema = schema,
      provider = Some("delta"),
      properties = tableProps))
  }

  def createTable(
      ident: Identifier,
      v1Table: CatalogTable,
      postCommitSnapshot: Snapshot): Unit = {
    if (drcEnabled) {
      ucDeltaClient.createTable(
        catalogName, ident.namespace()(0), ident.name(),
        v1Table.location.toString,
        postCommitSnapshot.metadata,
        postCommitSnapshot.protocol,
        v1Table.tableType == CatalogTableType.MANAGED,
        "DELTA",
        postCommitSnapshot.domainMetadata.asJava)
    } else {
      // Legacy: delegate to UCProxy which handles columns via legacy API
      val t = V1Table(v1Table)
      delegate.createTable(ident, t.columns(), t.partitioning, t.properties)
    }
  }
}

object DeltaCatalogClient {
  def apply(
      delegatePlugin: CatalogPlugin,
      drcEnabled: Boolean,
      catalogName: String): DeltaCatalogClient = {
    val apiClient = delegatePlugin match {
      case p: DeltaRestClientProvider
          if p.getApiClient().isInstanceOf[ApiClient] =>
        p.getApiClient().asInstanceOf[ApiClient]
      case _ =>
        throw new IllegalStateException(
          "Catalog delegate does not provide an ApiClient")
    }
    val client = new UCTokenBasedDeltaRestClient(apiClient, drcEnabled)
    new DeltaCatalogClient(client,
      delegatePlugin.asInstanceOf[TableCatalog], drcEnabled, catalogName)
  }
}
