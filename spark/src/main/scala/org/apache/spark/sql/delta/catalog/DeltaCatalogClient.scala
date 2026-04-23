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

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY
import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCTokenBasedRestClient}
import io.unitycatalog.client.delta.DeltaRestClientProvider
import io.unitycatalog.client.delta.model.{DataSourceFormat => DeltaDataSourceFormat, TableType => DeltaTableType}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

private class DeltaCatalogClient private (
    val ucDeltaClient: Option[UCDeltaClient],
    delegate: TableCatalog,
    catalogName: String) extends Logging {

  def loadTable(ident: Identifier): Table = {
    ucDeltaClient match {
      case Some(client) if ident.namespace().length == 1 =>
        val metadata = client
          .loadTable(catalogName, ident.namespace().head, ident.name())
          .getMetadata
        if (metadata.getDataSourceFormat == DeltaDataSourceFormat.DELTA) {
          V1Table(toCatalogTable(ident, metadata))
        } else {
          delegate.loadTable(ident)
        }
      case _ =>
        delegate.loadTable(ident)
    }
  }

  private def toCatalogTable(
      ident: Identifier,
      metadata: io.unitycatalog.client.delta.model.TableMetadata): CatalogTable = {
    CatalogTable(
      identifier =
        TableIdentifier(ident.name(), ident.namespace().lastOption, Some(catalogName)),
      tableType = metadata.getTableType match {
        case DeltaTableType.MANAGED => CatalogTableType.MANAGED
        case _ => CatalogTableType.EXTERNAL
      },
      storage = CatalogStorageFormat(
        locationUri = Some(CatalogUtils.stringToURI(metadata.getLocation)),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map(UC_TABLE_ID_KEY -> metadata.getTableUuid.toString)),
      schema = DeltaRestSchemaConverter.toSparkSchema(metadata.getColumns),
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = Option(metadata.getPartitionColumns)
        .map(_.asScala.toSeq)
        .getOrElse(Nil),
      properties = Option(metadata.getProperties)
        .map(_.asScala.toMap)
        .getOrElse(Map.empty))
  }
}

private object DeltaCatalogClient {
  def apply(delegatePlugin: CatalogPlugin): DeltaCatalogClient = {
    val delegate = delegatePlugin.asInstanceOf[TableCatalog]
    val ucDeltaClient = delegatePlugin match {
      case provider: DeltaRestClientProvider =>
        Some(new UCTokenBasedRestClient(
          provider.getApiClient(),
          provider.getDeltaTablesApi.isPresent))
      case _ =>
        None
    }
    new DeltaCatalogClient(ucDeltaClient, delegate, delegatePlugin.name())
  }
}
