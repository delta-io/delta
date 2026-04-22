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

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient
import io.unitycatalog.client.delta.model.{LoadTableResponse, TableType => UCTableType}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{Identifier, V1Table}

/**
 * Builds a Spark `V1Table` from a Delta REST Catalog (DRC) `LoadTableResponse`. Mirrors the
 * shape of what `UCProxy.loadTable` produces today so that downstream code in
 * `AbstractDeltaCatalog.loadTable` (`DeltaTableUtils.isDeltaTable`, `loadCatalogTable`, ...)
 * does not need to change.
 *
 * Pure function of (catalog name, identifier, DRC client). Credential vending is introduced
 * in a subsequent PR and will layer onto the same call path without changing this signature.
 */
private[delta] object DeltaRestTableLoader {

  /** Stashed on `CatalogTable.properties` so the commit path (later PR) can assert-etag. */
  val PROP_DRC_ETAG: String = "io.unitycatalog.drc.etag"
  /** Stashed on `CatalogTable.properties` so the commit path can assert-table-uuid. */
  val PROP_DRC_TABLE_ID: String = "io.unitycatalog.drc.table-id"
  /** Latest table version per DRC response -- informational. */
  val PROP_DRC_LATEST_VERSION: String = "io.unitycatalog.drc.latest-table-version"

  /**
   * Loads a table via the DRC client and assembles a `V1Table`.
   *
   * @param catalogName the Spark-side catalog name (e.g. "unity"); becomes the catalog segment
   *                    of the returned `TableIdentifier`.
   * @param ident the table identifier -- namespace must contain exactly the schema name.
   * @param client the DRC client (one is constructed per load; shares the UC-owned ApiClient).
   */
  def load(catalogName: String, ident: Identifier, client: UCDeltaClient): V1Table = {
    require(catalogName != null && catalogName.nonEmpty, "catalogName must be non-empty")
    require(ident != null, "ident must not be null")
    require(client != null, "client must not be null")
    require(ident.namespace() != null && ident.namespace().length == 1,
      s"DRC only supports single-level namespaces, got: ${ident}")

    val schemaName = ident.namespace()(0)
    val tableName = ident.name()
    val response: LoadTableResponse = client.loadTable(catalogName, schemaName, tableName)
    buildV1Table(catalogName, ident, response)
  }

  /** Visible for testing. */
  private[uccatalog] def buildV1Table(
      catalogName: String,
      ident: Identifier,
      response: LoadTableResponse): V1Table = {
    require(response != null, "DRC loadTable response must not be null")
    val md = Option(response.getMetadata).getOrElse(
      throw new IllegalStateException("DRC loadTable response is missing metadata"))

    val columns = Option(md.getColumns)
      .map(_.getFields)
      .getOrElse(java.util.Collections.emptyList())
    val sparkSchema = DeltaRestSchemaConverter.toSparkSchema(columns)

    val schemaName = ident.namespace()(0)
    val tableIdentifier = TableIdentifier(ident.name(), Some(schemaName), Some(catalogName))

    val locationUri = Option(md.getLocation).map(CatalogUtils.stringToURI)
    val properties = Option(md.getProperties)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

    val enrichedProps = properties ++ Map(
      PROP_DRC_ETAG -> Option(md.getEtag).getOrElse(""),
      PROP_DRC_TABLE_ID -> Option(md.getTableUuid).map(_.toString).getOrElse(""),
      PROP_DRC_LATEST_VERSION ->
        Option(response.getLatestTableVersion).map(_.toString).getOrElse(""))

    val tableType = md.getTableType match {
      case UCTableType.MANAGED => CatalogTableType.MANAGED
      case _ => CatalogTableType.EXTERNAL
    }

    val partitionCols = Option(md.getPartitionColumns)
      .map(_.asScala.toSeq)
      .getOrElse(Nil)

    val catalogTable = CatalogTable(
      identifier = tableIdentifier,
      tableType = tableType,
      storage = CatalogStorageFormat.empty.copy(locationUri = locationUri),
      schema = sparkSchema,
      provider = Some("delta"),
      partitionColumnNames = partitionCols,
      properties = enrichedProps
    )
    V1Table(catalogTable)
  }
}
