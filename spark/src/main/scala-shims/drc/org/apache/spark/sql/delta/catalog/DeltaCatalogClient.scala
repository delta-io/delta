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

import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCTokenBasedDeltaRestClient}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY
import io.unitycatalog.client.ApiClient
import io.unitycatalog.client.api.{TablesApi => LegacyTablesApi, TemporaryCredentialsApi}
import io.unitycatalog.client.delta.{DeltaApiProvider, DeltaRestClientProvider}
import io.unitycatalog.client.delta.api.{TablesApi => DeltaTablesApi}
import io.unitycatalog.client.model.{GenerateTemporaryTableCredential, TableOperation}

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
 * DRC-enabled Spark catalog client. Owns the shared ApiClient and creates
 * both DRC and legacy API clients from it. loadTable/createTable are
 * Spark-specific (schema conversion, credential vending) and live here,
 * not in the storage-module UCDeltaClient interface.
 */
class DeltaCatalogClient private (
    val ucDeltaClient: UCDeltaClient,
    apiClient: ApiClient,
    drcEnabled: Boolean,
    catalogName: String) {

  private lazy val deltaTablesApi: DeltaTablesApi =
    new DeltaTablesApi(apiClient)
  private lazy val legacyTablesApi: LegacyTablesApi =
    new LegacyTablesApi(apiClient)
  private lazy val credentialsApi: TemporaryCredentialsApi =
    new TemporaryCredentialsApi(apiClient)

  def loadTable(ident: Identifier): Table = {
    require(ident.namespace().length >= 1,
      s"Expected at least one namespace element for $ident")
    val schemaName = ident.namespace()(0)
    val tableName = ident.name()

    if (drcEnabled) loadTableDRC(schemaName, tableName)
    else loadTableLegacy(schemaName, tableName)
  }

  private def loadTableDRC(schemaName: String, tableName: String): Table = {
    val response = deltaTablesApi.loadTable(catalogName, schemaName, tableName)
    val meta = response.getMetadata
    val tableType = if (meta.getTableType != null &&
        meta.getTableType.getValue == "EXTERNAL") {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    val tableId = if (meta.getTableUuid != null) meta.getTableUuid.toString else ""

    // Convert DRC StructType directly to Spark StructType
    val schema = if (meta.getColumns != null) {
      DeltaRestSchemaConverter.toSparkStructType(meta.getColumns)
    } else {
      new StructType()
    }

    val credProps = vendCredentials(tableId)
    val tableProps = Option(meta.getProperties).map(_.asScala.toMap)
      .getOrElse(Map.empty) +
      (UC_TABLE_ID_KEY -> tableId) ++
      Option(meta.getEtag).map("io.unitycatalog.etag" -> _)

    V1Table(CatalogTable(
      identifier = TableIdentifier(tableName, Some(schemaName), Some(catalogName)),
      tableType = tableType,
      storage = DataSource.buildStorageFormatFromOptions(Map.empty)
        .copy(
          locationUri = Some(new java.net.URI(meta.getLocation)),
          properties = credProps),
      schema = schema,
      provider = Some("delta"),
      properties = tableProps))
  }

  private def loadTableLegacy(schemaName: String, tableName: String): Table = {
    val fullName = s"$catalogName.$schemaName.$tableName"
    val info = legacyTablesApi.getTable(fullName, null, null)
    val tableType = if (info.getTableType != null &&
        info.getTableType.getValue == "EXTERNAL") {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    val tableId = info.getTableId

    // Convert ColumnInfo to Spark StructType
    val schema = if (info.getColumns != null && !info.getColumns.isEmpty) {
      StructType(info.getColumns.asScala.map { col =>
        StructField(col.getName, DataType.fromDDL(col.getTypeText), col.getNullable)
      }.toArray)
    } else {
      new StructType()
    }

    val credProps = vendCredentials(tableId)
    val tableProps = Option(info.getProperties).map(_.asScala.toMap)
      .getOrElse(Map.empty) + (UC_TABLE_ID_KEY -> tableId)

    V1Table(CatalogTable(
      identifier = TableIdentifier(tableName, Some(schemaName), Some(catalogName)),
      tableType = tableType,
      storage = DataSource.buildStorageFormatFromOptions(Map.empty)
        .copy(
          locationUri = Some(new java.net.URI(info.getStorageLocation)),
          properties = credProps),
      schema = schema,
      provider = Some("delta"),
      properties = tableProps))
  }

  private def vendCredentials(tableId: String): Map[String, String] = {
    try {
      val creds = credentialsApi.generateTemporaryTableCredentials(
        new GenerateTemporaryTableCredential()
          .tableId(tableId)
          .operation(TableOperation.READ_WRITE))
      if (creds != null && creds.getAwsTempCredentials != null) {
        val aws = creds.getAwsTempCredentials
        Map(
          "fs.s3a.access.key" -> aws.getAccessKeyId,
          "fs.s3a.secret.key" -> aws.getSecretAccessKey,
          "fs.s3a.session.token" -> aws.getSessionToken,
          "fs.s3a.aws.credentials.provider" ->
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
      } else Map.empty
    } catch {
      case _: Exception => Map.empty
    }
  }

  def createTable(
      ident: Identifier,
      v1Table: CatalogTable,
      postCommitSnapshot: Snapshot): Unit = {
    ucDeltaClient.createTable(
      catalogName, ident.namespace()(0), ident.name(),
      v1Table.location.toString,
      postCommitSnapshot.metadata,
      postCommitSnapshot.protocol,
      v1Table.tableType == CatalogTableType.MANAGED,
      "DELTA",
      postCommitSnapshot.domainMetadata.asJava)
  }
}

object DeltaCatalogClient {
  def apply(
      delegate: CatalogPlugin,
      drcEnabled: Boolean,
      catalogName: String): DeltaCatalogClient = {
    val apiClient = delegate match {
      case p: DeltaRestClientProvider
          if p.getApiClient().isInstanceOf[ApiClient] =>
        p.getApiClient().asInstanceOf[ApiClient]
      case _ =>
        throw new IllegalStateException(
          "Catalog delegate does not provide an ApiClient")
    }
    val client = new UCTokenBasedDeltaRestClient(apiClient, drcEnabled)
    new DeltaCatalogClient(client, apiClient, drcEnabled, catalogName)
  }
}
