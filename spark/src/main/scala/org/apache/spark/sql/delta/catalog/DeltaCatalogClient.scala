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

import java.io.IOException
import java.net.URI
import java.util.{Collections, Locale}

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY
import io.delta.storage.commit.uccommitcoordinator.UCTokenBasedRestClient
import io.delta.storage.uc.UCDeltaClient
import io.unitycatalog.client.ApiException
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.delta.model.{
  CredentialOperation,
  CredentialsResponse,
  DataSourceFormat => DeltaDataSourceFormat,
  StorageCredential,
  TableType => DeltaTableType
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

private class DeltaCatalogClient private (
    private val ucDeltaClient: Option[UCDeltaClient],
    delegate: TableCatalog,
    catalogName: String) {

  import DeltaCatalogClient._

  def loadTable(ident: Identifier): Table = {
    ucDeltaClient match {
      case Some(client) if ident.namespace().length == 1 =>
        val schemaName = ident.namespace().head
        val tableName = ident.name()
        val metadata = try {
          client.loadTable(catalogName, schemaName, tableName).getMetadata
        } catch {
          case e: IOException =>
            throw translateLoadTableException(ident, e)
        }
        metadata.getDataSourceFormat match {
          case DeltaDataSourceFormat.DELTA =>
            val locationUri = CatalogUtils.stringToURI(metadata.getLocation)
            val credentials = Option.when(isCloudScheme(locationUri.getScheme)) {
              client.getTableCredentials(
                CredentialOperation.READ,
                catalogName,
                schemaName,
                tableName)
            }
            V1Table(toCatalogTable(ident, metadata, locationUri, credentials))
          case _ =>
            delegate.loadTable(ident)
        }
      case _ =>
        delegate.loadTable(ident)
    }
  }

  private def translateLoadTableException(ident: Identifier, e: IOException): Throwable = {
    e.getCause match {
      case api: ApiException if api.getCode == 404 =>
        new NoSuchTableException(ident)
      case _ =>
        e
    }
  }

  private def toCatalogTable(
      ident: Identifier,
      metadata: io.unitycatalog.client.delta.model.TableMetadata,
      locationUri: URI,
      credentials: Option[CredentialsResponse]): CatalogTable = {
    CatalogTable(
      identifier =
        TableIdentifier(ident.name(), ident.namespace().lastOption, Some(catalogName)),
      tableType = metadata.getTableType match {
        case DeltaTableType.MANAGED =>
          CatalogTableType.MANAGED
        case DeltaTableType.EXTERNAL =>
          CatalogTableType.EXTERNAL
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported Delta REST table type for " +
              s"$catalogName.${ident.namespace().mkString(".")}.${ident.name()}: $other")
      },
      storage = CatalogStorageFormat(
        locationUri = Some(locationUri),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = toStorageProperties(metadata, credentials, locationUri.getScheme)),
      schema = DeltaRestSchemaConverter.toSparkType(metadata.getColumns),
      provider = Some(DeltaSourceUtils.ALT_NAME),
      partitionColumnNames = Option(metadata.getPartitionColumns)
        .map(_.asScala.toSeq)
        .getOrElse(Nil),
      properties = Option(metadata.getProperties)
        .map(_.asScala.toMap)
        .getOrElse(Map.empty))
  }

  private def toStorageProperties(
      metadata: io.unitycatalog.client.delta.model.TableMetadata,
      credentials: Option[CredentialsResponse],
      locationScheme: String): Map[String, String] = {
    val storageCredentials = credentials.toSeq.flatMap(getStorageCredentials)
    val credentialProperties =
      if (!isCloudScheme(locationScheme)) {
        Map.empty[String, String]
      } else if (storageCredentials.isEmpty) {
        throw new IllegalArgumentException(
          s"Delta REST returned no storage credentials for cloud location ${metadata.getLocation}.")
      } else {
        selectStorageCredential(metadata.getLocation, storageCredentials)
          .map(storageCredentialToProperties)
          .map(withOptionPrefix)
          .getOrElse {
            throw new IllegalArgumentException(
              s"No storage credential matched Delta REST location ${metadata.getLocation}.")
          }
      }
    Map(UC_TABLE_ID_KEY -> metadata.getTableUuid.toString) ++ credentialProperties
  }

  private def getStorageCredentials(credentials: CredentialsResponse): Seq[StorageCredential] = {
    Option(credentials)
      .flatMap(c => Option(c.getStorageCredentials))
      .map(_.asScala.toSeq)
      .getOrElse(Nil)
  }

  private def selectStorageCredential(
      location: String,
      credentials: Seq[StorageCredential]): Option[StorageCredential] = {
    credentials
      .filter { credential =>
        Option(credential.getPrefix).exists(prefix =>
          prefix.nonEmpty && matchesCredentialPrefix(location, prefix))
      }
      .maxByOption(_.getPrefix.length)
  }

  private def matchesCredentialPrefix(location: String, prefix: String): Boolean = {
    location == prefix ||
      (location.startsWith(prefix) &&
        (prefix.endsWith("/") ||
          location.length == prefix.length ||
          location.charAt(prefix.length) == '/'))
  }

  private def withOptionPrefix(props: Map[String, String]): Map[String, String] = {
    props ++ props.map { case (key, value) => s"option.$key" -> value }
  }

  private def storageCredentialToProperties(
      credential: StorageCredential): Map[String, String] = {
    val config = credential.getConfig
    if (config == null) {
      throw new IllegalArgumentException(
        s"Delta REST storage credential for prefix ${credential.getPrefix} is missing config.")
    } else if (config.getS3AccessKeyId != null || config.getS3SecretAccessKey != null ||
        config.getS3SessionToken != null) {
      if (config.getS3AccessKeyId == null || config.getS3SecretAccessKey == null) {
        throw new IllegalArgumentException(
          s"Delta REST S3 credential for prefix ${credential.getPrefix} is incomplete.")
      }
      Map(
        "fs.s3a.path.style.access" -> "true",
        "fs.s3.impl.disable.cache" -> "true",
        "fs.s3a.impl.disable.cache" -> "true",
        "fs.s3a.access.key" -> config.getS3AccessKeyId,
        "fs.s3a.secret.key" -> config.getS3SecretAccessKey
      ) ++ Option(config.getS3SessionToken).map("fs.s3a.session.token" -> _)
    } else if (config.getAzureSasToken != null) {
      Map(
        "fs.azure.account.auth.type" -> "SAS",
        "fs.azure.account.hns.enabled" -> "true",
        "fs.abfs.impl.disable.cache" -> "true",
        "fs.abfss.impl.disable.cache" -> "true",
        "fs.azure.sas.fixed.token" -> config.getAzureSasToken)
    } else if (config.getGcsOauthToken != null) {
      Map(
        "fs.gs.create.items.conflict.check.enable" -> "true",
        "fs.gs.impl.disable.cache" -> "true",
        "fs.gs.auth.access.token.credential" -> config.getGcsOauthToken
      ) ++ Option(credential.getExpirationTimeMs)
        .map("fs.gs.auth.access.token.expiration" -> _.toString)
    } else {
      throw new IllegalArgumentException(
        s"Unsupported Delta REST storage credential config for prefix ${credential.getPrefix}.")
    }
  }
}

private object DeltaCatalogClient {
  private val CloudSchemes = Set("s3", "s3a", "gs", "abfs", "abfss")

  private def isCloudScheme(scheme: String): Boolean = {
    Option(scheme).exists(s => CloudSchemes.contains(s.toLowerCase(Locale.ROOT)))
  }

  def apply(delegatePlugin: CatalogPlugin, spark: SparkSession): DeltaCatalogClient = {
    val delegate = delegatePlugin.asInstanceOf[TableCatalog]
    val catalogName = delegatePlugin.name()
    val ucDeltaClient = if (spark.conf
        .get(s"spark.sql.catalog.$catalogName.deltaRestApi.enabled", "false")
        .toBoolean) {
      val (_, uri, authConfig) = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
        .collectFirst { case (`catalogName`, configuredUri, configuredAuthConfig) =>
          (catalogName, configuredUri, configuredAuthConfig)
        }
        .getOrElse {
          throw new IllegalArgumentException(
            s"DRC is enabled for catalog $catalogName, but its Unity Catalog configuration " +
              "is missing or incomplete.")
        }
      Some(new UCTokenBasedRestClient(
        uri,
        TokenProvider.create(authConfig.asJava),
        Collections.emptyMap()))
    } else {
      None
    }
    new DeltaCatalogClient(ucDeltaClient, delegate, catalogName)
  }
}
