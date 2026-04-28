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
import java.util.Locale

import scala.collection.JavaConverters._

import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCTokenBasedRestClient}
import io.unitycatalog.client.ApiException
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.client.delta.model.{
  CredentialOperation,
  CredentialsResponse,
  StorageCredential,
  StorageCredentialConfig,
  TableType => DeltaTableType
}
import io.unitycatalog.client.model.{
  AwsCredentials,
  AzureUserDelegationSAS,
  GcpOauthToken,
  TableOperation,
  TemporaryCredentials
}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, V1Table}
import org.apache.spark.sql.delta.catalog.credentials.CredPropsUtil
import org.apache.spark.sql.delta.catalog.credentials.fs.CredScopedFileSystem
import org.apache.spark.sql.delta.coordinatedcommits.{
  UCCommitCoordinatorBuilder,
  UCTokenBasedRestClientFactory
}

private class DeltaCatalogClient private (
    private val ucDeltaClient: Option[UCClient],
    catalogName: String,
    credentialContext: Option[UCDeltaRestCatalogApiCredentialContext]) {

  import DeltaCatalogClient._

  def loadTable(ident: Identifier): Option[Table] = {
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
        val locationUri = CatalogUtils.stringToURI(metadata.getLocation)
        val credentials = Option.when(isCloudScheme(locationUri.getScheme)) {
          try {
            // Prefer READ_WRITE so a loaded table can be used for writes without reloading
            // credentials; read-only principals fall back to READ below.
            client.getTableCredentials(
              CredentialOperation.READ_WRITE,
              catalogName,
              schemaName,
              tableName)
          } catch {
            case e: IOException if isAuthError(e) =>
              client.getTableCredentials(
                CredentialOperation.READ,
                catalogName,
                schemaName,
                tableName)
          }
        }
        Some(V1Table(toCatalogTable(ident, metadata, locationUri, credentials)))
      case _ =>
        // UC Delta Rest Catalog API only supports catalog.schema.table identifiers for named
        // tables.
        None
    }
  }

  private def isAuthError(e: IOException): Boolean = e.getCause match {
    case api: ApiException => api.getCode == 401 || api.getCode == 403
    case _ => false
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
            s"Unsupported UC Delta Rest Catalog API table type for " +
              s"$catalogName.${ident.namespace().mkString(".")}.${ident.name()}: $other")
      },
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(locationUri),
        properties = toStorageProperties(metadata, credentials, locationUri.getScheme)),
      schema = UCDeltaRestCatalogApiSchemaConverter.toSparkType(metadata.getColumns),
      provider = Some(metadata.getDataSourceFormat.getValue.toLowerCase(Locale.ROOT)),
      partitionColumnNames = Option(metadata.getPartitionColumns)
        .map(_.asScala.toSeq)
        .getOrElse(Nil))
  }

  private def toStorageProperties(
      metadata: io.unitycatalog.client.delta.model.TableMetadata,
      credentials: Option[CredentialsResponse],
      locationScheme: String): Map[String, String] = {
    val credentialProperties = toCredentialProperties(
      metadata.getLocation,
      credentials.toSeq.flatMap(getStorageCredentials),
      locationScheme,
      metadata.getTableUuid.toString)
    // V1Table exposes storage properties as option.* table properties. Keep UC Delta Rest Catalog
    // API table features here so the Delta load path receives them with the same option.* shape as
    // other storage-level UC properties, while CatalogTable.properties stays reserved for Spark
    // metadata.
    Option(metadata.getProperties).map(_.asScala.toMap).getOrElse(Map.empty) ++ credentialProperties
  }

  private def toCredentialProperties(
      location: String,
      storageCredentials: Seq[StorageCredential],
      locationScheme: String,
      tableId: String): Map[String, String] = {
    if (!isCloudScheme(locationScheme)) {
      Map.empty[String, String]
    } else if (storageCredentials.isEmpty) {
      throw new IllegalArgumentException(
        s"UC Delta Rest Catalog API returned no storage credentials for cloud location $location.")
    } else {
      selectStorageCredential(location, storageCredentials)
        .map { credential =>
          val context = credentialContext.getOrElse {
            throw new IllegalStateException(
              "UC Delta Rest Catalog API credential context is missing while credentials are " +
                "present.")
          }
          CredPropsUtil.createTableCredProps(
            context.renewCredentialEnabled,
            context.credScopedFsEnabled,
            context.fsImplProps.asJava,
            locationScheme.toLowerCase(Locale.ROOT),
            context.uri,
            context.tokenProvider,
            tableId,
            toTableOperation(requireOperation(credential)),
            toTemporaryCredentials(credential)).asScala.toMap
        }
        .getOrElse {
          throw new IllegalArgumentException(
            s"No storage credential matched UC Delta Rest Catalog API location $location.")
        }
    }
  }

  private def getStorageCredentials(credentials: CredentialsResponse): Seq[StorageCredential] = {
    Option(credentials)
      .flatMap(c => Option(c.getStorageCredentials))
      .map(_.asScala.toSeq)
      .getOrElse(Nil)
  }

  private def toTableOperation(operation: CredentialOperation): TableOperation = {
    operation match {
      case CredentialOperation.READ => TableOperation.READ
      case CredentialOperation.READ_WRITE => TableOperation.READ_WRITE
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported UC Delta Rest Catalog API credential operation: $other")
    }
  }

  private def toTemporaryCredentials(credential: StorageCredential): TemporaryCredentials = {
    val config = requireConfig(credential)
    val tempCredentials = new TemporaryCredentials().expirationTime(credential.getExpirationTimeMs)
    val hasS3 = Seq(
      config.getS3AccessKeyId,
      config.getS3SecretAccessKey,
      config.getS3SessionToken).exists(_ != null)
    val hasAzure = config.getAzureSasToken != null
    val hasGcs = config.getGcsOauthToken != null
    val configuredClouds = Seq(hasS3, hasAzure, hasGcs).count(identity)

    if (configuredClouds != 1) {
      throw new IllegalArgumentException(
        "UC Delta Rest Catalog API storage credential for prefix " +
          s"${credential.getPrefix} must contain " +
          "exactly one cloud credential config.")
    }

    if (hasS3) {
      tempCredentials.awsTempCredentials(new AwsCredentials()
        .accessKeyId(requireCredentialField(config.getS3AccessKeyId, credential, "S3 access key"))
        .secretAccessKey(requireCredentialField(
          config.getS3SecretAccessKey, credential, "S3 secret key"))
        .sessionToken(requireCredentialField(
          config.getS3SessionToken, credential, "S3 session token")))
    } else if (hasAzure) {
      tempCredentials.azureUserDelegationSas(new AzureUserDelegationSAS()
        .sasToken(config.getAzureSasToken))
    } else {
      tempCredentials.gcpOauthToken(new GcpOauthToken()
        .oauthToken(config.getGcsOauthToken))
    }
  }

  private def requireConfig(credential: StorageCredential): StorageCredentialConfig = {
    Option(credential.getConfig).getOrElse {
      throw new IllegalArgumentException(
        "UC Delta Rest Catalog API storage credential for prefix " +
          s"${credential.getPrefix} is missing config.")
    }
  }

  private def requireOperation(credential: StorageCredential): CredentialOperation = {
    Option(credential.getOperation).getOrElse {
      throw new IllegalArgumentException(
        "UC Delta Rest Catalog API storage credential for prefix " +
          s"${credential.getPrefix} is missing operation.")
    }
  }

  private def requireCredentialField(
      value: String,
      credential: StorageCredential,
      field: String): String = {
    Option(value).getOrElse {
      throw new IllegalArgumentException(
        "UC Delta Rest Catalog API storage credential for prefix " +
          s"${credential.getPrefix} is missing $field.")
    }
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
    val normalizedLocation = location.stripSuffix("/")
    val normalizedPrefix = prefix.stripSuffix("/")
    // Require a path boundary so s3://bucket/table does not match s3://bucket/table_backup.
    normalizedPrefix.nonEmpty &&
      (normalizedLocation == normalizedPrefix ||
        (normalizedLocation.startsWith(normalizedPrefix) &&
          normalizedLocation.charAt(normalizedPrefix.length) == '/'))
  }
}

private case class UCDeltaRestCatalogApiCredentialContext(
    uri: String,
    tokenProvider: TokenProvider,
    renewCredentialEnabled: Boolean,
    credScopedFsEnabled: Boolean,
    fsImplProps: Map[String, String])

private object DeltaCatalogClient {
  private[catalog] val UCDeltaRestCatalogApiEnabledKey = "deltaRestApi.enabled"
  private[catalog] val RenewCredentialEnabledKey = "renewCredential.enabled"
  private[catalog] val CredScopedFsEnabledKey = "credScopedFs.enabled"
  private val DefaultRenewCredentialEnabled = true
  private val DefaultCredScopedFsEnabled = false
  private val FsImplKeys = Set(
    "fs.s3.impl",
    "fs.s3a.impl",
    "fs.gs.impl",
    "fs.abfs.impl",
    "fs.abfss.impl",
    "fs.AbstractFileSystem.s3.impl",
    "fs.AbstractFileSystem.s3a.impl",
    "fs.AbstractFileSystem.gs.impl",
    "fs.AbstractFileSystem.abfs.impl",
    "fs.AbstractFileSystem.abfss.impl")
  private val CloudSchemes = Set("s3", "s3a", "gs", "abfs", "abfss")

  private[catalog] def deltaRestApiEnabledConf(catalogName: String): String = {
    s"spark.sql.catalog.$catalogName.$UCDeltaRestCatalogApiEnabledKey"
  }

  private[catalog] def renewCredentialEnabledConf(catalogName: String): String = {
    s"spark.sql.catalog.$catalogName.$RenewCredentialEnabledKey"
  }

  private[catalog] def credScopedFsEnabledConf(catalogName: String): String = {
    s"spark.sql.catalog.$catalogName.$CredScopedFsEnabledKey"
  }

  private def isCloudScheme(scheme: String): Boolean = {
    Option(scheme).exists(s => CloudSchemes.contains(s.toLowerCase(Locale.ROOT)))
  }

  def apply(delegatePlugin: CatalogPlugin, spark: SparkSession): DeltaCatalogClient = {
    val catalogName = delegatePlugin.name()
    var credentialContext = Option.empty[UCDeltaRestCatalogApiCredentialContext]
    val ucDeltaClient = if (spark.conf
        .get(deltaRestApiEnabledConf(catalogName), "false")
        .toBoolean) {
      val (_, uri, authConfig) = UCCommitCoordinatorBuilder.getCatalogConfigs(spark)
        .collectFirst { case (`catalogName`, configuredUri, configuredAuthConfig) =>
          (catalogName, configuredUri, configuredAuthConfig)
        }
        .getOrElse {
          throw new IllegalArgumentException(
            "UC Delta Rest Catalog API is enabled for catalog " +
              s"$catalogName, but its Unity Catalog " +
              "configuration is missing or incomplete.")
        }
      val tokenProvider = TokenProvider.create(authConfig.asJava)
      credentialContext = Some(UCDeltaRestCatalogApiCredentialContext(
        uri.toString,
        tokenProvider,
        spark.conf.get(
          renewCredentialEnabledConf(catalogName),
          DefaultRenewCredentialEnabled.toString).toBoolean,
        spark.conf.get(
          credScopedFsEnabledConf(catalogName),
          DefaultCredScopedFsEnabled.toString).toBoolean,
        sessionHadoopFsImplProps(spark)))
      val client = new UCTokenBasedRestClient(
        uri,
        tokenProvider,
        UCTokenBasedRestClientFactory.defaultAppVersionsAsJava,
        catalogName)
      if (client.supportsUCDeltaRestCatalogApi()) {
        Some(client)
      } else {
        client.close()
        throw new IllegalArgumentException(
          s"UC Delta Rest Catalog API is enabled for catalog $catalogName, but the Unity Catalog " +
            "server does not support the required UC Delta Rest Catalog API endpoints.")
      }
    } else {
      None
    }
    new DeltaCatalogClient(ucDeltaClient, catalogName, credentialContext)
  }

  private def sessionHadoopFsImplProps(spark: SparkSession): Map[String, String] = {
    val credScopedFsClass = classOf[CredScopedFileSystem].getName
    FsImplKeys.flatMap { key =>
      spark.conf.getOption(key)
        .orElse(spark.conf.getOption("spark.hadoop." + key))
        .filter(_ != credScopedFsClass)
        .map(key -> _)
    }.toMap
  }
}
