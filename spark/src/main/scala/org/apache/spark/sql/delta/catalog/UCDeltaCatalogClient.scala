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
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.util.{Locale, Map => JMap}

import scala.collection.JavaConverters._

import io.delta.storage.commit.actions.AbstractMetadata
import io.delta.storage.commit.uccommitcoordinator.UCDeltaClient
import io.unitycatalog.client.ApiException
import io.unitycatalog.client.auth.TokenProvider
import io.unitycatalog.hadoop.UCCredentialHadoopConfs
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.{PathOperation, TableOperation}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table, V1Table}
import org.apache.spark.sql.delta.coordinatedcommits.{
  UCCommitCoordinatorBuilder,
  UCTokenBasedRestClientFactory
}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils

/**
 * Spark-side client for using UC Delta Rest Catalog API responses inside `DeltaCatalog`.
 *
 * This class owns the UC-specific implementation behind the Spark-facing [[DeltaCatalogClient]]
 * interface: choosing when to try the Delta API path, translating Delta API metadata into Spark
 * catalog objects, fetching credentials for Spark/Hadoop, and returning `None` when
 * `DeltaCatalog` should fall back to the legacy catalog path.
 */
private class UCDeltaCatalogClient private (
    private val ucDeltaClient: Option[UCDeltaClient],
    catalogName: String,
    credentialContext: Option[UCDeltaRestCatalogApiCredentialContext]) extends DeltaCatalogClient {

  import UCDeltaCatalogClient._

  def loadTable(ident: Identifier): Option[Table] = {
    ucDeltaClient match {
      case Some(client) if isNamedTableIdentifier(ident) =>
        val schemaName = ident.namespace().head
        val tableName = ident.name()
        val metadata = try {
          client.loadTable(catalogName, schemaName, tableName)
        } catch {
          case e: IOException if isUnsupportedTableFormat(e) =>
            return None
          case e: IOException =>
            throw translateLoadTableException(ident, e)
        }
        val location = reflectedString(metadata, "getLocation")
        val locationUri = CatalogUtils.stringToURI(location)
        Some(V1Table(buildCatalogTableFromUCDeltaMetadata(
          ident,
          metadata,
          location,
          locationUri)))
      case _ =>
        // UC Delta Rest Catalog API only supports catalog.schema.table identifiers for named
        // tables.
        None
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

  /**
   * UC Delta APIs use this explicit 501 response when a table exists in UC but cannot be served
   * through the Delta endpoint, such as a metric view or another non-Delta table format.
   */
  private def isUnsupportedTableFormat(e: IOException): Boolean = e.getCause match {
    case api: ApiException =>
      api.getCode == 501 &&
        Option(api.getResponseBody).exists(_.contains(UnsupportedTableFormatExceptionType))
    case _ => false
  }

  private def isNamedTableIdentifier(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && !isDeltaPathIdentifier(ident)
  }

  private def isDeltaPathIdentifier(ident: Identifier): Boolean = {
    try {
      ident.namespace().length == 1 &&
        DeltaSourceUtils.isDeltaDataSourceName(ident.namespace().head) &&
        new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  /**
   * Builds the Spark V1 catalog table returned from UC Delta Rest Catalog API metadata.
   * The UC Delta response supplies the Spark table type, schema, provider, and storage metadata.
   */
  private def buildCatalogTableFromUCDeltaMetadata(
      ident: Identifier,
      metadata: AbstractMetadata,
      location: String,
      locationUri: URI): CatalogTable = {
    val schemaName = ident.namespace().head
    val tableName = ident.name()
    CatalogTable(
      identifier =
        TableIdentifier(ident.name(), ident.namespace().lastOption, Some(catalogName)),
      tableType = reflectedString(metadata, "getTableType") match {
        case ManagedTableType =>
          CatalogTableType.MANAGED
        case ExternalTableType =>
          CatalogTableType.EXTERNAL
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported UC Delta Rest Catalog API table type for " +
              s"$catalogName.${ident.namespace().mkString(".")}.${ident.name()}: $other")
      },
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(locationUri),
        properties = buildCatalogStorageProperties(
          metadata,
          location,
          locationUri.getScheme,
          schemaName,
          tableName)),
      schema = UCDeltaRestCatalogApiSchemaConverter.toSparkType(metadata.getSchemaString),
      provider = Option(metadata.getProvider),
      partitionColumnNames = Option(metadata.getPartitionColumns)
        .map(_.asScala.toSeq)
        .getOrElse(Nil))
  }

  /**
   * Builds CatalogStorageFormat.properties for the Spark V1 table.
   * V1Table later exposes these to Delta as option.* table properties.
   */
  private def buildCatalogStorageProperties(
      metadata: AbstractMetadata,
      location: String,
      locationScheme: String,
      schemaName: String,
      tableName: String): Map[String, String] = {
    val credentialProperties = buildHadoopCredentialPropertiesForTable(
      location,
      locationScheme,
      schemaName,
      tableName)
    // V1Table exposes storage properties as option.* table properties. Keep UC Delta Rest Catalog
    // API table features here so the Delta load path receives them with the same option.* shape as
    // other storage-level UC properties, while CatalogTable.properties stays reserved for Spark
    // metadata.
    Option(metadata.getConfiguration).map(_.asScala.toMap).getOrElse(Map.empty) ++
      credentialProperties
  }

  private def buildHadoopCredentialPropertiesForTable(
      location: String,
      locationScheme: String,
      schemaName: String,
      tableName: String): Map[String, String] = {
    if (!isCloudScheme(locationScheme)) {
      Map.empty[String, String]
    } else {
      val context = credentialContext.getOrElse {
        throw new IllegalStateException(
          "UC Delta Rest Catalog API credential context is missing for cloud location " + location)
      }
      val builder = UCCredentialHadoopConfs.builder(
          context.uri,
          locationScheme.toLowerCase(Locale.ROOT))
        .tokenProvider(context.tokenProvider)
        .enableCredentialRenewal(context.renewCredentialEnabled)
        .enableCredentialScopedFs(context.credScopedFsEnabled)
        .hadoopConf(context.hadoopConf)
        .addAppVersions(context.appVersions)
      try {
        // Prefer READ_WRITE so a loaded table can be used for writes without reloading
        // credentials; read-only principals fall back to READ below.
        builder.buildForTable(
            catalogName,
            schemaName,
            tableName,
            TableOperation.READ_WRITE,
            location)
          .asScala
          .toMap
      } catch {
        case e: ApiException if e.getCode == 401 || e.getCode == 403 =>
          builder.buildForTable(catalogName, schemaName, tableName, TableOperation.READ, location)
            .asScala
            .toMap
      }
    }
  }
}

private case class UCDeltaRestCatalogApiCredentialContext(
    uri: String,
    tokenProvider: TokenProvider,
    renewCredentialEnabled: Boolean,
    credScopedFsEnabled: Boolean,
    hadoopConf: Configuration,
    appVersions: JMap[String, String])

private[catalog] object UCDeltaCatalogClient {
  private[catalog] val UCDeltaRestCatalogApiEnabledKey = "deltaRestApi.enabled"
  private[catalog] val RenewCredentialEnabledKey = "renewCredential.enabled"
  private[catalog] val CredScopedFsEnabledKey = "credScopedFs.enabled"
  private val ManagedTableType = "MANAGED"
  private val ExternalTableType = "EXTERNAL"
  private val UnsupportedTableFormatExceptionType = "UnsupportedTableFormatException"
  private val DefaultCatalogConf = "spark.sql.defaultCatalog"
  private val DefaultRenewCredentialEnabled = true
  private val DefaultCredScopedFsEnabled = false
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

  /**
   * Returns UC Delta Rest Catalog API path credential options for raw path-based Delta access.
   *
   * Path-based access has no catalog identifier, so this uses the UC Delta Rest Catalog API-enabled
   * default catalog as the credential authority. If the session has no such default catalog, path
   * reads keep their original options.
   */
  private[delta] def pathCredentialOptions(
      spark: SparkSession,
      path: Path): Map[String, String] = {
    val location = path.toString
    val locationScheme = path.toUri.getScheme
    if (!isCloudScheme(locationScheme)) {
      return Map.empty[String, String]
    }

    selectedUCDeltaRestCatalogApiConfigForPathCredentials(spark)
      .map { context =>
        try {
          buildHadoopCredentialPropertiesForPath(location, locationScheme, context)
        } catch {
          case e: ApiException if e.getCode == 404 =>
            Map.empty[String, String]
        }
      }
      .getOrElse(Map.empty[String, String])
  }

  private def selectedUCDeltaRestCatalogApiConfigForPathCredentials(
      spark: SparkSession): Option[UCDeltaRestCatalogApiCredentialContext] = {
    spark.conf.getOption(DefaultCatalogConf)
      .filter(_.nonEmpty)
      .filter(catalogName =>
        spark.conf.get(deltaRestApiEnabledConf(catalogName), "false").toBoolean)
      .flatMap(catalogName => ucDeltaRestCatalogApiCredentialContext(spark, catalogName))
  }

  private def reflectedString(metadata: AbstractMetadata, methodName: String): String = {
    try {
      metadata.getClass.getMethod(methodName).invoke(metadata).asInstanceOf[String]
    } catch {
      case e: NoSuchMethodException =>
        throw new IllegalStateException(
          s"UC Delta metadata is missing required method $methodName.", e)
      case e: InvocationTargetException =>
        e.getCause match {
          case runtime: RuntimeException => throw runtime
          case error: Error => throw error
          case cause => throw new IllegalStateException(
            s"Failed to read $methodName from UC Delta metadata.", cause)
        }
    }
  }

  private def ucDeltaRestCatalogApiCredentialContext(
      spark: SparkSession,
      catalogName: String): Option[UCDeltaRestCatalogApiCredentialContext] = {
    if (!spark.conf.get(deltaRestApiEnabledConf(catalogName), "false").toBoolean) {
      return None
    }

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
    // Catalog load has no DeltaLog yet, so pass the Spark session Hadoop conf to the UC
    // credential builder.
    // scalastyle:off deltahadoopconfiguration
    val hadoopConf = spark.sessionState.newHadoopConf()
    // scalastyle:on deltahadoopconfiguration
    Some(UCDeltaRestCatalogApiCredentialContext(
      uri.toString,
      tokenProvider,
      spark.conf.get(
        renewCredentialEnabledConf(catalogName),
        DefaultRenewCredentialEnabled.toString).toBoolean,
      spark.conf.get(
        credScopedFsEnabledConf(catalogName),
        DefaultCredScopedFsEnabled.toString).toBoolean,
      hadoopConf,
      UCTokenBasedRestClientFactory.defaultAppVersionsAsJava))
  }

  private def buildHadoopCredentialPropertiesForPath(
      location: String,
      locationScheme: String,
      credentialContext: UCDeltaRestCatalogApiCredentialContext): Map[String, String] = {
    UCCredentialHadoopConfs.builder(
        credentialContext.uri,
        locationScheme.toLowerCase(Locale.ROOT))
      .tokenProvider(credentialContext.tokenProvider)
      .addAppVersions(credentialContext.appVersions)
      .enableCredentialRenewal(credentialContext.renewCredentialEnabled)
      .enableCredentialScopedFs(credentialContext.credScopedFsEnabled)
      .hadoopConf(credentialContext.hadoopConf)
      .buildForPath(location, PathOperation.PATH_READ)
      .asScala
      .toMap
  }

  def apply(delegatePlugin: CatalogPlugin, spark: SparkSession): UCDeltaCatalogClient = {
    val catalogName = delegatePlugin.name()
    val credentialContext = ucDeltaRestCatalogApiCredentialContext(spark, catalogName)
    val ucDeltaClient = credentialContext.flatMap { context =>
      UCTokenBasedRestClientFactory.createUCDeltaClient(
        context.uri,
        context.tokenProvider,
        context.appVersions,
        catalogName) match {
        case Some(client) if client.supportsUCDeltaRestCatalogApi() =>
          Some(client)
        case Some(client) =>
          client.close()
          throw new IllegalArgumentException(
            s"UC Delta Rest Catalog API is enabled for catalog $catalogName, but the Unity " +
              "Catalog server does not support the required UC Delta Rest Catalog API endpoints.")
        case None =>
          None
      }
    }
    new UCDeltaCatalogClient(ucDeltaClient, catalogName, credentialContext)
  }

}
