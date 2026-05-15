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

import java.net.URI
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier

import scala.jdk.CollectionConverters._

import io.delta.storage.commit.{TableIdentifier => StorageTableIdentifier}
import io.delta.storage.commit.uccommitcoordinator.{
  UCDeltaClient,
  UCDeltaModels,
  UCDeltaTokenBasedRestClient
}
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo
import io.delta.storage.commit.uccommitcoordinator.exceptions.{
  CredentialFetchFailedException,
  UnsupportedTableFormatException,
  NoSuchTableException => StorageNoSuchTableException
}
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType
}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, V1Table}
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * [[AbstractDeltaCatalogClient]] backed by a [[UCDeltaClient]]; translates between
 * Spark/Delta types and the storage-side UC types.
 */
private[catalog] class UCDeltaCatalogClientImpl(
    catalogName: String,
    ucClient: UCDeltaClient,
    serverSidePlanningEnabled: Boolean = false,
    fallbackLoadTable: Identifier => Table = UCDeltaCatalogClientImpl.defaultFallbackLoadTable)
  extends AbstractDeltaCatalogClient with Logging {

  override def loadTable(ident: Identifier): Table = {
    UCDeltaCatalogClientImpl.LOAD_TABLE_INVOCATIONS.incrementAndGet()
    val tid = toStorageTableIdent(ident)
    val info =
      try ucClient.loadTable(tid)
      catch {
        case _: StorageNoSuchTableException => throw new NoSuchTableException(ident)
        case e: UnsupportedTableFormatException =>
          logInfo(log"Table ${MDC(DeltaLogKeys.TABLE_NAME, ident)} is not in Delta format; " +
            log"falling back to the legacy catalog path. Cause: " +
            log"${MDC(DeltaLogKeys.EXCEPTION, e.getMessage)}")
          return fallbackLoadTable(ident)
        case e: CredentialFetchFailedException if serverSidePlanningEnabled =>
          logWarning(
            s"Credential fetch failed for ${fullQualifiedTableName(tid)}; enabling " +
              s"server-side planning fallback. Cause: ${e.getMessage}")
          enableServerSidePlanningConfig(ident)
          e.getTableInfoWithoutCredentials
      }
    UCDeltaCatalogClientImpl.SUCCESSFUL_DELTA_REST_API_LOADS.incrementAndGet()
    toV1Table(ident, info)
  }

  private def enableServerSidePlanningConfig(ident: Identifier): Unit = {
    SparkSession.getActiveSession match {
      case Some(spark) =>
        spark.conf.set(DeltaSQLConf.ENABLE_SERVER_SIDE_PLANNING.key, "true")
        logInfo(log"Server-side planning enabled for table " +
          log"${MDC(DeltaLogKeys.TABLE_NAME, ident)}; Delta will read via SSP with empty creds.")
      case None =>
        logWarning(log"Server-side planning requested for table " +
          log"${MDC(DeltaLogKeys.TABLE_NAME, ident)} but no active SparkSession found.")
    }
  }

  // ---------- conversions ----------

  private def toStorageTableIdent(ident: Identifier): StorageTableIdentifier = {
    val ns = ident.namespace()
    require(
      ns.length == 1,
      s"UC identifiers must be of the form <schema>.<table>; got namespace ${ns.mkString(".")}")
    new StorageTableIdentifier(Array(catalogName, ns(0)), ident.name())
  }

  /** Three-part dotted name from a `[catalog, schema]` + `name` storage identifier. */
  private def fullQualifiedTableName(t: StorageTableIdentifier): String = {
    val ns = t.getNamespace
    s"${ns(0)}.${ns(1)}.${t.getName}"
  }

  private def toV1Table(ident: Identifier, info: TableInfo): V1Table = {
    val m = info.getMetadata
    val properties = Option(m.getConfiguration)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val partitionColumns = Option(m.getPartitionColumns)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty[String])
    val schema = Option(m.getSchemaString)
      .map(DataType.fromJson(_).asInstanceOf[StructType])
      .getOrElse(new StructType())
    val storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(new URI(info.getLocation)),
      properties = properties ++ info.getStorageProperties.asScala.toMap)
    val catalogTable = CatalogTable(
      identifier = TableIdentifier(ident.name(), ident.namespace().headOption, Some(catalogName)),
      tableType = fromUcTableType(info.getTableType),
      storage = storage,
      schema = schema,
      provider = Option(m.getProvider).map(_.toLowerCase(java.util.Locale.ROOT)),
      partitionColumnNames = partitionColumns,
      comment = Option(m.getDescription),
      createTime = if (m.getCreatedTime != null) m.getCreatedTime else 0L,
      tracksPartitionsInCatalog = false)
    V1Table(catalogTable)
  }

  private def fromUcTableType(t: UCDeltaModels.TableType): CatalogTableType = t match {
    case UCDeltaModels.TableType.MANAGED => CatalogTableType.MANAGED
    case UCDeltaModels.TableType.EXTERNAL => CatalogTableType.EXTERNAL
  }
}

object UCDeltaCatalogClientImpl extends AbstractDeltaCatalogClientFactory with Logging {
  /** Bumped at every loadTable entry, regardless of outcome. */
  val LOAD_TABLE_INVOCATIONS: AtomicLong = new AtomicLong(0L)

  /**
   * Bumped only when loadTable returned a Delta table from the Delta REST API (no fallback,
   * no rethrow). Use this for "Delta REST actually served the load" assertions.
   */
  val SUCCESSFUL_DELTA_REST_API_LOADS: AtomicLong = new AtomicLong(0L)

  private[catalog] val RenewCredentialEnabledKey: String = "renewCredential.enabled"
  private[catalog] val CredScopedFsEnabledKey: String = "credScopedFs.enabled"
  private[catalog] val ServerSidePlanningEnabledKey: String = "serverSidePlanning.enabled"

  private[catalog] val defaultFallbackLoadTable: Identifier => Table = ident =>
    throw new IllegalStateException(
      s"Non-Delta table $ident cannot be served via the Delta REST API path and no " +
        "fallback catalog was configured.")

  /**
   * Builds a [[UCDeltaCatalogClientImpl]] from catalog options. The `deltaRestApi.enabled`
   * gate is the caller's responsibility
   * ([[AbstractDeltaCatalogClient.fromCatalogOptionsIfEnabled]]).
   * {@code fallbackLoadTable} is invoked when UC reports {@code UnsupportedTableFormatException}.
   */
  override def fromCatalogOptions(
      catalogName: String,
      options: CaseInsensitiveStringMap,
      fallbackLoadTable: Identifier => Table
  ): UCDeltaCatalogClientImpl = {
    val uri = Option(options.get("uri")).getOrElse(throw new IllegalArgumentException(
      s"'uri' is required when 'deltaRestApi.enabled' is true (catalog '$catalogName')"))
    val authConfigs = extractAuthConfigs(options, catalogName)
    val appVersions = UCTokenBasedRestClientFactory.defaultAppVersionsAsJava
    val renewCredEnabled = options.getBoolean(RenewCredentialEnabledKey, true)
    val credScopedFsEnabled = options.getBoolean(CredScopedFsEnabledKey, false)
    val sspEnabled = options.getBoolean(ServerSidePlanningEnabledKey, false)
    val hadoopConfSupplier: Supplier[Configuration] =
      () => SparkSession.getActiveSession
        .map(_.sparkContext.hadoopConfiguration)
        .getOrElse(new Configuration())
    val restClient = UCDeltaTokenBasedRestClient.create(
      uri,
      authConfigs,
      appVersions,
      renewCredEnabled,
      credScopedFsEnabled,
      hadoopConfSupplier)
    new UCDeltaCatalogClientImpl(catalogName, restClient, sspEnabled, fallbackLoadTable)
  }

  /**
   * `auth.*` sub-keys (prefix stripped) feed `TokenProvider.create`. Legacy bare `token`
   * is translated to `{type=static, token=<value>}`, only when no `auth.*` is present.
   */
  private[catalog] def extractAuthConfigs(
      options: CaseInsensitiveStringMap,
      catalogName: String): java.util.Map[String, String] = {
    val authConfigs = new java.util.HashMap[String, String]()
    val authPrefix = "auth."
    // CaseInsensitiveStringMap.entrySet() returns keys already lowercased.
    options.entrySet().asScala.foreach { e =>
      val key = e.getKey
      if (key.startsWith(authPrefix)) {
        authConfigs.put(key.substring(authPrefix.length), e.getValue)
      }
    }
    if (authConfigs.isEmpty) {
      Option(options.get("token")).foreach { tok =>
        authConfigs.put("type", "static")
        authConfigs.put("token", tok)
      }
    }
    if (authConfigs.isEmpty) {
      throw new IllegalArgumentException(
        s"auth configuration is required when 'deltaRestApi.enabled' is true " +
          s"(catalog '$catalogName'). Set either 'auth.type' (with the corresponding " +
          s"auth.* keys) or the legacy 'token' option.")
    }
    authConfigs
  }
}
