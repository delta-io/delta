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

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.delta.storage.commit.{TableIdentifier => StorageTableIdentifier}
import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCDeltaModels}
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo
import io.delta.storage.commit.uccommitcoordinator.exceptions.{
  CredentialFetchFailedException,
  UnsupportedTableFormatException,
  NoSuchTableException => StorageNoSuchTableException
}

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
import org.apache.spark.sql.delta.IcebergConstants
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
    fallbackLoadTableFunc: Identifier => Table
    = UCDeltaCatalogClientImpl.defaultFallbackLoadTableFunc)
  extends AbstractDeltaCatalogClient with Logging {

  override def loadTable(ident: Identifier): Table = {
    UCDeltaCatalogClientImpl.loadTableInvocationsCounter.incrementAndGet()
    val tid = toStorageTableIdent(ident)
    val info =
      try ucClient.loadTable(tid)
      catch {
        case _: StorageNoSuchTableException => throw new NoSuchTableException(ident)
        case e: UnsupportedTableFormatException =>
          logInfo(log"Table ${MDC(DeltaLogKeys.TABLE_NAME, ident)} is not in Delta format; " +
            log"falling back to the legacy catalog path. Cause: " +
            log"${MDC(DeltaLogKeys.EXCEPTION, e.getMessage)}")
          return fallbackLoadTableFunc(ident)
        case e: CredentialFetchFailedException if serverSidePlanningEnabled =>
          logWarning(log"Credential fetch failed for " +
            log"${MDC(DeltaLogKeys.TABLE_NAME, fullQualifiedTableName(tid))}; enabling " +
            log"server-side planning fallback. Cause: " +
            log"${MDC(DeltaLogKeys.EXCEPTION, e.getMessage)}")
          enableServerSidePlanningConfig(ident)
          e.getTableInfoWithoutCredentials
      }
    UCDeltaCatalogClientImpl.successfulDeltaRestApiLoadsCounter.incrementAndGet()
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
      s"UC identifiers must be of the form <schema>.<table>; got namespace of length " +
        s"${ns.length}: '${ns.mkString(".")}' (full identifier: '${ident.toString}')")
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
    // workaround for tracking UniForm metadata inside catalogTable
    // Those are only kept in-memory by client and would not be written back to UC
    val uniformProps = Option(info.getUniformMetadata)
      .flatMap(u => u.getIcebergMetadata.toScala)
      .map { iceberg =>
        Map(
          IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP ->
            iceberg.getMetadataLocation,
          IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP ->
            iceberg.getConvertedDeltaVersion.toString,
          IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP ->
            iceberg.getConvertedDeltaTimestamp
        )
      }.getOrElse(Map.empty[String, String])
    val storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(new URI(info.getLocation)),
      properties = properties ++ info.getStorageProperties.asScala.toMap ++ uniformProps)
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
  // Test-only instrumentation. The mutable counters are encapsulated so production code
  // can neither read nor write them; read access is exposed via the `*ForTesting` methods
  // below so cross-package integration tests (e.g. `io.sparkuctest.*`) don't need
  // reflection.

  /** Bumped at every `loadTable` entry regardless of outcome. Read via the *ForTesting API. */
  private val loadTableInvocationsCounter: AtomicLong = new AtomicLong(0L)

  /**
   * Bumped only when `loadTable` returned a Delta table via the Delta REST API (no fallback,
   * no rethrow). Read via the *ForTesting API.
   */
  private val successfulDeltaRestApiLoadsCounter: AtomicLong = new AtomicLong(0L)

  /**
   * Test-only read accessor for the `loadTable` invocation counter. Used by integration
   * tests to verify the Delta REST API code path ran. Not part of any public API; production
   * code must not depend on it.
   */
  def loadTableInvocationsForTesting: Long = loadTableInvocationsCounter.get()

  /**
   * Test-only read accessor for the count of `loadTable` calls served by the Delta REST API
   * (no fallback, no rethrow). Not part of any public API.
   */
  def successfulDeltaRestApiLoadsForTesting: Long = successfulDeltaRestApiLoadsCounter.get()

  private[catalog] val ServerSidePlanningEnabledKey: String = "serverSidePlanning.enabled"

  private[catalog] val defaultFallbackLoadTableFunc: Identifier => Table = ident =>
    throw new IllegalStateException(
      s"Non-Delta table $ident cannot be served via the Delta REST API path and no " +
        "fallback catalog was configured.")

  /**
   * Builds a [[UCDeltaCatalogClientImpl]] from catalog options. The `deltaRestApi.enabled` gate
   * is the caller's responsibility ([[AbstractDeltaCatalogClient.fromCatalogOptionsIfEnabled]]).
   * `fallbackLoadTableFunc` is invoked when UC reports `UnsupportedTableFormatException`. UC client
   * construction is delegated to [[UCTokenBasedRestClientFactory]] with `renewCredential.enabled`
   * defaulted to `true` and `credScopedFs.enabled` defaulted to `false` when not set.
   */
  override def fromCatalogOptions(
      catalogName: String,
      options: CaseInsensitiveStringMap,
      fallbackLoadTableFunc: Identifier => Table
  ): UCDeltaCatalogClientImpl = {
    // Pre-flight: keep our user-facing errors instead of the factory's less specific ones.
    if (options.get(UriKey) == null) {
      throw new IllegalArgumentException(s"'$UriKey' is required (catalog '$catalogName')")
    }
    validateAuthConfigured(options, catalogName)

    // `asCaseSensitiveMap()` preserves the user's original key case; `containsKey` is
    // case-insensitive so defaults don't create duplicate keys.
    val merged = new java.util.HashMap[String, String](options.asCaseSensitiveMap())
    Seq(
      UCTokenBasedRestClientFactory.DELTA_REST_API_ENABLED_KEY -> "true",
      UCTokenBasedRestClientFactory.RENEW_CREDENTIAL_ENABLED_KEY -> "true",
      UCTokenBasedRestClientFactory.CRED_SCOPED_FS_ENABLED_KEY -> "false"
    ).foreach { case (k, v) => if (!options.containsKey(k)) merged.put(k, v) }
    val ucClient = UCTokenBasedRestClientFactory
      .createUCClient(new CaseInsensitiveStringMap(merged))
      .asInstanceOf[UCDeltaClient]

    val sspEnabled = options.getBoolean(ServerSidePlanningEnabledKey, false)
    new UCDeltaCatalogClientImpl(catalogName, ucClient, sspEnabled, fallbackLoadTableFunc)
  }

  private val UriKey: String = "uri"
  private val AuthPrefix: String = "auth."
  private val LegacyTokenKey: String = "token"

  /**
   * Pre-flight: ensure at least one of `auth.*` or legacy `token` is present, so the user
   * sees a clear error (and catalog name) instead of the factory's internal failure when
   * `TokenProvider.create` is handed an empty config.
   */
  private[catalog] def validateAuthConfigured(
      options: CaseInsensitiveStringMap,
      catalogName: String): Unit = {
    val hasAuthPrefix = options.entrySet().asScala.exists(_.getKey.startsWith(AuthPrefix))
    val hasLegacyToken = options.get(LegacyTokenKey) != null
    if (!hasAuthPrefix && !hasLegacyToken) {
      throw new IllegalArgumentException(
        s"auth configuration is required when 'deltaRestApi.enabled' is true " +
          s"(catalog '$catalogName'). Set either '${AuthPrefix}type' (with the corresponding " +
          s"$AuthPrefix* keys) or the legacy '$LegacyTokenKey' option.")
    }
  }
}
