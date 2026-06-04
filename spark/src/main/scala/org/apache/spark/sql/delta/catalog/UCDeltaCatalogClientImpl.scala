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
import java.util
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.delta.storage.commit.{TableIdentifier => StorageTableIdentifier}
import io.delta.storage.commit.actions.{AbstractDomainMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.{UCDeltaClient, UCDeltaModels}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.UC_TABLE_ID_KEY
import io.delta.storage.commit.uccommitcoordinator.UCDeltaModels.TableInfo
import io.delta.storage.commit.uccommitcoordinator.exceptions.{
  CredentialFetchFailedException,
  UnsupportedTableFormatException,
  NoSuchTableException => StorageNoSuchTableException
}

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType
}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog, V1Table}
import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, ClusteringTableFeature, DeltaConfigs, DeltaErrors, TableFeature}
import org.apache.spark.sql.delta.actions.{DomainMetadata, Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED
import org.apache.spark.sql.delta.coordinatedcommits.UCTokenBasedRestClientFactory
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.IcebergConstants
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * [[AbstractDeltaCatalogClient]] backed by a [[UCDeltaClient]]. Owns all of the catalog-specific
 * managed-Delta create work (staging + finalization), so that `AbstractDeltaCatalog` only
 * needs to invoke the trait entry points (`loadTable`, `createStagingTable`, `createTable`).
 */
private[catalog] class UCDeltaCatalogClientImpl(
    catalogName: String,
    ucClient: UCDeltaClient,
    serverSidePlanningEnabled: Boolean = false,
    fallbackLoadTableFunc: Identifier => Table
    = UCDeltaCatalogClientImpl.defaultFallbackLoadTableFunc)
  extends AbstractDeltaCatalogClient with Logging {

  // -------------------------------------------------------------------------
  // DeltaCatalogClient: tableExists
  // -------------------------------------------------------------------------

  override def tableExists(ident: Identifier): Boolean = {
    try {
      ucClient.loadTable(toStorageTableIdent(ident))
      true
    } catch {
      case _: StorageNoSuchTableException => false
      // UC acknowledged the table; we just couldn't process it further (non-Delta format or
      // credential fetch failed). The table still exists in the catalog.
      case _: UnsupportedTableFormatException | _: CredentialFetchFailedException => true
    }
  }

  // -------------------------------------------------------------------------
  // DeltaCatalogClient: loadTable
  // -------------------------------------------------------------------------

  override def loadTable(ident: Identifier): Table = {
    UCDeltaCatalogClientImpl.loadTableInvocationsCounter.incrementAndGet()
    val tid = toStorageTableIdent(ident)
    val info =
      try ucClient.loadTable(tid)
      catch {
        case _: StorageNoSuchTableException => throw new NoSuchTableException(ident)
        case e: UnsupportedTableFormatException =>
          logInfo(log"Table ${MDC(DeltaLogKeys.TABLE_NAME, ident)} is not supported by " +
            log"UCDeltaClient; falling back to the legacy catalog path. Cause: " +
            log"${MDC(DeltaLogKeys.EXCEPTION, e.getMessage)}")
          return fallbackLoadTableFunc(ident)
        case e: CredentialFetchFailedException if serverSidePlanningEnabled =>
          logWarning(log"Credential fetch failed for " +
            log"${MDC(DeltaLogKeys.TABLE_NAME, fullQualifiedTableName(ident))}; enabling " +
            log"server-side planning fallback. Cause: " +
            log"${MDC(DeltaLogKeys.EXCEPTION, e.getMessage)}")
          enableServerSidePlanningConfig(ident)
          e.getTableInfoWithoutCredentials
      }
    UCDeltaCatalogClientImpl.successfulDeltaRestApiLoadsCounter.incrementAndGet()
    toV1Table(ident, info)
  }

  // -------------------------------------------------------------------------
  // DeltaCatalogClient: createStagingTable
  // -------------------------------------------------------------------------

  override def createStagingTable(
      ident: Identifier,
      properties: util.Map[String, String]): util.Map[String, String] = {
    requireUnpreparedManagedDeltaCreate(ident, properties)
    val stagingInfo = ucClient.createStagingTable(toStorageTableIdent(ident))
    val augmented = new util.HashMap[String, String](properties)
    augmented.put(TableCatalog.PROP_LOCATION, stagingInfo.getLocation)
    augmented.put(UC_TABLE_ID_KEY, stagingInfo.getTableId.toString)
    augmented.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
    // Required first so the conflict-check sees only user input; suggested then defers via
    // putIfAbsent, silently yielding to any required value already set.
    applyRequiredProperties(augmented, stagingInfo.getRequiredProperties, ident)
    applyProtocolFeatures(
      augmented,
      stagingInfo.getRequiredProtocol,
      ident,
      required = true)
    applySuggestedProperties(augmented, stagingInfo.getSuggestedProperties)
    applyProtocolFeatures(
      augmented,
      stagingInfo.getSuggestedProtocol,
      ident,
      required = false)
    stagingInfo.getStorageProperties.asScala.foreach { case (k, v) =>
      augmented.put(k, v)
      // Delta currently expects credential options under both the bare and `option.`-prefixed
      // keys (mirrors UCSingleCatalog#setCredentialProps); duplicate so the downstream
      // `getTablePropsAndWriteOptions` routes credentials into writeOptions.
      augmented.put(TableCatalog.OPTION_PREFIX + k, v)
    }
    augmented
  }

  /** Applies UC's suggested properties to `augmented`. Caller-supplied values win. */
  private def applySuggestedProperties(
      augmented: util.Map[String, String],
      suggested: util.Map[String, String]): Unit = {
    if (suggested == null) return
    // `null` values are engine-generated-at-commit sentinels (e.g. row-tracking materialized
    // column names); Delta rejects them as unknown configs at stage time, so skip and let
    // the engine substitute them on finalize. Entries that wouldn't survive Delta config
    // validation (unknown key, non-editable key, or value that doesn't parse) are also
    // dropped to keep CREATE from failing later in the staging flow.
    suggested.asScala.foreach { case (k, v) =>
      if (v != null && passesDeltaConfigValidation(k, v)) augmented.putIfAbsent(k, v)
    }
  }

  private val propertiesToSkipCarryForwardOnReplace = Set(
    // Clustering has DDL-only enablement (`CLUSTER BY`); carrying it as a property would
    // both throw on REPLACE (`DELTA_CREATE_TABLE_SET_CLUSTERING_TABLE_FEATURE_NOT_ALLOWED`)
    // and block legitimate transitions away from a clustered table.
    TableFeatureProtocolUtils.propertyKey(ClusteringTableFeature),
    // delta.columnMapping.maxColumnId changes as table schema changes. Not worth carrying forward.
    DeltaConfigs.COLUMN_MAPPING_MAX_ID.key)

  /**
   * Returns whether `(key, value)` from an existing table's config should be carried forward
   * when building the REPLACE-augmented property map.
   */
  private def isSafeToCarryForwardOnReplace(key: String, value: String): Boolean = {
    val lKey = key.toLowerCase(java.util.Locale.ROOT)
    // Only `delta.*` keys are carried; non-`delta.*` user properties (e.g. `Foo=Bar`)
    // follow normal REPLACE semantics -- the user's new TBLPROPERTIES is authoritative.
    if (!lKey.startsWith("delta.")) return false
    if (propertiesToSkipCarryForwardOnReplace.contains(key)) return false
    // Defer remaining acceptance to Delta's own per-entry validator -- single source of truth
    // for "would this survive the next commit". This would also filter out Delta-internal metadata
    // like `delta.lastCommitTimestamp` (unregistered) and `delta.columnMapping.maxColumnId`
    // (non-editable) that `validateConfigurations` would otherwise reject, if they weren't
    // filtered out by propertiesToSkipCarryForwardOnReplace already.
    passesDeltaConfigValidation(key, value)
  }

  /**
   * True iff [[DeltaConfigs.validateConfiguration]] would accept `(key, value)` as a Delta
   * table property. Wraps the validator to convert its throw-on-bad behavior into a
   * boolean predicate.
   */
  private def passesDeltaConfigValidation(key: String, value: String): Boolean = {
    try {
      DeltaConfigs.validateConfiguration(
        key, value, allowArbitraryProperties = false, allConfigurations = Map.empty)
      true
    } catch {
      // Delta's structured-error exceptions for bad keys (DELTA_UNKNOWN_CONFIGURATION,
      // DELTA_CANNOT_MODIFY_TABLE_PROPERTY, etc.), and the raw IllegalArgumentException
      // raised by `require(...)` in the value parser.
      case _: org.apache.spark.sql.delta.DeltaThrowable => false
      case _: IllegalArgumentException => false
    }
  }

  /**
   * Applies UC's required properties to `augmented`. Caller-supplied values that conflict
   * with a required key throw.
   */
  private def applyRequiredProperties(
      augmented: util.Map[String, String],
      required: util.Map[String, String],
      ident: Identifier): Unit = {
    if (required == null) return
    // `null` values are engine-generated-at-commit sentinels (see [[applySuggestedProperties]]).
    required.asScala.foreach { case (k, v) =>
      if (v != null) putRequiredOrThrow(augmented, k, v, ident)
    }
  }

  /**
   * Encodes a UC protocol as `delta.feature.<name>=supported` keys so the standard
   * `CreateDeltaTableCommand` protocol-upgrade flow picks them up. A feature unknown to
   * this Delta version throws when `required`, is silently skipped when suggested.
   */
  private def applyProtocolFeatures(
      augmented: util.Map[String, String],
      protocol: AbstractProtocol,
      ident: Identifier,
      required: Boolean): Unit = {
    if (protocol == null) return
    val features =
      (Option(protocol.getReaderFeatures).map(_.asScala).getOrElse(Iterable.empty) ++
        Option(protocol.getWriterFeatures).map(_.asScala).getOrElse(Iterable.empty)).toSet
    features.foreach { feature =>
      if (TableFeature.featureNameToFeature(feature).isDefined) {
        val key = TableFeatureProtocolUtils.propertyKey(feature)
        if (required) putRequiredFeatureOrThrow(augmented, key, ident)
        else augmented.putIfAbsent(key, FEATURE_PROP_SUPPORTED)
      } else if (required) {
        // Compatibility gap: the catalog requires a Delta protocol feature this client's
        // bundled Delta version does not understand. Throw with guidance.
        val qualifiedName = fullQualifiedTableName(ident)
        throw new IllegalArgumentException(
          s"Cannot create table $qualifiedName: catalog requires Delta protocol feature " +
            s"'$feature' but this Delta version does not support it. Upgrade Delta or " +
            "ask the catalog to relax the requirement. See " +
            "https://github.com/delta-io/delta/blob/master/PROTOCOL.md for the full " +
            "Delta protocol feature list.")
      }
    }
    // Intentionally do NOT emit `delta.minReaderVersion` / `delta.minWriterVersion`: Delta
    // derives those from the feature list, and pinning them here would put Delta into
    // explicit table-features mode, suppressing implicit legacy writer features
    // (appendOnly, invariants) in the resulting protocol.
  }

  /**
   * Puts a UC-required key/value into `augmented`, throwing if the caller already supplied
   * a different value for the same key.
   */
  private def putRequiredOrThrow(
      augmented: util.Map[String, String],
      key: String,
      requiredValue: String,
      ident: Identifier): Unit = {
    val existing = augmented.get(key)
    if (existing != null && existing != requiredValue) {
      // UC's required properties are policy-level enforcement; overriding silently would
      // defeat the policy.
      val qualifiedName = fullQualifiedTableName(ident)
      throw new IllegalArgumentException(
        s"Cannot create table $qualifiedName: catalog requires table property '$key'=" +
          s"'$requiredValue' but the caller supplied '$key'='$existing'. Remove the " +
          s"conflicting TBLPROPERTIES entry and retry.")
    }
    augmented.put(key, requiredValue)
  }

  /** Variant of [[putRequiredOrThrow]] for `delta.feature.<name>` feature flags. */
  private def putRequiredFeatureOrThrow(
      augmented: util.Map[String, String],
      featureKey: String,
      ident: Identifier): Unit = {
    val existing = augmented.get(featureKey)
    if (existing != null && existing != FEATURE_PROP_SUPPORTED) {
      // `delta.feature.<name>` only accepts the value `supported`; any other value reaching
      // here is a user-side mistake. Phrase the error as "table feature" rather than "table
      // property" so the user knows it's a feature-flag conflict.
      val qualifiedName = fullQualifiedTableName(ident)
      throw new IllegalArgumentException(
        s"Cannot create table $qualifiedName: catalog requires Delta table feature " +
          s"'$featureKey'='$FEATURE_PROP_SUPPORTED' but the caller supplied " +
          s"'$featureKey'='$existing'. Remove the conflicting TBLPROPERTIES entry and retry.")
    }
    augmented.put(featureKey, FEATURE_PROP_SUPPORTED)
  }

  // -------------------------------------------------------------------------
  // DeltaCatalogClient: loadTableAndBuildReplaceProps
  // -------------------------------------------------------------------------

  /** Loads the existing UC table and builds the REPLACE-augmented property map. */
  override def loadTableAndBuildReplaceProps(
      ident: Identifier,
      properties: util.Map[String, String]): util.Map[String, String] = {
    val qualifiedName = fullQualifiedTableName(ident)
    // Reject caller-supplied UC-system keys, `delta.feature.catalogManaged` overrides, and
    // `PROP_LOCATION` (the existing managed table cannot be relocated via REPLACE).
    rejectSystemManagedProperties(properties, qualifiedName)
    rejectCatalogManagedOverride(properties, qualifiedName)
    if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      throw new UnsupportedOperationException(
        s"REPLACE TABLE cannot specify '${TableCatalog.PROP_LOCATION}' on the existing " +
          s"UC-managed Delta table $qualifiedName.")
    }
    val info =
      try ucClient.loadTable(toStorageTableIdent(ident))
      catch { case _: StorageNoSuchTableException => throw new NoSuchTableException(ident) }
    requireCatalogManagedDeltaTable(info, qualifiedName)
    val existingProvider =
      Option(info.getMetadata.getProvider)
        .map(_.toLowerCase(java.util.Locale.ROOT))
        .get
    // REPLACE cannot change the table's storage format.
    Option(properties.get(TableCatalog.PROP_PROVIDER))
      .filterNot(_.equalsIgnoreCase(existingProvider))
      .foreach(_ => throw DeltaErrors.cannotChangeProvider())
    val augmented = new util.HashMap[String, String](properties)
    // Carry forward the existing table's `delta.*` properties (feature flags, checkpoint
    // policy, column-mapping mode, row-tracking state, etc.) so REPLACE doesn't silently
    // strip critical configs that downstream (Table Service / commit validation) would
    // reject as unsupported changes. Caller-supplied TBLPROPERTIES on the REPLACE win via
    // `putIfAbsent`. See [[isSafeToCarryForwardOnReplace]] for the carry-forward criteria.
    Option(info.getMetadata.getConfiguration).foreach { existingConfig =>
      existingConfig.asScala.foreach { case (k, v) =>
        if (v != null && isSafeToCarryForwardOnReplace(k, v)) augmented.putIfAbsent(k, v)
      }
    }
    // Re-emit the existing provider so downstream sees USING <provider> even if the caller
    // omitted it. Mark the location as system-managed; `PROP_LOCATION` is intentionally not
    // set so downstream Delta resolves the location from the existing table snapshot.
    augmented.put(TableCatalog.PROP_PROVIDER, existingProvider)
    augmented.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
    // Mirror credentials into the `option.`-prefixed namespace so
    // `getTablePropsAndWriteOptions` routes them into writeOptions for Hadoop config
    // injection (mirrors `UCSingleCatalog.setCredentialProps`).
    info.getStorageProperties.asScala.foreach { case (k, v) =>
      if (k.startsWith("fs.")) {
        augmented.put(k, v)
        augmented.put(TableCatalog.OPTION_PREFIX + k, v)
      }
    }
    augmented
  }

  /** Reject caller attempts to set properties that the catalog manages. */
  private def rejectSystemManagedProperties(
      properties: util.Map[String, String], qualifiedName: String): Unit = {
    if (properties.containsKey(UC_TABLE_ID_KEY)) {
      throw new IllegalArgumentException(
        s"REPLACE TABLE on $qualifiedName cannot specify catalog-managed property " +
          s"'$UC_TABLE_ID_KEY'.")
    }
  }

  /** Reject `delta.feature.catalogManaged` override to anything other than `supported`. */
  private def rejectCatalogManagedOverride(
      properties: util.Map[String, String], qualifiedName: String): Unit = {
    val key = TableFeatureProtocolUtils.propertyKey(CatalogOwnedTableFeature)
    val value = properties.get(key)
    if (value != null && value != FEATURE_PROP_SUPPORTED) {
      throw new IllegalArgumentException(
        s"REPLACE TABLE on $qualifiedName cannot override '$key'='$value'; expected " +
          s"'$FEATURE_PROP_SUPPORTED'.")
    }
  }

  /**
   * Existing table must be tableType=MANAGED, provider=delta, and carry the
   * `delta.feature.catalogManaged=supported` configuration.
   */
  private def requireCatalogManagedDeltaTable(info: TableInfo, qualified: String): Unit = {
    if (info.getTableType != UCDeltaModels.TableType.MANAGED) {
      throw new UnsupportedOperationException(
        s"REPLACE TABLE is only supported for catalog-managed UC Delta tables; " +
          s"$qualified is ${info.getTableType}.")
    }
    val provider = Option(info.getMetadata.getProvider).map(_.toLowerCase(java.util.Locale.ROOT))
    if (!provider.contains("delta")) {
      throw DeltaErrors.notADeltaTableException("REPLACE TABLE", qualified)
    }
    val catalogManagedKey = TableFeatureProtocolUtils.propertyKey(CatalogOwnedTableFeature)
    val isCatalogManaged = Option(info.getMetadata.getConfiguration)
      .map(_.asScala.toMap)
      .exists(_.get(catalogManagedKey).contains(FEATURE_PROP_SUPPORTED))
    if (!isCatalogManaged) {
      throw new UnsupportedOperationException(
        s"REPLACE TABLE is only supported for catalog-managed UC Delta tables; " +
          s"$qualified is not catalog-managed ('$catalogManagedKey' is not set).")
    }
  }

  // -------------------------------------------------------------------------
  // DeltaCatalogClient: createTable
  // -------------------------------------------------------------------------

  override def createTable(
      ident: Identifier,
      table: CatalogTable,
      metadata: Metadata,
      domainMetadata: Seq[DomainMetadata],
      protocol: Protocol,
      lastCommitTimestampMs: Long): Unit = {
    if (table.tableType != CatalogTableType.MANAGED) {
      throw new IllegalArgumentException(
        s"UCDeltaClient createTable only supports MANAGED tables; " +
          s"got ${table.tableType} for ${fullQualifiedTableName(ident)}.")
    }
    val locationUri = table.storage.locationUri.getOrElse(throw new IllegalArgumentException(
      s"createTable requires a storage location on the CatalogTable for " +
        s"${fullQualifiedTableName(ident)}"))
    // Strip V2-only catalog keys (location, owner, ...) before sending the configuration
    // to UC; they don't belong in table properties.
    val cleanedConfiguration =
      metadata.configuration.view
        .filterKeys(k => !UCDeltaCatalogClientImpl.ReservedV2TableProperties.contains(k))
        .toMap
    ucClient.createTable(
      locationUri,
      toStorageTableIdent(ident),
      toUcTableType(table.tableType),
      metadata.copy(
        description = table.comment.orNull,
        configuration = cleanedConfiguration),
      protocol,
      domainMetadata.map(d => d: AbstractDomainMetadata).asJava,
      lastCommitTimestampMs)
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /**
   * Asserts that `properties` is the raw, caller-supplied form expected on the fresh-create
   * entrypoint -- no LOCATION / IS_MANAGED_LOCATION / EXTERNAL markers set, and the
   * identifier is not path-based.
   */
  private def requireUnpreparedManagedDeltaCreate(
      ident: Identifier,
      properties: util.Map[String, String]): Unit = {
    // Defense in depth: upstream routing is supposed to filter external / REPLACE-existing /
    // path-based requests before reaching this client. If those markers appear here, fail
    // loud rather than re-stage an already-prepared or mislabeled request.
    val qualifiedName = fullQualifiedTableName(ident)
    def bail(reason: String): Nothing = throw new IllegalStateException(
      s"Managed Delta create for $qualifiedName reached the UCDeltaClient path in an " +
        s"unexpected state: $reason. The upstream catalog must only route fresh managed " +
        "Delta CREATE / CTAS here with raw caller-supplied properties.")
    if (properties.containsKey(TableCatalog.PROP_LOCATION)) {
      bail(s"${TableCatalog.PROP_LOCATION} is already set")
    }
    if (properties.containsKey(TableCatalog.PROP_IS_MANAGED_LOCATION)) {
      bail(s"${TableCatalog.PROP_IS_MANAGED_LOCATION} is already set")
    }
    if (properties.containsKey(TableCatalog.PROP_EXTERNAL)) {
      bail(s"${TableCatalog.PROP_EXTERNAL} is set (EXTERNAL request on the managed path)")
    }
    // Path-based identifiers come in as a single-component namespace whose name is an
    // absolute filesystem path (e.g. `delta`.`/tmp/foo`); UC has no entry for those.
    val ns = ident.namespace()
    if (ns.length == 1 && new Path(ident.name()).isAbsolute) {
      bail("identifier is path-based")
    }
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
      s"UC table identifier must be one of <schema>.<table> or <catalog>.<schema>.<table>; " +
        s"got namespace of length ${ns.length}: '${ns.mkString(".")}' " +
        s"(full identifier: '${ident.toString}')")
    new StorageTableIdentifier(Array(catalogName, ns(0)), ident.name())
  }

  /** Three-part dotted `catalog.schema.table` name for a Spark V2 `Identifier`. */
  private def fullQualifiedTableName(ident: Identifier): String =
    s"$catalogName.${ident.namespace().mkString(".")}.${ident.name()}"

  private def toV1Table(ident: Identifier, info: TableInfo): V1Table = {
    val m = info.getMetadata
    val tableConfig = Option(m.getConfiguration)
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
    val uniformProps = info.getUniformMetadata.toScala
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
    // Match UCSingleCatalog's V1Table shape: pack tableConfig + credentials + UniForm into
    // `storage.properties`, leave `catalogTable.properties` empty. Required for
    // downstream streaming/routing compatibility.
    val storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(new URI(info.getLocation)),
      properties = tableConfig ++ info.getStorageProperties.asScala.toMap ++ uniformProps)
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

  private def toUcTableType(t: CatalogTableType): UCDeltaModels.TableType = t match {
    case CatalogTableType.MANAGED => UCDeltaModels.TableType.MANAGED
    case CatalogTableType.EXTERNAL => UCDeltaModels.TableType.EXTERNAL
    case other =>
      throw new IllegalArgumentException(s"Unsupported CatalogTableType for UC: $other")
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
   * Bumped only when `loadTable` returned a Delta table via the UCDeltaClient (no fallback,
   * no rethrow). Read via the *ForTesting API.
   */
  private val successfulDeltaRestApiLoadsCounter: AtomicLong = new AtomicLong(0L)

  /**
   * Test-only read accessor for the `loadTable` invocation counter. Used by integration
   * tests to verify the UCDeltaClient code path ran. Not part of any public API; production
   * code must not depend on it.
   */
  def loadTableInvocationsForTesting: Long = loadTableInvocationsCounter.get()

  /**
   * Test-only read accessor for the count of `loadTable` calls served by the UCDeltaClient
   * (no fallback, no rethrow). Not part of any public API.
   */
  def successfulDeltaRestApiLoadsForTesting: Long = successfulDeltaRestApiLoadsCounter.get()

  /**
   * V2-only catalog property keys that must be stripped before sending the configuration to
   * UC (`location`, `owner`, `provider`, ...). Reuses Spark's canonical reserved-key list so
   * future additions on the Spark side flow through automatically.
   */
  private val ReservedV2TableProperties: Set[String] = CatalogV2Util.TABLE_RESERVED_PROPERTIES.toSet

  private[catalog] val ServerSidePlanningEnabledKey: String = "serverSidePlanning.enabled"

  private[catalog] val defaultFallbackLoadTableFunc: Identifier => Table = ident =>
    throw new IllegalStateException(
      s"Non-Delta table $ident cannot be served via the UCDeltaClient path and no " +
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
      fallbackLoadTableFunc: Identifier => Table): UCDeltaCatalogClientImpl = {
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
