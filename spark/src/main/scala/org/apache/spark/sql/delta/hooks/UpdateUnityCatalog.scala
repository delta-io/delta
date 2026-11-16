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

package org.apache.spark.sql.delta.hooks

// scalastyle:off import.ordering.noEmptyLine
import scala.util.control.NonFatal

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.{UC_ETAG_KEY, UC_EXTERNAL_TABLE_ID_KEY}
import org.apache.spark.sql.delta.{CatalogManagedTableMaintenanceOperation, CommittedTransaction, DeltaConfigs, IcebergConstants, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.AbstractDeltaCatalogClient
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.metrics.UpdateMetricsHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, V1Table}

/**
 * A post-commit hook that keeps the Unity Catalog entry of an EXTERNAL Delta table in line with
 * the table's latest schema, partition columns, properties, protocol and clustering, so
 * that engines reading the table's metadata from Unity Catalog see it. The entry is updated
 * through the UC Delta API without a commit, and the version it records is the version at which it
 * was last synced: commits that change none of this leave it untouched.
 *
 * The comparison and the version guard of [[UpdateCatalogBase.execute]] run against the entry's
 * state as loaded with the table, which [[table]] holds. The update is conditional on that
 * state's table id and etag. If another writer changed the entry in between, the update is
 * rejected, and the hook loads the entry again and retries, up to
 * [[UpdateUnityCatalog.ETAG_RETRIES]] times.
 *
 * The update is sent on the committing thread, so it has finished when the commit returns, and
 * it is best effort: failures are logged and never affect the commit.
 */
case class UpdateUnityCatalog(initialTable: CatalogTable) extends UpdateCatalogBase {

  override val name: String = "Update Unity Catalog"

  // The entry's current state: as loaded with the table, then as reloaded after a conflict.
  @volatile private var _table: CatalogTable = UpdateUnityCatalog.withEntryProperties(initialTable)
  @volatile private var client: AbstractDeltaCatalogClient = _
  @volatile private var isConflict: Boolean = false

  override protected def table: CatalogTable = _table

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    // Only commits that carry metadata can change what the entry mirrors; the others, which are
    // most commits, cost nothing.
    if (UpdateUnityCatalog.changesTableMetadata(txn.committedActions)) {
      executeOnWrite(spark, txn.postCommitSnapshot)
    }
  }

  override def executeOnWrite(spark: SparkSession, snapshot: Snapshot): Unit = {
    if (!shouldRun(spark, snapshot)) return
    try {
      sync(spark, snapshot)
    } catch {
      case NonFatal(e) =>
        logWarning(
          log"Failed to update the Unity Catalog entry of " +
          log"${MDC(DeltaLogKeys.TABLE_NAME, initialTable.identifier)} to version " +
          log"${MDC(DeltaLogKeys.VERSION, snapshot.version)}", e)
    }
  }

  override protected def shouldRun(spark: SparkSession, snapshot: Snapshot): Boolean = {
    snapshot.version >= 0 &&
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_UC_EXTERNAL_TABLE_METADATA_SYNC_ENABLED)
  }

  /**
   * Brings the entry in line with `snapshot`. If the update is rejected because the entry changed,
   * loads the entry again and retries, up to [[UpdateUnityCatalog.ETAG_RETRIES]] times.
   */
  private def sync(spark: SparkSession, snapshot: Snapshot): Unit =
    createClient(spark, initialTable.identifier.catalog.get).foreach { c =>
      client = c
      var attempts = 0
      do {
        isConflict = false
        execute(spark, snapshot)
        attempts += 1
      } while (isConflict && attempts <= UpdateUnityCatalog.ETAG_RETRIES && reload(c, snapshot))
    }

  /**
   * Loads the entry again. Returns false if there is nothing to update: the entry is gone, or the
   * name now refers to another table.
   */
  private def reload(c: AbstractDeltaCatalogClient, snapshot: Snapshot): Boolean =
    loadEntry(c).filter(isSameTable(_, snapshot)).exists { t =>
      _table = UpdateUnityCatalog.withEntryProperties(t)
      true
    }

  /** The table as loaded for reads, without the credentials that updating its entry doesn't use. */
  private def loadEntry(c: AbstractDeltaCatalogClient): Option[CatalogTable] =
    try {
      c.loadTable(
        Identifier.of(initialTable.identifier.database.toArray, initialTable.identifier.table),
        fetchCredentials = false) match {
        case t: V1Table => Some(t.v1Table)
        case _ => None
      }
    } catch {
      case _: NoSuchTableException => None
    }

  /** The client for the table's catalog, built per update; a seam for tests. */
  protected def createClient(
      spark: SparkSession,
      catalogName: String): Option[AbstractDeltaCatalogClient] = {
    val options = UpdateMetricsHook.catalogOptionsFor(spark, catalogName)
    AbstractDeltaCatalogClient
      .fromCatalogOptionsIfEnabled(catalogName, options, UpdateMetricsHook.UnusedLoader)
  }

  // Compared as Hadoop paths: UC returns file:/// and no trailing slash; Hadoop returns file:/.
  private def isSameTable(t: CatalogTable, snapshot: Snapshot): Boolean =
    t.storage.locationUri.exists { uri =>
      new Path(uri.toString).toUri == snapshot.deltaLog.dataPath.toUri
    }

  override protected def schemaHasChanged(snapshot: Snapshot, spark: SparkSession): Boolean = {
    snapshot.schema != table.schema ||
      snapshot.metadata.partitionColumns != table.partitionColumnNames
  }

  override protected[sql] def propertiesHaveChanged(
      properties: Map[String, String],
      metadata: Metadata,
      spark: SparkSession): Boolean = {
    // The entry also holds properties that Unity Catalog derives itself; the new properties
    // don't, so leave them out of the comparison.
    val previous = table.properties.filter { case (k, _) =>
      !UpdateUnityCatalog.CATALOG_ONLY_PROPERTIES.contains(k)
    }
    properties != previous
  }

  override protected def clusteringColumnsChanged(snapshot: Snapshot): Boolean =
    UpdateUnityCatalog.clusteringUpdate(snapshot, table.properties).isDefined

  override def updateSchema(spark: SparkSession, snapshot: Snapshot): Unit =
    updateEntry(snapshot)

  override protected def updateProperties(spark: SparkSession, snapshot: Snapshot): Unit =
    updateEntry(snapshot)

  /**
   * Sends the entry what differs from `snapshot`: the metadata, the protocol and the clustering
   * domain, each only if it changed. Properties that the entry has but the table doesn't are
   * removed, except those Unity Catalog derives itself.
   */
  private def updateEntry(snapshot: Snapshot): Unit = {
    val entryMetadata = snapshot.metadata.copy(
      schemaString = table.schema.json,
      partitionColumns = table.partitionColumnNames,
      configuration = table.properties.filter { case (k, _) => !UpdateUnityCatalog.isDerived(k) })
    val newProtocol = Some(snapshot.protocol)
      .filter(_ => UpdateUnityCatalog.protocolChanged(snapshot, table.properties))
    val isUpdated = client.updateTableMetadata(
      table,
      entryMetadata,
      snapshot.metadata,
      newProtocol,
      UpdateUnityCatalog.clusteringUpdate(snapshot, table.properties).toSeq,
      snapshot.version,
      snapshot.timestamp)
    // The entry changed since it was loaded.
    isConflict = !isUpdated
  }
}

object UpdateUnityCatalog {

  // How often an update that was rejected because the entry changed is retried, as in DBR.
  private[hooks] val ETAG_RETRIES = 3

  /**
   * Whether `table` is an EXTERNAL table of a Unity Catalog that was loaded with its entry's etag,
   * which this hook keeps in sync. A table loaded without an etag, because the UC Delta API was
   * not used to load it, cannot be updated.
   */
  def appliesTo(table: CatalogTable, spark: SparkSession): Boolean = {
    table.tableType == CatalogTableType.EXTERNAL &&
      table.storage.properties.contains(UC_ETAG_KEY) &&
      table.identifier.catalog.exists { catalogName =>
        spark.conf.getOption(s"spark.sql.catalog.$catalogName")
          .contains(UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS)
      }
  }

  /**
   * Whether the commit changes metadata that the entry mirrors. Row-tracking domain changes alone
   * don't count: the high watermark advances on every insert.
   */
  private[hooks] def changesTableMetadata(committedActions: Seq[Action]): Boolean =
    committedActions.exists {
      case _: Metadata | _: Protocol => true
      case d: DomainMetadata => d.domain == ClusteringMetadataDomain.domainName
      case _ => false
    }

  /**
   * `table` with the entry's own properties in [[CatalogTable.properties]]. A table loaded for
   * reads keeps them in `storage.properties`, mixed with its credentials, the entry's table id
   * and etag, and other bookkeeping, which are left out.
   */
  private[hooks] def withEntryProperties(table: CatalogTable): CatalogTable =
    table.copy(properties = table.storage.properties.filter { case (k, _) => !isLoadingDetail(k) })

  // The keys of `storage.properties` that are not properties of the entry. Credentials, and the
  // connector settings that go with them, are all under `fs.`.
  private val LOADING_DETAIL_KEYS = Set(
    UC_EXTERNAL_TABLE_ID_KEY,
    UC_ETAG_KEY,
    CatalogManagedTableMaintenanceOperation.ALLOWED_OPERATIONS_PROPERTY,
    IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP,
    IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP,
    IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP)

  private def isLoadingDetail(key: String): Boolean =
    key.startsWith("fs.") || LOADING_DETAIL_KEYS.contains(key)

  // Properties that exist only in the entry: the version at which it was last synced and the
  // ones derived from the domain metadata.
  private val CATALOG_ONLY_PROPERTIES = Set(
    DeltaConfigs.METASTORE_LAST_UPDATE_VERSION,
    DeltaConfigs.METASTORE_LAST_COMMIT_TIMESTAMP,
    CatalogTable.PROP_CLUSTERING_COLUMNS,
    "delta.rowTracking.rowIdHighWaterMark")

  private def isProtocolProperty(key: String): Boolean =
    key == Protocol.MIN_READER_VERSION_PROP ||
      key == Protocol.MIN_WRITER_VERSION_PROP ||
      key.startsWith(TableFeatureProtocolUtils.FEATURE_PROP_PREFIX)

  // The properties that the entry derives itself, which must never be removed.
  private def isDerived(key: String): Boolean =
    CATALOG_ONLY_PROPERTIES.contains(key) || isProtocolProperty(key)

  /**
   * Whether the protocol of `snapshot` differs from the entry's, judging by the properties that
   * the entry derives from its protocol.
   */
  private[hooks] def protocolChanged(
      snapshot: Snapshot,
      entryProperties: Map[String, String]): Boolean =
    snapshot.getProperties.toMap.filter { case (k, _) => isProtocolProperty(k) } !=
      entryProperties.filter { case (k, _) => isProtocolProperty(k) }

  /**
   * The clustering domain to set, or to clear, so that the entry's clustering columns are the
   * snapshot's; `None` if they already are.
   */
  private[hooks] def clusteringUpdate(
      snapshot: Snapshot,
      entryProperties: Map[String, String]): Option[DomainMetadata] = {
    val entryColumns = entryProperties.get(CatalogTable.PROP_CLUSTERING_COLUMNS)
      .map(JsonUtils.fromJson[Seq[Seq[String]]](_))
    snapshot.domainMetadata.find(_.domain == ClusteringMetadataDomain.domainName) match {
      case Some(domain) if !entryColumns.contains(
          ClusteringMetadataDomain.unapply(domain).get._1.clusteringColumns) => Some(domain)
      case None if entryColumns.isDefined =>
        Some(DomainMetadata(ClusteringMetadataDomain.domainName, "{}", removed = true))
      case _ => None
    }
  }
}
