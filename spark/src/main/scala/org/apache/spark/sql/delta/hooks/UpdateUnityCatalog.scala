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
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaConfigs, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.{AbstractDeltaCatalogClient, CatalogTableEntry}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.metrics.UpdateMetricsHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.internal.SQLConf

/**
 * A post-commit hook that keeps the Unity Catalog entry of an EXTERNAL Delta table in line with
 * the table's latest schema, partition columns, comment, properties, protocol and clustering, so
 * that engines reading the table's metadata from Unity Catalog see it. The entry is updated
 * through the UC Delta API without a commit, and the version it records is the version at which it
 * was last synced: commits that change none of this leave it untouched.
 *
 * The comparison and the version guard of [[UpdateCatalogBase.execute]] run against the entry's
 * current state, which this hook loads from Unity Catalog and keeps in [[table]]: a
 * [[CatalogTable]] loaded for reads carries neither the entry's properties nor its etag.
 *
 * The update is asynchronous and best effort: failures are logged and never affect the commit.
 */
case class UpdateUnityCatalog(initialTable: CatalogTable) extends UpdateCatalogBase {

  override val name: String = "Update Unity Catalog"

  // The entry's current state, refreshed on every attempt. Only one attempt runs at a time.
  @volatile private var _table: CatalogTable = initialTable
  @volatile private var entry: CatalogTableEntry = _
  @volatile private var client: AbstractDeltaCatalogClient = _
  @volatile private var isConflict: Boolean = false
  // The UC Delta API doesn't return a table's comment, so the entry can't be compared with it; the
  // comment before the commit stands in for the entry's.
  @volatile private var previousDescription: String = _

  override protected def table: CatalogTable = _table

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    // Only commits that carry metadata can change what the entry mirrors; the others, which are
    // most commits, cost nothing.
    if (UpdateUnityCatalog.changesTableMetadata(txn.committedActions)) {
      previousDescription = txn.readSnapshot.metadata.description
      executeOnWrite(spark, txn.postCommitSnapshot)
    }
  }

  override def executeOnWrite(spark: SparkSession, snapshot: Snapshot): Unit = {
    if (!shouldRun(spark, snapshot)) return
    UpdateUnityCatalog.activeRequests.incrementAndGet()
    implicit val ec: ExecutionContext =
      UpdateUnityCatalog.getOrCreateExecutionContext(spark.sessionState.conf)
    Future[Unit] {
      sync(spark, snapshot)
    }.onComplete { _ =>
      UpdateUnityCatalog.activeRequests.decrementAndGet()
    }
  }

  override protected def shouldRun(spark: SparkSession, snapshot: Snapshot): Boolean = {
    snapshot.version >= 0 &&
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_UC_EXTERNAL_TABLE_METADATA_SYNC_ENABLED)
  }

  /**
   * Loads the entry and brings it in line with `snapshot`. If another update changes the entry in
   * between, loads it again and retries once.
   */
  private def sync(spark: SparkSession, snapshot: Snapshot): Unit = {
    val catalogName = initialTable.identifier.catalog.get
    try {
      createClient(spark, catalogName)
        .foreach { c =>
          client = c
          var attempts = 0
          var isDone = false
          while (!isDone && attempts < 2) {
            attempts += 1
            isDone = c.loadTableEntry(initialTable) match {
              // The entry is gone, or the name now refers to another table.
              case None => true
              case Some(e) if !isSameTable(e, snapshot) => true
              case Some(e) =>
                _table = e.table
                entry = e
                isConflict = false
                execute(spark, snapshot)
                !isConflict
            }
          }
        }
    } catch {
      case NonFatal(e) =>
        logWarning(
          log"Failed to update the Unity Catalog entry of " +
          log"${MDC(DeltaLogKeys.TABLE_NAME, initialTable.identifier)} to version " +
          log"${MDC(DeltaLogKeys.VERSION, snapshot.version)}", e)
    }
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
  private def isSameTable(e: CatalogTableEntry, snapshot: Snapshot): Boolean =
    e.table.storage.locationUri.exists { uri =>
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
    properties != previous || previousDescription != metadata.description
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
      description = previousDescription,
      schemaString = table.schema.json,
      partitionColumns = table.partitionColumnNames,
      configuration = table.properties.filter { case (k, _) => !UpdateUnityCatalog.isDerived(k) })
    val newProtocol = Some(snapshot.protocol)
      .filter(_ => UpdateUnityCatalog.protocolChanged(snapshot, table.properties))
    val isUpdated = client.updateTableMetadata(
      initialTable,
      entry,
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

  /** Whether `table` is an EXTERNAL table of a Unity Catalog, which this hook keeps in sync. */
  def appliesTo(table: CatalogTable, spark: SparkSession): Boolean = {
    table.tableType == CatalogTableType.EXTERNAL &&
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

  private val CLUSTERING_PROPERTY = CatalogTable.PROP_CLUSTERING_COLUMNS

  // Properties that exist only in the entry: the version at which it was last synced and the
  // ones derived from the domain metadata.
  private val CATALOG_ONLY_PROPERTIES = Set(
    DeltaConfigs.METASTORE_LAST_UPDATE_VERSION,
    DeltaConfigs.METASTORE_LAST_COMMIT_TIMESTAMP,
    CLUSTERING_PROPERTY,
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
    val entryColumns = entryProperties.get(CLUSTERING_PROPERTY)
      .map(JsonUtils.fromJson[Seq[Seq[String]]](_))
    snapshot.domainMetadata.find(_.domain == ClusteringMetadataDomain.domainName) match {
      case Some(domain) if !entryColumns.contains(
          ClusteringMetadataDomain.unapply(domain).get._1.clusteringColumns) => Some(domain)
      case None if entryColumns.isDefined =>
        Some(DomainMetadata(ClusteringMetadataDomain.domainName, "{}", removed = true))
      case _ => None
    }
  }

  // One thread pool for all tables, like [[UpdateCatalog]].
  private var executionContext: ExecutionContext = _

  private def getOrCreateExecutionContext(conf: SQLConf): ExecutionContext = synchronized {
    if (executionContext == null) {
      executionContext = ExecutionContext.fromExecutorService(
        DeltaThreadPool.newDaemonCachedThreadPool(
          "delta-unity-catalog-update",
          conf.getConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_THREAD_POOL_SIZE)))
    }
    executionContext
  }

  private val activeRequests = new AtomicInteger(0)

  /**
   * Test only: waits until the dispatched updates finish. Public so that cross-package
   * integration tests (e.g. `io.sparkuctest.*`) can call it.
   */
  def awaitCompletionForTesting(timeoutMs: Long): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (activeRequests.get() > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    activeRequests.get() == 0
  }
}
