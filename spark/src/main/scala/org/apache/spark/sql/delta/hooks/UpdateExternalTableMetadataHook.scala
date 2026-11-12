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

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.delta.actions.{Action, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.AbstractDeltaCatalogClient
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.metrics.UpdateMetricsHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool

/**
 * Post-commit hook that updates the Unity Catalog entry of an EXTERNAL Delta table after a
 * commit that changes the table's schema, properties, protocol, or clustering, so that engines
 * reading the catalog see the table's current metadata. The Delta log stays the source of truth;
 * the catalog entry is updated without a commit.
 *
 * Built like [[UpdateMetricsHook]]: a fresh [[AbstractDeltaCatalogClient]] per commit inside an
 * async dispatch, so commit latency is unaffected. Gated by
 * [[DeltaSQLConf.DELTA_UC_EXTERNAL_TABLE_METADATA_SYNC_ENABLED]]. Silent no-op when the table's
 * catalog is not a Unity Catalog or is not configured for `deltaRestApi.enabled=true`. Errors
 * from the async dispatch are logged and swallowed; commit success is unaffected.
 *
 * @param catalogTable catalog metadata of the committed EXTERNAL table
 */
case class UpdateExternalTableMetadataHook(catalogTable: CatalogTable)
    extends PostCommitHook with DeltaLogging {

  override val name: String = "Update catalog metadata of an external table"

  override def run(spark: SparkSession, txn: CommittedTransaction): Unit = {
    if (!spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_UC_EXTERNAL_TABLE_METADATA_SYNC_ENABLED)) {
      return
    }
    val catalogName = catalogTable.identifier.catalog.getOrElse {
      // CatalogTable has no V2 catalog identifier (e.g. path-based table) -- silent no-op.
      return
    }
    val isUnityCatalog = spark.conf.getOption(s"spark.sql.catalog.$catalogName")
      .contains(UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS)
    if (!isUnityCatalog ||
        !UpdateExternalTableMetadataHook.changesTableMetadata(txn.committedActions)) {
      return
    }
    sendUpdateAsync(spark, catalogName, txn)
  }

  private def sendUpdateAsync(
      spark: SparkSession,
      catalogName: String,
      txn: CommittedTransaction): Unit = {
    val logPath = txn.deltaLog.logPath
    val committedVersion = txn.committedVersion

    implicit val ec: ExecutionContext =
      UpdateExternalTableMetadataHook.getOrCreateExecutionContext()
    UpdateExternalTableMetadataHook.activeRequests.incrementAndGet()
    Future {
      val opts = UpdateMetricsHook.catalogOptionsFor(spark, catalogName)
      AbstractDeltaCatalogClient
        .fromCatalogOptionsIfEnabled(catalogName, opts, UpdateMetricsHook.UnusedLoader)
        .foreach { client =>
          // The post-commit snapshot can already include a racing writer's later commits.
          val snapshot =
            if (txn.postCommitSnapshot.version == committedVersion) txn.postCommitSnapshot
            else txn.deltaLog.getSnapshotAt(committedVersion, catalogTableOpt = txn.catalogTable)
          // The snapshot holds only live domains; the commit's removals tell UC what to drop.
          val removedDomains = txn.committedActions.collect {
            case d: DomainMetadata if d.removed => d
          }
          val updated = client.updateTableMetadata(
            catalogTable,
            txn.deltaLog.dataPath.toUri,
            txn.readSnapshot.metadata,
            snapshot.metadata,
            snapshot.protocol,
            snapshot.domainMetadata ++ removedDomains,
            committedVersion,
            snapshot.timestamp)
          logDebug(
            log"Catalog metadata update for table " +
            log"${MDC(DeltaLogKeys.PATH, logPath)} version " +
            log"${MDC(DeltaLogKeys.VERSION, committedVersion)}: " +
            log"${MDC(DeltaLogKeys.STATUS, if (updated) "updated" else "skipped")}")
        }
    }.recover {
      case e: Exception =>
        logWarning(
          log"Failed to update the catalog metadata of table " +
          log"${MDC(DeltaLogKeys.PATH, logPath)} " +
          log"version " +
          log"${MDC(DeltaLogKeys.VERSION, committedVersion)}" +
          log": ${MDC(DeltaLogKeys.ERROR, e.getMessage)}",
          e)
    }.onComplete { _ =>
      UpdateExternalTableMetadataHook.activeRequests.decrementAndGet()
    }
  }
}

object UpdateExternalTableMetadataHook {

  /**
   * Whether the commit changes metadata that the catalog entry mirrors. Row-tracking domain
   * changes alone don't count: the high watermark advances on every insert.
   */
  private[hooks] def changesTableMetadata(committedActions: Seq[Action]): Boolean =
    committedActions.exists {
      case _: Metadata | _: Protocol => true
      case d: DomainMetadata => d.domain == ClusteringMetadataDomain.domainName
      case _ => false
    }

  // -- Async thread pool --

  // Metadata-changing commits are rare. A single thread runs the updates in commit order, so an
  // earlier commit's update can't land after a later one has made it a no-op.
  private val ThreadPoolSize = 1

  @volatile private var tp: ExecutionContext = _

  private def getOrCreateExecutionContext(): ExecutionContext = synchronized {
    if (tp == null) {
      tp = ExecutionContext.fromExecutorService(
        DeltaThreadPool.newDaemonCachedThreadPool(
          "uc-external-table-metadata-sync", ThreadPoolSize))
    }
    tp
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
