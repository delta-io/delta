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

package org.apache.spark.sql.delta.hooks.metrics

// scalastyle:off import.ordering.noEmptyLine
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.CommittedTransaction
import org.apache.spark.sql.delta.catalog.AbstractDeltaCatalogClient
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.CatalogTableUtils
import org.apache.spark.sql.delta.util.threads.DeltaThreadPool
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Post-commit hook that sends commit metrics to Unity Catalog
 * for UC-managed Delta tables.
 *
 * The hook reads the catalog's options out of `spark.conf` for the table's V2 catalog name
 * and builds a fresh [[AbstractDeltaCatalogClient]] per commit (inside the async dispatch),
 * then forwards the per-commit telemetry to it. The client knows how to shape and ship the
 * payload; the hook itself never sees the wire types.
 *
 * Building a client per commit avoids caching anything that would transitively pin the
 * [[SparkSession]] in a long-lived map. The construction cost (HTTP client / token
 * provider init) lives entirely inside the background `Future`, so commit latency is
 * unaffected.
 *
 * Gated by [[DeltaSQLConf.DELTA_UC_COMMIT_METRICS_ENABLED]]. Silent no-op when the
 * catalog is not configured for `deltaRestApi.enabled=true`. Errors from the async
 * dispatch are logged and swallowed; commit success is unaffected.
 *
 * @param catalogTable catalog metadata for UC-managed detection and identifier lookup
 */
case class UpdateMetricsHook(catalogTable: Option[CatalogTable])
    extends PostCommitHook with DeltaLogging {

  override val name: String = "Update catalog metrics"

  override def run(
      spark: SparkSession,
      txn: CommittedTransaction): Unit = {
    if (!spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_UC_COMMIT_METRICS_ENABLED)) {
      return
    }
    val ct = catalogTable.orNull
    if (ct == null ||
        !CatalogTableUtils.isUnityCatalogManagedTable(ct)) {
      return
    }
    sendMetricsAsync(spark, ct, txn)
  }

  private def sendMetricsAsync(
      spark: SparkSession,
      ct: CatalogTable,
      txn: CommittedTransaction): Unit = {
    val catalogName = ct.identifier.catalog.getOrElse {
      // CatalogTable has no V2 catalog identifier (e.g. path-based table) -- silent no-op.
      return
    }
    val committedActions = txn.committedActions
    val committedVersion = txn.committedVersion
    // Read histogram from the CRC only -- avoids triggering state reconstruction.
    val snapshotHistogram = txn.postCommitSnapshot.checksumOpt.flatMap(_.fileSizeHistogram)
    val logPath = txn.deltaLog.logPath

    implicit val ec: ExecutionContext =
      UpdateMetricsHook.getOrCreateExecutionContext(spark)
    UpdateMetricsHook.activeRequests.incrementAndGet()
    Future {
      // Build the client inside the Future so its construction cost (HTTP client / token
      // provider init) doesn't sit on the commit thread, and so the client and any
      // SparkSession it transitively captures become GC-eligible as soon as the Future
      // completes.
      val opts = UpdateMetricsHook.catalogOptionsFor(spark, catalogName)
      AbstractDeltaCatalogClient
        .fromCatalogOptionsIfEnabled(catalogName, opts, UpdateMetricsHook.UnusedLoader)
        .foreach { client =>
          client.reportMetrics(ct, committedActions, committedVersion, snapshotHistogram)
          logDebug(
            log"Successfully sent UC metrics for table " +
            log"${MDC(DeltaLogKeys.PATH, logPath)} " +
            log"version " +
            log"${MDC(DeltaLogKeys.VERSION, committedVersion)}")
        }
    }.recover {
      case e: Exception =>
        logWarning(
          log"Failed to send UC metrics for table " +
          log"${MDC(DeltaLogKeys.PATH, logPath)} " +
          log"version " +
          log"${MDC(DeltaLogKeys.VERSION, committedVersion)}" +
          log": ${MDC(DeltaLogKeys.ERROR, e.getMessage)}",
          e)
    }.onComplete { _ =>
      UpdateMetricsHook.activeRequests.decrementAndGet()
    }
  }
}

// Follows the same async dispatch pattern as UpdateCatalog.
object UpdateMetricsHook {

  // -- Async thread pool --

  @volatile private var tp: ExecutionContext = _

  private def getOrCreateExecutionContext(
      spark: SparkSession): ExecutionContext = synchronized {
    if (tp == null) {
      val poolSize = spark.sessionState.conf.getConf(
        DeltaSQLConf.DELTA_UC_COMMIT_METRICS_THREAD_POOL_SIZE)
      tp = ExecutionContext.fromExecutorService(
        DeltaThreadPool.newDaemonCachedThreadPool(
          "uc-metrics-sender", poolSize))
    }
    tp
  }

  private[metrics] val activeRequests = new AtomicInteger(0)

  // Test only.
  private[metrics] def awaitCompletion(
      timeoutMs: Long): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (activeRequests.get() > 0 &&
           System.currentTimeMillis() < deadline) {
      Thread.sleep(50)
    }
    activeRequests.get() == 0
  }

  // -- Per-commit client construction --

  /**
   * Assembles the catalog options for `catalogName` from the live `spark.conf` by walking
   * `spark.sql.catalog.<name>.*` keys, stripping the prefix, and packing the result into a
   * [[CaseInsensitiveStringMap]] -- the same shape `AbstractDeltaCatalog.initialize`
   * receives. Returns an empty map if no entries exist (the resulting client lookup will
   * silently no-op via the `deltaRestApi.enabled=false` default).
   */
  private[metrics] def catalogOptionsFor(
      spark: SparkSession, catalogName: String): CaseInsensitiveStringMap = {
    val prefix = s"spark.sql.catalog.$catalogName."
    val opts = spark.conf.getAll.iterator.collect {
      case (k, v) if k.startsWith(prefix) => k.substring(prefix.length) -> v
    }.toMap
    new CaseInsensitiveStringMap(opts.asJava)
  }

  /**
   * `fromCatalogOptionsIfEnabled` requires a `Identifier => Table` loader for the catalog
   * client's `loadTable` fallback path. The metrics path never invokes `loadTable`, so we
   * pass a throwing stub: it surfaces loudly if a future code change unexpectedly starts
   * calling it from `reportMetrics`, rather than returning a misleading `null`.
   */
  private[metrics] val UnusedLoader: Identifier => Table =
    _ => throw new UnsupportedOperationException(
      "UpdateMetricsHook's per-commit client does not support loadTable")
}
