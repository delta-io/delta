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
package io.delta.spark.internal.v2.tablemanager

import java.util.concurrent.atomic.AtomicReference

import scala.jdk.OptionConverters._

import org.apache.spark.sql.delta.storage.LogStoreProvider
import io.delta.spark.internal.v2.DeltaV2Logging
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager
import io.delta.spark.internal.v2.kernel.KernelContext
import io.delta.spark.internal.v2.snapshot.SnapshotManagerFactory
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Process-cached [[DeltaV2TableManager]] implementation.
 *
 * The manager owns the table-scoped [[KernelContext]] but does not retain snapshot state. Each
 * [[snapshotManager]] call uses [[SnapshotManagerFactory]] to create an uncached manager backed by
 * the Kernel Engine shared through that context.
 *
 * @param qualifiedTableDataPath the fully-qualified table data directory (parent of `_delta_log`).
 * @param sessionInvariantFsOptions filesystem-prefixed credential options (`fs.*`, `dfs.*`) that
 *   were used to resolve the table path. Retained for downstream engine construction.
 * @param initialCatalogTableOpt the catalog table supplied by the first caller that loaded this
 *   entry, if any.
 */
private[tablemanager] class DeltaV2TableManagerImpl(
    val qualifiedTableDataPath: Path,
    val sessionInvariantFsOptions: Map[String, String],
    val initialCatalogTableOpt: Option[CatalogTable])
    extends DeltaV2TableManager
    with DeltaV2Logging
    with LogStoreProvider
{

  private val _unsafeVolatileCatalogTable =
    new AtomicReference[CatalogTable]()

  private[tablemanager] def unsafeVolatileCatalogTable: Option[CatalogTable] =
    Option(_unsafeVolatileCatalogTable.get())

  override private[tablemanager] def withUnsafeVolatileCatalogTable(
      table: CatalogTable): DeltaV2TableManager = {
    val oldTable = _unsafeVolatileCatalogTable.getAndSet(table)
    if (oldTable != null && oldTable.identifier != table.identifier) {
      recordDeltaEvent(
        null,
        "deltaV2.catalog.multipleTablesForSameLog",
        data = Map(
          "oldTableIdentifier" -> oldTable.identifier,
          "newTableIdentifier" -> table.identifier),
        path = Some(tablePath))
    }
    this
  }

  initialCatalogTableOpt.foreach(withUnsafeVolatileCatalogTable)

  /** The table's data directory, fully qualified. */
  def tablePath: Path = qualifiedTableDataPath

  /** Used to read and write physical log files and checkpoints. */
  private[tablemanager] lazy val logStore = createLogStore(SparkSession.active)

  override private[v2] lazy val kernelContext =
    KernelContext(sessionInvariantFsOptions, logStore)

  override def snapshotManager(): DeltaV2SnapshotManager =
    SnapshotManagerFactory.create(
      tablePath.toString,
      kernelContext.getDefaultEngine(),
      unsafeVolatileCatalogTable.toJava)
}
