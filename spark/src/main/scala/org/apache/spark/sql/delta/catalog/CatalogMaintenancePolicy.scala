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

import org.apache.spark.sql.delta.{DeltaErrors, Snapshot}

private[delta] sealed abstract class CatalogMaintenanceOperation(val name: String)

private[delta] object CatalogMaintenanceOperation {
  case object Optimize extends CatalogMaintenanceOperation("OPTIMIZE")
  case object Vacuum extends CatalogMaintenanceOperation("VACUUM")

  // REORG is not part of the operations recognized by this client version. Keeping it as a
  // distinct command identity prevents OPTIMIZE permission from authorizing delegated REORG work.
  case object Reorg extends CatalogMaintenanceOperation("REORG")
}

/** Optional client-only policy carried by a catalog's resolved Spark table. */
private[delta] trait SupportsCatalogMaintenancePolicy {
  def additionalClientMaintenanceOperations: Set[CatalogMaintenanceOperation]
}

private[delta] object CatalogMaintenancePolicy {

  def requireAllowed(
      table: DeltaTableV2,
      snapshot: Snapshot,
      operation: CatalogMaintenanceOperation): Unit = {
    if (
      !isAllowed(
        snapshot.isCatalogOwned,
        table.additionalClientMaintenanceOperations,
        operation)
    ) {
      throw DeltaErrors.operationBlockedOnCatalogManagedTable(operation.name)
    }
  }

  private[delta] def isAllowed(
      isCatalogOwned: Boolean,
      additionalOperations: Set[CatalogMaintenanceOperation],
      operation: CatalogMaintenanceOperation): Boolean = {
    !isCatalogOwned || (operation != CatalogMaintenanceOperation.Reorg &&
      additionalOperations.contains(operation))
  }
}
