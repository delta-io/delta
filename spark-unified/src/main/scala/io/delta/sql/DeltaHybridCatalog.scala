/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, V1Table}
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.catalog.AbstractDeltaCatalog

/**
 * Delta catalog implementation that returns HybridDeltaTable for catalog-managed Delta tables.
 *
 * This catalog extends AbstractDeltaCatalog and overrides loadTable to return
 * HybridDeltaTable instances for catalog-managed tables only. Path-based tables and
 * other table types go through the normal flow without hybrid behavior.
 *
 * Scope: Only catalog-managed tables accessed by identifier get the hybrid V1/V2 behavior.
 * This ensures SparkTable (V2) is only used for well-defined catalog tables where metadata
 * is fully available.
 */
class DeltaHybridCatalog extends AbstractDeltaCatalog {

  override def loadTable(ident: Identifier): Table = {
    // Delegate to parent which handles both catalog and path-based tables
    super.loadTable(ident) match {
      case v1: V1Table if DeltaTableUtils.isDeltaTable(v1.catalogTable) =>
        // Catalog-managed Delta table - wrap with HybridDeltaTable to enable V2 for streaming
        // Pass catalogTable to preserve catalog metadata (location, storage properties, etc.)
        new HybridDeltaTable(
          spark = SparkSession.active,
          identifier = ident,
          tablePath = v1.catalogTable.location.toString,
          catalogTable = Some(v1.catalogTable),
          options = v1.catalogTable.storage.properties
        )

      case o => o  // Path-based tables, Iceberg, non-Delta, etc. - return as-is (normal V1 flow)
    }
  }
}

