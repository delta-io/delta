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

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.delta.util.CatalogTableUtils
import org.apache.spark.sql.types.StructType

/**
 * Builds a minimal [[CatalogTable]] that satisfies [[UCUtils.extractTableInfo]] for use after
 * CREATE TABLE, where a full catalog-round-trip CatalogTable is not yet available.
 *
 * Only the fields that the snapshot-manager routing path inspects are populated:
 * identifier (with catalog), location, and UC storage properties (tableId + feature flag).
 */
object CatalogTableFactory {

  /**
   * Constructs a minimal [[CatalogTable]] from UC staging metadata.
   *
   * @param ident     DSv2 Identifier (contains namespace and table name)
   * @param catalogName the catalog this table belongs to
   * @param tableId   Unity Catalog table ID
   * @param tablePath filesystem path to the Delta table root
   */
  def buildUCCatalogTable(
      ident: Identifier,
      catalogName: String,
      tableId: String,
      tablePath: String): CatalogTable = {
    val storageProps = Map(
      UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableId,
      CatalogTableUtils.FEATURE_CATALOG_MANAGED -> "supported"
    )

    CatalogTable(
      identifier = TableIdentifier(ident.name(), Some(ident.namespace()(0)), Some(catalogName)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = Some(new URI(tablePath)),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = storageProps
      ),
      schema = new StructType(),
      provider = Some("delta")
    )
  }
}
