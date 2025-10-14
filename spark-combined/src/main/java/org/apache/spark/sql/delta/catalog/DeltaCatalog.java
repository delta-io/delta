/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.catalog;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.delta.DeltaTableIdentifier;
import org.apache.spark.sql.delta.DeltaTableUtils;

/**
 * Delta Catalog implementation that can delegate to both V1 and V2 implementations. This class sits
 * in delta-spark (combined) module and can access: - V1: org.apache.spark.sql.delta.* (full version
 * with DeltaLog) - V2: io.delta.kernel.spark.*
 */
public class DeltaCatalog extends AbstractDeltaCatalog {

  @Override
  public Table loadTable(Identifier identifier) {
    try {
      // Load table from delegate catalog directly
      Table delegateTable = ((TableCatalog) delegate).loadTable(identifier);
      // If delegate table is a V1Table and it's a Delta table, return SparkTable
      if (delegateTable instanceof V1Table) {
        V1Table v1Table = (V1Table) delegateTable;
        if (DeltaTableUtils.isDeltaTable(v1Table.catalogTable())) {
          return new io.delta.kernel.spark.table.SparkTable(
              identifier, v1Table.catalogTable().location().toString());
        }
      }
      // Otherwise return the delegate table as-is
      return delegateTable;
    } catch (AnalysisException e) {
      // Handle NoSuchTableException and its related exceptions
      if (e instanceof NoSuchTableException
          || e instanceof NoSuchNamespaceException
          || e instanceof NoSuchDatabaseException) {
        if (isPathIdentifier(identifier)) {
          return newDeltaPathTable(identifier);
        } else if (isIcebergPathIdentifier(identifier)) {
          return newIcebergPathTable(identifier);
        }
      } else if (DeltaTableIdentifier.gluePermissionError(e) && isPathIdentifier(identifier)) {
        // Handle Glue permission errors for path identifiers
        return newDeltaPathTable(identifier);
      }
      // Rethrow as RuntimeException since AnalysisException is checked
      throw new RuntimeException(e);
    }
  }
}
