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
package io.delta.spark.dsv2.catalog;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.delta.kernel.defaults.engine.DefaultEngine;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import io.delta.kernel.TableManager;
import io.delta.kernel.engine.Engine;
import io.delta.spark.dsv2.table.DeltaKernelTable;
import io.delta.kernel.internal.SnapshotImpl;
import org.apache.hadoop.conf.Configuration;

/**
 * A {@link TableCatalog} implementation that uses Delta Kernel for table operations.
 * This catalog is used for facilitating testing for spark-dsv2 code path.
 */
public class TestCatalog implements TableCatalog {

  private String catalogName;
  private String basePath;
  // TODO: Support catalog owned commit.
  private final Map<String, String> tablePaths = new ConcurrentHashMap<>();
  private final Engine engine = DefaultEngine.create(new Configuration());

  @Override
  public Identifier[] listTables(String[] namespace) {
    // Return all registered tables for the given namespace
    return tablePaths.entrySet().stream()
        .filter(entry -> {
          String tableName = entry.getKey();
          // Simple namespace matching - in a real implementation, you'd want more sophisticated logic
          return namespace.length == 0 || tableName.startsWith(String.join(".", namespace) + ".");
        })
        .map(entry -> {
          String tableName = entry.getKey();
          String[] nameParts = tableName.split("\\.");
          String[] tableNamespace = nameParts.length > 1 ? 
              java.util.Arrays.copyOf(nameParts, nameParts.length - 1) : new String[0];
          String tableIdentifier = nameParts[nameParts.length - 1];
          return Identifier.of(tableNamespace, tableIdentifier);
        })
        .toArray(Identifier[]::new);
  }

  @Override
  public Table loadTable(Identifier ident) {
    String tableKey = getTableKey(ident);
    String tablePath = tablePaths.get(tableKey);
    
    if (tablePath == null) {
      throw new RuntimeException("Table not found: " + ident);
    }
    
    try {
      // Use TableManager.loadTable to load the table
      SnapshotImpl snapshot = (SnapshotImpl) TableManager.loadSnapshot(tablePath).build(engine);
      return new DeltaKernelTable(ident, snapshot);
    } catch (Exception e) {
      throw new RuntimeException("Failed to load table: " + ident, e);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) {
    String tableKey = getTableKey(ident);
    String tablePath = basePath + UUID.randomUUID() + "/";
    tablePaths.put(tableKey, tablePath);
    
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException("alterTable method is not implemented");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    String tableKey = getTableKey(ident);
    return tablePaths.remove(tableKey) != null;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException("renameTable method is not implemented");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.basePath = options.get("base_path");
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Helper method to get the table key from identifier.
   */
  private String getTableKey(Identifier ident) {
    if (ident.namespace().length == 0) {
      return ident.name();
    } else {
      return String.join(".", ident.namespace()) + "." + ident.name();
    }
  }
}
