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
package io.delta.kernel.spark.catalog;

import io.delta.kernel.Operation;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.catalog.SparkTable;
import io.delta.kernel.spark.utils.SchemaUtils;
import io.delta.kernel.utils.CloseableIterable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A {@link TableCatalog} implementation that uses Delta Kernel for table operations. This catalog
 * is used for facilitating testing for spark-dsv2 code path.
 *
 * <p>This catalog is initialized with a base path where all tables will be created. The catalog
 * maintains a mapping of table identifiers to their physical paths on the filesystem. When a table
 * is created, it gets a unique subdirectory under the base path to store its data.
 */
public class TestCatalog implements TableCatalog {

  /** The name of this catalog instance, set during initialization. */
  private String catalogName;

  /**
   * The base directory path where all tables created by this catalog will be stored. Each table
   * gets a unique subdirectory under this path.
   */
  private String basePath;

  // TODO: Support catalog owned commit.
  private final Map<String, String> tablePaths = new ConcurrentHashMap<>();
  private final Configuration hadoopConf = new Configuration();
  private final Engine engine = DefaultEngine.create(hadoopConf);

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException("listTables method is not implemented");
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    // Check if this is a path-based table identifier
    String tablePath;
    if (isPathIdentifier(ident)) {
      tablePath = ident.name();
    } else {
      // Handle catalog-managed tables
      String tableKey = getTableKey(ident);
      tablePath = tablePaths.get(tableKey);
    }
    if (tablePath == null) {
      throw new NoSuchTableException(ident);
    }
    try {
      return new SparkTable(ident, tablePath, Optional.empty());
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
    try {
      // TODO: migrate to use CCv2 table
      io.delta.kernel.Table kernelTable = io.delta.kernel.Table.forPath(engine, tablePath);
      List<String> partitionColumns = new ArrayList<>();
      for (Transform partition : partitions) {
        // Extract column name from partition transform
        String columnName = partition.references()[0].describe();
        partitionColumns.add(columnName);
      }

      // TODO: migrate to use CCv2's committer API
      io.delta.kernel.Table.forPath(engine, tablePath)
          .createTransactionBuilder(
              engine, "kernel-spark-dsv2-test-catalog", Operation.CREATE_TABLE)
          .withSchema(engine, SchemaUtils.convertSparkSchemaToKernelSchema(schema))
          .withPartitionColumns(engine, partitionColumns)
          .withTableProperties(engine, properties)
          .build(engine)
          .commit(engine, CloseableIterable.emptyIterable());

      // Load the created table and return SparkTable
      return new SparkTable(ident, tablePath, Optional.empty());

    } catch (Exception e) {
      // Remove the table entry if creation fails
      tablePaths.remove(tableKey);
      throw new RuntimeException("Failed to create table: " + ident, e);
    }
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
    this.catalogName = name;
    // Use a default path if base_path is not provided
    this.basePath = options.getOrDefault("base_path", "/tmp/dsv2_test/");
  }

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Check if the given identifier represents a path-based table. Path-based tables are identified
   * by having a delta namespace. This follows the same logic as Delta Spark's
   * SupportsPathIdentifier.
   */
  private boolean isPathIdentifier(Identifier ident) {
    // For testing, simply check if it has a delta namespace
    return ident.namespace().length == 1 && ident.namespace()[0].toLowerCase().equals("delta");
  }

  /** Helper method to get the table key from identifier. */
  private String getTableKey(Identifier ident) {
    if (ident.namespace().length == 0) {
      return ident.name();
    } else {
      return String.join(".", ident.namespace()) + "." + ident.name();
    }
  }
}
