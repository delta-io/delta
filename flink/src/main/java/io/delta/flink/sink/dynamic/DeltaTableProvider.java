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

package io.delta.flink.sink.dynamic;

import io.delta.flink.table.DeltaTable;
import io.delta.kernel.types.StructType;
import java.io.Serializable;
import java.util.List;

/**
 * Resolves or creates a {@link DeltaTable} for a given table name and schema.
 *
 * <p>Implementations are responsible for looking up an existing table or creating a new one when
 * the table does not yet exist. The provider is called lazily from within the sink writer as new
 * table names are encountered, and its results are held in an LRU cache.
 *
 * <p>Implementations must be {@link Serializable} so that they can be distributed to Flink task
 * managers.
 *
 * <p>Built-in providers:
 *
 * <ul>
 *   <li>{@link HadoopTableProvider} — {@link io.delta.flink.table.HadoopCatalog} + {@link
 *       io.delta.flink.table.HadoopTable} (stream {@code tableName} is a filesystem table root URI)
 *   <li>{@link UnityCatalogTableProvider} — {@link io.delta.flink.table.UnityCatalog} + {@link
 *       io.delta.flink.table.CatalogManagedTable} (stream {@code tableName} is a UC table id, e.g.
 *       {@code catalog.schema.table})
 * </ul>
 */
public interface DeltaTableProvider extends Serializable {

  /**
   * Returns a {@link DeltaTable} for the given table name, creating it if necessary.
   *
   * <p>The returned table must already be {@link DeltaTable#open() opened} and ready for writing.
   *
   * @param tableName table identifier, e.g. {@code "catalog.schema.table"}
   * @param schema Delta schema used only when creating a new table; ignored if the table exists
   * @param partitionColumns ordered list of partition column names; may be empty
   * @return a {@link DeltaTable} instance ready for writing
   */
  DeltaTable getOrCreate(String tableName, StructType schema, List<String> partitionColumns);
}
