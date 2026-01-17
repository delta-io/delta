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

package io.delta.flink.table;

import io.delta.kernel.types.StructType;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * A {@code Catalog} abstracts interaction with an external table catalog or metadata service.
 *
 * <p>The catalog is responsible for resolving logical table identifiers into concrete table
 * metadata and for providing the credentials required to access the underlying storage system. This
 * abstraction allows different catalog implementations (e.g., filesystem-based catalogs,
 * metastore-backed catalogs, or REST-based catalogs) to be used interchangeably by higher-level
 * components.
 *
 * <p>Typical responsibilities of a {@code Catalog} include:
 *
 * <ul>
 *   <li>mapping table identifiers to physical table locations,
 *   <li>providing stable table UUIDs for identification and caching, and
 *   <li>supplying credential or configuration information required for table access.
 * </ul>
 */
public interface DeltaCatalog extends Serializable {

  /**
   * Init the catalog instance and make it ready for use. Must be called at least once before the
   * catalog can be safely used. Calling open on an already opened table has no effect.
   */
  default void open() {}

  /**
   * Loads metadata for a table identified by the given table identifier.
   *
   * <p>The identifier format and naming conventions are defined by the specific catalog
   * implementation. Implementations may interpret the identifier as a logical name, a
   * fully-qualified path, or another catalog-specific reference.
   *
   * @param tableId the logical identifier of the table to load; must not be {@code null}
   * @return a {@link TableDescriptor} object describing the resolved table
   * @throws IllegalArgumentException if the identifier is invalid
   * @throws ExceptionUtils.ResourceNotFoundException if the table cannot be resolved or loaded
   */
  TableDescriptor getTable(String tableId);

  /**
   * Creates a new table in the catalog with the given schema, partitioning, and properties.
   *
   * <p>The table is identified by {@code tableId} and is initialized with the provided {@link
   * StructType} schema. Optional partition columns define how the table data is physically
   * organized, and table properties supply additional configuration such as format-specific options
   * or metadata.
   *
   * @param tableId The unique identifier of the table to create within the catalog.
   * @param schema The logical schema of the table, describing column names, data types, and
   *     nullability.
   * @param partitions A list of column names used for partitioning the table; an empty list
   *     indicates an unpartitioned table.
   * @param properties A map of table properties for configuration and metadata; may be empty but
   *     must not be {@code null}.
   * @throws ExceptionUtils.ResourceAlreadyExistException If a table with the same identifier
   *     already exists in the catalog.
   */
  void createTable(
      String tableId, StructType schema, List<String> partitions, Map<String, String> properties);

  /**
   * Returns the credentials or configuration properties required to access the table identified by
   * the given UUID.
   *
   * <p>The returned map may contain authentication information, endpoint configuration, or other
   * filesystem- or catalog-specific properties. The exact contents and semantics are defined by the
   * catalog implementation.
   *
   * @param uuid the unique identifier of the table
   * @return a map of credential or configuration properties; may be empty but never {@code null}
   */
  Map<String, String> getCredentials(String uuid);

  /**
   * A container for table metadata resolved by a {@link DeltaCatalog}.
   *
   * <p>{@code TableInfo} describes the essential properties needed to locate and access a table,
   * independent of the underlying catalog implementation.
   */
  class TableDescriptor {

    /** The logical identifier used to resolve the table. */
    String tableId;

    /** A usually non-readable string that catalog internally uses to locate the table. */
    String internalId;

    /** The normalized physical location of the table. */
    URI tablePath;
  }
}
