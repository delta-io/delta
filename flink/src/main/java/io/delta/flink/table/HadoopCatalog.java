/*
 *  Copyright (2021) The Delta Lake Project Authors.
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
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * A {@code HadoopCatalog} is a file-system–backed catalog implementation that resolves tables using
 * Hadoop-compatible file system paths.
 *
 * <p>This catalog treats the table path itself as the table identifier. As a result, table
 * discovery does not rely on an external metastore or catalog service. Instead, tables are located
 * directly by resolving the provided identifier as a filesystem URI.
 *
 * <p>{@code HadoopCatalog} supports static credential configuration supplied at construction time.
 * These credentials are returned verbatim and are intended for use by downstream components (e.g.,
 * table loaders or writers) when accessing the underlying storage system.
 *
 * <p>This catalog is suitable for environments where:
 *
 * <ul>
 *   <li>tables are stored in Hadoop-compatible file systems (e.g., HDFS, S3A, ABFS),
 *   <li>table identity is defined by filesystem location, and
 *   <li>credentials are configured statically rather than dynamically resolved.
 * </ul>
 *
 * <p>The catalog does not perform validation of table existence or schema during lookup; it simply
 * resolves the table path and returns the corresponding {@link TableDescriptor}.
 */
public class HadoopCatalog implements DeltaCatalog {

  private final Map<String, String> configurations;

  /**
   * Creates a {@code HadoopCatalog} with the given static credential configuration.
   *
   * <p>The provided configuration map is expected to contain all necessary key-value pairs required
   * to access the underlying Hadoop-compatible file system (for example, access keys, secrets, or
   * endpoint configurations).
   *
   * @param conf a map of static configuration and credential properties
   */
  public HadoopCatalog(Map<String, String> conf) {
    this.configurations = conf;
  }

  /**
   * Loads a table using the given table identifier, which is interpreted as a filesystem path.
   *
   * <p>The {@code tableId} is normalized and resolved into a {@link URI} representing the table
   * location. The same identifier is also used as the table UUID, as this catalog does not maintain
   * a separate identifier namespace.
   *
   * @param tableId the table identifier, interpreted as a filesystem path or URI
   * @return a {@link TableDescriptor} describing the resolved table
   */
  @Override
  public TableDescriptor getTable(String tableId) {
    URI tablePath = AbstractKernelTable.normalize(URI.create(tableId));
    TableDescriptor info = new TableDescriptor();
    info.tableId = tableId;
    info.tablePath = tablePath;
    info.uuid = tableId;
    return info;
  }

  /**
   * Creates a table at the given location in a file-based catalog.
   *
   * <p>The {@code tableId} is interpreted as a filesystem path or URI that identifies the table
   * location. This operation does not create any data or log files at the specified path.
   *
   * @param tableId The table identifier, interpreted as a filesystem path or URI that specifies the
   *     table location.
   * @param schema The logical schema of the table, describing column names, data types, and
   *     nullability.
   * @param partitions A list of column names used for partitioning the table; an empty list
   *     indicates an unpartitioned table.
   * @param properties A map of table properties for configuration and metadata; may be empty but
   *     must not be {@code null}.
   */
  @Override
  public void createTable(
      String tableId, StructType schema, List<String> partitions, Map<String, String> properties) {}

  /**
   * Returns the static credential configuration associated with this catalog.
   *
   * <p>Because this catalog only supports static credentials, the returned configuration is
   * independent of the provided UUID and is shared across all tables.
   *
   * @param uuid the table UUID (ignored by this implementation)
   * @return a map of static credential and configuration properties
   */
  @Override
  public Map<String, String> getCredentials(String uuid) {
    return configurations;
  }
}
