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

import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.FileSystemClient;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

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

  private AbstractKernelTable engineLoader;
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

  public void setEngineLoader(AbstractKernelTable engineLoader) {
    this.engineLoader = engineLoader;
  }

  /** Does a simple check of existence of _delta_log folder */
  @Override
  public TableDescriptor getTable(String tableId) {
    URI tablePath = AbstractKernelTable.normalize(URI.create(tableId));
    try {
      Engine engine = this.engineLoader.getEngine();
      FileSystemClient fs = engine.getFileSystemClient();
      fs.getFileStatus(tablePath.resolve("_delta_log").toString());
    } catch (IOException e) {
      throw new ExceptionUtils.ResourceNotFoundException(e.getMessage());
    }
    TableDescriptor info = new TableDescriptor();
    info.tableId = tableId;
    info.tablePath = tablePath;
    info.uuid = tableId;
    return info;
  }

  @Override
  public void createTable(
      String tableId,
      StructType schema,
      List<String> partitions,
      Map<String, String> properties,
      Consumer<TableDescriptor> callback) {
    TableDescriptor desc = new TableDescriptor(tableId, tableId, URI.create(tableId));
    callback.accept(desc);
  }

  /** @return nothing as this catalog does not vend credentials */
  @Override
  public Map<String, String> getCredentials(String uuid) {
    return configurations;
  }
}
