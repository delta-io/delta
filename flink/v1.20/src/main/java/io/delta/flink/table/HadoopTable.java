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

package io.delta.flink.table;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.types.StructType;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code HadoopTable} is a {@link DeltaTable} implementation backed by a Hadoop file system. This
 * implementation loads and manages Delta table metadata directly from the underlying storage layer
 * rather than from an external catalog service.
 *
 * <p>{@code HadoopTable} is typically used in environments where:
 *
 * <ul>
 *   <li>tables are stored in distributed file systems such as HDFS, ABFS, S3A (via Hadoop FS), or
 *       other file-system–compatible backends,
 *   <li>no metastore or catalog service is required for table discovery, and
 *   <li>file-system paths are the primary means of identifying and accessing tables.
 * </ul>
 *
 * <p>This implementation is suitable for standalone deployments, filesystem-based analytics
 * pipelines, and connector implementations where the Hadoop FileSystem abstraction is available.
 */
public class HadoopTable extends AbstractKernelTable {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopTable.class);

  public HadoopTable(URI tablePath, Map<String, String> conf) {
    this(tablePath, conf, null, null);
  }

  /** Default constructor that works with a HadoopCatalog */
  public HadoopTable(
      URI tablePath, Map<String, String> conf, StructType schema, List<String> partitionColumns) {
    this(new HadoopCatalog(conf), tablePath.toString(), conf, schema, partitionColumns);
  }

  /**
   * HadoopTable also works with non-Hadoop catalogs (e.g., UnityCatalog) and do path-based access.
   */
  public HadoopTable(DeltaCatalog catalog, String tableId, Map<String, String> conf) {
    this(catalog, tableId, conf, null, null);
  }

  HadoopTable(
      DeltaCatalog catalog,
      String tableId,
      Map<String, String> conf,
      StructType schema,
      List<String> partitionColumns) {
    super(catalog, tableId, conf, schema, partitionColumns);
  }

  @Override
  protected Snapshot loadLatestSnapshot() {
    return TableManager.loadSnapshot(getTablePath().toString()).build(getEngine());
  }
}
