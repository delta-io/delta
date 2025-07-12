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

package io.delta.kernel;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Optional;

/**
 * Represents a Delta table resolved to a specific version.
 *
 * <p>A {@code ResolvedTable} is a snapshot of a Delta table at a specific point in time, identified
 * by a version number. It provides access to the table's metadata, schema, and capabilities for
 * both reading and writing data. This interface is the result of resolving a table through a {@link
 * ResolvedTableBuilder} and serves as the entry point for table operations.
 *
 * <p>The resolved table represents a consistent view of the table at the resolved version. All
 * operations on this table will see the same data and metadata, ensuring consistency across reads
 * and writes.
 */
@Experimental
public interface ResolvedTable {

  /** @return the file system path to the this table */
  String getPath();

  /** @return the version number of this table snapshot */
  long getVersion();

  /**
   * @return the timestamp (in milliseconds since Unix epoch) of when this version was committed
   */
  long getTimestamp(Engine engine);

  /**
   * @return the partition columns of the table at this version, in the order they are defined in
   *     the table schema. Returns an empty list if the table is not partitioned
   */
  List<Column> getPartitionColumns();

  /**
   * @param domain the domain name to query
   * @return the configuration for the provided {@code domain} if it exists in the table, or empty
   *     if the {@code domain} is not present in the table.
   */
  Optional<String> getDomainMetadata(String domain);

  /** @return the schema of the table at this version */
  StructType getSchema();

  /** @return a scan builder for constructing scans to read data from this table */
  ScanBuilder getScanBuilder();

  /** @return a committer that owns and controls commits to this table */
  Committer getCommitter();
}
