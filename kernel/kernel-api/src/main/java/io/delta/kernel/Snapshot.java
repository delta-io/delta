/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.commit.PublishFailedException;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Represents a snapshot of a Delta table at a specific version.
 *
 * <p>A {@code Snapshot} is a consistent view of a Delta table at a specific point in time,
 * identified by a version number. It provides access to the table's metadata, schema, and
 * capabilities for both reading and writing data. This interface serves as the entry point for
 * table operations after resolving a table through a {@link SnapshotBuilder}.
 *
 * <p>The snapshot represents a consistent view of the table at the resolved version. All operations
 * on this snapshot will see the same data and metadata, ensuring consistency across reads and
 * writes within the same snapshot.
 *
 * <p>There are two ways to create a {@code Snapshot}:
 *
 * <ul>
 *   <li><b>New API (recommended):</b> Use {@link TableManager#loadSnapshot(String)} to get a {@link
 *       SnapshotBuilder}, which can then be configured and built into a snapshot
 *   <li><b>Legacy API:</b> Use {@code Table.forPath(path)} followed by methods like {@code
 *       getLatestSnapshot()}, {@code getSnapshotAtTimestamp()}, etc.
 * </ul>
 *
 * @since 3.0.0
 */
@Evolving
public interface Snapshot {

  /** @return the file system path to this table */
  String getPath();

  /** @return the version of this snapshot in the Delta table */
  long getVersion();

  /**
   * Get the names of the partition columns in the Delta table at this snapshot.
   *
   * <p>The partition column names are returned in the order they are defined in the Delta table
   * schema. If the table does not define any partition columns, this method returns an empty list.
   *
   * @return a list of partition column names, or an empty list if the table is not partitioned.
   */
  List<String> getPartitionColumnNames();

  /**
   * Get the timestamp (in milliseconds since the Unix epoch) of the latest commit in this snapshot.
   *
   * @param engine the engine to use for IO operations
   * @return the timestamp of the latest commit
   */
  long getTimestamp(Engine engine);

  /** @return the schema of the Delta table at this snapshot */
  StructType getSchema();

  /**
   * Returns the configuration for the provided domain if it exists in the snapshot. Returns empty
   * if the domain is not present in the snapshot.
   *
   * @param domain the domain to look up
   * @return the domain configuration or empty
   */
  Optional<String> getDomainMetadata(String domain);

  /**
   * Get all table properties for the Delta table at this snapshot.
   *
   * @return a {@link Map} of table properties.
   */
  Map<String, String> getTableProperties();

  /** @return a scan builder to construct a {@link Scan} to read data from this snapshot */
  ScanBuilder getScanBuilder();

  /**
   * @return a {@link UpdateTableTransactionBuilder} to build an update table transaction
   * @since 3.4.0
   */
  UpdateTableTransactionBuilder buildUpdateTableTransaction(String engineInfo, Operation operation);

  /**
   * Publishes all catalog commits at this table version. Applicable only to catalog-managed tables.
   * This method is a no-op for filesystem-managed tables, if the committer doesn't support
   * publishing, or if there's no catalog commits to publish.
   *
   * <p>Publishing copies ratified catalog commits to the Delta log as published Delta files,
   * reducing catalog storage requirements and enabling some table maintenance operations, like
   * checkpointing.
   *
   * @param engine the engine to use for publishing commits
   * @see io.delta.kernel.commit.SupportsPublishing#publish
   * @throws PublishFailedException if the publish operation fails
   */
  // TODO: Return a new Snapshot reflecting the published state
  void publish(Engine engine) throws PublishFailedException;
}
