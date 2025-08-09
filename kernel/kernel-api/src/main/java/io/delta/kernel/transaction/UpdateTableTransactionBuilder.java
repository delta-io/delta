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

package io.delta.kernel.transaction;

import io.delta.kernel.Transaction;
import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builder for creating a {@link Transaction} to update an existing Delta table.
 *
 * @since 3.4.0
 */
@Evolving
public interface UpdateTableTransactionBuilder {

  /**
   * Set a new schema for the table, enabling schema evolution.
   *
   * <p>Schema evolution allows you to modify the table's structure by adding, removing, renaming,
   * or reordering columns. Column mapping must be enabled on the table for schema evolution to be
   * supported.
   *
   * <p>The provided schema should preserve field metadata (such as field IDs and physical names)
   * for existing columns. Columns without metadata will be considered new columns and be assigned
   * new IDs and physical names automatically.
   *
   * <p>Supported schema evolution operations:
   *
   * <ul>
   *   <li><b>Add columns:</b> New columns can be added at any position
   *   <li><b>Rename columns:</b> Change the logical name while preserving the physical name
   *   <li><b>Type widening:</b> Compatible type changes (e.g., int to long)
   *   <li><b>Reorder columns:</b> Change the position of columns in the schema
   *   <li><b>Drop columns:</b> Remove columns (with restrictions)
   * </ul>
   *
   * @param schema The new schema for the table. Cannot be null. Must be compatible with the current
   *     schema and follow schema evolution rules.
   */
  UpdateTableTransactionBuilder withUpdatedSchema(StructType schema);

  /**
   * Add or update table properties (configuration).
   *
   * <p>Properties specified here will be added to the table or override existing values. To remove
   * properties, use {@link #withTablePropertiesRemoved(Set)}.
   *
   * @param properties A map of property names to their values. The properties will be validated and
   *     normalized. Cannot be null.
   */
  UpdateTableTransactionBuilder withTablePropertiesAdded(Map<String, String> properties);

  /**
   * Remove table properties from the table configuration.
   *
   * <p>The specified property keys will be removed from the table's configuration. Attempting to
   * remove a property that doesn't exist is not an error.
   *
   * <p>Currently only user-properties (in other words, ones that are not prefixed by 'delta.') can
   * be removed using this API. Adding and removing the same key in the same transaction is not
   * allowed.
   *
   * @param propertyKeys A set of property names to remove. Cannot be null.
   * @throws IllegalArgumentException if attempting to remove a 'delta.' property
   */
  UpdateTableTransactionBuilder withTablePropertiesRemoved(Set<String> propertyKeys);

  /**
   * Update the clustering columns for the table and enable clustering if it is not already enabled.
   * Note: clustering cannot be enabled for a partitioned table.
   *
   * @param clusteringColumns The columns to cluster by. Cannot be null.
   * @throws IllegalArgumentException if the table is partitioned
   */
  // TODO: should this be a DataLayoutSpec instead?
  UpdateTableTransactionBuilder withClusteringColumns(List<Column> clusteringColumns);

  /**
   * Set a transaction identifier for idempotent operations.
   *
   * <p>Transaction identifiers allow you to implement idempotent operations by ensuring that
   * multiple attempts to perform the same logical operation don't result in duplicate effects. This
   * is useful for:
   *
   * <ul>
   *   <li>Retry logic in distributed systems
   *   <li>Exactly-once processing guarantees
   *   <li>Recovery from failures
   * </ul>
   *
   * <p>If a transaction with the same application ID and version (or higher) has already been
   * committed the transaction will fail.
   *
   * @param applicationId A unique identifier for the application or process. Cannot be null.
   * @param transactionVersion A monotonically increasing version number for this application ID.
   */
  UpdateTableTransactionBuilder withTransactionId(String applicationId, long transactionVersion);

  /**
   * Set the maximum number of retries for handling concurrent write conflicts.
   *
   * <p>When multiple writers attempt to modify the same Delta table simultaneously, conflicts can
   * occur. This setting controls how many times the operation will be retried with conflict
   * resolution before giving up.
   *
   * @param maxRetries The maximum number of retries. Must be >= 0. Default is 200.
   */
  UpdateTableTransactionBuilder withMaxRetries(int maxRetries);

  /**
   * Set the log compaction interval for optimizing the transaction log.
   *
   * <p>Log compaction creates periodic checkpoint files that consolidate multiple transaction log
   * entries, improving read performance and reducing the number of files that need to be processed
   * when reading table metadata.
   *
   * <p>A value of 0 disables automatic log compaction for this transaction. Positive values specify
   * how many commits should occur between compactions. Defaults to 0.
   *
   * @param logCompactionInterval The number of commits between checkpoints. Must be >= 0. A value
   *     of 0 disables log compaction.
   */
  UpdateTableTransactionBuilder withLogCompactionInterval(int logCompactionInterval);

  /**
   * Build the transaction for updating the Delta table.
   *
   * <p>This validates all the configuration and creates a {@link Transaction} that can be used to
   * update the existing Delta table. The transaction must be committed using {@link
   * Transaction#commit(Engine, CloseableIterable)} to actually apply the changes.
   *
   * @param engine The {@link Engine} instance to use for the transaction. Cannot be null.
   * @return A configured {@link Transaction} for updating the table.
   * @throws ConcurrentTransactionException if the table already has a committed transaction with
   *     the same given transaction identifier.
   */
  Transaction build(Engine engine);
}
