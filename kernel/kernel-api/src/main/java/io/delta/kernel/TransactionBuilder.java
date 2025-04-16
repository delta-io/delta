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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentTransactionException;
import io.delta.kernel.exceptions.DomainDoesNotExistException;
import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.exceptions.TableAlreadyExistsException;
import io.delta.kernel.exceptions.UnknownConfigurationException;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;

/**
 * Builder for creating a {@link Transaction} to mutate a Delta table.
 *
 * @since 3.2.0
 */
@Evolving
public interface TransactionBuilder {
  /**
   * Set the schema of the table. If setting the schema on an existing table for a schema evolution,
   * then column mapping must be enabled. This API will preserve field metadata for fields such as
   * field IDs and physical names. If field metadata is not specified for a field, it is considered
   * as a new column and new IDs/physical names will be specified. The possible schema evolutions
   * supported include column additions, removals, renames, and moves. If a schema evolution is
   * performed, implementations must perform the following validations:
   *
   * <ul>
   *   <li>No duplicate columns are allowed
   *   <li>Column names contain only valid characters
   *   <li>Data types are supported
   *   <li>No new non-nullable fields are added
   *   <li>Physical column name consistency is preserved in the new schema
   *   <li>No type changes
   *   <li>ToDo: Nested IDs for array/map types are preserved in the new schema
   *   <li>ToDo: Validate invalid field reorderings
   * </ul>
   *
   * @param engine {@link Engine} instance to use.
   * @param schema The new schema of the table.
   * @return updated {@link TransactionBuilder} instance.
   * @throws io.delta.kernel.exceptions.KernelException in case column mapping is not enabled
   * @throws IllegalArgumentException in case of any validation failure
   */
  TransactionBuilder withSchema(Engine engine, StructType schema);

  /**
   * Set the list of partitions columns when create a new partitioned table.
   *
   * @param engine {@link Engine} instance to use.
   * @param partitionColumns The partition columns of the table. These should be a subset of the
   *     columns in the schema. Only top-level columns are allowed to be partitioned. Note:
   *     Clustering columns and partition columns cannot coexist in a table.
   * @return updated {@link TransactionBuilder} instance.
   */
  TransactionBuilder withPartitionColumns(Engine engine, List<String> partitionColumns);

  /**
   * Set the list of clustering columns when create a new clustered table.
   *
   * @param engine {@link Engine} instance to use.
   * @param clusteringColumns The clustering columns of the table. These should be a subset of the
   *     columns in the schema. Both top-level and nested columns are allowed to be clustered. Note:
   *     Clustering columns and partition columns cannot coexist in a table.
   * @return updated {@link TransactionBuilder} instance.
   */
  TransactionBuilder withClusteringColumns(Engine engine, List<Column> clusteringColumns);

  /**
   * Set the transaction identifier for idempotent writes. Incremental processing systems (e.g.,
   * streaming systems) that track progress using their own application-specific versions need to
   * record what progress has been made, in order to avoid duplicating data in the face of failures
   * and retries during writes. By setting the transaction identifier, the Delta table can ensure
   * that the data with same identifier is not written multiple times. For more information refer to
   * the Delta protocol section <a
   * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers">
   * Transaction Identifiers</a>.
   *
   * @param engine {@link Engine} instance to use.
   * @param applicationId The application ID that is writing to the table.
   * @param transactionVersion The version of the transaction. This should be monotonically
   *     increasing with each write for the same application ID.
   * @return updated {@link TransactionBuilder} instance.
   */
  TransactionBuilder withTransactionId(
      Engine engine, String applicationId, long transactionVersion);

  /**
   * Set the table properties for the table. When the table already contains the property with same
   * key, it gets replaced if it doesn't have the same value.
   *
   * @param engine {@link Engine} instance to use.
   * @param properties The table properties to set. These are key-value pairs that can be used to
   *     configure the table. And these properties are stored in the table metadata.
   * @return updated {@link TransactionBuilder} instance.
   * @since 3.3.0
   */
  TransactionBuilder withTableProperties(Engine engine, Map<String, String> properties);

  /**
   * Set the maximum number of times to retry a transaction if a concurrent write is detected. This
   * defaults to 200
   *
   * @param maxRetries The number of times to retry
   * @return updated {@link TransactionBuilder} instance
   */
  TransactionBuilder withMaxRetries(int maxRetries);

  /**
   * Enables support for Domain Metadata on this table if it is not supported already. The table
   * feature _must_ be supported on the table to add or remove domain metadata using {@link
   * Transaction#addDomainMetadata} or {@link Transaction#removeDomainMetadata}. See <a
   * href="https://docs.delta.io/latest/versioning.html#how-does-delta-lake-manage-feature-compatibility">
   * How does Delta Lake manage feature compatibility?</a> for more details on table feature
   * support.
   *
   * <p>See the Delta protocol for more information on how to use <a
   * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#domain-metadata">Domain
   * Metadata</a>. This may break existing writers that do not support the Domain Metadata feature;
   * readers will be unaffected.
   */
  TransactionBuilder withDomainMetadataSupported();

  /**
   * Build the transaction. Also validates the given info to ensure that a valid transaction can be
   * created.
   *
   * @param engine {@link Engine} instance to use.
   * @throws ConcurrentTransactionException if the table already has a committed transaction with
   *     the same given transaction identifier.
   * @throws InvalidConfigurationValueException if the value of the property is invalid.
   * @throws UnknownConfigurationException if any of the properties are unknown to {@link
   *     TableConfig}.
   * @throws DomainDoesNotExistException if removing a domain that does not exist in the latest
   *     version of the table
   * @throws TableAlreadyExistsException if the operation provided when calling {@link
   *     Table#createTransactionBuilder(Engine, String, Operation)} is CREATE_TABLE and the table
   *     already exists
   */
  Transaction build(Engine engine);
}
