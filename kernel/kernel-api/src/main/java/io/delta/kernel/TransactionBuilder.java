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

import java.util.List;
import java.util.Map;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentTransactionException;
import io.delta.kernel.exceptions.InvalidConfigurationValueException;
import io.delta.kernel.exceptions.UnknownConfigurationException;
import io.delta.kernel.types.StructType;
import io.delta.kernel.internal.TableConfig;

/**
 * Builder for creating a {@link Transaction} to mutate a Delta table.
 *
 * @since 3.2.0
 */
@Evolving
public interface TransactionBuilder {
    /**
     * Set the schema of the table when creating a new table.
     *
     * @param engine {@link Engine} instance to use.
     * @param schema The new schema of the table.
     * @return updated {@link TransactionBuilder} instance.
     */
    TransactionBuilder withSchema(Engine engine, StructType schema);

    /**
     * Set the list of partitions columns when create a new partitioned table.
     *
     * @param engine           {@link Engine} instance to use.
     * @param partitionColumns The partition columns of the table. These should be a subset of the
     *                         columns in the schema.
     * @return updated {@link TransactionBuilder} instance.
     */
    TransactionBuilder withPartitionColumns(Engine engine, List<String> partitionColumns);

    /**
     * Set the transaction identifier for idempotent writes. Incremental processing systems (e.g.,
     * streaming systems) that track progress using their own application-specific versions need to
     * record what progress has been made, in order to avoid duplicating data in the face of
     * failures and retries during writes. By setting the transaction identifier, the Delta table
     * can ensure that the data with same identifier is not written multiple times. For more
     * information refer to the Delta protocol section <a
     * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#transaction-identifiers">
     * Transaction Identifiers</a>.
     *
     * @param engine             {@link Engine} instance to use.
     * @param applicationId      The application ID that is writing to the table.
     * @param transactionVersion The version of the transaction. This should be monotonically
     *                           increasing with each write for the same application ID.
     * @return updated {@link TransactionBuilder} instance.
     */
    TransactionBuilder withTransactionId(
            Engine engine,
            String applicationId,
            long transactionVersion);

    /**
     * Set the table properties for the table. When the table already contains the property with
     * same key, it gets replaced if it doesn't have the same value.
     *
     * @param engine     {@link Engine} instance to use.
     * @param properties The table properties to set. These are key-value pairs that can be used to
     *                   configure the table. And these properties are stored in the table metadata.
     * @return updated {@link TransactionBuilder} instance.
     *
     * @since 3.3.0
     */
    TransactionBuilder withTableProperties(Engine engine, Map<String, String> properties);

    /**
     * Build the transaction. Also validates the given info to ensure that a valid transaction can
     * be created.
     *
     * @param engine {@link Engine} instance to use.
     * @throws ConcurrentTransactionException if the table already has a committed transaction with
     *                                        the same given transaction identifier.
     * @throws InvalidConfigurationValueException if the value of the property is invalid.
     * @throws UnknownConfigurationException if any of the properties are unknown to
     *                                      {@link TableConfig}.
     */
    Transaction build(Engine engine);
}
