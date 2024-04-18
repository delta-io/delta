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

import java.util.Set;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;

/**
 * Builder for creating a {@link Transaction} to mutate a Delta table.
 *
 * @since 3.2.0
 */
@Evolving
public interface TransactionBuilder {
    /**
     * Set the new schema of the table. This is used when creating the table for the first time.
     *
     * @param tableClient {@link TableClient} instance to use.
     * @param schema      The new schema of the table.
     * @return
     */
    TransactionBuilder withSchema(TableClient tableClient, StructType schema);

    /**
     * Set the partition columns of the table. Partition columns can only be set when creating the
     * table for the first time. Subsequent updates to the partition columns are not allowed.
     *
     * @param tableClient      {@link TableClient} instance to use.
     * @param partitionColumns The partition columns of the table. These should be a subset of the
     *                         columns in the schema.
     * @return
     */
    TransactionBuilder withPartitionColumns(
            TableClient tableClient,
            Set<String> partitionColumns);

    /**
     * Set the transaction identifier for idempotent writes.
     *
     * @param tableClient        {@link TableClient} instance to use.
     * @param applicationId      The application ID that is writing to the table.
     * @param transactionVersion The version of the transaction. This is used to ensure that the
     * @return
     */
    TransactionBuilder withTransactionId(
            TableClient tableClient,
            String applicationId,
            long transactionVersion);

    /**
     * Set the predicate of what qualifying files from and what version of the table are read in
     * order to generate the updates for this transaction.
     *
     * @param tableClient {@link TableClient} instance
     * @param readVersion What version of the table is read for generating the updates?
     * @param predicate   What set of files are read for generating the updates
     * @return
     */
    TransactionBuilder withReadSet(
            TableClient tableClient,
            long readVersion,
            Predicate predicate);

    /**
     * Build the transaction.
     *
     * @param tableClient {@link TableClient} instance to use.
     * @throws ConcurrentTransactionException if the table already has a committed transaction with
     *                                        the same given transaction identifier (using
     *                                        #withTransactionId) as this transaction.
     */
    Transaction build(TableClient tableClient);
}
