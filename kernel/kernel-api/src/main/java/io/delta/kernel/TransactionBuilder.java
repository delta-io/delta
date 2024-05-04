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

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;

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
     * Build the transaction. Also validates the given info to ensure that a valida transaction
     * can be created.
     *
     * @param engine {@link Engine} instance to use.
     */
    Transaction build(Engine engine);
}
