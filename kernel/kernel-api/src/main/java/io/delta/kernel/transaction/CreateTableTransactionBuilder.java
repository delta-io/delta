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
import io.delta.kernel.commit.Committer;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import java.util.Map;

/**
 * Builder for creating a {@link Transaction} to create a new Delta table.
 *
 * @since 3.4.0
 */
@Evolving
public interface CreateTableTransactionBuilder {

  /**
   * Set table properties for the new Delta table.
   *
   * <p>Note, user-properties (those without a '.delta' prefix) are case-sensitive. Delta-properties
   * are case-insensitive and are normalized to their expected case before writing to the log.
   *
   * @param properties A map of table property names to their values.
   */
  CreateTableTransactionBuilder withTableProperties(Map<String, String> properties);

  /**
   * Set the data layout specification for the new Delta table.
   *
   * <p>The data layout specification determines how data files are organized within the table, such
   * as partitioning and clustering strategies.
   *
   * <p>The default, if not specified in the builder, is unpartitioned.
   *
   * @param spec The data layout specification.
   * @see DataLayoutSpec
   */
  CreateTableTransactionBuilder withDataLayoutSpec(DataLayoutSpec spec);

  /**
   * Set the maximum number of retries to retry the commit in the case of a retryable error.
   *
   * @param maxRetries The maximum number of retries. Must be at least 0. Default is 200.
   */
  CreateTableTransactionBuilder withMaxRetries(int maxRetries);

  /**
   * Provides a custom committer to use at transaction commit time.
   *
   * <p>Catalog implementations that wish to support the catalogManaged Delta table feature should
   * provide to engines their own catalog-specific Committer implementation which may, for example,
   * send a commit RPC to the catalog service to finalize the commit.
   *
   * <p>If no committer is provided, a default committer will be created that only supports writing
   * into filesystem-managed Delta tables.
   *
   * @param committer the committer to use
   * @return a new builder instance with the provided committer
   * @see Committer
   */
  CreateTableTransactionBuilder withCommitter(Committer committer);

  /**
   * Build the transaction for creating the Delta table.
   *
   * <p>This validates all the configuration and creates a {@link Transaction} that can be used to
   * create the new Delta table. The transaction must be committed using {@link
   * Transaction#commit(Engine, CloseableIterable)} to actually create the table.
   *
   * @param engine The {@link Engine} instance to use.
   * @return A configured {@link Transaction} for creating the table.
   */
  Transaction build(Engine engine);
}
