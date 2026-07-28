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
import java.util.Map;

/**
 * Builds a {@link Transaction} to replace an existing Delta table.
 *
 * <p>Replace table creates a new table definition (schema, properties, layout) and atomically
 * replaces the existing table.
 *
 * @since 3.4.0
 */
@Evolving
public interface ReplaceTableTransactionBuilder {

  /**
   * Set table properties for the new table definition.
   *
   * @param properties A map of table property names to their values.
   */
  ReplaceTableTransactionBuilder withTableProperties(Map<String, String> properties);

  /**
   * Set the data layout specification for the new table definition.
   *
   * @param spec The data layout specification.
   * @see DataLayoutSpec
   */
  ReplaceTableTransactionBuilder withDataLayoutSpec(DataLayoutSpec spec);

  /**
   * Set the maximum number of retries to retry the commit in the case of a retryable error.
   *
   * @param maxRetries The maximum number of retries. Must be at least 0. Default is 200.
   */
  ReplaceTableTransactionBuilder withMaxRetries(int maxRetries);

  /**
   * Build the transaction for replacing the Delta table.
   *
   * <p>The transaction must be committed using {@link Transaction#commit(Engine,
   * io.delta.kernel.utils.CloseableIterable)} to apply the changes.
   *
   * @param engine The {@link Engine} instance to use for the transaction. Cannot be null.
   * @return A configured {@link Transaction} for replacing the table.
   */
  Transaction build(Engine engine);
}
