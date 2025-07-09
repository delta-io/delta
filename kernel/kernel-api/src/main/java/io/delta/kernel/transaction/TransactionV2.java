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
import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.commit.CommitContext;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Map;

/** Represents a transaction to mutate a Delta table. */
@Experimental
public interface TransactionV2 {

  // TODO: Add other APIs as needed, e.g. getting the schema, partitionCols, etc.

  /**
   * Get the internal state of the transaction as an opaque {@link Row}.
   *
   * <p>The state helps Kernel do the transformations to logical data according to the Delta
   * protocol and table features enabled on the table. The engine should use this at the data writer
   * task to transform logical data into physical data that goes in data files using {@link
   * Transaction#transformLogicalData(Engine, Row, CloseableIterator, Map)}.
   *
   * @return the internal state {@link Row}
   */
  Row getTransactionState();

  /**
   * Get the {@link CommitContext} that can be used only for the very first commit to the table.
   * This context contains (a) the finalized actions, including metadata and data actions, and (b)
   * additional {@link CommitMetadata}.
   *
   * @param engine the {@link Engine} instance to use to help generate the {@link CommitContext}
   * @param dataActions Iterator of data actions to commit
   * @return the {@link CommitContext} instance to commit
   */
  CommitContext getInitialCommitContext(Engine engine, CloseableIterator<Row> dataActions);

  // TODO: detectConflictsAndRebase API
}
