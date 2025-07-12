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

/**
 * Represents a transaction to mutate a Delta table.
 *
 * <p>This interface will eventually replace the {@link Transaction} interface, and differs from its
 * predecessor in that {@link TransactionV2} no longer directly controls how a commit is performed.
 * It exposes the {@link CommitContext} (actions to commit, additional commit metadata) that Engines
 * may use to have better control over the commit process. This has a several benefits:
 *
 * <ul>
 *   <li>Engines can use the {@link CommitContext} to provide the necessary inputs to the {@link
 *       io.delta.kernel.commit.Committer#commit} method, which is then responsible for the commit.
 *       This is particularly important for catalog-managed tables, where the {@link
 *       io.delta.kernel.commit.Committer} may perform catalog-specific operations during the
 *       commit. Some examples of this include:
 *       <ul>
 *         <li>writing actions into staged commit files instead of directly into the `_delta_log`
 *         <li>sending commits inline to the catalog
 *         <li>publishing previously-ratified commits
 *         <li>sending additional commit metadata to the catalog, such as the latest schema, latest
 *             table properties, commit timestamp, and more
 *       </ul>
 *   <li>Engines can now directly control if the {@code finalizedActions} to commit are materialized
 *       or kept as a one-time-only iterator. Note that these actions must be materialized in order
 *       to support retries.
 *   <li>Engines can now directly control retry logic, e.g. to perform exponential backoff.
 * </ul>
 */
@Experimental
public interface TransactionV2 {

  // TODO: Add other APIs as needed, e.g. getting the schema, partitionCols, etc.

  /**
   * Get the internal state of the transaction as an opaque {@link Row}.
   *
   * <p>This state helps Kernel do the transformations to logical data according to the Delta
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
