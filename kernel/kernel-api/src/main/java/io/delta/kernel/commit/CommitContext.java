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

package io.delta.kernel.commit;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.data.Row;
import io.delta.kernel.transaction.TransactionV2;
import io.delta.kernel.utils.CloseableIterator;

/**
 * A container class for all the information that an engine needs to commit to a table.
 *
 * <p>This interface encapsulates both the actions to be committed and the associated metadata
 * required for the commit process. Engines use this context to provide inputs to the {@link
 * Committer#commit} method.
 *
 * @see TransactionV2#getInitialCommitContext
 */
@Experimental
public interface CommitContext {

  /**
   * Returns the finalized actions that the engine must forward to the {@link Committer} to commit
   * to the table.
   *
   * <p>These actions represent the changes this transaction will make, including data file
   * additions, metadata updates, and protocol changes.
   *
   * <p><b>Important limitations:</b>
   *
   * <ul>
   *   <li>This iterator can only be accessed and consumed once
   *   <li>For retry support, engines must materialize these actions before each commit attempt,
   *       allowing them to be replayed and then updated during conflict resolution with the latest
   *       table state
   * </ul>
   */
  CloseableIterator<Row> getFinalizedActions();

  /**
   * Returns the {@link CommitMetadata} associated with this commit, which contains additional
   * metadata required to commit the finalized actions to the table, such as the commit version,
   * Delta log path, and more.
   */
  CommitMetadata getCommitMetadata();
}
