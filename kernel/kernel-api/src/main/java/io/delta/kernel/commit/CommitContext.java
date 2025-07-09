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
import io.delta.kernel.utils.CloseableIterator;

/** A container class for all the information than an engine needs to commit to a table. */
@Experimental
public interface CommitContext {

  /**
   * The finalized actions that the engine must forward to the {@link Committer} to commit to the
   * table.
   *
   * <p>If the engine wishes to support commit retries, the engine must materialize this actions
   * iterator so that it can be replayed and updated in accordance with the latest table state.
   */
  CloseableIterator<Row> getFinalizedActions();

  /**
   * Get the {@link CommitMetadata} associated with this commit, which contains additional metadata
   * required to commit the finalized actions to the table, such as the commit version.
   */
  CommitMetadata getCommitMetadata();
}
