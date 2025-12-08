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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Interface for committing changes to Delta tables, supporting both filesystem-managed and
 * catalog-managed tables.
 */
@Experimental
public interface Committer {

  /**
   * Commits the given {@code finalizedActions} and {@code commitMetadata} to the table.
   *
   * <p>Filesystem-managed tables: Implementations must write the {@code finalizedActions} into a
   * new Delta JSON file at version {@link CommitMetadata#getVersion()} using atomic file operations
   * (PUT-if-absent semantics).
   *
   * <p>Catalog-managed tables: Implementations must follow the commit rules and requirements as
   * dictated by the managing catalog to ensure commit atomicity and consistency. This may involve:
   *
   * <ol>
   *   <li>Writing the finalized actions into a staged commit file
   *   <li>Calling catalog commit APIs with the staged commit location (or inline content) and
   *       additional metadata (such as the commit Protocol and Metadata)
   *   <li>Publishing ratified catalog commits into the Delta log
   * </ol>
   *
   * @param engine the {@link Engine} instance used for committing changes
   * @param finalizedActions the iterator of finalized actions to be committed
   * @param commitMetadata the {@link CommitMetadata} associated with this commit, which contains
   *     additional metadata required to commit the finalized actions to the table, such as the
   *     commit version, Delta log path, and more.
   * @return CommitResponse containing the resultant commit
   * @throws CommitFailedException if the commit operation fails
   */
  CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException;
}
