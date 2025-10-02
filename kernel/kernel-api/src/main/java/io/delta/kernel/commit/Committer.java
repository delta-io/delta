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

/** Interface for commits and publishing operations for Delta tables. */
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
   * @param finalizedActions the iterator of finalized actions to be committed, taken from {@link
   *     CommitContext#getFinalizedActions()}. Callers must either
   *     <ul>
   *       <li>Pass the iterator directly from the {@link CommitContext} into this call site
   *       <li>First materialize the iterator contents (e.g., into a {@link java.util.List}) and
   *           then create a new iterator from the materialized data
   *     </ul>
   *
   * @param commitMetadata the {@link CommitMetadata} associated with this commit, which contains
   *     additional metadata required to commit the finalized actions to the table, such as the
   *     commit version, Delta log path, and more.
   * @return CommitResponse containing the resultant commit
   * @throws CommitFailedException if the commit operation fails
   */
  CommitResponse commit(
      Engine engine, CloseableIterator<Row> finalizedActions, CommitMetadata commitMetadata)
      throws CommitFailedException;

  /**
   * Publishes catalog commits to the Delta log. Applicable only to catalog-managed tables. For
   * filesystem-managed tables, this method is a no-op.
   *
   * <p>Publishing is the act of copying ratified catalog commits to the Delta log as published
   * Delta files (e.g., {@code _delta_log/00000000000000000001.json}).
   *
   * <p>The benefits of publishing include:
   *
   * <ul>
   *   <li>Reduces the number of commits the catalog needs to store internally and serve to readers
   *   <li>Enables table maintenance operations that must operate on published versions only, such
   *       as checkpointing and log compaction
   * </ul>
   *
   * <p>Requirements:
   *
   * <ul>
   *   <li>This method must ensure that all catalog commits are published to the Delta log up to and
   *       including the snapshot version specified in {@code publishMetadata}
   *   <li>Commits must be published in order: version V-1 must be published before version V
   * </ul>
   *
   * <p>Catalog-specific semantics: Each catalog implementation may specify its own rules and
   * semantics for publishing, including whether it expects to be notified immediately upon
   * publishing success, whether published deltas must appear with PUT-if-absent semantics in the
   * Delta log, and whether publishing happens client-side or server-side.
   *
   * @param engine the {@link Engine} instance used for publishing commits
   * @param publishMetadata the {@link PublishMetadata} containing the snapshot version up to which
   *     all catalog commits must be published, the log path, and list of catalog commits
   */
  default void publish(Engine engine, PublishMetadata publishMetadata) {
    if (!publishMetadata.getAscendingCatalogCommits().isEmpty()) {
      throw new UnsupportedOperationException("Publishing not supported by this committer");
    }
  }
}
