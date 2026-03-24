/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.ddl;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;

/**
 * Generic Kernel transaction committer for the DSv2 + Kernel + CCv2 path.
 *
 * <p>This is the common commit boundary between transaction building and catalog publication. It
 * takes a prepared transaction and data actions, commits via Kernel, and returns the committed
 * result.
 */
public final class TableCommitter {

  private TableCommitter() {}

  /**
   * Commits a prepared transaction with the given data actions.
   *
   * @param prepared the prepared transaction to commit
   * @param dataActions data action rows to include (empty for DDL-only operations like CREATE
   *     TABLE)
   * @return the committed transaction result with post-commit snapshot
   */
  public static CommittedTableTxn commit(
      PreparedTableTxn prepared, CloseableIterable<Row> dataActions) {
    requireNonNull(prepared, "prepared is null");
    requireNonNull(dataActions, "dataActions is null");

    Engine engine = prepared.getEngine();
    TransactionCommitResult commitResult = prepared.getTransaction().commit(engine, dataActions);

    Snapshot postCommitSnapshot =
        commitResult
            .getPostCommitSnapshot()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Post-commit snapshot not available after commit at version "
                            + commitResult.getVersion()));

    return new CommittedTableTxn(commitResult, postCommitSnapshot);
  }

  /**
   * Commits a prepared transaction with no data actions.
   *
   * <p>Convenience overload for DDL operations (CREATE TABLE, ALTER TABLE, etc.) that only write
   * protocol/metadata actions and no data files.
   */
  public static CommittedTableTxn commit(PreparedTableTxn prepared) {
    return commit(prepared, CloseableIterable.emptyIterable());
  }
}
