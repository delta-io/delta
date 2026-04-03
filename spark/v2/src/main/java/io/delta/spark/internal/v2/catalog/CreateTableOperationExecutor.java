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
package io.delta.spark.internal.v2.catalog;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.utils.CloseableIterable;

/** Commits a prepared CREATE TABLE operation as Delta version 0. */
public final class CreateTableOperationExecutor {

  public CommittedCreateTableOperation commitPreparedCreate(
      PreparedCreateTableOperation operation,
      String engineInfo,
      CloseableIterable<Row> dataActions) {
    Transaction txn =
        operation
            .getSnapshotManager()
            .buildCreateTableTransaction(
                operation.getKernelSchema(),
                operation.getTableProperties(),
                operation.getDataLayoutSpec(),
                engineInfo);
    TransactionCommitResult commitResult = txn.commit(operation.getEngine(), dataActions);
    Snapshot postCommitSnapshot =
        commitResult
            .getPostCommitSnapshot()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Kernel CREATE TABLE did not return a post-commit snapshot"));
    return new CommittedCreateTableOperation(
        operation.getEngine(), (SnapshotImpl) postCommitSnapshot);
  }
}
