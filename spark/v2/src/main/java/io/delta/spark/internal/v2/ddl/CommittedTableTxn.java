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

/**
 * DTO representing a successfully committed Kernel transaction.
 *
 * <p>Holds the {@link TransactionCommitResult} (version, post-commit hooks) and the post-commit
 * {@link Snapshot} that reflects the newly created table state.
 */
public class CommittedTableTxn {
  private final TransactionCommitResult commitResult;
  private final Snapshot postCommitSnapshot;

  public CommittedTableTxn(TransactionCommitResult commitResult, Snapshot postCommitSnapshot) {
    this.commitResult = requireNonNull(commitResult, "commitResult is null");
    this.postCommitSnapshot = requireNonNull(postCommitSnapshot, "postCommitSnapshot is null");
  }

  public TransactionCommitResult getCommitResult() {
    return commitResult;
  }

  public Snapshot getPostCommitSnapshot() {
    return postCommitSnapshot;
  }

  public long getVersion() {
    return commitResult.getVersion();
  }
}
