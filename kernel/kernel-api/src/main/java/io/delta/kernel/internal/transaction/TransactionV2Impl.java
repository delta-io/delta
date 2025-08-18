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

package io.delta.kernel.internal.transaction;

import static io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;

import io.delta.kernel.commit.CommitContext;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.commit.CommitContextImpl;
import io.delta.kernel.internal.data.TransactionStateRow;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.transaction.TransactionV2;
import io.delta.kernel.utils.CloseableIterator;

/** An implementation of {@link TransactionV2}. */
public class TransactionV2Impl implements TransactionV2 {

  private final TransactionV2State txnState;

  public TransactionV2Impl(TransactionV2State txnState) {
    this.txnState = txnState;
  }

  @Override
  public Row getTransactionState() {
    // TODO: We probably shouldn't have to pass in maxRetries?
    return TransactionStateRow.of(
        txnState.getEffectiveMetadataForFirstCommitAttempt(),
        txnState.dataPath,
        0 /* maxRetries */);
  }

  @Override
  public CommitContext getCommitContextForFirstCommitAttempt(
      Engine engine, CloseableIterator<Row> dataActions) {
    if (TableFeatures.isRowTrackingSupported(txnState.getEffectiveProtocol())) {
      throw new UnsupportedOperationException("Row Tracking not yet supported in TransactionV2");
    }
    if (IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(
        txnState.getEffectiveMetadataForFirstCommitAttempt())) {
      throw new UnsupportedOperationException(
          "In Commit Timestamps not yet supported in TransactionV2");
    }

    // TODO: update data actions with rowId stuff

    return CommitContextImpl.forFirstCommitAttempt(engine, txnState, dataActions);
  }
}
