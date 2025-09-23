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

package io.delta.kernel.internal.commit;

import static io.delta.kernel.internal.actions.SingleAction.*;
import static io.delta.kernel.internal.actions.SingleAction.createTxnSingleAction;
import static io.delta.kernel.internal.util.Utils.toCloseableIterator;

import io.delta.kernel.Meta;
import io.delta.kernel.commit.CommitContext;
import io.delta.kernel.commit.CommitMetadata;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.transaction.TransactionV2State;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CommitContextImpl implements CommitContext {

  ////////////////////////////
  // Static factory methods //
  ////////////////////////////

  /**
   * Creates a {@link CommitContext} to be used for the first commit attempt of a transaction only
   * (e.g. not a retry, which requires conflict detection and potential action reconciliation and
   * rebasing).
   *
   * @param finalizedDataActions the finalized data actions to be committed. That is, all updates to
   *     the data actions (e.g. row tracking updates) have been applied. Note that these are the
   *     data actions, not the metadata actions (like CommitInfo, Protocol, Metadata,
   *     SetTransaction, DomainMetadata, etc.).
   */
  public static CommitContextImpl forFirstCommitAttempt(
      Engine engine, TransactionV2State txnState, CloseableIterator<Row> finalizedDataActions) {
    return new CommitContextImpl(engine, txnState, finalizedDataActions);
  }

  // TODO: forRebasedCommit

  //////////////////////////////////
  // Member variables and methods //
  //////////////////////////////////

  private final TransactionV2State txnState;
  private final CloseableIterator<Row> finalizedDataActions;
  private final long commitAttemptTimestampMs;
  private final CommitInfo commitInfo;
  private final Metadata metadata;
  private AtomicBoolean iteratorConsumed;

  private CommitContextImpl(
      Engine engine, TransactionV2State txnState, CloseableIterator<Row> finalizedDataActions) {
    this.txnState = txnState;
    this.finalizedDataActions = finalizedDataActions;
    this.commitAttemptTimestampMs = txnState.clock.getTimeMillis();

    // TODO: update with ICT enablement info on conflict
    this.metadata = txnState.getEffectiveMetadataForFirstCommitAttempt();
    this.commitInfo = getCommitInfo();
    this.iteratorConsumed = new AtomicBoolean(false);
  }

  /////////////////
  // Public APIs //
  /////////////////

  @Override
  public CloseableIterator<Row> getFinalizedActions() {
    if (!iteratorConsumed.compareAndSet(false, true)) {
      throw new IllegalStateException("Finalized actions iterator has already been consumed.");
    }
    return getMetadataActions().combine(finalizedDataActions);
  }

  @Override
  public CommitMetadata getCommitMetadata() {
    return new CommitMetadata(
        getCommitAsVersion(),
        txnState.logPath,
        commitInfo,
        Collections.emptyList(), // TODO: support passing in domain metadata
        Collections::emptyMap, /* committerProperties */
        txnState.readTableOpt.map(x -> new Tuple2<>(x.getProtocol(), x.getMetadata())),
        txnState.updatedProtocolOpt,
        txnState.isMetadataUpdate() ? Optional.of(metadata) : Optional.empty());
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  private CloseableIterator<Row> getMetadataActions() {
    final List<Row> metadataActions = new ArrayList<>();

    metadataActions.add(createCommitInfoSingleAction(commitInfo.toRow()));

    if (txnState.isProtocolUpdate()) {
      metadataActions.add(createProtocolSingleAction(txnState.getEffectiveProtocol().toRow()));
    }

    if (txnState.isMetadataUpdate()) {
      metadataActions.add(createMetadataSingleAction(metadata.toRow()));
    }

    txnState.setTxnOpt.ifPresent(
        setTxn -> metadataActions.add(createTxnSingleAction(setTxn.toRow())));

    return toCloseableIterator(metadataActions.iterator());
  }

  private long getCommitAsVersion() {
    return txnState.readTableOpt.map(readTable -> readTable.getVersion() + 1).orElse(0L);
  }

  private CommitInfo getCommitInfo() {
    return new CommitInfo(
        Optional.of(commitAttemptTimestampMs), /* inCommitTimestampOpt */ // TODO: support ICT
        commitAttemptTimestampMs,
        "Kernel-" + Meta.KERNEL_VERSION + "/" + txnState.engineInfo, /* engineInfo */
        txnState.operation.getDescription(), /* description */
        getOperationParameters(), /* operationParameters */
        true, /* isBlindAppend */
        txnState.txnId, /* txnId */
        Collections.emptyMap() /* operationMetrics */);
  }

  private Map<String, String> getOperationParameters() {
    if (txnState.isCreateOrReplace) {
      List<String> partitionCols = VectorUtils.toJavaList(metadata.getPartitionColumns());
      String partitionBy =
          partitionCols.stream()
              .map(col -> "\"" + col + "\"")
              .collect(Collectors.joining(",", "[", "]"));
      return Collections.singletonMap("partitionBy", partitionBy);
    }
    return Collections.emptyMap();
  }
}
