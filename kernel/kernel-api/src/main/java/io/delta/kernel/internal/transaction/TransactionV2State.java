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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.table.ResolvedTableInternal;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.transaction.TransactionV2;
import java.util.Optional;
import java.util.UUID;

/**
 * Data class to hold the immutable state of a {@link TransactionV2}.
 *
 * <p>These fields should never change from commit to commit, even after rebasing.
 */
public class TransactionV2State {

  // ===== Core transaction identity =====
  public final String txnId;
  public final boolean isCreateOrReplace;
  public final String engineInfo;
  public final Operation operation;

  // ===== File System paths =====
  public final String dataPath;
  public final String logPath;

  // ===== Table state =====
  /** The base table version from which this transaction is started. */
  public final Optional<ResolvedTableInternal> readTableOpt;

  /**
   * The updated protocol, if any, to use for this transaction, updated with all applicable input
   * from the transaction builder.
   *
   * <p>Non-empty if there is any change from the `readTableOpt`'s protocol, else empty.
   *
   * <p>As of this writing, the protocol never changes during a transaction, e.g. from commit
   * attempt to commit attempt.
   */
  public final Optional<Protocol> updatedProtocolOpt;

  /**
   * The updated metadata, if any, to use for the first commit attempt of this transaction, updated
   * with all the applicable input from the transaction builder.
   *
   * <p>Non-empty if there is any change from the `readTableOpt`'s metadata, else empty.
   *
   * <p>Note that the metadata might change from commit attempt to commit attempt. For example, when
   * ICT is enabled, each commit attempt needs to be updated to write the Delta table property
   * 'delta.inCommitTimestampEnablementVersion', which changes from commit attempt to commit
   * attempt.
   */
  public final Optional<Metadata> updatedMetadataForFirstCommitAttemptOpt;

  // ===== Update flags =====
  // TODO: final boolean shouldUpdateClusteringDomainMetadata

  // ===== Test infrastructure =====
  public final Clock clock;

  // ===== Table feature specific fields =====
  public final Optional<SetTransaction> setTxnOpt;

  // TODO public final Optional<List<Column>> clusteringColumnsOpt;

  public TransactionV2State(
      boolean isCreateOrReplace,
      String engineInfo,
      Operation operation,
      String dataPath,
      Optional<ResolvedTableInternal> readTableOpt,
      Optional<Protocol> updatedProtocolOpt,
      Optional<Metadata> updatedMetadataForFirstCommitAttemptOpt,
      Clock clock,
      Optional<SetTransaction> setTxnOpt) {
    requireNonNull(readTableOpt, "readTableOpt is null");
    requireNonNull(updatedProtocolOpt, "updatedProtocolOpt is null");
    requireNonNull(
        updatedMetadataForFirstCommitAttemptOpt, "updatedMetadataForFirstCommitAttemptOpt is null");
    checkArgument(
        updatedProtocolOpt.isPresent() || readTableOpt.isPresent(),
        "Either updatedProtocolOpt or readTableOpt must be present");
    checkArgument(
        updatedMetadataForFirstCommitAttemptOpt.isPresent() || readTableOpt.isPresent(),
        "Either updatedMetadataForFirstCommitAttemptOpt or readTableOpt must be present");

    this.txnId = UUID.randomUUID().toString();
    this.isCreateOrReplace = isCreateOrReplace;
    this.engineInfo = requireNonNull(engineInfo, "engineInfo is null");
    this.operation = requireNonNull(operation, "operation is null");

    this.dataPath = requireNonNull(dataPath, "dataPath is null");
    this.logPath = new Path(dataPath, "_delta_log").toString();

    this.readTableOpt = readTableOpt;
    this.updatedProtocolOpt = updatedProtocolOpt;
    this.updatedMetadataForFirstCommitAttemptOpt = updatedMetadataForFirstCommitAttemptOpt;

    this.clock = requireNonNull(clock, "clock is null");

    this.setTxnOpt = requireNonNull(setTxnOpt, "setTxnOpt is null");
  }

  public Protocol getEffectiveProtocol() {
    return updatedProtocolOpt.orElseGet(() -> readTableOpt.get().getProtocol());
  }

  public Metadata getEffectiveMetadataForFirstCommitAttempt() {
    return updatedMetadataForFirstCommitAttemptOpt.orElseGet(
        () -> readTableOpt.get().getMetadata());
  }

  public boolean isProtocolUpdate() {
    return updatedProtocolOpt.isPresent();
  }

  public boolean isMetadataUpdate() {
    return updatedMetadataForFirstCommitAttemptOpt.isPresent();
  }

  public boolean isReplace() {
    return isCreateOrReplace && readTableOpt.isPresent() && readTableOpt.get().getVersion() >= 0;
  }
}
