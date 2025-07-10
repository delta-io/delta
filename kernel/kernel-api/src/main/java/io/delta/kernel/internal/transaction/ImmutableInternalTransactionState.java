/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import io.delta.kernel.Operation;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Data class to hold the common immutable and internal state that the implementations of both
 * {@link io.delta.kernel.Transaction} and {@link io.delta.kernel.transaction.TransactionV2}
 * require.
 *
 * <p>These fields should never change from commit to commit, even after rebasing.
 */
public class ImmutableInternalTransactionState {
  // ===== Core transaction identity =====
  public final String txnId;
  public final boolean isCreateOrReplace;
  public final String engineInfo;
  public final Operation operation;

  // ===== File System paths =====
  public final Path dataPath;
  public final Path logPath;

  // ===== Table state =====
  public final SnapshotImpl readSnapshot;
  public final Protocol protocol;
  public final Metadata initialMetadata;

  // ===== Update flags =====
  public final boolean shouldUpdateProtocol;
  public final boolean shouldUpdateMetadata;
  public final boolean shouldUpdateClusteringDomainMetadata;

  // ===== Test infrastructure =====
  public final Clock clock;

  // ===== Optional fields =====
  public final Optional<SetTransaction> setTxnOpt;
  public final Optional<List<Column>> clusteringColumnsOpt;

  public ImmutableInternalTransactionState(
      boolean isCreateOrReplace,
      String engineInfo,
      Operation operation,
      SnapshotImpl readSnapshot,
      Protocol protocol,
      Metadata initialMetadata,
      boolean shouldUpdateProtocol,
      boolean shouldUpdateMetadata,
      boolean shouldUpdateClusteringDomainMetadata,
      Clock clock,
      Optional<SetTransaction> setTxnOpt,
      Optional<List<Column>> clusteringColumnsOpt) {
    this.txnId = UUID.randomUUID().toString();
    this.isCreateOrReplace = isCreateOrReplace;
    this.engineInfo = engineInfo;
    this.operation = operation;

    this.dataPath = readSnapshot.getDataPath();
    this.logPath = readSnapshot.getLogPath();

    this.readSnapshot = readSnapshot;
    this.protocol = protocol;
    this.initialMetadata = initialMetadata;

    this.shouldUpdateProtocol = shouldUpdateProtocol;
    this.shouldUpdateMetadata = shouldUpdateMetadata;
    this.clusteringColumnsOpt = clusteringColumnsOpt;

    this.clock = clock;

    this.setTxnOpt = setTxnOpt;
    this.shouldUpdateClusteringDomainMetadata = shouldUpdateClusteringDomainMetadata;
  }
}
