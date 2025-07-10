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
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class ImmutableTransactionState {
  public final String txnId;
  public final boolean isCreateOrReplace;
  public final String engineInfo;
  public final Operation operation;
  public final Path dataPath;
  public final Path logPath;
  public final Protocol protocol;
  public final SnapshotImpl readSnapshot;
  public final boolean shouldUpdateProtocol;
  public final boolean shouldUpdateClusteringDomainMetadata;
  public final Clock clock;
  public final Optional<SetTransaction> setTxnOpt;
  public final Optional<List<Column>> clusteringColumnsOpt;

  public ImmutableTransactionState(
      boolean isCreateOrReplace,
      String engineInfo,
      Operation operation,
      Path dataPath,
      Path logPath,
      Protocol protocol,
      SnapshotImpl readSnapshot,
      boolean shouldUpdateProtocol,
      boolean shouldUpdateClusteringDomainMetadata,
      Clock clock,
      Optional<SetTransaction> setTxnOpt,
      Optional<List<Column>> clusteringColumnsOpt) {
    this.txnId = UUID.randomUUID().toString();
    this.isCreateOrReplace = isCreateOrReplace;
    this.engineInfo = engineInfo;
    this.operation = operation;
    this.dataPath = dataPath;
    this.logPath = logPath;
    this.protocol = protocol;
    this.readSnapshot = readSnapshot;
    this.setTxnOpt = setTxnOpt;
    this.clusteringColumnsOpt = clusteringColumnsOpt;
    this.clock = clock;
    this.shouldUpdateProtocol = shouldUpdateProtocol;
    this.shouldUpdateClusteringDomainMetadata = shouldUpdateClusteringDomainMetadata;
  }
}
