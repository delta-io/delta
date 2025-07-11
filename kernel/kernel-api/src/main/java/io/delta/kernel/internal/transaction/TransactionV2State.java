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

import io.delta.kernel.Operation;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.actions.SetTransaction;
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
  public final Optional<TransactionDataSource> readTableOpt;
  public final Protocol protocol;
  public final Metadata initialMetadata;

  // ===== Update flags =====
  public final boolean isProtocolUpdate;
  public final boolean isMetadataUpdate;
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
      String logPath,
      Optional<TransactionDataSource> readTableOpt,
      Protocol protocol,
      Metadata initialMetadata,
      boolean isProtocolUpdate,
      boolean isMetadataUpdate,
      Clock clock,
      Optional<SetTransaction> setTxnOpt) {
    this.txnId = UUID.randomUUID().toString();
    this.isCreateOrReplace = isCreateOrReplace;
    this.engineInfo = engineInfo;
    this.operation = operation;

    // TODO: Would be great to add this to TransactionDataSource, but ResolvedTable uses String
    //       and Snapshot uses Path
    this.dataPath = dataPath;
    this.logPath = logPath;

    this.readTableOpt = readTableOpt;
    this.protocol = protocol;
    this.initialMetadata = initialMetadata;

    this.isProtocolUpdate = isProtocolUpdate;
    this.isMetadataUpdate = isMetadataUpdate;

    this.clock = clock;

    this.setTxnOpt = setTxnOpt;
  }
}
