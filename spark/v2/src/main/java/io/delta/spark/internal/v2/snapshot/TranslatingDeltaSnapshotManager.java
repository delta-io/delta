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
package io.delta.spark.internal.v2.snapshot;

import static io.delta.spark.internal.v2.utils.KernelUnsupportedFeatureTranslator.translatingUnsupportedFeature;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.spark.internal.v2.exception.VersionNotFoundException;
import java.util.Optional;

/**
 * A {@link DeltaSnapshotManager} decorator that translates Kernel's {@link
 * io.delta.kernel.exceptions.UnsupportedTableFeatureException}, thrown while eagerly building a
 * snapshot, into Delta's {@link org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException}
 * ({@code DELTA_UNSUPPORTED_FEATURES_FOR_READ}).
 */
final class TranslatingDeltaSnapshotManager implements DeltaSnapshotManager {

  private final DeltaSnapshotManager delegate;

  TranslatingDeltaSnapshotManager(DeltaSnapshotManager delegate) {
    this.delegate = requireNonNull(delegate, "delegate is null");
  }

  @Override
  public Snapshot loadLatestSnapshot() {
    return translatingUnsupportedFeature(delegate::loadLatestSnapshot);
  }

  @Override
  public Snapshot loadSnapshotAt(long version) {
    return translatingUnsupportedFeature(() -> delegate.loadSnapshotAt(version));
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    // Eagerly loads the latest snapshot internally, so an unsupported reader feature surfaces here.
    return translatingUnsupportedFeature(
        () ->
            delegate.getActiveCommitAtTime(
                timestampMillis, canReturnLastCommit, mustBeRecreatable, canReturnEarliestCommit));
  }

  @Override
  public void checkVersionExists(long version, boolean mustBeRecreatable, boolean allowOutOfRange)
      throws VersionNotFoundException {
    // Eagerly loads the latest snapshot internally, so an unsupported reader feature surfaces here.
    translatingUnsupportedFeature(
        () -> {
          delegate.checkVersionExists(version, mustBeRecreatable, allowOutOfRange);
          return null;
        });
  }

  @Override
  public CommitRange getTableChanges(Engine engine, long startVersion, Optional<Long> endVersion) {
    // Deliberately NOT wrapped: this returns a lazy CommitRange whose build() only validates
    // boundary arguments, not the protocol. An unsupported reader feature is not thrown here, it
    // surfaces later when the range's actions are iterated.
    return delegate.getTableChanges(engine, startVersion, endVersion);
  }
}
