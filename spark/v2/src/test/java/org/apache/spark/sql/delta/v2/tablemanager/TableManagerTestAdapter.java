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
package org.apache.spark.sql.delta.v2.tablemanager;

import io.delta.kernel.CommitRange;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.DeltaSingleCommit;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.storage.LogStore;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager.GetTableChangesParam;
import org.apache.spark.sql.delta.v2.kernel.KernelContext;
import scala.Option;

/** Preserves raw snapshot-manager fixtures while testing callers of the table-manager API. */
public final class TableManagerTestAdapter implements DeltaV2TableManager {
  private final DeltaV2SnapshotManager delegate;

  private TableManagerTestAdapter(DeltaV2SnapshotManager delegate) {
    this.delegate = delegate;
  }

  public static DeltaV2TableManager apply(DeltaV2SnapshotManager delegate) {
    return new TableManagerTestAdapter(delegate);
  }

  @Override
  public KernelContext kernelContext() {
    throw new UnsupportedOperationException("Test adapter does not own a KernelContext");
  }

  @Override
  public LogStore logStore() {
    throw new UnsupportedOperationException("Test adapter does not own a LogStore");
  }

  @Override
  public DeltaV2SnapshotManager snapshotManager(Option<CatalogTable> catalogTableOpt) {
    return delegate;
  }

  @Override
  public Snapshot loadLatestSnapshot(DeltaV2QueryContext queryContext) {
    return delegate.loadLatestSnapshot(queryContext);
  }

  @Override
  public Snapshot loadSnapshotAt(long version, DeltaV2QueryContext queryContext) {
    return delegate.loadSnapshotAt(version, queryContext);
  }

  @Override
  public DeltaHistoryManager.Commit getActiveCommitAtTime(
      long timestampMillis,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit,
      DeltaV2QueryContext queryContext) {
    return delegate.getActiveCommitAtTime(
        timestampMillis,
        canReturnLastCommit,
        mustBeRecreatable,
        canReturnEarliestCommit,
        queryContext);
  }

  @Override
  public void checkVersionExists(
      long version,
      boolean mustBeRecreatable,
      boolean allowOutOfRange,
      DeltaV2QueryContext queryContext) {
    delegate.checkVersionExists(version, mustBeRecreatable, allowOutOfRange, queryContext);
  }

  @Override
  public CommitRange getTableChanges(
      Engine kernelEngine, GetTableChangesParam params, DeltaV2QueryContext queryContext) {
    return delegate.getTableChanges(kernelEngine, params, queryContext);
  }

  @Override
  public ClosableIterator<DeltaSingleCommit> getSingleCommitIterator(
      long startVersion,
      Option<Object> endVersion,
      Option<CatalogTable> catalogTableOpt,
      boolean failOnDataLoss) {
    throw new UnsupportedOperationException("Test adapter does not read single commits");
  }

  @Override
  public void retire() {}
}
