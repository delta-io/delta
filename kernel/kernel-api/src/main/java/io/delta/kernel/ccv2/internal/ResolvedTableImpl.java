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

package io.delta.kernel.ccv2.internal;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.ccv2.ResolvedMetadata;
import io.delta.kernel.ccv2.ResolvedTable;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.metrics.SnapshotQueryContext;
import io.delta.kernel.internal.metrics.SnapshotReportImpl;
import io.delta.kernel.internal.snapshot.SnapshotManager;
import io.delta.kernel.metrics.SnapshotReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvedTableImpl implements ResolvedTable {

  private static final Logger logger = LoggerFactory.getLogger(ResolvedTableImpl.class);

  private final ResolvedMetadata resolvedMetadata;
  private final Snapshot snapshot;

  public ResolvedTableImpl(Engine engine, ResolvedMetadata rm) {
    validateResolvedMetadata(rm);

    this.resolvedMetadata = rm;

    final Path tablePath = new Path(resolvedMetadata.getPath());

    final SnapshotQueryContext sqc =
        SnapshotQueryContext.forVersionSnapshot(rm.getPath(), rm.getVersion());

    try {
      this.snapshot =
          new SnapshotManager(tablePath)
              .getSnapshotUsingResolvedMetadata(engine, resolvedMetadata, sqc);
    } catch (Exception e) {
      recordSnapshotErrorReport(engine, sqc, e);
      throw e;
    }
  }

  /////////////////
  // Public APIs //
  /////////////////

  @Override
  public Snapshot getSnapshot() {
    return snapshot;
  }

  @Override
  public TransactionBuilder createTransactionBuilder(String engineInfo, Operation operation) {
    return null;
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  private static void validateResolvedMetadata(ResolvedMetadata rm) {
    requireNonNull(rm, "resolvedMetadata is null");
    requireNonNull(rm.getPath(), "ResolvedMetadata.getPath() is null");
    requireNonNull(rm.getLogSegment(), "ResolvedMetadata.getLogSegment() is null");
    requireNonNull(rm.getProtocol(), "ResolvedMetadata.getProtocol() is null");
    requireNonNull(rm.getMetadata(), "ResolvedMetadata.getMetadata() is null");
    requireNonNull(rm.getSchemaString(), "ResolvedMetadata.getSchemaString() is null");

    checkArgument(
        rm.getProtocol().isPresent() == rm.getMetadata().isPresent(),
        "Protocol and Metadata must be present or absent together");

    rm.getLogSegment()
        .ifPresent(
            logSegment -> {
              checkArgument(
                  logSegment.getVersion() == rm.getVersion(),
                  "ResolvedMetadata.getVersion() does not match the version of the LogSegment");
            });
  }

  // TODO: this is duplicated and copied from TableImpl.java
  private void recordSnapshotErrorReport(
      Engine engine, SnapshotQueryContext snapshotContext, Exception e) {
    SnapshotReport snapshotReport = SnapshotReportImpl.forError(snapshotContext, e);
    engine.getMetricsReporters().forEach(reporter -> reporter.report(snapshotReport));
  }
}
