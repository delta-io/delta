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
package io.delta.spark.internal.v2.write;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ConcurrentTransactionException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import java.util.function.Function;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.delta.DeltaConfigs;
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingWrite for DSv2 streaming <b>Append</b>. Spark's {@code V2Writes} rebuilds this per
 * micro-batch; {@link DeltaV2Write} supplies the executor write state ({@link
 * DeltaV2DataWriterFactory}), built from a throwaway transaction whose operation-independent
 * context is serialized into the factory. This class adds only the driver-side commit.
 *
 * <p>{@link #commit} builds its transaction from a freshly reloaded snapshot (so it commits at
 * {@code latest+1}), mirroring V1's per-batch {@code deltaLog.startTransaction()}.
 *
 * <p><b>Idempotency:</b> {@code withTransactionId(queryId, epochId)} records a {@code
 * SetTransaction}; {@link #commit} pre-checks the committed epoch for {@code queryId} and skips a
 * replay, as V1 ({@code txn.txnVersion}) does. A concurrent same-epoch commit that races the
 * pre-check is still caught as {@link ConcurrentTransactionException} and skipped.
 *
 * <p><b>Layout guard:</b> {@link #commit} fails the query if the reloaded snapshot's schema or
 * protocol has diverged from the write state's, since Kernel does not re-validate the
 * executor-written files at commit. The variant shredding property is checked alongside them: it
 * decides the file layout but changes neither schema nor protocol, so the other two checks do not
 * see it. TODO(#7140): rebuild the write state against the new schema/protocol so a compatible
 * change (e.g. an added nullable column) is tolerated instead of forcing a query restart.
 */
class DeltaV2StreamingWrite implements StreamingWrite {

  private static final Logger logger = LoggerFactory.getLogger(DeltaV2StreamingWrite.class);

  private final Engine engine;
  private final DeltaV2SnapshotManager snapshotManager;
  private final String queryId;
  private final DeltaV2DataWriterFactory dataWriterFactory;
  // The write state's schema/protocol baseline; the per-epoch guard fails if the table diverges.
  private final StructType writeSchema;
  private final Protocol writeProtocol;
  // Same idea for the shredding layout: the executor-side Parquet writer is built once from this
  // value, so an epoch committed after the property changed would carry the stale layout.
  private final boolean writeVariantShreddingEnabled;
  // Whether the property could actually change this write's layout (version supports shredding,
  // kill switch on, schema has a variant column). When false, a property change cannot affect the
  // files, so the guard below must not act on it. Frozen with the write state.
  private final boolean variantLayoutFollowsProperty;

  /**
   * @param engine Kernel engine (driver-only)
   * @param initialSnapshot the batch's planned snapshot; write-state source and guard baseline
   * @param snapshotManager reloads the latest snapshot per epoch (see {@link #commit})
   * @param queryId streaming query id; the transaction application id for cross-restart idempotency
   * @param variantShreddingEnabled the table's shredding property as the write state was built with
   *     it; the per-epoch guard baseline
   * @param variantLayoutFollowsProperty whether that property can actually change this write's
   *     layout; when false the guard ignores a change to it
   * @param dataWriterFactoryBuilder builds the executor write state; supplied by {@link
   *     DeltaV2Write} to share construction with the batch path
   */
  DeltaV2StreamingWrite(
      Engine engine,
      Snapshot initialSnapshot,
      DeltaV2SnapshotManager snapshotManager,
      String queryId,
      boolean variantShreddingEnabled,
      boolean variantLayoutFollowsProperty,
      Function<Transaction, DeltaV2DataWriterFactory> dataWriterFactoryBuilder) {
    this.engine = requireNonNull(engine, "engine is null");
    requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    this.queryId = requireNonNull(queryId, "queryId is null");
    requireNonNull(dataWriterFactoryBuilder, "dataWriterFactoryBuilder is null");
    this.writeSchema = initialSnapshot.getSchema();
    this.writeProtocol = ((SnapshotImpl) initialSnapshot).getProtocol();
    this.writeVariantShreddingEnabled = variantShreddingEnabled;
    this.variantLayoutFollowsProperty = variantLayoutFollowsProperty;
    // We only need this transaction's serialized write context for the factory, not the commit
    // (commit() builds its own per epoch).
    Transaction stateTxn =
        initialSnapshot
            .buildUpdateTableTransaction(DeltaV2Write.getEngineInfo(), Operation.STREAMING_UPDATE)
            .build(engine);
    this.dataWriterFactory = dataWriterFactoryBuilder.apply(stateTxn);
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return dataWriterFactory;
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    // TODO: Expose streaming transaction construction and latest transaction-version lookup
    // through the snapshot facade so this path does not depend on SnapshotImpl.
    // Kernel-only: needs SnapshotImpl.buildUpdateTableTransaction
    // (TransactionBuilder) for the streaming commit, and
    // getLatestTransactionVersion for the epoch-skip check.
    // One reload, so the skip check, guards, and the transaction below all judge the same snapshot.
    SnapshotImpl latestSnapshot =
        DeltaV2Snapshot$.MODULE$.getKernelSnapshot(snapshotManager.loadLatestSnapshot());

    // Skip an already-committed epoch before any guard runs. StreamingWrite.commit may be called
    // more than once for one epoch and must be idempotent, so a repeated commit of a committed
    // epoch is an unconditional no-op -- its data already landed under the layout in force then,
    // and a table change since must not turn that success into a failure. Its executor-written
    // files for this repeat are orphaned (VACUUM'd). The next uncommitted epoch still hits the
    // guards.
    long committedEpoch =
        ((SnapshotImpl) latestSnapshot).getLatestTransactionVersion(engine, queryId).orElse(-1L);
    if (committedEpoch >= epochId) {
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
      return;
    }

    // TODO(#7140): no implicit type cast and mergeSchema. Fail loudly on a concurrent
    // schema/protocol change.
    assertSchemaAndProtocolUnchanged(latestSnapshot);
    assertVariantShreddingUnchanged(
        DeltaV2WriteBuilder.isVariantShreddingEnabled(
            latestSnapshot.getMetadata().getConfiguration()));

    // TODO(#7140): no self-scan guard. A stream reading and writing the same table commits
    //  as a blind append, skipping the conflict check V1 gets via readWholeTable().

    try {
      Transaction txn =
          latestSnapshot
              .buildUpdateTableTransaction(DeltaV2Write.getEngineInfo(), Operation.STREAMING_UPDATE)
              .withTransactionId(queryId, epochId)
              .build(engine);
      CloseableIterable<Row> dataActions = DeltaV2WriterCommitMessage.toDataActions(messages);
      long version = txn.commit(engine, dataActions).getVersion();
      logger.info(
          "DSv2 streaming epoch {} for query {} committed at version {}",
          epochId,
          queryId,
          version);
    } catch (ConcurrentTransactionException e) {
      // Backstop for a concurrent writer racing the same epoch between the pre-check and commit.
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
    }
  }

  /** Fails the epoch if the fresh snapshot's schema/protocol diverged from the write's baseline. */
  private void assertSchemaAndProtocolUnchanged(Snapshot latestSnapshot) {
    if (!writeSchema.equals(latestSnapshot.getSchema())) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table schema changed after the stream started. Restart the "
              + "query to pick up the new schema.");
    }
    if (!writeProtocol.equals(((SnapshotImpl) latestSnapshot).getProtocol())) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table protocol changed after the stream started. Restart "
              + "the query to pick up the new protocol.");
    }
  }

  /**
   * Fails the epoch if the table's variant shredding property diverged from the write state's.
   *
   * <p>Neither of the checks above catches this. Turning shredding off -- by unsetting the property
   * or through {@code REORG ... APPLY (UNSHRED VARIANT)} -- leaves the schema untouched and leaves
   * the {@code variantShredding} feature in the protocol, so the epoch would otherwise commit files
   * in a layout the table no longer asks for, on top of an explicit opt-out.
   *
   * <p>Only acts when {@link #variantLayoutFollowsProperty} holds: where the property cannot change
   * the file layout (no variant column, kill switch off, or a Spark version without shredding) a
   * change to it is irrelevant and must not fail the epoch.
   */
  private void assertVariantShreddingUnchanged(boolean latestVariantShreddingEnabled) {
    if (variantLayoutFollowsProperty
        && latestVariantShreddingEnabled != writeVariantShreddingEnabled) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table property "
              + DeltaConfigs.ENABLE_VARIANT_SHREDDING().key()
              + " changed from "
              + writeVariantShreddingEnabled
              + " to "
              + latestVariantShreddingEnabled
              + " after the stream started. Restart the query to write the new layout.");
    }
  }

  @Override
  public void abort(long epochId, WriterCommitMessage[] messages) {
    logger.warn(
        "DSv2 streaming epoch {} for query {} aborted; {} task message(s) not committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        epochId,
        queryId,
        messages != null ? messages.length : 0);
  }
}
