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
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import java.util.function.Function;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
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
 * <p><b>Schema/protocol guard:</b> {@link #commit} fails the query if the reloaded snapshot's
 * schema or protocol has diverged from the write state's, since Kernel does not re-validate the
 * executor-written files at commit. TODO(#7140): rebuild the write state against the new
 * schema/protocol so a compatible change (e.g. an added nullable column) is tolerated instead of
 * forcing a query restart.
 */
class DeltaV2StreamingWrite implements StreamingWrite {

  private static final Logger logger = LoggerFactory.getLogger(DeltaV2StreamingWrite.class);

  private final Engine engine;
  private final DeltaSnapshotManager snapshotManager;
  private final String queryId;
  private final DeltaV2DataWriterFactory dataWriterFactory;
  // The write state's schema/protocol baseline; the per-epoch guard fails if the table diverges.
  private final StructType writeSchema;
  private final Protocol writeProtocol;

  /**
   * @param engine Kernel engine (driver-only)
   * @param initialSnapshot the batch's planned snapshot; write-state source and guard baseline
   * @param snapshotManager reloads the latest snapshot per epoch (see {@link #commit})
   * @param queryId streaming query id; the transaction application id for cross-restart idempotency
   * @param dataWriterFactoryBuilder builds the executor write state; supplied by {@link
   *     DeltaV2Write} to share construction with the batch path
   */
  DeltaV2StreamingWrite(
      Engine engine,
      Snapshot initialSnapshot,
      DeltaSnapshotManager snapshotManager,
      String queryId,
      Function<Transaction, DeltaV2DataWriterFactory> dataWriterFactoryBuilder) {
    this.engine = requireNonNull(engine, "engine is null");
    requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.snapshotManager = requireNonNull(snapshotManager, "snapshotManager is null");
    this.queryId = requireNonNull(queryId, "queryId is null");
    requireNonNull(dataWriterFactoryBuilder, "dataWriterFactoryBuilder is null");
    this.writeSchema = initialSnapshot.getSchema();
    this.writeProtocol = initialSnapshot.getProtocol();
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
    // Refresh so this epoch commits at latest+1.
    Snapshot latestSnapshot = snapshotManager.loadLatestSnapshot();

    // TODO(#7140): no implicit type cast and mergeSchema. Fail loudly on a concurrent
    // schema/protocol change.
    assertSchemaAndProtocolUnchanged(latestSnapshot);

    // TODO(#7140): no self-scan guard. A stream reading and writing the same table commits
    //  as a blind append, skipping the conflict check V1 gets via readWholeTable().

    // Skip an already-committed epoch. Its executor-written files are then orphaned (VACUUM'd).
    long committedEpoch = latestSnapshot.getLatestTransactionVersion(engine, queryId).orElse(-1L);
    if (committedEpoch >= epochId) {
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
      return;
    }

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
    if (!writeProtocol.equals(latestSnapshot.getProtocol())) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table protocol changed after the stream started. Restart "
              + "the query to pick up the new protocol.");
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
