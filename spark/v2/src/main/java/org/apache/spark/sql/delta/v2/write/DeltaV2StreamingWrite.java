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
package org.apache.spark.sql.delta.v2.write;

import static java.util.Objects.requireNonNull;

import io.delta.exceptions.ConcurrentTransactionException;
import io.delta.kernel.Operation;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.delta.DeltaConfigs;
import org.apache.spark.sql.delta.DeltaOperations;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.Protocol;
import org.apache.spark.sql.delta.actions.SetTransaction;
import org.apache.spark.sql.delta.v2.interop.DeltaV2OptimisticTransaction;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext;
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager;
import org.apache.spark.sql.delta.v2.utils.ScalaUtils;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

/**
 * StreamingWrite for DSv2 streaming <b>Append</b>. Spark's {@code V2Writes} rebuilds this per
 * micro-batch; {@link DeltaV2Write} supplies the executor write state ({@link
 * DeltaV2DataWriterFactory}), built from a throwaway transaction whose operation-independent
 * context is serialized into the factory. This class adds only the driver-side commit.
 *
 * <p>{@link #commit} builds an optimistic transaction from a freshly reloaded snapshot for each
 * epoch. Batch and streaming share the optimistic commit implementation and task-action decoding.
 *
 * <p><b>Idempotency:</b> {@code txnVersion(queryId)} checks the committed epoch and registers the
 * application-id read dependency. A {@code SetTransaction} is committed atomically with the files.
 * A concurrent application-id update is skipped only if a fresh snapshot confirms this epoch has
 * already committed.
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

  private final DeltaV2TableManager tableManager;
  private final DeltaV2QueryContext queryContext;
  private final Option<CatalogTable> catalogTable;
  private final String queryId;
  private final DeltaV2WriteContext context;
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
   * @param tableManager reloads the latest snapshot per epoch (see {@link #commit})
   * @param catalogTable current catalog metadata for the target
   * @param context shared write setup
   */
  DeltaV2StreamingWrite(
      DeltaV2TableManager tableManager,
      Optional<CatalogTable> catalogTable,
      DeltaV2WriteContext context) {
    this.context = requireNonNull(context, "context is null");
    this.tableManager = requireNonNull(tableManager, "tableManager is null");
    this.catalogTable =
        ScalaUtils.toScalaOption(requireNonNull(catalogTable, "catalogTable is null"));
    this.queryContext = DeltaV2QueryContext.apply(catalogTable);
    this.queryId = requireNonNull(context.getWriteInfo().queryId(), "queryId is null");
    Snapshot initialSnapshot =
        requireNonNull(context.getInitialSnapshot(), "initialSnapshot is null");
    this.writeSchema = initialSnapshot.schema();
    this.writeProtocol = initialSnapshot.protocol();
    this.writeVariantShreddingEnabled = context.variantShreddingEnabled();
    this.variantLayoutFollowsProperty = context.variantLayoutFollowsProperty();
  }

  @Override
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
    return context.buildDataWriterFactory(Operation.STREAMING_UPDATE);
  }

  @Override
  public boolean useCommitCoordinator() {
    return false;
  }

  @Override
  public void commit(long epochId, WriterCommitMessage[] messages) {
    // One reload, so the skip check, guards, and the transaction below all judge the same snapshot.
    DeltaV2Snapshot latestSnapshot = loadLatestSnapshot();
    DeltaV2OptimisticTransaction txn =
        new DeltaV2OptimisticTransaction(catalogTable, latestSnapshot, tableManager);

    // Skip an already-committed epoch before any guard runs. StreamingWrite.commit may be called
    // more than once for one epoch and must be idempotent, so a repeated commit of a committed
    // epoch is an unconditional no-op -- its data already landed under the layout in force then,
    // and a table change since must not turn that success into a failure. Its executor-written
    // files for this repeat are orphaned (VACUUM'd). The next uncommitted epoch still hits the
    // guards.
    long committedEpoch = txn.txnVersion(queryId);
    if (committedEpoch >= epochId) {
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
      return;
    }

    // TODO(#7140): no implicit type cast and mergeSchema. Fail loudly on a concurrent
    // schema/protocol change.
    assertSchemaAndProtocolUnchanged(latestSnapshot);
    assertVariantShreddingUnchanged(DeltaV2WriteBuilder.isVariantShreddingEnabled(latestSnapshot));

    // TODO(#7140): no self-scan guard. A stream reading and writing the same table commits
    //  as a blind append, skipping the conflict check V1 gets via readWholeTable().

    try {
      List<Action> actions = DeltaV2WriterCommitMessage.toAddFileActions(messages);
      actions.add(new SetTransaction(queryId, epochId, Option.apply(txn.clock().getTimeMillis())));
      long version =
          txn.commit(CollectionConverters.asScala(actions).toSeq(), streamingOperation(epochId));
      logger.info(
          "DSv2 streaming epoch {} for query {} committed at version {}",
          epochId,
          queryId,
          version);
    } catch (ConcurrentTransactionException e) {
      // A concurrent update for an older epoch is a conflict, not proof this epoch succeeded.
      if (loadLatestSnapshot().getLatestTransactionVersion(queryId).orElse(-1L) < epochId) {
        throw e;
      }
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
    }
  }

  private DeltaV2Snapshot loadLatestSnapshot() {
    return (DeltaV2Snapshot) tableManager.loadLatestSnapshot(queryContext);
  }

  private DeltaOperations.StreamingUpdate streamingOperation(long epochId) {
    return new DeltaOperations.StreamingUpdate(
        OutputMode.Append(), queryId, epochId, Option.empty());
  }

  /** Fails the epoch if the fresh snapshot's schema/protocol diverged from the write's baseline. */
  private void assertSchemaAndProtocolUnchanged(Snapshot latestSnapshot) {
    if (!writeSchema.equals(latestSnapshot.schema())) {
      throw new IllegalStateException(
          "DSv2 streaming write to query "
              + queryId
              + " cannot continue: the table schema changed after the stream started. Restart the "
              + "query to pick up the new schema.");
    }
    if (!writeProtocol.equals(latestSnapshot.protocol())) {
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
