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
import io.delta.kernel.utils.CloseableIterable;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingWrite for DSv2 streaming <b>Append</b>. The shared executor write state ({@link
 * DeltaV2DataWriterFactory}) is built by {@link DeltaV2Write}; this class adds only the driver-side
 * commit, which runs once per micro-batch (epoch) with an idempotent transaction.
 *
 * <p><b>Idempotency:</b> each epoch commits a transaction built with {@code
 * withTransactionId(queryId, epochId)}. A duplicate epoch makes Kernel throw {@link
 * ConcurrentTransactionException}, which is caught here and treated as a successful idempotent skip
 * (the Kernel equivalent of the V1 sink's {@code SetTransaction} version check).
 */
class DeltaV2StreamingWrite implements StreamingWrite {

  private static final Logger logger = LoggerFactory.getLogger(DeltaV2StreamingWrite.class);

  private static String getEngineInfo() {
    return "Delta-Spark-DSv2-Streaming/" + org.apache.spark.package$.MODULE$.SPARK_VERSION();
  }

  private final Engine engine;
  private final Snapshot initialSnapshot;
  private final String queryId;
  private final DeltaV2DataWriterFactory dataWriterFactory;

  DeltaV2StreamingWrite(
      Engine engine,
      Snapshot initialSnapshot,
      String queryId,
      DeltaV2DataWriterFactory dataWriterFactory) {
    this.engine = requireNonNull(engine, "engine is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.queryId = requireNonNull(queryId, "queryId is null");
    this.dataWriterFactory = requireNonNull(dataWriterFactory, "dataWriterFactory is null");
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
    final Transaction txn;
    try {
      txn =
          initialSnapshot
              .buildUpdateTableTransaction(getEngineInfo(), Operation.STREAMING_UPDATE)
              .withTransactionId(queryId, epochId)
              .build(engine);
    } catch (ConcurrentTransactionException e) {
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
      return;
    }

    CloseableIterable<Row> dataActions = DeltaV2WriterCommitMessage.toDataActions(messages);
    try {
      long version = txn.commit(engine, dataActions).getVersion();
      logger.info(
          "DSv2 streaming epoch {} for query {} committed at version {}",
          epochId,
          queryId,
          version);
    } catch (ConcurrentTransactionException e) {
      logger.info("Skipping already committed epoch {} for query {}", epochId, queryId);
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
