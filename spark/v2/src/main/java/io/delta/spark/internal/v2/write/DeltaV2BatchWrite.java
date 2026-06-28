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
package io.delta.spark.internal.v2.write;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchWrite for DSv2 batch append. The shared executor write state ({@link
 * DeltaV2DataWriterFactory}) is built by {@link DeltaV2Write}; this class adds only the batch
 * commit -- a single {@link Operation#WRITE} transaction over the {@code AddFile} actions reported
 * by the writers.
 */
class DeltaV2BatchWrite implements BatchWrite {

  private static final Logger logger = LoggerFactory.getLogger(DeltaV2BatchWrite.class);

  private static String getEngineInfo() {
    return "Delta-Spark-DSv2/" + org.apache.spark.package$.MODULE$.SPARK_VERSION();
  }

  private final Engine engine;
  private final Snapshot initialSnapshot;
  private final DeltaV2DataWriterFactory dataWriterFactory;

  DeltaV2BatchWrite(
      Engine engine, Snapshot initialSnapshot, DeltaV2DataWriterFactory dataWriterFactory) {
    this.engine = engine;
    this.initialSnapshot = initialSnapshot;
    this.dataWriterFactory = dataWriterFactory;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return dataWriterFactory;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    Transaction txn =
        initialSnapshot.buildUpdateTableTransaction(getEngineInfo(), Operation.WRITE).build(engine);
    CloseableIterable<Row> dataActions = DeltaV2WriterCommitMessage.toDataActions(messages);
    TransactionCommitResult result = txn.commit(engine, dataActions);
    logger.info("DSv2 batch write committed at version {}", result.getVersion());
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    logger.warn(
        "DSv2 batch write aborted. {} task messages will not be committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        messages != null ? messages.length : 0);
  }
}
