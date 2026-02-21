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
import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * DSv2 BatchWrite that uses the Delta Kernel Transaction API. Creates the transaction on the
 * driver, passes serialized transaction state to executor tasks via {@link
 * DeltaKernelDataWriterFactory}, and commits on the driver when all tasks succeed.
 */
public class DeltaKernelBatchWrite implements BatchWrite {

  private static final String ENGINE_INFO = "Spark-Delta-Kernel";

  private final String tablePath;
  private final Configuration hadoopConf;
  private final io.delta.kernel.Snapshot initialSnapshot;
  private final StructType writeSchema;
  private final String queryId;
  private final java.util.Map<String, String> options;
  private final List<String> partitionColumnNames;

  private final Transaction transaction;
  private final SerializableKernelRowWrapper serializedTxnState;

  public DeltaKernelBatchWrite(
      String tablePath,
      Configuration hadoopConf,
      io.delta.kernel.Snapshot initialSnapshot,
      StructType writeSchema,
      String queryId,
      java.util.Map<String, String> options,
      List<String> partitionColumnNames) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.initialSnapshot = initialSnapshot;
    this.writeSchema = writeSchema;
    this.queryId = queryId;
    this.options = options != null ? options : java.util.Collections.emptyMap();
    this.partitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : java.util.Collections.emptyList();

    Engine engine = DefaultEngine.create(hadoopConf);
    UpdateTableTransactionBuilder txnBuilder =
        initialSnapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE);
    this.transaction = txnBuilder.build(engine);
    Row txnState = transaction.getTransactionState(engine);
    this.serializedTxnState = new SerializableKernelRowWrapper(txnState);
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return new DeltaKernelDataWriterFactory(
        tablePath, hadoopConf, serializedTxnState, writeSchema, partitionColumnNames, options);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<Row> allActions = new ArrayList<>();
    for (WriterCommitMessage message : messages) {
      DeltaKernelWriterCommitMessage msg = (DeltaKernelWriterCommitMessage) message;
      for (SerializableKernelRowWrapper wrapper : msg.getActionRows()) {
        allActions.add(wrapper.getRow());
      }
    }
    CloseableIterator<Row> actionsIter =
        io.delta.kernel.internal.util.Utils.toCloseableIterator(allActions.iterator());
    CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(actionsIter);
    Engine engine = DefaultEngine.create(hadoopConf);
    transaction.commit(engine, dataActionsIterable);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // No Kernel commit; optionally clean up staged files in the future.
  }
}
