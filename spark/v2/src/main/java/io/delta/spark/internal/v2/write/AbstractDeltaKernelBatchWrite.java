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
import io.delta.spark.internal.v2.utils.SchemaUtils;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * Base class for DSv2 BatchWrite implementations backed by the Delta Kernel Transaction API.
 * Handles transaction creation, writer factory setup, and action collection. Subclasses implement
 * {@link #commit(WriterCommitMessage[])} to define the specific commit logic (append-only vs COW).
 */
public abstract class AbstractDeltaKernelBatchWrite implements BatchWrite {

  private static final String ENGINE_INFO = "Spark-Delta-Kernel";

  protected final String tablePath;
  protected final Configuration hadoopConf;
  protected final io.delta.kernel.Snapshot initialSnapshot;
  protected final Map<String, String> options;
  protected final List<String> partitionColumnNames;
  protected final Transaction transaction;
  protected final SerializableKernelRowWrapper serializedTxnState;

  protected AbstractDeltaKernelBatchWrite(
      String tablePath,
      Configuration hadoopConf,
      io.delta.kernel.Snapshot initialSnapshot,
      Operation operation,
      Map<String, String> options,
      List<String> partitionColumnNames) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.initialSnapshot = initialSnapshot;
    this.options = options != null ? options : Collections.emptyMap();
    this.partitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : Collections.emptyList();

    Engine engine = DefaultEngine.create(hadoopConf);
    UpdateTableTransactionBuilder txnBuilder =
        initialSnapshot.buildUpdateTableTransaction(ENGINE_INFO, operation);
    this.transaction = txnBuilder.build(engine);
    Row txnState = transaction.getTransactionState(engine);
    this.serializedTxnState = new SerializableKernelRowWrapper(txnState);
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    Map<String, String> hadoopConfMap = HadoopConfSerialization.toMap(hadoopConf);
    StructType tableSchemaSpark =
        SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    return new DeltaKernelDataWriterFactory(
        tablePath,
        hadoopConfMap,
        serializedTxnState,
        tableSchemaSpark,
        partitionColumnNames,
        options);
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    // No Kernel commit; optionally clean up staged files in the future.
  }

  /** Collects AddFile action rows from all writer commit messages. */
  protected List<Row> collectAddActions(WriterCommitMessage[] messages) {
    List<Row> actions = new ArrayList<>();
    for (WriterCommitMessage message : messages) {
      DeltaKernelWriterCommitMessage msg = (DeltaKernelWriterCommitMessage) message;
      for (SerializableKernelRowWrapper wrapper : msg.getActionRows()) {
        actions.add(wrapper.getRow());
      }
    }
    return actions;
  }

  /** Commits the given action rows (AddFile and/or RemoveFile) via the Kernel Transaction. */
  protected void commitActions(List<Row> actions) {
    CloseableIterator<Row> actionsIter =
        io.delta.kernel.internal.util.Utils.toCloseableIterator(actions.iterator());
    CloseableIterable<Row> dataActionsIterable = CloseableIterable.inMemoryIterable(actionsIter);
    Engine engine = DefaultEngine.create(hadoopConf);
    transaction.commit(engine, dataActionsIterable);
  }
}
