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
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.read.SparkScan;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * DSv2 BatchWrite for Copy-on-Write row-level operations (DELETE, UPDATE, MERGE).
 *
 * <p>At commit time, this generates both RemoveFile actions (for files read during the scan) and
 * AddFile actions (for newly written files), then commits them atomically via the Kernel
 * Transaction API.
 */
public class DeltaCopyOnWriteBatchWrite extends AbstractDeltaKernelBatchWrite {

  private final SparkScan configuredScan;

  public DeltaCopyOnWriteBatchWrite(
      String tablePath,
      Configuration hadoopConf,
      io.delta.kernel.Snapshot initialSnapshot,
      SparkScan configuredScan,
      StructType writeSchema,
      String queryId,
      Map<String, String> options,
      List<String> partitionColumnNames,
      RowLevelOperation.Command command) {
    super(
        tablePath,
        hadoopConf,
        initialSnapshot,
        toKernelOperation(command),
        options,
        partitionColumnNames);
    this.configuredScan = configuredScan;
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<Row> addActions = collectAddActions(messages);
    List<Row> removeActions = generateRemoveActions();

    List<Row> allActions = new ArrayList<>(removeActions.size() + addActions.size());
    allActions.addAll(removeActions);
    allActions.addAll(addActions);
    commitActions(allActions);
  }

  /**
   * Generates RemoveFile actions from the scan file rows collected during the read phase. When
   * configuredScan is null (e.g. DELETE WHERE true optimized away the scan), falls back to scanning
   * all files from the snapshot to generate RemoveFile actions.
   */
  private List<Row> generateRemoveActions() {
    Engine engine = DefaultEngine.create(hadoopConf);
    Row txnState = serializedTxnState.getRow();

    CloseableIterator<Row> scanFileIter;
    if (configuredScan != null) {
      List<Row> scanFileRows = configuredScan.getScanFileRows();
      scanFileIter =
          io.delta.kernel.internal.util.Utils.toCloseableIterator(scanFileRows.iterator());
    } else {
      io.delta.kernel.Scan fullScan = initialSnapshot.getScanBuilder().build();
      List<Row> allFileRows = new ArrayList<>();
      CloseableIterator<io.delta.kernel.data.FilteredColumnarBatch> batches =
          fullScan.getScanFiles(engine);
      while (batches.hasNext()) {
        io.delta.kernel.data.FilteredColumnarBatch batch = batches.next();
        CloseableIterator<Row> rows = batch.getRows();
        try {
          while (rows.hasNext()) {
            allFileRows.add(rows.next());
          }
        } finally {
          try {
            rows.close();
          } catch (Exception ignored) {
          }
        }
      }
      scanFileIter =
          io.delta.kernel.internal.util.Utils.toCloseableIterator(allFileRows.iterator());
    }

    CloseableIterator<Row> removeActionIter =
        Transaction.generateRemoveActions(engine, txnState, scanFileIter);

    List<Row> removeActions = new ArrayList<>();
    while (removeActionIter.hasNext()) {
      removeActions.add(removeActionIter.next());
    }
    return removeActions;
  }

  private static Operation toKernelOperation(RowLevelOperation.Command command) {
    switch (command) {
      case DELETE:
        return Operation.DELETE;
      case UPDATE:
        return Operation.UPDATE;
      case MERGE:
        return Operation.MERGE;
      default:
        throw new IllegalArgumentException("Unsupported row-level command: " + command);
    }
  }
}
