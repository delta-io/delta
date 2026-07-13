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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchWrite for DSv2 batch append using Spark's Parquet path. Creates a Kernel transaction on the
 * driver, obtains the target directory from the Kernel write context, creates a Spark Parquet
 * OutputWriterFactory via the shared {@link PartitionUtils#createDeltaParquetFileFormat} factory,
 * and serializes everything into a {@link DeltaV2DataWriterFactory} for executor transport.
 *
 * <p>The {@link Transaction} object lives only on the driver and is never serialized. Executors
 * receive only serializable state: transaction state row, Hadoop conf, OutputWriterFactory, schema,
 * and target directory.
 */
class DeltaV2BatchWrite implements Write, BatchWrite {

  private static final Logger LOG = LoggerFactory.getLogger(DeltaV2BatchWrite.class);

  static String getEngineInfo() {
    return DeltaV2BatchWriteContext.getEngineInfo();
  }

  private final DeltaV2BatchWriteContext context;
  private final String targetDirectory;

  DeltaV2BatchWrite(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      LogicalWriteInfo writeInfo) {
    this.context =
        DeltaV2BatchWriteContext.create(engine, hadoopConf, tablePath, initialSnapshot, writeInfo);
    this.targetDirectory = context.getTargetDirectory(Collections.emptyMap());
  }

  @Override
  public BatchWrite toBatch() {
    return this;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return new DeltaV2DataWriterFactory(
        targetDirectory,
        context.getSerializableHadoopConf(),
        context.getSerializedTxnState(),
        context.getDataSchema(),
        context.getOutputWriterFactory());
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<Row> allActionRows = new ArrayList<>();
    for (WriterCommitMessage msg : messages) {
      if (msg instanceof DeltaV2WriterCommitMessage) {
        for (SerializableKernelRowWrapper wrapper :
            ((DeltaV2WriterCommitMessage) msg).getActionRows()) {
          allActionRows.add(wrapper.getRow());
        }
      }
    }

    CloseableIterable<Row> dataActions =
        CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(allActionRows.iterator()));

    TransactionCommitResult result =
        context.getTransaction().commit(context.getEngine(), dataActions);
    LOG.info("DSv2 batch write committed at version {}", result.getVersion());
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.warn(
        "DSv2 batch write aborted. {} task messages will not be committed. "
            + "Orphaned data files will be cleaned up by VACUUM.",
        messages != null ? messages.length : 0);
  }
}
