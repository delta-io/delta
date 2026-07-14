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
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;

/**
 * Driver-side context for a Delta DSv2 <b>batch</b> write. Extends {@link DeltaV2WriteContext} with
 * the batch transaction lifecycle: it creates a single {@code Operation.WRITE} transaction off
 * {@code initialSnapshot} and eagerly serializes its transaction state (the batch commits once, off
 * this transaction). The operation-independent write state (Parquet factory, schema split, Hadoop
 * conf) is built by the superclass.
 */
class DeltaV2BatchWriteContext extends DeltaV2WriteContext {

  private final Transaction transaction;
  private final SerializableKernelRowWrapper serializedTxnState;

  static DeltaV2BatchWriteContext create(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      StructType dataSchema,
      LogicalWriteInfo writeInfo) {
    return new DeltaV2BatchWriteContext(
        engine, hadoopConf, tablePath, initialSnapshot, dataSchema, writeInfo);
  }

  private DeltaV2BatchWriteContext(
      Engine engine,
      Configuration hadoopConf,
      String tablePath,
      Snapshot initialSnapshot,
      StructType dataSchema,
      LogicalWriteInfo writeInfo) {
    super(engine, hadoopConf, tablePath, initialSnapshot, dataSchema, writeInfo);
    this.transaction =
        initialSnapshot
            .buildUpdateTableTransaction(getEngineInfo(), Operation.WRITE)
            .build(getEngine());
    Row txnState = transaction.getTransactionState(getEngine());
    this.serializedTxnState = new SerializableKernelRowWrapper(txnState);
  }

  Transaction getTransaction() {
    return transaction;
  }

  String getTargetDirectory(Map<String, Literal> partitionLiterals) {
    return Transaction.getWriteContext(getEngine(), serializedTxnState.getRow(), partitionLiterals)
        .getTargetDirectory();
  }

  SerializableKernelRowWrapper getSerializedTxnState() {
    return serializedTxnState;
  }
}
