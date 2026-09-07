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

import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Serializable factory sent to executors to create {@link DeltaV2DataWriter} instances. Carries the
 * serialized Hadoop config, Kernel transaction state, and partition layout needed by each writer.
 * Serves both batch and streaming writes -- the {@link DeltaV2DataWriter} is epoch-agnostic, so the
 * streaming overload ignores {@code epochId}.
 */
class DeltaV2DataWriterFactory
    implements DataWriterFactory, StreamingDataWriterFactory, Serializable {

  private final SerializableConfiguration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType dataSchema;
  private final StructType partitionSchema;
  private final int[] dataOrdinals;
  private final int[] partitionOrdinals;
  private final OutputWriterFactory outputWriterFactory;

  DeltaV2DataWriterFactory(
      SerializableConfiguration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType dataSchema,
      StructType partitionSchema,
      int[] dataOrdinals,
      int[] partitionOrdinals,
      OutputWriterFactory outputWriterFactory) {
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.dataSchema = dataSchema;
    this.partitionSchema = partitionSchema;
    this.dataOrdinals = dataOrdinals;
    this.partitionOrdinals = partitionOrdinals;
    this.outputWriterFactory = outputWriterFactory;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new DeltaV2DataWriter(
        hadoopConf,
        serializedTxnState,
        dataSchema,
        partitionSchema,
        dataOrdinals,
        partitionOrdinals,
        outputWriterFactory,
        partitionId,
        taskId);
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
    return createWriter(partitionId, taskId);
  }
}
