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

import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Serializable factory sent to executors to create {@link DeltaSparkParquetDataWriter} instances.
 * Carries the serialized Hadoop config and Kernel transaction state needed by each writer.
 */
public class DeltaSparkParquetDataWriterFactory implements DataWriterFactory, Serializable {

  private final String targetDirectory;
  private final SerializableConfiguration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType tableSchema;
  private final OutputWriterFactory outputWriterFactory;

  public DeltaSparkParquetDataWriterFactory(
      String targetDirectory,
      SerializableConfiguration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType tableSchema,
      OutputWriterFactory outputWriterFactory) {
    this.targetDirectory = targetDirectory;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.tableSchema = tableSchema;
    this.outputWriterFactory = outputWriterFactory;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new DeltaSparkParquetDataWriter(
        targetDirectory,
        hadoopConf,
        serializedTxnState,
        tableSchema,
        outputWriterFactory,
        partitionId,
        taskId);
  }
}
