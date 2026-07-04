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
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/**
 * Serializable factory sent to executors to create ReplaceData writers.
 *
 * <p>Carries everything a task needs to write a Parquet file that will later be committed as an
 * AddFile: target directory, Hadoop conf, serialized Kernel transaction state, data/partition
 * schemas, the shared {@link OutputWriterFactory}, pre-resolved partition-value strings, and the
 * session time zone for partition literal coercion. All fields are serializable; non-serializable
 * Kernel objects (Transaction, Engine) are recreated executor-side from the transaction state.
 */
class DeltaReplaceDataWriterFactory implements DataWriterFactory, Serializable {

  private final String targetDirectory;
  private final SerializableConfiguration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType dataSchema;
  private final OutputWriterFactory outputWriterFactory;
  private final StructType partitionSchema;
  private final Map<String, String> partitionValueStrings;
  private final String sessionTimeZone;

  DeltaReplaceDataWriterFactory(
      String targetDirectory,
      SerializableConfiguration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType dataSchema,
      OutputWriterFactory outputWriterFactory,
      StructType partitionSchema,
      Map<String, String> partitionValueStrings,
      String sessionTimeZone) {
    this.targetDirectory = targetDirectory;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.dataSchema = dataSchema;
    this.outputWriterFactory = outputWriterFactory;
    this.partitionSchema = partitionSchema;
    this.partitionValueStrings = partitionValueStrings;
    this.sessionTimeZone = sessionTimeZone;
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new DeltaReplaceDataWriter(
        targetDirectory,
        hadoopConf,
        serializedTxnState,
        dataSchema,
        outputWriterFactory,
        partitionSchema,
        partitionValueStrings,
        sessionTimeZone,
        partitionId,
        taskId);
  }
}
