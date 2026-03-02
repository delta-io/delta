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

import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

/** Serializable factory that will create executor-side parquet data writers. */
public class SparkParquetDataWriterFactory implements DataWriterFactory {
  private final String tablePath;
  private final String queryId;
  private final List<String> partitionColumnNames;
  private final StructType writeSchema;
  private final String targetDirectory;
  private final SerializableKernelRowWrapper serializableTxnState;
  private final SerializableConfiguration serializableHadoopConf;

  public SparkParquetDataWriterFactory(
      String tablePath,
      String queryId,
      List<String> partitionColumnNames,
      StructType writeSchema,
      String targetDirectory,
      SerializableKernelRowWrapper serializableTxnState,
      SerializableConfiguration serializableHadoopConf) {
    this.tablePath = requireNonNull(tablePath, "table path is null");
    this.queryId = requireNonNull(queryId, "query id is null");
    this.partitionColumnNames =
        Collections.unmodifiableList(
            requireNonNull(partitionColumnNames, "partition column names is null"));
    this.writeSchema = requireNonNull(writeSchema, "write schema is null");
    this.targetDirectory = requireNonNull(targetDirectory, "target directory is null");
    this.serializableTxnState = requireNonNull(serializableTxnState, "transaction state is null");
    this.serializableHadoopConf = requireNonNull(serializableHadoopConf, "hadoop conf is null");
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new SparkParquetDataWriter(
        tablePath,
        queryId,
        partitionColumnNames,
        writeSchema,
        targetDirectory,
        serializableTxnState,
        serializableHadoopConf,
        partitionId,
        taskId);
  }
}
