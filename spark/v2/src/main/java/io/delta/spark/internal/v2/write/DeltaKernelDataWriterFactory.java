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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Serializable factory that creates {@link DeltaKernelDataWriter} instances on executors. Carries
 * table path, Hadoop config, serialized transaction state, write schema, and partition column
 * names.
 */
public class DeltaKernelDataWriterFactory implements DataWriterFactory, Serializable {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final SerializableKernelRowWrapper serializedTxnState;
  private final StructType writeSchema;
  private final List<String> partitionColumnNames;
  private final Map<String, String> options;

  public DeltaKernelDataWriterFactory(
      String tablePath,
      Configuration hadoopConf,
      SerializableKernelRowWrapper serializedTxnState,
      StructType writeSchema,
      List<String> partitionColumnNames,
      Map<String, String> options) {
    this.tablePath = tablePath;
    this.hadoopConf = hadoopConf;
    this.serializedTxnState = serializedTxnState;
    this.writeSchema = writeSchema;
    this.partitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : java.util.Collections.emptyList();
    this.options = options != null ? options : java.util.Collections.emptyMap();
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new DeltaKernelDataWriter(
        tablePath,
        hadoopConf,
        serializedTxnState,
        writeSchema,
        partitionColumnNames,
        options,
        partitionId,
        taskId);
  }
}
