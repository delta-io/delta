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
 * table path, Hadoop config (as a serializable map; see {@link HadoopConfSerialization}),
 * serialized transaction state, table schema (for Kernel nullability match), and partition columns.
 *
 * <p>POC: We store Hadoop config as {@code Map<String,String>} instead of {@link Configuration}
 * because Configuration is not Serializable and this factory is sent to executors.
 */
public class DeltaKernelDataWriterFactory implements DataWriterFactory, Serializable {

  private final String tablePath;
  /** Serializable copy of Hadoop config; reconstructed to Configuration on the executor. */
  private final Map<String, String> hadoopConfMap;

  private final SerializableKernelRowWrapper serializedTxnState;
  /** Table schema (from snapshot); used for Kernel so data schema nullability matches table. */
  private final StructType tableSchema;

  private final List<String> partitionColumnNames;
  private final Map<String, String> options;

  public DeltaKernelDataWriterFactory(
      String tablePath,
      Map<String, String> hadoopConfMap,
      SerializableKernelRowWrapper serializedTxnState,
      StructType tableSchema,
      List<String> partitionColumnNames,
      Map<String, String> options) {
    this.tablePath = tablePath;
    this.hadoopConfMap = hadoopConfMap != null ? hadoopConfMap : java.util.Collections.emptyMap();
    this.serializedTxnState = serializedTxnState;
    this.tableSchema = tableSchema;
    this.partitionColumnNames =
        partitionColumnNames != null ? partitionColumnNames : java.util.Collections.emptyList();
    this.options = options != null ? options : java.util.Collections.emptyMap();
  }

  @Override
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    Configuration hadoopConf = HadoopConfSerialization.fromMap(hadoopConfMap);
    return new DeltaKernelDataWriter(
        tablePath,
        hadoopConf,
        serializedTxnState,
        tableSchema,
        partitionColumnNames,
        options,
        partitionId,
        taskId);
  }
}
