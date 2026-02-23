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

package io.delta.spark.internal.v2.write

import java.util.Collections

import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * Option B: Serializable factory that creates DataWriters which use Spark's Parquet
 * writer (no InternalRow→Kernel conversion). Still passes hadoopConfMap and txn state
 * so the writer can call Kernel's generateAppendActions after writing files.
 */
class DeltaSparkParquetDataWriterFactory(
    val targetDirectory: String,
    val hadoopConfMap: java.util.Map[String, String],
    val serializedTxnState: SerializableKernelRowWrapper,
    val tableSchema: StructType,
    val outputWriterFactory: org.apache.spark.sql.execution.datasources.OutputWriterFactory,
    val partitionColumnNames: java.util.List[String],
    val options: java.util.Map[String, String])
  extends DataWriterFactory with Serializable {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    new DeltaSparkParquetDataWriter(
      targetDirectory = targetDirectory,
      hadoopConfMap = if (hadoopConfMap != null) hadoopConfMap else Collections.emptyMap(),
      serializedTxnState = serializedTxnState,
      tableSchema = tableSchema,
      outputWriterFactory = outputWriterFactory,
      partitionId = partitionId,
      taskId = taskId
    )
  }
}
