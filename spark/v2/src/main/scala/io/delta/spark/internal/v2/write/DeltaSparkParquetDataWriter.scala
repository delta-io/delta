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

import java.util.{Collections, UUID}

import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.util.Utils
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus}
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType

/**
 * Option B DataWriter: writes InternalRow with Spark's Parquet OutputWriter,
 * then builds DataFileStatus and calls Kernel's generateAppendActions (bridge).
 * No InternalRow→Kernel conversion; same Parquet format as read/V1 path.
 */
class DeltaSparkParquetDataWriter(
    targetDirectory: String,
    hadoopConfMap: java.util.Map[String, String],
    serializedTxnState: SerializableKernelRowWrapper,
    tableSchema: StructType,
    outputWriterFactory: org.apache.spark.sql.execution.datasources.OutputWriterFactory,
    partitionId: Int,
    taskId: Long)
  extends DataWriter[InternalRow] {

  private val rowBuffer = new ArrayBuffer[InternalRow]()

  override def write(record: InternalRow): Unit = {
    rowBuffer += record.copy()
  }

  override def commit(): WriterCommitMessage = {
    if (rowBuffer.isEmpty) {
      return new DeltaKernelWriterCommitMessage(java.util.Collections.emptyList())
    }

    val conf = HadoopConfSerialization.fromMap(hadoopConfMap)
    val engine: Engine = DefaultEngine.create(conf)
    val txnState: Row = serializedTxnState.getRow
    val partitionValues = Collections.emptyMap[String, Literal]()
    val writeContext = io.delta.kernel.Transaction.getWriteContext(engine, txnState, partitionValues)

    val fileName = s"part-$partitionId-$taskId-${UUID.randomUUID()}.parquet"
    val outputPath = new Path(targetDirectory, fileName)
    val pathStr = outputPath.toString

    val taskAttemptContext = new TaskAttemptContextImpl(
      new JobConf(conf),
      new TaskAttemptID("", 0, TaskType.REDUCE, partitionId, 0))

    val writer: OutputWriter = outputWriterFactory.newInstance(
      pathStr,
      tableSchema,
      taskAttemptContext)

    try {
      rowBuffer.foreach(writer.write)
    } finally {
      writer.close()
    }

    val fs = outputPath.getFileSystem(conf)
    val fileStatus = fs.getFileStatus(outputPath)
    val dataFileStatus = new DataFileStatus(
      pathStr,
      fileStatus.getLen,
      fileStatus.getModificationTime,
      java.util.Optional.empty())

    val dataFilesIter: CloseableIterator[DataFileStatus] =
      Utils.toCloseableIterator(java.util.Collections.singletonList(dataFileStatus).iterator())

    val actionRowsIter = io.delta.kernel.Transaction.generateAppendActions(
      engine, txnState, dataFilesIter, writeContext)

    val actionRows = new java.util.ArrayList[SerializableKernelRowWrapper]()
    try {
      while (actionRowsIter.hasNext) {
        actionRows.add(new SerializableKernelRowWrapper(actionRowsIter.next()))
      }
    } finally {
      actionRowsIter.close()
    }

    new DeltaKernelWriterCommitMessage(actionRows)
  }

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
