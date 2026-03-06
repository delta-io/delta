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

import scala.jdk.CollectionConverters._

import io.delta.kernel.Operation
import io.delta.kernel.Transaction
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.utils.CloseableIterable
import io.delta.spark.internal.v2.utils.SchemaUtils
import io.delta.spark.internal.v2.utils.SerializableKernelRowWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/**
 * Option B (Spark Parquet path) BatchWrite.
 * Driver: create Kernel transaction, get target directory from getWriteContext,
 * create Spark Parquet OutputWriterFactory, pass to executor.
 * Commit: same as Kernel path (collect action rows, Transaction.commit).
 */
class DeltaSparkParquetBatchWrite(
    tablePath: String,
    hadoopConf: Configuration,
    initialSnapshot: io.delta.kernel.Snapshot,
    writeSchema: StructType,
    queryId: String,
    options: java.util.Map[String, String],
    partitionColumnNames: java.util.List[String])
  extends BatchWrite {

  private val ENGINE_INFO = "Spark-Delta-Kernel-OptionB"

  private val engine: Engine = DefaultEngine.create(hadoopConf)
  private val txnBuilder = initialSnapshot.buildUpdateTableTransaction(ENGINE_INFO, Operation.WRITE)
  private val transaction: Transaction = txnBuilder.build(engine)
  private val txnState: Row = transaction.getTransactionState(engine)
  private val serializedTxnState = new SerializableKernelRowWrapper(txnState)

  // Target directory for this (unpartitioned) write; executor will write Parquet here
  private val partitionValues = Collections.emptyMap[String, io.delta.kernel.expressions.Literal]()
  private val writeContext = Transaction.getWriteContext(engine, txnState, partitionValues)
  private val targetDirectory: String = writeContext.getTargetDirectory

  // Table schema (from snapshot) for nullability match
  private val tableSchema: StructType =
    SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema)

  // Spark Parquet writer factory (serialized to executor). Serialize job conf *after* prepareWrite
  // so executor gets Parquet write support class and all format options.
  private val (outputWriterFactory, hadoopConfMap): (
      org.apache.spark.sql.execution.datasources.OutputWriterFactory,
      java.util.Map[String, String]) = {
    val session = SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("SparkSession not active (Option B write needs it for Parquet)"))
    val job = Job.getInstance(hadoopConf)
    val format = new ParquetFileFormat()
    val opts = if (options != null) options.asScala.toMap else Map.empty[String, String]
    val factory = format.prepareWrite(session, job, opts, tableSchema)
    (factory, HadoopConfSerialization.toMap(job.getConfiguration))
  }

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    new DeltaSparkParquetDataWriterFactory(
      targetDirectory = targetDirectory,
      hadoopConfMap = hadoopConfMap,
      serializedTxnState = serializedTxnState,
      tableSchema = tableSchema,
      outputWriterFactory = outputWriterFactory,
      partitionColumnNames =
        if (partitionColumnNames != null) partitionColumnNames else Collections.emptyList(),
      options = if (options != null) options else Collections.emptyMap()
    )
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val allActions = new java.util.ArrayList[Row]()
    messages.foreach { msg =>
      val km = msg.asInstanceOf[DeltaKernelWriterCommitMessage]
      km.getActionRows.asScala.foreach { w => allActions.add(w.getRow) }
    }
    val actionsIter = io.delta.kernel.internal.util.Utils.toCloseableIterator(allActions.iterator())
    val dataActionsIterable = CloseableIterable.inMemoryIterable(actionsIter)
    transaction.commit(engine, dataActionsIterable)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
