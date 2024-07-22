/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.sources

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.StreamingUpdate
import org.apache.spark.sql.delta.actions.{FileAction, SetTransaction}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.hadoop.fs.Path

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.internal.MDC
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric
import org.apache.spark.sql.execution.streaming.{Sink, StreamExecution}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.NullType
import org.apache.spark.util.Utils

/**
 * A streaming sink that writes data into a Delta Table.
 */
case class DeltaSink(
    sqlContext: SQLContext,
    path: Path,
    partitionColumns: Seq[String],
    outputMode: OutputMode,
    options: DeltaOptions,
    catalogTable: Option[CatalogTable] = None)
  extends Sink
    with ImplicitMetadataOperation
    with DeltaLogging {

  private val deltaLog = DeltaLog.forTable(sqlContext.sparkSession, path)

  private val sqlConf = sqlContext.sparkSession.sessionState.conf

  // This have to be lazy because queryId is a thread local property that is not available
  // when the Sink object is created.
  lazy val queryId = sqlContext.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)

  override protected val canOverwriteSchema: Boolean =
    outputMode == OutputMode.Complete() && options.canOverwriteSchema

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  case class PendingTxn(batchId: Long,
                        optimisticTransaction: OptimisticTransaction,
                        streamingUpdate: StreamingUpdate,
                        newFiles: Seq[FileAction],
                        deletedFiles: Seq[FileAction]) {
    def commit(): Unit = {
      val sc = sqlContext.sparkContext
      val metrics = Map[String, SQLMetric](
        "numAddedFiles" -> createMetric(sc, "number of files added"),
        "numRemovedFiles" -> createMetric(sc, "number of files removed")
      )
      metrics("numRemovedFiles").set(deletedFiles.size)
      metrics("numAddedFiles").set(newFiles.size)
      optimisticTransaction.registerSQLMetrics(sqlContext.sparkSession, metrics)
      val setTxn = SetTransaction(appId = queryId, version = batchId,
        lastUpdated = Some(deltaLog.clock.getTimeMillis())) :: Nil
      val (_, durationMs) = Utils.timeTakenMs {
        optimisticTransaction
          .commit(actions = setTxn ++ newFiles ++ deletedFiles
            , op = streamingUpdate)
      }
      logInfo(
        log"Committed transaction, batchId=${MDC(DeltaLogKeys.BATCH_ID, batchId)}, " +
        log"duration=${MDC(DeltaLogKeys.DURATION, durationMs)} ms, " +
        log"added ${MDC(DeltaLogKeys.NUM_FILES, newFiles.size)} files, " +
        log"removed ${MDC(DeltaLogKeys.NUM_FILES2, deletedFiles.size)} files.")
      val executionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      SQLMetrics.postDriverMetricUpdates(sc, executionId, metrics.values.toSeq)
    }
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    addBatchWithStatusImpl(batchId, data)
  }


  private def addBatchWithStatusImpl(batchId: Long, data: DataFrame): Boolean = {
    val txn = deltaLog.startTransaction(catalogTable)
    assert(queryId != null)

    if (SchemaUtils.typeExistsRecursively(data.schema)(_.isInstanceOf[NullType])) {
      throw DeltaErrors.streamWriteNullTypeException
    }


    // If the batch reads the same Delta table as this sink is going to write to, then this
    // write has dependencies. Then make sure that this commit set hasDependencies to true
    // by injecting a read on the whole table. This needs to be done explicitly because
    // MicroBatchExecution has already enforced all the data skipping (by forcing the generation
    // of the executed plan) even before the transaction was started.
    val selfScan = data.queryExecution.analyzed.collectFirst {
      case DeltaTable(index) if index.deltaLog.isSameLogAs(txn.deltaLog) => true
    }.nonEmpty
    if (selfScan) {
      txn.readWholeTable()
    }

    // Streaming sinks can't blindly overwrite schema. See Schema Management design doc for details
    updateMetadata(data.sparkSession, txn, data.schema, partitionColumns, Map.empty,
      outputMode == OutputMode.Complete(), rearrangeOnly = false)

    val currentVersion = txn.txnVersion(queryId)
    if (currentVersion >= batchId) {
      logInfo(log"Skipping already complete epoch ${MDC(DeltaLogKeys.BATCH_ID, batchId)}, " +
        log"in query ${MDC(DeltaLogKeys.QUERY_ID, queryId)}")
      return false
    }

    val deletedFiles = outputMode match {
      case o if o == OutputMode.Complete() =>
        DeltaLog.assertRemovable(txn.snapshot)
        txn.filterFiles().map(_.remove)
      case _ => Nil
    }
    val (newFiles, writeFilesTimeMs) = Utils.timeTakenMs{
      txn.writeFiles(data, Some(options))
    }
    val totalSize = newFiles.map(_.getFileSize).sum
    val totalLogicalRecords = newFiles.map(_.numLogicalRecords.getOrElse(0L)).sum
    logInfo(
      log"Wrote ${MDC(DeltaLogKeys.NUM_FILES, newFiles.size)} files, with total size " +
      log"${MDC(DeltaLogKeys.NUM_BYTES, totalSize)}, " +
      log"${MDC(DeltaLogKeys.NUM_RECORDS, totalLogicalRecords)} logical records, " +
      log"duration=${MDC(DeltaLogKeys.DURATION, writeFilesTimeMs)} ms.")

    val info = DeltaOperations.StreamingUpdate(outputMode, queryId, batchId, options.userMetadata
                                               )
    val pendingTxn = PendingTxn(batchId, txn, info, newFiles, deletedFiles)
    pendingTxn.commit()
    return true
  }


  override def toString(): String = s"DeltaSink[$path]"
}
