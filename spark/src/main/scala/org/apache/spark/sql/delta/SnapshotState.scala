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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.{Metadata, Protocol, SetTransaction}
import org.apache.spark.sql.delta.actions.DomainMetadata
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeletedRecordCountsHistogram
import org.apache.spark.sql.delta.stats.DeletedRecordCountsHistogramUtils
import org.apache.spark.sql.delta.stats.FileSizeHistogram

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, collect_set, count, last, lit, sum, when}
import org.apache.spark.util.Utils


/**
 * Metrics and metadata computed around the Delta table.
 *
 * @param sizeInBytes The total size of the table (of active files, not including tombstones).
 * @param numOfSetTransactions Number of streams writing to this table.
 * @param numOfFiles The number of files in this table.
 * @param numOfRemoves The number of tombstones in the state.
 * @param numDeletedRecordsOpt The total number of records deleted with Deletion Vectors.
 * @param numDeletionVectorsOpt The number of Deletion Vectors present in the table.
 * @param numOfMetadata The number of metadata actions in the state. Should be 1.
 * @param numOfProtocol The number of protocol actions in the state. Should be 1.
 * @param setTransactions The streaming queries writing to this table.
 * @param metadata The metadata of the table.
 * @param protocol The protocol version of the Delta table.
 * @param fileSizeHistogram A Histogram class tracking the file counts and total bytes
 *                          in different size ranges.
 * @param deletedRecordCountsHistogramOpt A histogram of deletion records counts distribution
 *                                        for all files.
 */
case class SnapshotState(
  sizeInBytes: Long,
  numOfSetTransactions: Long,
  numOfFiles: Long,
  numOfRemoves: Long,
  numDeletedRecordsOpt: Option[Long],
  numDeletionVectorsOpt: Option[Long],
  numOfMetadata: Long,
  numOfProtocol: Long,
  setTransactions: Seq[SetTransaction],
  domainMetadata: Seq[DomainMetadata],
  metadata: Metadata,
  protocol: Protocol,
  fileSizeHistogram: Option[FileSizeHistogram] = None,
  deletedRecordCountsHistogramOpt: Option[DeletedRecordCountsHistogram] = None
)

/**
 * A helper class that manages the SnapshotState for a given snapshot. Will generate it only
 * when necessary.
 */
trait SnapshotStateManager extends DeltaLogging { self: Snapshot =>

  // For implicits which re-use Encoder:
  import implicits._
  /** Whether computedState is already computed or not */
  @volatile protected var _computedStateTriggered: Boolean = false


  /** A map to look up transaction version by appId. */
  lazy val transactions: Map[String, Long] = setTransactions.map(t => t.appId -> t.version).toMap

  /**
   * Compute the SnapshotState of a table. Uses the stateDF from the Snapshot to extract
   * the necessary stats.
   */
  protected lazy val computedState: SnapshotState = {
    withStatusCode("DELTA", s"Compute snapshot for version: $version") {
      recordFrameProfile("Delta", "snapshot.computedState") {
        val startTime = System.nanoTime()
        val _computedState = extractComputedState(stateDF)
        if (_computedState.protocol == null) {
          recordDeltaEvent(
            deltaLog,
            opType = "delta.assertions.missingAction",
            data = Map(
              "version" -> version.toString, "action" -> "Protocol", "source" -> "Snapshot"))
          throw DeltaErrors.actionNotFoundException("protocol", version)
        } else if (_computedState.protocol != protocol) {
          recordDeltaEvent(
            deltaLog,
            opType = "delta.assertions.mismatchedAction",
            data = Map(
              "version" -> version.toString, "action" -> "Protocol", "source" -> "Snapshot",
              "computedState.protocol" -> _computedState.protocol,
              "extracted.protocol" -> protocol))
          throw DeltaErrors.actionNotFoundException("protocol", version)
        }

        if (_computedState.metadata == null) {
          recordDeltaEvent(
            deltaLog,
            opType = "delta.assertions.missingAction",
            data = Map(
              "version" -> version.toString, "action" -> "Metadata", "source" -> "Metadata"))
          throw DeltaErrors.actionNotFoundException("metadata", version)
        } else if (_computedState.metadata != metadata) {
          recordDeltaEvent(
            deltaLog,
            opType = "delta.assertions.mismatchedAction",
            data = Map(
              "version" -> version.toString, "action" -> "Metadata", "source" -> "Snapshot",
              "computedState.metadata" -> _computedState.metadata,
              "extracted.metadata" -> metadata))
          throw DeltaErrors.actionNotFoundException("metadata", version)
        }

        _computedStateTriggered = true
        _computedState
      }
    }
  }

  /**
   * Extract the SnapshotState from the provided dataframe of actions. Requires that the dataframe
   * has already been deduplicated (either through logReplay or some other method).
   */
  protected def extractComputedState(stateDF: DataFrame): SnapshotState = {
    recordFrameProfile("Delta", "snapshot.computedState.aggregations") {
      val aggregations =
        aggregationsToComputeState.map { case (alias, agg) => agg.as(alias) }.toSeq
      stateDF.select(aggregations: _*).as[SnapshotState].first()
    }
  }

  /**
   * A Map of alias to aggregations which needs to be done to calculate the `computedState`
   */
  protected def aggregationsToComputeState: Map[String, Column] = {
    val checksumDVMetricsEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED)
    val deletedRecordCountsHistogramEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)
    lazy val persistentDVsOnTableSupported =
      DeletionVectorUtils.deletionVectorsWritable(this)

    val computeChecksumDVMetrics = checksumDVMetricsEnabled &&
      persistentDVsOnTableSupported
    val persistentDVsAggs =
      if (computeChecksumDVMetrics) {
        Map(
          "numDeletedRecordsOpt" -> sum(coalesce(col("add.deletionVector.cardinality"), lit(0L))),
          "numDeletionVectorsOpt" -> count(col("add.deletionVector")))
      } else {
        Map("numDeletedRecordsOpt" -> lit(null), "numDeletionVectorsOpt" -> lit(null))
      }

    val histogramDVsAggExpr = if (computeChecksumDVMetrics && deletedRecordCountsHistogramEnabled) {
      DeletedRecordCountsHistogramUtils.histogramAggregate(
        when(col("add").isNotNull, coalesce(col("add.deletionVector.cardinality"), lit(0L))))
    } else {
      lit(null).cast(DeletedRecordCountsHistogram.schema)
    }

    val histogramDVsAgg = Seq("deletedRecordCountsHistogramOpt" -> histogramDVsAggExpr)

    Map(
      // sum may return null for empty data set.
      "sizeInBytes" -> coalesce(sum(col("add.size")), lit(0L)),
      "numOfSetTransactions" -> count(col("txn")),
      "numOfFiles" -> count(col("add")),
      "numOfRemoves" -> count(col("remove")),
      "numOfMetadata" -> count(col("metaData")),
      "numOfProtocol" -> count(col("protocol")),
      "setTransactions" -> collect_set(col("txn")),
      "domainMetadata" -> collect_set(col("domainMetadata")),
      "metadata" -> last(col("metaData"), ignoreNulls = true),
      "protocol" -> last(col("protocol"), ignoreNulls = true),
      "fileSizeHistogram" -> lit(null).cast(FileSizeHistogram.schema)
    ) ++ persistentDVsAggs ++ histogramDVsAgg
  }

  /**
   * The following is a list of convenience methods for accessing the computedState.
   */
  def sizeInBytes: Long = computedState.sizeInBytes
  def numOfSetTransactions: Long = computedState.numOfSetTransactions
  def numOfFiles: Long = computedState.numOfFiles
  def numOfRemoves: Long = computedState.numOfRemoves
  def numOfMetadata: Long = computedState.numOfMetadata
  def numOfProtocol: Long = computedState.numOfProtocol
  def setTransactions: Seq[SetTransaction] = computedState.setTransactions
  def fileSizeHistogram: Option[FileSizeHistogram] = computedState.fileSizeHistogram
  def domainMetadata: Seq[DomainMetadata] = computedState.domainMetadata
  protected[delta] def sizeInBytesIfKnown: Option[Long] = Some(sizeInBytes)
  protected[delta] def setTransactionsIfKnown: Option[Seq[SetTransaction]] = Some(setTransactions)
  protected[delta] def numOfFilesIfKnown: Option[Long] = Some(numOfFiles)
  protected[delta] def domainMetadatasIfKnown: Option[Seq[DomainMetadata]] = Some(domainMetadata)
  def numDeletedRecordsOpt: Option[Long] = computedState.numDeletedRecordsOpt
  def numDeletionVectorsOpt: Option[Long] = computedState.numDeletionVectorsOpt
  def deletedRecordCountsHistogramOpt: Option[DeletedRecordCountsHistogram] =
    computedState.deletedRecordCountsHistogramOpt

  protected def deletionVectorsReadableAndMetricsEnabled: Boolean = {
    val checksumDVMetricsEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED)
    val dvsReadable = DeletionVectorUtils.deletionVectorsReadable(snapshotToScan)
    checksumDVMetricsEnabled && dvsReadable
  }

  protected def deletionVectorsReadableAndHistogramEnabled: Boolean = {
    val deletedRecordCountsHistogramEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)
    deletionVectorsReadableAndMetricsEnabled && deletedRecordCountsHistogramEnabled
  }

  /** Generate a default SnapshotState of a new table given the table metadata and the protocol. */
  protected def initialState(metadata: Metadata, protocol: Protocol): SnapshotState = {
    val deletedRecordCountsHistogramOpt = if (spark.sessionState.conf.getConf(
      DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)) {
      Some(DeletedRecordCountsHistogramUtils.emptyHistogram)
    } else None

    SnapshotState(
      sizeInBytes = 0L,
      numOfSetTransactions = 0L,
      numOfFiles = 0L,
      numOfRemoves = 0L,
      // DV metrics are initialized to Some(0) to allow incremental computation. For tables where
      // DVs are disabled, there are turned to None by the incremental computation.
      numDeletedRecordsOpt = Some(0),
      numDeletionVectorsOpt = Some(0),
      numOfMetadata = 1L,
      numOfProtocol = 1L,
      setTransactions = Nil,
      domainMetadata = Nil,
      metadata = metadata,
      protocol = protocol,
      deletedRecordCountsHistogramOpt = deletedRecordCountsHistogramOpt
    )
  }
}
