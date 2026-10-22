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
import org.apache.spark.sql.delta.stats.FileSizeHistogramUtils
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{coalesce, col, collect_set, count, last, lit, sum, when}
import org.apache.spark.sql.internal.SQLConf
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

  protected def fileSizeHistogramEnabled: Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_FILE_SIZE_HISTOGRAM_ENABLED)

  protected def deletedRecordCountsHistogramEnabled: Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)

  /**
   * Whether [[aggregationsToComputeState]] computes the deletion vector metrics
   * (`numDeletedRecordsOpt` / `numDeletionVectorsOpt`, and with
   * [[deletedRecordCountsHistogramEnabled]] also `deletedRecordCountsHistogramOpt`); when it does
   * not, those aggregates are `null` and the corresponding fields end up as [[None]]. This is the
   * *writable* predicate; the DV accessors instead gate their reads on the *readable* predicate
   * ([[deletionVectorsReadableAndMetricsEnabled]]).
   */
  protected def checksumDVMetricsComputed: Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED) &&
      DeletionVectorUtils.deletionVectorsWritable(this)

  /** Whether computedState is already computed or not */
  @volatile protected var _computedStateTriggered: Boolean = false

  /** Set when accessing computedState, recording which accessor forced the reconstruction. */
  @volatile protected var lastComputedStateAccessor: String = ""

  /**
   * Runs `thunk`, the access to [[computedState]] that `accessor` is forcing, recording
   * `lastComputedStateAccessor` for logging. All callers that reach [[computedState]] should go
   * through this method.
   */
  protected def recordComputedStateAccess[T](accessor: String)(thunk: => T): T = {
    lastComputedStateAccessor = accessor
    thunk
  }

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
    val computeChecksumDVMetrics = checksumDVMetricsComputed
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

    val histogramAgg = if (fileSizeHistogramEnabled) {
      FileSizeHistogramUtils.histogramAggregate(coalesce(col("add.size"), lit(-1L)).expr)
    } else {
      lit(null).cast(FileSizeHistogram.schema)
    }

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
      "fileSizeHistogram" -> histogramAgg
    ) ++ persistentDVsAggs ++ histogramDVsAgg
  }

  /**
   * The checksum this snapshot serves its state fields from, when present. This is the snapshot's
   * own checksum, gated on [[DeltaSQLConf.FAST_QUERY_PATH_ENABLED]].
   */
  protected def checksumOptForState: Option[VersionChecksum] =
    if (spark.sessionState.conf.getConf(DeltaSQLConf.FAST_QUERY_PATH_ENABLED)) checksumOpt
    else None

  /**
   * The following is a list of convenience methods for accessing the computedState.
   *
   * Each of them first tries to answer from [[checksumOptForState]] and only falls back to
   * [[computedState]] -- which triggers the expensive aggregation over the state reconstruction --
   * when the checksum cannot serve the value. [[recordComputedStateAccess]] records which accessor
   * forced the fallback.
   */
  def sizeInBytes: Long =
    checksumOptForState.map(_.tableSizeBytes).getOrElse {
      recordComputedStateAccess("sizeInBytes") { computedState.sizeInBytes }
    }
  def numOfSetTransactions: Long =
    setTransactionsIfKnown.map(_.size.toLong).getOrElse {
      recordComputedStateAccess("numOfSetTransactions") { computedState.numOfSetTransactions }
    }
  def numOfFiles: Long =
    checksumOptForState.map(_.numFiles).getOrElse {
      recordComputedStateAccess("numOfFiles") { computedState.numOfFiles }
    }
  // The number of tombstones is not tracked by the checksum, so this always needs the state
  // reconstruction.
  def numOfRemoves: Long =
    recordComputedStateAccess("numOfRemoves") { computedState.numOfRemoves }
  def numOfMetadata: Long =
    checksumOptForState.map(_.numMetadata).getOrElse {
      recordComputedStateAccess("numOfMetadata") { computedState.numOfMetadata }
    }
  def numOfProtocol: Long =
    checksumOptForState.map(_.numProtocol).getOrElse {
      recordComputedStateAccess("numOfProtocol") { computedState.numOfProtocol }
    }
  def setTransactions: Seq[SetTransaction] =
    setTransactionsIfKnown.getOrElse {
      recordComputedStateAccess("setTransactions") { computedState.setTransactions }
    }
  def fileSizeHistogram: Option[FileSizeHistogram] =
    if (fileSizeHistogramEnabled) {
      checksumOptForState.flatMap(_.fileSizeHistogram).orElse {
        recordComputedStateAccess("fileSizeHistogram") { computedState.fileSizeHistogram }
      }
    } else None
  def domainMetadata: Seq[DomainMetadata] =
    domainMetadatasIfKnown.getOrElse {
      recordComputedStateAccess("domainMetadata") { computedState.domainMetadata }
    }

  /**
   * Returns the table size in bytes if it is cheaply available from the checksum or from
   * already-computed state, and [[None]] if answering would require state reconstruction.
   */
  protected[delta] def sizeInBytesIfKnown: Option[Long] =
    getFieldFromVersionChecksumIfKnown(c => Some(c.tableSizeBytes), sizeInBytes)

  /**
   * Returns the number of files if it is cheaply available from the checksum or from
   * already-computed state, and [[None]] if answering would require state reconstruction.
   */
  protected[delta] def numOfFilesIfKnown: Option[Long] =
    getFieldFromVersionChecksumIfKnown(c => Some(c.numFiles), numOfFiles)

  /**
   * Returns the [[SetTransaction]]s if they are already pre-computed or available via the
   * checksum, and [[None]] if answering would require state reconstruction.
   */
  protected[delta] def setTransactionsIfKnown: Option[Seq[SetTransaction]] =
    checksumOptForState
      .filter(_ => spark.conf.get(DeltaSQLConf.DELTA_READ_SET_TRANSACTIONS_FROM_CRC))
      .flatMap(_.setTransactions)
      .map { setTransactionActions =>
        recordDeltaEvent(deltaLog, "delta.snapshot.setTransactions.viaCRC")
        setTransactionActions
      }
      .orElse { if (_computedStateTriggered) Some(computedState.setTransactions) else None }

  /**
   * Returns the [[DomainMetadata]]s if they are already pre-computed or available via the
   * checksum, and [[None]] if answering would require state reconstruction.
   */
  protected[delta] def domainMetadatasIfKnown: Option[Seq[DomainMetadata]] =
    metadataDomainFromChecksumOpt.orElse {
      if (_computedStateTriggered) Some(computedState.domainMetadata) else None
    }

  /** The [[DomainMetadata]]s carried by [[checksumOptForState]], if any. */
  private lazy val metadataDomainFromChecksumOpt: Option[Seq[DomainMetadata]] =
    checksumOptForState
      .flatMap(_.domainMetadata)
      .map { domainMetadata =>
        recordDeltaEvent(deltaLog, "delta.snapshot.domainMetadata.viaCRC")
        domainMetadata
      }

  // For DV metrics (numDeletedRecordsOpt / numDeletionVectorsOpt / deletedRecordCountsHistogramOpt)
  // we only return a value for tables where DVs and DV metrics are readable, and fall back to a
  // state recomputation when the checksum cannot serve them. The "IfKnown" getters only return a
  // value when the answer is available without a fresh state reconstruction.
  def numDeletedRecordsOpt: Option[Long] = {
    if (!deletionVectorsReadableAndMetricsEnabled) return None
    checksumOptForState.flatMap(_.numDeletedRecordsOpt).orElse {
      recordComputedStateAccess("numDeletedRecordsOpt") { computedState.numDeletedRecordsOpt }
    }
  }
  def numDeletionVectorsOpt: Option[Long] = {
    if (!deletionVectorsReadableAndMetricsEnabled) return None
    checksumOptForState.flatMap(_.numDeletionVectorsOpt).orElse {
      recordComputedStateAccess("numDeletionVectorsOpt") { computedState.numDeletionVectorsOpt }
    }
  }
  def deletedRecordCountsHistogramOpt: Option[DeletedRecordCountsHistogram] = {
    if (!deletionVectorsReadableAndHistogramEnabled) return None
    checksumOptForState.flatMap(_.deletedRecordCountsHistogramOpt).orElse {
      recordComputedStateAccess("deletedRecordCountsHistogramOpt") {
        computedState.deletedRecordCountsHistogramOpt
      }
    }
  }

  /**
   * Returns `Some(fromState)` when the field is known without a fresh state reconstruction, else
   * `None`. It is known when [[checksumOptForState]] carries the field (`fromChecksum` is defined)
   * or the state is already computed; only the presence of `fromChecksum` is used -- the value, and
   * any of its own gating (e.g. DV readability), come from `fromState`.
   */
  protected def getFieldFromVersionChecksumIfKnown[T](
      fromChecksum: VersionChecksum => Option[Any],
      fromState: => T): Option[T] =
    if (checksumOptForState.flatMap(fromChecksum).isEmpty && !_computedStateTriggered) None
    else Some(fromState)

  protected[delta] def numDeletedRecordsOptIfKnown: Option[Option[Long]] =
    getFieldFromVersionChecksumIfKnown(_.numDeletedRecordsOpt, numDeletedRecordsOpt)

  protected[delta] def numDeletionVectorsOptIfKnown: Option[Option[Long]] =
    getFieldFromVersionChecksumIfKnown(_.numDeletionVectorsOpt, numDeletionVectorsOpt)

  protected[delta] def deletedRecordCountsHistogramOptIfKnown:
      Option[Option[DeletedRecordCountsHistogram]] =
    getFieldFromVersionChecksumIfKnown(
      _.deletedRecordCountsHistogramOpt, deletedRecordCountsHistogramOpt)

  protected def deletionVectorsReadableAndMetricsEnabled: Boolean = {
    val checksumDVMetricsEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED)
    val dvsReadable = DeletionVectorUtils.deletionVectorsReadable(snapshotToScan)
    checksumDVMetricsEnabled && dvsReadable
  }

  protected def deletionVectorsReadableAndHistogramEnabled: Boolean = {
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
      fileSizeHistogram =
        Option.when(fileSizeHistogramEnabled)(FileSizeHistogramUtils.emptyHistogram),
      deletedRecordCountsHistogramOpt = deletedRecordCountsHistogramOpt
    )
  }
}
