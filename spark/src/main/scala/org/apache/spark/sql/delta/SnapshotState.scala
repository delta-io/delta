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

  /** Whether computedState is already computed or not */
  @volatile protected var _computedStateTriggered: Boolean = false

  /**
   * Memoized result of [[computedStateFromStateReconstruction]]. When populated,
   * [[computedState]] reuses this value instead of triggering the CRC fast path or running
   * another reconstruction. This preserves the original behavior where forcing the
   * reconstructed state via [[validateChecksum]] also primes [[computedState]] so subsequent
   * reads are free, and prevents repeated [[validateChecksum]] calls from re-running the
   * aggregation.
   */
  @volatile private var _reconstructedState: Option[SnapshotState] = None


  /** A map to look up transaction version by appId. */
  lazy val transactions: Map[String, Long] = setTransactions.map(t => t.appId -> t.version).toMap

  /**
   * Compute the SnapshotState of a table. Uses the stateDF from the Snapshot to extract
   * the necessary stats.
   */
  private[delta] lazy val computedState: SnapshotState = {
    val state = _reconstructedState match {
      case Some(reconstructed) =>
        // State reconstruction has already been performed (e.g. via validateChecksum). Reuse
        // its result rather than running the fast path or another reconstruction.
        reconstructed
      case None =>
        withStatusCode("DELTA", s"Compute snapshot for version: $version") {
          recordFrameProfile("Delta", "snapshot.computedState") {
            // Fast path: try to construct the SnapshotState directly from the checksum (CRC)
            // file. When the CRC has all the required fields, this avoids the expensive Spark
            // aggregation over the state reconstruction DataFrame.
            computeStateFromChecksum match {
              case Some(stateFromCrc) =>
                recordDeltaEvent(
                  deltaLog,
                  opType = "delta.snapshot.computedStateFromChecksum",
                  data = Map("version" -> version.toString))
                stateFromCrc
              case None =>
                computedStateFromStateReconstruction
            }
          }
        }
    }
    _computedStateTriggered = true
    state
  }

  /**
   * Attempt to construct a [[SnapshotState]] from the checksum file without running a Spark
   * aggregation over the state reconstruction. Returns [[Some]] only when the checksum file is
   * present and contains all the fields needed to produce a complete and correct SnapshotState
   * equivalent to what [[extractComputedState]] would have produced.
   *
   * Notes:
   *  - `numOfRemoves` (the tombstone count) is not part of the [[SnapshotState]] case class.
   *    It is exposed as a separate lazy val on [[SnapshotStateManager]] and is computed on
   *    demand from `stateDS` only if a caller actually reads it. This keeps the case class
   *    free of partial / sentinel values and preserves the fast path benefit when callers
   *    never need the tombstone count.
   *  - The checksum's `protocol` and `metadata` are cross-checked against the snapshot's
   *    resolved [[protocol]] and [[metadata]]. This is most meaningful when
   *    [[DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED]] is false: in that
   *    case the snapshot resolved its protocol/metadata independently from the log, so a
   *    mismatch here indicates CRC drift that would also invalidate the CRC's aggregate
   *    fields (`sizeInBytes`, `numFiles`, ...). When the flag is true the snapshot resolved
   *    protocol/metadata from this same CRC and the check is trivially satisfied. On a
   *    mismatch we record the drift and return [[None]] so that [[computedState]] falls back
   *    to state reconstruction: the transaction log stays the source of truth and the CRC
   *    drift is reported by [[ValidateChecksum.validateChecksum]] honoring
   *    [[DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL]], rather than making every read of
   *    this snapshot fail. The snapshot's resolved `protocol`/`metadata` are copied into the
   *    returned [[SnapshotState]] so the fast path is consistent regardless of source.
   */
  private[delta] def computeStateFromChecksum: Option[SnapshotState] = {
    if (!spark.sessionState.conf.getConf(
        DeltaSQLConf.USE_SNAPSHOT_STATE_FROM_CHECKSUM_ENABLED)) {
      return None
    }
    val checksum = checksumOpt.getOrElse { return None }

    // Required fields. The checksum is allowed to carry nulls for protocol/metadata for
    // backward compatibility with older checksum files, so guard against that explicitly.
    if (checksum.metadata == null || checksum.protocol == null) return None

    // Cross-check the checksum's protocol/metadata against the snapshot's resolved
    // protocol/metadata. On drift, fall back to state reconstruction (return None) instead of
    // failing the read: the log is the source of truth and validateChecksum reports the
    // mismatch per DELTA_CHECKSUM_MISMATCH_IS_FATAL. See the class-level doc above.
    if (checksum.protocol != protocol) {
      recordDeltaEvent(
        deltaLog,
        opType = "delta.assertions.mismatchedActionFromChecksum",
        data = Map(
          "version" -> version.toString,
          "action" -> "Protocol",
          "source" -> "Checksum",
          "checksum.protocol" -> checksum.protocol,
          "snapshot.protocol" -> protocol))
      return None
    }
    if (checksum.metadata != metadata) {
      recordDeltaEvent(
        deltaLog,
        opType = "delta.assertions.mismatchedActionFromChecksum",
        data = Map(
          "version" -> version.toString,
          "action" -> "Metadata",
          "source" -> "Checksum",
          "checksum.metadata" -> checksum.metadata,
          "snapshot.metadata" -> metadata))
      return None
    }

    // The aggregation always collects setTransactions and domainMetadata, so the fast path
    // can only run when the CRC carries both. (CRCs written by older versions or by writers
    // that don't persist these fields will gracefully fall back.)
    val setTransactionsFromCrc = checksum.setTransactions.getOrElse { return None }
    val domainMetadataFromCrc = checksum.domainMetadata.getOrElse { return None }

    // Match the predicate used by `aggregationsToComputeState` so the fast path returns the
    // same shape of optional fields the aggregation would have produced.
    val checksumDVMetricsEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CHECKSUM_DV_METRICS_ENABLED)
    val deletedRecordCountsHistogramEnabled =
      spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_DELETED_RECORD_COUNTS_HISTOGRAM_ENABLED)
    val persistentDVsOnTableSupported = DeletionVectorUtils.deletionVectorsWritable(this)
    val computeChecksumDVMetrics = checksumDVMetricsEnabled && persistentDVsOnTableSupported

    val (numDeletedRecordsOptResolved, numDeletionVectorsOptResolved) =
      if (computeChecksumDVMetrics) {
        (checksum.numDeletedRecordsOpt, checksum.numDeletionVectorsOpt) match {
          case (Some(_), Some(_)) =>
            (checksum.numDeletedRecordsOpt, checksum.numDeletionVectorsOpt)
          case _ => return None
        }
      } else {
        (None, None)
      }

    val deletedRecordCountsHistogramResolved =
      if (computeChecksumDVMetrics && deletedRecordCountsHistogramEnabled) {
        checksum.deletedRecordCountsHistogramOpt match {
          case Some(_) => checksum.deletedRecordCountsHistogramOpt
          case None => return None
        }
      } else {
        None
      }

    val fileSizeHistogramResolved =
      if (fileSizeHistogramEnabled) {
        checksum.histogramOpt match {
          case Some(_) => checksum.histogramOpt
          case None => return None
        }
      } else {
        None
      }

    Some(SnapshotState(
      sizeInBytes = checksum.tableSizeBytes,
      numOfSetTransactions = setTransactionsFromCrc.length,
      numOfFiles = checksum.numFiles,
      numDeletedRecordsOpt = numDeletedRecordsOptResolved,
      numDeletionVectorsOpt = numDeletionVectorsOptResolved,
      numOfMetadata = checksum.numMetadata,
      numOfProtocol = checksum.numProtocol,
      setTransactions = setTransactionsFromCrc,
      domainMetadata = domainMetadataFromCrc,
      metadata = metadata,
      protocol = protocol,
      fileSizeHistogram = fileSizeHistogramResolved,
      deletedRecordCountsHistogramOpt = deletedRecordCountsHistogramResolved
    ))
  }

  /**
   * Compute the [[SnapshotState]] by running an aggregation over the state reconstruction
   * DataFrame, bypassing the CRC fast path. Also performs the protocol/metadata sanity checks
   * against the snapshot's resolved protocol/metadata. Used by both [[computedState]]'s slow
   * path and by checksum validation so the latter compares the persisted checksum against an
   * independently-computed state instead of one potentially derived from the same checksum.
   *
   * The result is memoized in [[_reconstructedState]] so that repeated calls (e.g. multiple
   * [[validateChecksum]] invocations) do not re-run the aggregation, and so that a subsequent
   * read of [[computedState]] reuses this value without triggering the CRC fast path or
   * another reconstruction.
   */
  private[delta] def computedStateFromStateReconstruction: SnapshotState = synchronized {
    _reconstructedState.getOrElse {
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

      // Running state reconstruction means the snapshot now has access to fields that may
      // need to be propagated into the next CRC (e.g. setTransactions, domainMetadata). Flip
      // the trigger flag so callers like `validateChecksum` that invoke this method directly
      // -- bypassing the [[computedState]] lazy val -- still preserve the original semantics:
      // any path that materializes the reconstructed state marks it as triggered.
      _computedStateTriggered = true
      _reconstructedState = Some(_computedState)
      _computedState
    }
  }

  /**
   * Extract the SnapshotState from the provided dataframe of actions. Requires that the dataframe
   * has already been deduplicated (either through logReplay or some other method).
   */
  private[delta] def extractComputedState(stateDF: DataFrame): SnapshotState = {
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
   * The number of tombstones (RemoveFile actions) in this snapshot's state.
   *
   * This is intentionally NOT part of the [[SnapshotState]] case class so that the CRC fast
   * path ([[computeStateFromChecksum]]), which does not have access to a tombstone count,
   * can return a complete and valid [[SnapshotState]] without forcing eager state
   * reconstruction. The value is computed lazily from `stateDS` on first access. Callers that
   * never read `numOfRemoves` pay nothing extra.
   *
   * Cost depends on how `computedState` was resolved and on whether `stateDS` is cached
   * (i.e. [[DeltaSQLConf.DELTA_SNAPSHOT_CACHE_STORAGE_LEVEL]] is not `NONE`, which is the
   * default):
   *  - Slow path: state reconstruction already materialized `stateDS` while computing
   *    `computedState`, so this `count` runs against the cached data and is cheap relative to
   *    the original (now-eliminated) `count(remove)` term of the `aggregationsToComputeState`
   *    job.
   *  - Fast path: `computedState` was served from the CRC and never touched `stateDS`, so the
   *    first read of `numOfRemoves` triggers a full state reconstruction. The only production
   *    caller is checkpoint part sizing ([[Checkpoints]]), reached only when a checkpoint part
   *    size is configured or V2 checkpoints are enabled -- and that write has to scan `stateDS`
   *    regardless, so the reconstruction is not extra work. Pure readers that only need the
   *    CRC-backed fields keep the fast-path benefit and never pay this cost.
   */
  lazy val numOfRemoves: Long = {
    recordFrameProfile("Delta", "snapshot.numOfRemoves") {
      stateDS.where(col("remove").isNotNull).count()
    }
  }

  /**
   * The following is a list of convenience methods for accessing the computedState.
   */
  def sizeInBytes: Long = computedState.sizeInBytes
  def numOfSetTransactions: Long = computedState.numOfSetTransactions
  def numOfFiles: Long = computedState.numOfFiles
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
