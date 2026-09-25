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

package org.apache.spark.sql.delta

import scala.collection.immutable.NumericRange
import scala.collection.mutable

import org.apache.spark.sql.delta.cic.IdentitySequenceServices
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.cic.IdentitySequenceService

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.cic.IdentitySequenceCoordinator
import org.apache.spark.util.Utils

/**
 * Reserves and stores concurrent-safe intervals for identity-column values inside
 * [[reservedSlots]]; the DML command rewrites each [[Expression]] to draw from them.
 *
 * Instantiated during a DML command on a table with the
 * [[ConcurrentIdentityColumnsTableFeature]] feature, and destroyed once the command finishes.
 * Constructed two ways: [[ConcurrentIdentityColumnWriteReservation.maybeReserveForWrite]] for the
 * INSERT / append path, and directly by the MERGE command for MERGE, bound to the merge
 * target's log / catalog.
 *
 * Must stay driver-only while [[identitySequenceService]] is set: do not capture the instance in
 * any closure shipped to executors.
 *
 * @param spark resolves the backend (unless `serviceOverride` is set) and supplies the configs
 *              read during reservation. Driver-only; never shipped to executors.
 * @param serviceOverride optional backend override; when omitted (`None`, the default) the only
 *              backend is resolved from `spark`. Tests inject a stub here.
 */
class IdentityColumnReservation(
    val deltaLog: DeltaLog,
    val catalog: Option[CatalogTable],
    val spark: SparkSession,
    serviceOverride: Option[IdentitySequenceService] = None) {

  /**
   * The CIC backend (the only identity backend), driver-only: never capture it in an executor
   * closure. Resolved from `spark` unless a `serviceOverride` was supplied (tests).
   */
  val identitySequenceService: Option[IdentitySequenceService] =
    serviceOverride.orElse(Some(IdentitySequenceServices.resolve(spark)))


  /** Map of column name to the reserved intervals for the identity columns. */
  def reservedSlots: Map[String, NumericRange[Long]] = _reservedSlots

  /**
   * Map of column name to the CIC service sequenceId it reserves from. Populated by
   * [[reserveValuesViaService]]. Plumbed to the executor generator so it can reserve a fresh range
   * from the driver once its initial reserved range is exhausted.
   */
  def reservedSequenceIds: Map[String, String] = _reservedSequenceIds

  /**
   * Provides the bounds for the [[PartitionIdentityValueGenerator]] of a specific identity column.
   * Shared by the INSERT and MERGE generator-construction so the
   * Returns:
   *   - `reservedEndOpt`: the inclusive end of the initial reserved range, the bound at which
   *     the generator reserves more.
   *   - `cicReserveConfig`: the reserve-more config (sequence/table scope, credential scope). The
   *     buffer sizing knobs are read on the driver by the coordinator, not here.
   * Both are None only for the degenerate empty-source slot (an empty MERGE), which carries no
   * reserved sequence.
   */
  def generatorBounds(
      columnName: String,
      reservedRange: NumericRange[Long]): (Option[Long], Option[CicReserveConfig]) = {
    val reserveMore =
      for {
        sequenceId <- reservedSequenceIds.get(columnName)
        tableId <- _reservedServiceTableId
      } yield CicReserveConfig(
        sequenceId,
        tableId)
    // The bound is set iff there is a reserve-more config (service backend with a reserved
    // sequence).
    val reservedEndOpt = reserveMore.map(_ => reservedRange.end)
    // `(None, None)` only for the degenerate empty-source slot (an empty MERGE: `numRows <= 0`).
    // That slot is registered without a reserved sequence, so the for-comprehension above yields
    // None and the generator gets no reserve-more bound; harmless since no rows ever flow from it.
    (reservedEndOpt, reserveMore)
  }

  /** Mutable version of [[reservedSlots]] for safe internal and concurrent usage. */
  private var _reservedSlots: Map[String, NumericRange[Long]] = Map.empty

  /** Mutable version of [[reservedSequenceIds]]. */
  private var _reservedSequenceIds: Map[String, String] = Map.empty

  /**
   * The resolved service table id the last reservation reserved under
   * ([[ConcurrentIdentityColumnSchema.sequenceServiceTableId]]); plumbed to the executor
   * generator together with [[reservedSequenceIds]] so a mid-write reserve-more carries
   * the same `(tableId, sequenceId)` scope as the original reservation. None until a
   * service reservation ran.
   */
  private var _reservedServiceTableId: Option[String] = None

  /**
   * Prepares this reservation to serve `numRows` identity values per identity column on the
   * target table, recording per-column state in [[_reservedSlots]]. Values
   * are not reserved up front here; each executor task reserves its own range from the
   * coordinator on demand (see [[reserveValuesViaService]]).
   *
   * If prior reservation state already covers `numRows`, the call is a no-op; otherwise stale
   * state is dropped before the next service call. No retry today (a single RPC per column;
   * transient failures surface to the caller); retry is future work.
   * @param numRows the number of values the caller expects to write.
   */
  def reserveValuesForIdentityColumns(numRows: Long): Unit = {
    // Callers must gate on hasIdentityColumn, not just feature support
    if (Utils.isTesting) {
      assert(
        ColumnWithDefaultExprUtils.hasIdentityColumn(
          deltaLog.unsafeVolatileSnapshot.metadata.schema),
        "reserveValuesForIdentityColumns called on a table with no identity column")
    }

    if (_reservedSlots.nonEmpty) {
      val allIntervalsValid = _reservedSlots.forall {
        // math.abs so a descending (negative-step) range, whose end < start, is measured by
        // magnitude; otherwise the slot would never be reused and every call re-reserves.
        case (_, range) => math.abs(range.end - range.start) >= numRows
      }
      if (allIntervalsValid) {
        return
      }
      // This may drop unused values. Gaps in identity columns does not go against the protocol but
      // may provide a bad user experience.
      _reservedSlots = Map.empty
    }

    // Kill switch: blocks every identity-generating write to a service-backed table.
    if (!spark.conf.get(DeltaSQLConf.CONCURRENT_IDENTITY_COLUMN_ENABLED)) {
      throw ConcurrentIdentityColumnErrors.concurrentIdentityColumnsDisabled(
        operation = "write to", tableId = deltaLog.update().metadata.id)
    }


    val seqService = identitySequenceService.getOrElse {
      // Every CIC reserver supplies the service unconditionally; None means a new caller
      // forgot to wire it, not a routing choice (the legacy metadata-domain backend is gone).
      throw SparkException.internalError(
        "Concurrent identity column reservation requires an IdentitySequenceService.")
    }
    reserveValuesViaService(seqService, numRows)
  }

  /**
   * Service-backed reservation setup; the identity sequence service is authoritative for the
   * high-water mark. This does NOT reserve values up front: each executor task reserves its own
   * first range from the coordinator on demand (cold start). It only registers the coordinator
   * endpoint and records, per identity column, the sequence scope the executor generator needs to
   * reserve-more.
   *
   *   1. Refresh the snapshot.
   *   2. For each identity column, read the sequenceId from schema metadata
   *      ([[ConcurrentIdentityColumnSchema.SEQUENCE_ID]]); fail loud
   *      if absent (CREATE TABLE or the feature opt-in conversion must have stamped it).
   *   3. Register the coordinator endpoint ([[IdentitySequenceCoordinator.ensureDriverEndpoint]]).
   *   4. Populate [[_reservedSequenceIds]] / [[_reservedServiceTableId]] (the reserve-more scope)
   *      and a degenerate [[_reservedSlots]] entry per column (its range end is unused on the
   *      reserve-more path). Sequence existence and step are validated by the first executor
   *      reserve, not here.
   */
  private def reserveValuesViaService(seqService: IdentitySequenceService, numRows: Long): Unit = {
    val snapshot = deltaLog.update()
    if (numRows <= 0L) {
      // No identity values are needed (e.g. an empty MERGE source). The service contract
      // requires count > 0, so don't call reserveIds; instead register a degenerate
      // single-point slot per identity column, so the MERGE generator rewrite and the
      // write-path guards see a populated slot. No rows flow, so no value is ever emitted
      // from it.
      _reservedSlots = IdentityColumn.getIdentityColumns(snapshot.metadata.schema).map { col =>
        val info = IdentityColumn.getIdentityInfo(col)
        col.name -> NumericRange.inclusive(info.start, info.start, info.step)
      }.toMap
      _reservedSequenceIds = Map.empty
      _reservedServiceTableId = None
      return
    }
    // metadata.id is used ONLY in the diagnostic conversionIncomplete error below, never as a
    // service key.
    val metadataId = snapshot.metadata.id
    // The table scope every service call is keyed by, resolved via the same resolver
    // registration used ([[ConcurrentIdentityColumnSchema.sequenceServiceTableId]]): the UC
    // table id for catalog-owned tables, the metadata.id only as a TEST-ONLY fallback for
    // path-based tables and the local stub.
    val serviceTableId = ConcurrentIdentityColumnSchema.sequenceServiceTableId(snapshot.metadata)
    val newSlots = mutable.Map.empty[String, NumericRange[Long]]
    val newSequenceIds = mutable.Map.empty[String, String]
      // Register the coordinator endpoint before the write job launches, so executor tasks that
      // exhaust their reserved range can look it up and reserve a fresh one.
      IdentitySequenceCoordinator.ensureDriverEndpoint(seqService)
      // Pre-scan: collect (column, identityInfo, sequenceId) for every identity column up
      // front, failing if any column is missing concurrent schema metadata.
      // Keeps the setup all-or-nothing (a missing stamp on the second column
      // must not leave the first column half-registered).
      val columnsToReserve = IdentityColumn.getIdentityColumns(snapshot.metadata.schema)
        .map { col =>
          val sequenceId = ConcurrentIdentityColumnSchema.getSequenceId(col).getOrElse {
            throw ConcurrentIdentityColumnErrors.conversionIncomplete(col.name, metadataId)
          }
          (col, IdentityColumn.getIdentityInfo(col), sequenceId)
        }
      // No up-front reserve: record only the sequence scope the reserve-more generator needs,
      // plus a degenerate slot (range end unused on the reserve-more path). Each executor
      // reserves its own first range from the coordinator on demand.
      columnsToReserve.foreach { case (col, identityInfo, sequenceId) =>
        val slot =
          NumericRange.inclusive(identityInfo.start, identityInfo.start, identityInfo.step)
        newSlots(col.name) = slot
        newSequenceIds(col.name) = sequenceId
      }
      _reservedSlots = newSlots.toMap
      _reservedSequenceIds = newSequenceIds.toMap
      _reservedServiceTableId = Some(serviceTableId)
  }

}
