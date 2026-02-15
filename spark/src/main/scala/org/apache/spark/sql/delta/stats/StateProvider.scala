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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.delta.{DeltaLogFileIndex, Snapshot, StateReconstructionUtils}
import org.apache.spark.sql.delta.actions.{AddFile, SingleAction}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.functions.col

/**
 * Provides raw and reconstructed state DataFrames for scan planning.
 *
 * Implementations own the full pipeline:
 *   raw source -> state reconstruction -> extract add files
 *
 * [[DefaultStateProvider]] is the shared implementation used by both V1 and V2.
 */
private[delta] trait DeltaStateProvider {

  /** Full reconstructed state as a typed Dataset (all action types). */
  def stateDS: Dataset[SingleAction]

  /** Full reconstructed state as a plain DataFrame. */
  def stateDF: DataFrame

  /** Flat AddFile rows extracted from the reconstructed state. */
  def allAddFiles: DataFrame
}

/**
 * Shared [[DeltaStateProvider]] implementation used by both V1 and V2.
 *
 * Owns the full pipeline from raw log actions to reconstructed state:
 *
 *  1. State reconstruction: loadActions -> canonicalize -> repartition -> replay
 *  2. Optional cache of full state
 *  3. Extract AddFile rows
 *
 * V1/V2 differences are captured through constructor parameters:
 *
 *  - V1: passes `Snapshot.loadActions`, `getCanonicalPathUdf`,
 *    retention timestamps, and cache factory from [[org.apache.spark.sql.delta.util.StateCache]].
 *  - V2: passes `DistributedLogReplayHelper.loadActions`,
 *    `callUDF("canonicalizePath", _)`, no retention, no caching.
 *
 * V1's `Snapshot` owns a `DefaultStateProvider` instance and delegates
 * `stateDS`, `stateDF`, `allFiles` to it. This eliminates the
 * separate `stateReconstruction` + `cachedState` chain in `Snapshot`.
 *
 * @param loadActions            Supplier for the union of checkpoint + delta
 *                               log files as a DataFrame. Must include
 *                               `commitVersion` and `add_stats_to_use` columns.
 * @param numPartitions          Number of Spark partitions for state
 *                               reconstruction (V1: config, V2: passed in)
 * @param canonicalizeUdf        UDF to canonicalize file paths
 *                               (V1: deltaLog.getCanonicalPathUdf,
 *                                V2: callUDF("canonicalizePath", _))
 * @param minFileRetentionTimestamp
 *                               Tombstone expiry threshold (V1: from config,
 *                               V2: None -- keep all)
 * @param minSetTransactionRetentionTimestamp
 *                               Transaction expiry threshold (V1: from config,
 *                               V2: None)
 * @param stateCacheFactory      Optional factory to cache the full state
 *                               Dataset[SingleAction]. V1 passes
 *                               `ds => cacheDS(ds, name).getDS`;
 *                               V2 passes None.
 */
private[delta] class DefaultStateProvider(
    loadActions: () => DataFrame,
    numPartitions: Int,
    canonicalizeUdf: Column => Column,
    minFileRetentionTimestamp: Option[Long] = None,
    minSetTransactionRetentionTimestamp: Option[Long] = None,
    stateCacheFactory: Option[Dataset[SingleAction] => Dataset[SingleAction]] = None
) extends DeltaStateProvider {

  /**
   * Full state after log replay, optionally cached.
   *
   * Pipeline: loadActions() -> canonicalize paths -> repartition by path
   *   -> sort by version -> InMemoryLogReplay per partition -> optional cache
   *
   * For V1 this replaces `Snapshot.stateReconstruction` + `cachedState`.
   * For V2 this replaces the V2 distributed log replay pipeline.
   */
  override lazy val stateDS: Dataset[SingleAction] = {
    val reconstructed = StateReconstructionUtils.canonicalizeRepartitionAndReconstruct(
      loadActions(),
      numPartitions,
      canonicalizeUdf,
      DeltaLogFileIndex.COMMIT_VERSION_COLUMN,
      Snapshot.ADD_STATS_TO_USE_COL_NAME)
    val replayed = StateReconstructionUtils.replayPerPartition(
      reconstructed,
      minFileRetentionTimestamp,
      minSetTransactionRetentionTimestamp)
    stateCacheFactory.map(_(replayed)).getOrElse(replayed)
  }

  /** DataFrame view of the full state. */
  override lazy val stateDF: DataFrame = stateDS.toDF()

  /**
   * Extract flat AddFile rows from the reconstructed state.
   * Equivalent to V1's `Snapshot.allFilesViaStateReconstruction`.
   */
  override lazy val allAddFiles: DataFrame =
    stateDS.where("add IS NOT NULL")
      .select(col("add").as[AddFile])
      .toDF
}
