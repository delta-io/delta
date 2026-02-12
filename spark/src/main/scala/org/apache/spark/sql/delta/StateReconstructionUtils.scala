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

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.delta.actions.{InMemoryLogReplay, SingleAction}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.functions._

/**
 * Shared utilities for the state reconstruction algorithm used by both V1 (Snapshot) and
 * V2 (DistributedLogReplayHelper).
 *
 * The state reconstruction algorithm:
 *   1. Load actions (checkpoint + deltas) with commitVersion and add_stats_to_use columns
 *   2. Add canonical path columns for add and remove actions
 *   3. Repartition by canonical path and sort by commitVersion within each partition
 *   4. Reconstruct add/remove structs with canonical paths
 *   5. Deduplicate via InMemoryLogReplay per partition (mapPartitions)
 *
 * Steps 2-4 are identical between V1 and V2 and are extracted here.
 * Step 5 is also shared via [[replayPerPartition]].
 */
object StateReconstructionUtils {

  val ADD_PATH_CANONICAL_COL_NAME = "add_path_canonical"
  val REMOVE_PATH_CANONICAL_COL_NAME = "remove_path_canonical"

  /**
   * Applies the common state reconstruction transformations (steps 2-4):
   *   - Add canonical path columns for add and remove actions
   *   - Repartition by canonical path, sort by commitVersion
   *   - Reconstruct add/remove structs with canonical paths
   *
   * After calling this method, the caller applies deduplication via [[replayPerPartition]].
   *
   * @param actions DataFrame with add/remove columns, commitVersion, and addStatsColName
   * @param numPartitions number of partitions for repartitioning
   * @param canonicalizeUdf function that takes a Column and returns a canonicalized Column
   *                        - V1 passes deltaLog.getCanonicalPathUdf()(col)
   *                        - V2 passes callUDF("canonicalizePath", col)
   * @param commitVersionCol name of the commit version column
   * @param addStatsColName name of the column holding the stats to use for add actions
   * @return DataFrame after canonicalization, repartition, sort, and struct reconstruction
   */
  def canonicalizeRepartitionAndReconstruct(
      actions: DataFrame,
      numPartitions: Int,
      canonicalizeUdf: Column => Column,
      commitVersionCol: String,
      addStatsColName: String): DataFrame = {

    // Step 2: Add canonical path columns
    // V1 ref: Snapshot.scala lines 485-488
    actions
      .withColumn(ADD_PATH_CANONICAL_COL_NAME, when(
        col("add.path").isNotNull, canonicalizeUdf(col("add.path"))))
      .withColumn(REMOVE_PATH_CANONICAL_COL_NAME, when(
        col("remove.path").isNotNull, canonicalizeUdf(col("remove.path"))))
      // Step 3: Repartition by path, sort by version
      // V1 ref: Snapshot.scala lines 489-492
      .repartition(
        numPartitions,
        coalesce(col(ADD_PATH_CANONICAL_COL_NAME), col(REMOVE_PATH_CANONICAL_COL_NAME)))
      .sortWithinPartitions(commitVersionCol)
      // Step 4: Reconstruct add/remove structs with canonical paths
      // V1 ref: Snapshot.scala lines 493-510
      .withColumn("add", when(
        col("add.path").isNotNull,
        struct(
          col(ADD_PATH_CANONICAL_COL_NAME).as("path"),
          col("add.partitionValues"),
          col("add.size"),
          col("add.modificationTime"),
          col("add.dataChange"),
          col(addStatsColName).as("stats"),
          col("add.tags"),
          col("add.deletionVector"),
          col("add.baseRowId"),
          col("add.defaultRowCommitVersion"),
          col("add.clusteringProvider")
        )))
      .withColumn("remove", when(
        col("remove.path").isNotNull,
        col("remove").withField("path", col(REMOVE_PATH_CANONICAL_COL_NAME))))
  }

  /**
   * Apply InMemoryLogReplay per partition to deduplicate actions (step 5).
   *
   * This is the core log replay logic shared by both V1 and V2. InMemoryLogReplay correctly
   * handles:
   *   - (path, deletionVector.uniqueId) as the primary key (not just path)
   *   - Add/Remove reconciliation (Remove cancels corresponding Add)
   *   - Tombstone expiry based on minFileRetentionTimestamp
   *   - SetTransaction deduplication per appId
   *   - Protocol / Metadata: latest wins
   *
   * A simple window function (row_number) is NOT sufficient because it only keeps "latest
   * row per path" and misses DV-aware dedup, add/remove cancellation, and tombstone expiry.
   *
   * @param reconstructed DataFrame from [[canonicalizeRepartitionAndReconstruct]]
   * @param minFileRetentionTimestamp tombstone expiry threshold (None to keep all)
   * @param minSetTransactionRetentionTimestamp txn expiry threshold (None to keep all)
   * @return Dataset[SingleAction] after log replay
   */
  def replayPerPartition(
      reconstructed: DataFrame,
      minFileRetentionTimestamp: Option[Long],
      minSetTransactionRetentionTimestamp: Option[Long]): Dataset[SingleAction] = {
    reconstructed
      .as[SingleAction]
      .mapPartitions { iter =>
        val state = new InMemoryLogReplay(
          minFileRetentionTimestamp,
          minSetTransactionRetentionTimestamp)
        state.append(0, iter.map(_.unwrap))
        state.checkpoint.map(_.wrap)
      }
  }
}
