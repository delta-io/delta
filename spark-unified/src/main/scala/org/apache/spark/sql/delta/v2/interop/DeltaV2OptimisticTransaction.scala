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

package org.apache.spark.sql.delta.v2.interop

import org.apache.spark.sql.delta.{
  CurrentTransactionInfo,
  DeltaLog,
  LogSegment,
  OptimisticTransaction,
  VersionChecksum
}
import io.delta.storage.commit.Commit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.{Clock, SystemClock}

/**
 * OptimisticTransaction backed by Delta Kernel.
 *
 * Extends V1 [[org.apache.spark.sql.delta.OptimisticTransaction]] directly.
 * Constructed with `deltaLog = null`: the transaction must not depend on the V1 DeltaLog,
 * so any missed V1 reach surfaces loudly instead of silently using V1 state.
 *
 * This is a skeleton: it wires up only the construction path so the transaction can be built and
 * its read state (`readVersion`, `metadata`, `protocol`, `snapshot`) resolved from the Kernel
 * snapshot. The commit lifecycle ([[writeCommitFile]] and the rest of the Kernel commit path) is
 * intentionally left unimplemented and fails loudly; it is a follow-up.
 */
private[v2] class DeltaV2OptimisticTransaction(
    catalogTable: Option[CatalogTable],
    val deltaV2Snapshot: DeltaV2Snapshot)
  extends OptimisticTransaction(
    null.asInstanceOf[DeltaLog],
    catalogTable,
    deltaV2Snapshot) {

  // Opt in to the base null-deltaLog guardrail: this transaction legitimately has no V1 DeltaLog
  override protected def allowNullDeltaLog: Boolean = true

  // Kernel-sourced path / conf.
  override def dataPath: Path = deltaV2Snapshot.dataPath

  override def logPath: Path = deltaV2Snapshot.logPath

  // No V1 deltaLog to source a Hadoop conf from, so use the session Hadoop conf.
  // scalastyle:off deltahadoopconfiguration
  override def newDeltaHadoopConf(): Configuration = spark.sessionState.newHadoopConf()
  // scalastyle:on deltahadoopconfiguration

  // A Kernel-backed transaction maintains no V1 incremental-commit CRC state currently.
  override protected def computeIncrementalCommitEnabled: Boolean = false
  override protected def computeShouldVerifyIncrementalCommit: Boolean = false

  // A Kernel-backed transaction has no V1 LogSegment (its snapshot's `logSegment` is null), so
  // seed the pre-commit segment as null
  override protected def initialPreCommitLogSegment: LogSegment = null

  // Kernel snapshots have no V1 segment or commits to backfill.
  // TODO: Before supporting coordinated commits or FsToCC, add Kernel recovery for interrupted
  // CC->FS downgrades to prevent gaps in the backfilled commit sequence.
  override protected def maybeBackfillOnConstruction(): Unit = ()

  // A Kernel-backed transaction has no V1 DeltaLog to source a clock from; use a system clock
  override def clock: Clock = new SystemClock


  /**
   * Commit-IO seam. Not implemented in this skeleton yet.
   */
  override protected[interop] def writeCommitFile(
      attemptVersion: Long,
      jsonActions: Iterator[String],
      currentTransactionInfo: CurrentTransactionInfo)
      : (Option[VersionChecksum], Commit, CurrentTransactionInfo) =
    throw new UnsupportedOperationException(
      "DeltaV2OptimisticTransaction commit is not implemented yet")
}
