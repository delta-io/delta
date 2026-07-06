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
  CheckpointProvider,
  DeltaColumnMappingMode,
  DeltaLogFileIndex,
  Snapshot,
  VersionChecksum
}
import org.apache.spark.sql.delta.actions.{
  AddFile,
  Metadata,
  Protocol,
  RemoveFile,
  SingleAction
}
import org.apache.spark.sql.delta.coordinatedcommits.TableCommitCoordinatorClient
import org.apache.spark.sql.delta.stats.DeltaStatsColumnSpec
import org.apache.hadoop.fs.Path
import io.delta.kernel.internal.{SnapshotImpl => KernelSnapshot}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Snapshot backed by Delta Kernel to be used in v2 Connector.
 *
 * Construction strategy:
 *   - Passes `null` for both [[Snapshot.logSegment]] and `deltaLog` (guardrail): the Kernel path
 *     has no V1 LogSegment or DeltaLog. The construction-path overrides above ensure these nulls
 *     are never dereferenced while building the snapshot.
 *   - All data/state members (metadata, protocol, allFiles, stateDF, ...) are unimplemented for
 *     now and fail loudly, so unmigrated callers cannot silently read empty V1 state.
 */
private[v2] class DeltaV2Snapshot(
    // Typed as the internal SnapshotImpl (aliased KernelSnapshot), not the public Kernel
    // `Snapshot` interface: the decoded data path is only exposed on SnapshotImpl in the
    // OSS-published Kernel. The public io.delta.kernel.Snapshot has no `getDataPath`, and its
    // `getPath` is URL-encoded, which breaks Hadoop `Path` for table roots containing spaces.
    kernelSnapshot: KernelSnapshot,
    sparkSession: SparkSession)
  extends Snapshot(
    // toString keeps this compiling across Kernel versions: getDataPath returns String in the
    // currently pinned Kernel and io.delta.kernel.internal.fs.Path in newer Kernels.
    path = new Path(kernelSnapshot.getDataPath.toString),
    version = kernelSnapshot.getVersion,
    // The Kernel-backed snapshot must not depend on the V1 LogSegment or DeltaLog. Both are null;
    // the construction-path members that would otherwise dereference them are overridden below, so
    // the nulls are never dereferenced on the scan path.
    logSegment = null,
    deltaLog = null,
    checksumOpt = None) {

  override protected def spark: SparkSession = sparkSession

  // --- logSegment/deltaLog = null guardrail: construction-path overrides ----------------------
  // These are the members dereferenced while the super `Snapshot` constructor runs (init() and the
  // "Created snapshot" logInfo at the end of the Snapshot body), plus the eager
  // tableCommitCoordinatorClientOpt base val. They must have real, V1-storage-free implementations
  // so a DeltaV2Snapshot can be constructed; everything else stays unimplemented.

  // Opt out of the base Snapshot invariant that requires a non-null logSegment/deltaLog: the
  // Kernel-backed snapshot has neither (see class doc).
  override protected def allowNullLogSegmentAndDeltaLog: Boolean = true

  // Kernel already validated protocol + feature support when it loaded the snapshot, so the V1
  // init() re-validation (which derefs the null deltaLog) is redundant and skipped.
  override protected def init(): Unit = ()

  // A Kernel-backed snapshot has no V1 commit coordinator. Override the compute hook (not the
  // `val`, whose base initializer still runs during super construction) so the lookup that derefs
  // the null deltaLog / unimplemented metadata is skipped.
  override protected def computeTableCommitCoordinatorClientOpt
      : Option[TableCommitCoordinatorClient] = None

  // The base toString references `metadata` (unimplemented here) and is evaluated eagerly by the
  // "Created snapshot" logInfo during super construction; keep it to the wired-up members only.
  override def toString: String = s"DeltaV2Snapshot(path=$path, version=$version)"

  // The "Created snapshot" logInfo reads the table id from the V1 deltaLog during super
  // construction; this Kernel-backed snapshot has none, so report an empty id.
  override protected def tableId: String = ""

  // No V1 LogSegment, so there is no backfilled-version tracking; report the sentinel rather than
  // dereferencing the null logSegment.
  override protected def initialLastKnownBackfilledVersion: Long = -1L


  // --- SnapshotDescriptor / state surface ----------------------------------------------------

  override lazy val metadata: Metadata = unimplemented

  override lazy val protocol: Protocol = unimplemented

  override def columnMappingMode: DeltaColumnMappingMode = unimplemented

  override def numOfFiles: Long = unimplemented

  override protected[delta] def numOfFilesIfKnown: Option[Long] = unimplemented

  override protected[delta] def sizeInBytesIfKnown: Option[Long] = unimplemented

  override protected[delta] lazy val deltaFileIndexOpt: Option[DeltaLogFileIndex] = unimplemented

  override lazy val checkpointProvider: CheckpointProvider = unimplemented

  // --- Files surfaced via Kernel scan ---------------------------------------------------------

  override lazy val allFiles: Dataset[AddFile] = unimplemented

  // --- StatisticsCollection ------------------------------------------------------------------

  override lazy val statsColumnSpec: DeltaStatsColumnSpec = unimplemented

  // --- V1 state-reconstruction paths: fail loudly ---------------------------------------------

  override def stateDF: DataFrame = unimplemented
  override def stateDS: Dataset[SingleAction] = unimplemented
  override def tombstones: Dataset[RemoveFile] = unimplemented
  override def computeChecksum: VersionChecksum = unimplemented

  private def unimplemented: Nothing =
    throw new UnsupportedOperationException(
      "This member is not yet supported on a Kernel-backed DeltaV2Snapshot")
}
