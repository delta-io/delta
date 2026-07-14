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
import org.apache.spark.sql.delta.stats.{DeltaStatsColumnSpec, StatisticsCollection}
import org.apache.hadoop.fs.Path
import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.{SnapshotImpl => KernelSnapshot}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * A [[Snapshot]] backed by a Delta Kernel snapshot, for the v2 Connector. It extends the V1
 * [[Snapshot]] so the existing data-skipping read path (`filesForScan`) works unchanged, reading
 * metadata / protocol / allFiles from Kernel.
 *
 * `logSegment` and `deltaLog` are both `null` -- this snapshot has no V1 log state -- so every
 * member the construction or scan path would dereference them through is overridden below. V1 state
 * reconstruction (`stateDF` etc.) is not wired to Kernel yet and throws.
 */
private[v2] class DeltaV2Snapshot(
    // Typed as the internal SnapshotImpl (aliased KernelSnapshot), not the public Kernel
    // `Snapshot` interface: the decoded data path is only exposed on SnapshotImpl in the
    // OSS-published Kernel. The public io.delta.kernel.Snapshot has no `getDataPath`, and its
    // `getPath` is URL-encoded, which breaks Hadoop `Path` for table roots containing spaces.
    kernelSnapshot: KernelSnapshot,
    sparkSession: SparkSession,
    // Kernel engine for reading scan files and the commit timestamp -- owned and passed by the
    // connector. The secondary constructor below builds a default one for callers without a Kernel
    // engine of their own.
    // The separating comma lives inside the edge block: it is only needed when the edge-only
    // catalogTableOpt parameter follows. Without it, the OSS export would strip catalogTableOpt and
    // leave a trailing comma that scalac accepts but scalastyle's parser rejects.
    kernelEngine: Engine
  )
  extends Snapshot(
    // toString keeps this compiling across Kernel versions: getDataPath returns String in the
    // currently pinned Kernel and io.delta.kernel.internal.fs.Path in newer Kernels.
    path = new Path(kernelSnapshot.getDataPath.toString),
    version = kernelSnapshot.getVersion,
    // A DeltaV2Snapshot must not depend on the V1 LogSegment or DeltaLog. Both are null; the
    // construction-path members that would otherwise dereference them are overridden below, so
    // the nulls are never dereferenced on the scan path.
    logSegment = null,
    deltaLog = null,
    checksumOpt = None) {

  override protected def spark: SparkSession = sparkSession

  /** Convenience for callers without their own Kernel engine: builds a fresh DefaultEngine. */
  // scalastyle:off deltahadoopconfiguration
  // No DeltaLog to source a Hadoop conf from, so use the session Hadoop conf for the engine.
  def this(kernelSnapshot: KernelSnapshot, sparkSession: SparkSession) =
    this(kernelSnapshot, sparkSession,
      DefaultEngine.create(sparkSession.sessionState.newHadoopConf()))
  // scalastyle:on deltahadoopconfiguration

  // --- logSegment/deltaLog = null guardrail: construction-path overrides ----------------------
  // These are the members dereferenced while the super `Snapshot` constructor runs (init() and the
  // "Created snapshot" logInfo at the end of the Snapshot body), plus the eager
  // tableCommitCoordinatorClientOpt base val. They must have real, V1-storage-free implementations
  // so a DeltaV2Snapshot can be constructed.

  // Opt out of the base Snapshot invariant that requires a non-null logSegment/deltaLog: a
  // DeltaV2Snapshot has neither (see class doc).
  override protected def allowNullLogSegmentAndDeltaLog: Boolean = true

  // Kernel already validated protocol + feature support when it loaded the snapshot, so the V1
  // init() re-validation (which derefs the null deltaLog) is redundant and skipped.
  override protected def init(): Unit = ()

  // A DeltaV2Snapshot has no V1 commit coordinator. Override the compute hook (not the `val`, whose
  // base initializer still runs during super construction) so the lookup that derefs the null
  // deltaLog / unimplemented metadata is skipped.
  override protected def computeTableCommitCoordinatorClientOpt
      : Option[TableCommitCoordinatorClient] = None

  // The base toString references `metadata` and is evaluated eagerly by the "Created snapshot"
  // logInfo during super construction; keep it to the wired-up members only.
  override def toString: String = s"DeltaV2Snapshot(path=$path, version=$version)"

  // The Delta table id (V1 sources the same value via deltaLog.unsafeVolatileTableId). The eager
  // "Created snapshot" logInfo reads tableId during super construction, before the `kernelSnapshot`
  // field is assigned; report an empty id then, the real id afterwards.
  override protected def tableId: String = if (kernelSnapshot == null) "" else metadata.id

  // V1 seeds this from `logSegment.lastBackfilledVersionInSegment`, but it is only read on the
  // commit / coordinated-commit paths (backfill hints, table-feature downgrade), never on the
  // read path a DeltaV2Snapshot exercises. Report the uninitialized sentinel rather than reaching
  // into a LogSegment we do not have.
  override protected def initialLastKnownBackfilledVersion: Long = -1L

  // The table's data path, already known from Kernel (passed as `path`). Not edge-guarded: it
  // overrides the base Snapshot.dataPath (`deltaLog.dataPath`), which exists in OSS too and would
  // dereference the null deltaLog.
  override def dataPath: Path = path


  // --- SnapshotDescriptor / state surface ----------------------------------------------------

  override lazy val metadata: Metadata =
    DeltaV2SnapshotConversionsUtils.metadataFromKernel(kernelSnapshot.getMetadata)

  override lazy val protocol: Protocol =
    DeltaV2SnapshotConversionsUtils.protocolFromKernel(kernelSnapshot.getProtocol)

  override def columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode

  override def numOfFiles: Long = numOfFilesIfKnown.getOrElse(allFiles.count())

  override protected[delta] def numOfFilesIfKnown: Option[Long] = {
    None
  }

  // Kernel does not surface a cheap total table size, so report None; the data-skipping path
  // derives the scanned size from per-file sizes instead.
  override protected[delta] def sizeInBytesIfKnown: Option[Long] = None

  // No V1 commit-file index; not used by the Kernel scan path.
  override protected[delta] lazy val deltaFileIndexOpt: Option[DeltaLogFileIndex] = unimplemented

  // No V1 checkpoint provider; not used by the Kernel scan path.
  override lazy val checkpointProvider: CheckpointProvider = unimplemented

  // V1 reads these from logSegment; report zero.
  override def numDeltaFiles: Long = 0L
  override def totalDeltaFilesByteSize: Long = 0L

  // Commit timestamp of this snapshot's version, sourced from Kernel: the ICT when in-commit
  // timestamps are enabled, otherwise the commit file's modification time -- the same semantics as
  // the V1 `getInCommitTimestampOpt.getOrElse(logSegment.lastCommitFileModificationTimestamp)`,
  // which a DeltaV2Snapshot cannot use directly because it has no V1 LogSegment.
  override def timestamp: Long = kernelSnapshot.getTimestamp(kernelEngine)

  // --- Files surfaced via Kernel scan ---------------------------------------------------------

  override lazy val allFiles: Dataset[AddFile] =
    KernelSnapshotUtils.buildAllFiles(kernelSnapshot, sparkSession, kernelEngine)

  // --- StatisticsCollection ------------------------------------------------------------------

  override lazy val statsColumnSpec: DeltaStatsColumnSpec =
    StatisticsCollection.configuredDeltaStatsColumnSpec(metadata)


  // --- V1 state-reconstruction paths: not yet used --------------------------------------------
  // The DeltaV2 read path sources its files from Kernel (`allFiles`), not from V1 log-replay state
  // reconstruction, so these members are not exercised by the data-skipping path today. They throw
  // (rather than return empty V1 state) so that an unmigrated caller which does reach one fails
  // loudly instead of silently reading wrong data.
  override def stateDF: DataFrame = unimplemented
  override def stateDS: Dataset[SingleAction] = unimplemented
  override def tombstones: Dataset[RemoveFile] = unimplemented
  override def computeChecksum: VersionChecksum = unimplemented

  private def unimplemented: Nothing =
    throw new UnsupportedOperationException(
      "This member is not yet supported on a Kernel-backed DeltaV2Snapshot")
}
