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

// scalastyle:off import.ordering.noEmptyLine
import java.nio.file.FileAlreadyExistsException
import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaLog, LogSegment, OptimisticTransaction, Snapshot, VersionChecksum}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, CommitInfo, Protocol}
import org.apache.spark.sql.delta.hooks.{CheckpointHook, ChecksumHook, HudiConverterHook, IcebergConverterHook}
import org.apache.spark.sql.delta.util.{DeltaFileOperations, FileNames}
import io.delta.storage.commit.Commit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import io.delta.kernel.{Operation, Table, Transaction}
import io.delta.kernel.data.{Row => KernelRow}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.util.{PartitionUtils, Utils => KernelUtils}
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types.{DataType, StringType}
import io.delta.kernel.utils.{CloseableIterable, DataFileStatus}

// scalastyle:on import.ordering.noEmptyLine
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.{Clock, SystemClock}

/**
 * OptimisticTransaction backed by Delta Kernel for the commit write.
 *
 * Extends V1 [[org.apache.spark.sql.delta.OptimisticTransaction]] directly, mirroring
 * [[DeltaV2Snapshot]] on the read side. Constructed with `deltaLog = null` (guardrail): every V1
 * DeltaLog reach on the commit lifecycle is either handled by an overridable base hook this class
 * overrides, or neutralized by unregistering the V1-only post-commit hooks in the constructor, so
 * any missed dependency surfaces loudly instead of silently using V1 state.
 *
 * Scope of this increment: blind appends of [[AddFile]] actions committed through Kernel's
 * `Transaction.commit`, single-writer. The surrounding V1 machinery (prepareCommit, retry loop,
 * post-commit install) runs unchanged. Everything else fails loudly with a "kernel wrapper gap"
 * message: RemoveFile and any non-AddFile action, protocol changes, and -- because the conflict
 * path is not wired to a Kernel log reader yet -- concurrent-writer retries. MST and coordinated
 * commits are out of scope.
 *
 * TODO: Column-mapped tables (`delta.columnMapping.mode` = `name` / `id`) are NOT yet handled.
 * `AddFile`'s `partitionValues` keys and stats
 * JSON keys are PHYSICAL names, while this class reads the Kernel LOGICAL schema, so partition
 * typing and stats deserialization both look up names that cannot match.
 */
private[v2] class DeltaV2OptimisticTransaction(
    catalogTable: Option[CatalogTable],
    val deltaV2Snapshot: DeltaV2Snapshot,
    private val engine: Engine)
  extends OptimisticTransaction(
    null.asInstanceOf[DeltaLog],
    catalogTable,
    deltaV2Snapshot) {

  private def kernelSnapshot: SnapshotImpl = deltaV2Snapshot.kernelSnapshot

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


  // Drop the post-commit hooks registered eagerly by the base constructor. They deref the
  // null deltaLog / null logSegment.
  // TODO: Will re-register these hooks when compatible with V2 path.
  unregisterPostCommitHooksWhere { hook =>
      hook == ChecksumHook ||
      hook == CheckpointHook ||
      hook == IcebergConverterHook ||
      hook == HudiConverterHook
  }

  // The Kernel commit is atomic on its own, and there is no V1 driver cache.
  override private[delta] def isCommitLockEnabled: Boolean = false


  // Commit-stats telemetry: the table id is sourced from the Kernel snapshot (no V1 deltaLog).
  override protected def commitTableId: String = snapshot.metadata.id

  // Kernel validated the table's protocol when it loaded the snapshot; protocol-CHANGING commits
  // are a kernel wrapper gap and must fail loudly.
  override protected def validateProtocolWrite(protocol: Protocol): Unit = {
    if (protocol != snapshot.protocol) {
      throw new UnsupportedOperationException(
        "DeltaV2OptimisticTransaction cannot commit protocol changes yet (kernel wrapper gap)")
    }
  }

  // Post-commit snapshot through Kernel: reload at the committed version and wrap.
  override protected def installPostCommitSnapshot(
      committedVersion: Long,
      commitOpt: Option[Commit],
      newChecksumOpt: Option[VersionChecksum],
      catalogTableOpt: Option[CatalogTable],
      amtCheckpointOpt: Option[Checkpoint],
      isIdempotentRetry: Boolean): Snapshot = {
    val postCommitKernelSnapshot = Table
      .forPath(engine, dataPath.toString)
      .getLatestSnapshot(engine)
      .asInstanceOf[SnapshotImpl]
    require(
      postCommitKernelSnapshot.getVersion >= committedVersion,
      s"Kernel reload returned version ${postCommitKernelSnapshot.getVersion}, older than the " +
        s"just-committed version $committedVersion")
    new DeltaV2Snapshot(postCommitKernelSnapshot, spark, engine)
  }

  /**
   * Commit-IO seam: write the commit through Kernel.
   *
   * Translates the transaction's staged [[AddFile]]s (grouped per partition, with full statistics)
   * to Kernel action rows and commits through `Transaction.commit`. Honors the V1 contract: commits
   * at exactly `attemptVersion` or throws [[FileAlreadyExistsException]].
   */
  override protected[interop] def writeCommitFile(
      attemptVersion: Long,
      jsonActions: Iterator[String],
      currentTransactionInfo: CurrentTransactionInfo)
      : (Option[VersionChecksum], Commit, CurrentTransactionInfo) = {
    val actions = currentTransactionInfo.finalActionsToCommit
    val kernelSnapshotForCommit = Table
      .forPath(engine, dataPath.toString)
      .getLatestSnapshot(engine)
    if (kernelSnapshotForCommit.getVersion != attemptVersion - 1) {
      throw new FileAlreadyExistsException(
        s"Cannot commit version $attemptVersion through Kernel: the log is already at version " +
          s"${kernelSnapshotForCommit.getVersion}; deferring to V1 conflict resolution")
    }

    val addFiles = actions.flatMap {
      case a: AddFile => Some(a)
      case _: CommitInfo => None // Kernel generates its own; V1 operation provenance is an FFI gap.
      case other =>
        throw new UnsupportedOperationException(
          "DeltaV2OptimisticTransaction only supports AddFile actions yet; cannot commit action " +
            s"${other.getClass.getSimpleName} (kernel wrapper gap)")
    }

    val kernelTxn = kernelSnapshotForCommit
      .buildUpdateTableTransaction("DeltaV2OptimisticTransaction", Operation.WRITE)
      .build(engine)
    try {
      val txnState = kernelTxn.getTransactionState(engine)
      val actionRows =
        generateKernelAppendActionRows(addFiles, txnState, dataPath.toString)
      val commitResult = kernelTxn.commit(
        engine,
        CloseableIterable.inMemoryIterable(
          KernelUtils.toCloseableIterator(actionRows.iterator())))
      val resultVersion = commitResult.getVersion
      if (resultVersion != attemptVersion) {
        throw new IllegalStateException(
          s"Kernel committed version $resultVersion but the transaction expected $attemptVersion")
      }
      val deltaFile = FileNames.unsafeDeltaFile(logPath, attemptVersion)
      val fs = deltaFile.getFileSystem(newDeltaHadoopConf())
      val fileStatus = fs.getFileStatus(deltaFile)
      (None, new Commit(attemptVersion, fileStatus, fileStatus.getModificationTime),
        currentTransactionInfo)
    } finally {
    }
  }

  /**
   * Groups staged [[AddFile]]s by partition and translates each group to Kernel append action rows.
   * `generateAppendActions` requires one write context per partition.
   */
  private def generateKernelAppendActionRows(
      addFiles: Iterable[AddFile],
      txnState: KernelRow,
      dataPathStr: String): java.util.ArrayList[KernelRow] = {
    val actionRows = new java.util.ArrayList[KernelRow]()
    val columnTypes = columnTypesByName
    addFiles.groupBy(_.partitionValues).foreach { case (partitionValues, group) =>
      val literalPartitionValues: java.util.Map[String, Literal] =
        partitionValues.map { case (name, value) =>
          val dataType = columnTypes.getOrElse(name, StringType.STRING)
          name -> PartitionUtils.literalForPartitionValue(dataType, value)
        }.asJava
      val writeContext = Transaction.getWriteContext(engine, txnState, literalPartitionValues)
      val dataFileStatuses = group.map { add =>
        new DataFileStatus(
          DeltaFileOperations.absolutePath(dataPathStr, add.path).toString,
          add.size,
          add.modificationTime,
          kernelStatistics(add.stats))
      }
      val rows = Transaction.generateAppendActions(
        engine,
        txnState,
        KernelUtils.toCloseableIterator(dataFileStatuses.asJava.iterator()),
        writeContext)
      try {
        while (rows.hasNext) { actionRows.add(rows.next()) }
      } finally {
        rows.close()
      }
    }
    actionRows
  }

  /**
   * The Kernel logical table schema as a name -> Kernel [[DataType]] map.
   *
   * TODO: Handle column mapping. Keys here are LOGICAL names, but `AddFile.partitionValues` is
   * keyed by PHYSICAL names.
   */
  private def columnTypesByName: Map[String, DataType] =
    kernelSnapshot.getSchema.fields().asScala.map(f => f.getName -> f.getDataType).toMap

  /**
   * Carries the FULL V1 AddFile stats JSON (numRecords/min/max/nullCount/tightBounds) into the
   * kernel add action.
   */
  private def kernelStatistics(statsJson: String): Optional[DataFileStatistics] = {
    if (statsJson == null) return Optional.empty()
    DataFileStatistics.deserializeFromJson(statsJson, kernelSnapshot.getSchema)
  }
}
