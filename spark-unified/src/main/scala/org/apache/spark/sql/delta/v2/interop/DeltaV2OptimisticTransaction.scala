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
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.spark.sql.delta.{CurrentTransactionInfo, DeltaLog, LogSegment, OptimisticTransaction, Snapshot, VersionChecksum}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint, CommitInfo, Protocol}
import org.apache.spark.sql.delta.amt.AMTCheckpointProvider
import org.apache.spark.sql.delta.hooks.{CheckpointHook, ChecksumHook, HudiConverterHook, IcebergConverterHook, PostCommitHook}
import org.apache.spark.sql.delta.util.{DeltaFileOperations, FileNames}
import io.delta.storage.commit.Commit
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import io.delta.kernel.{DataWriteContext => KernelDataWriteContext, Operation => KernelOperation, Snapshot => KernelSnapshot, Table => KernelTable, Transaction => KernelTransaction}
import io.delta.kernel.data.{Row => KernelRow}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.expressions.{Literal => KernelLiteral}
import io.delta.kernel.internal.{SnapshotImpl => KernelSnapshotImpl}
import io.delta.kernel.internal.util.{PartitionUtils => KernelPartitionUtils, Utils => KernelUtils}
import io.delta.kernel.statistics.{DataFileStatistics => KernelDataFileStatistics}
import io.delta.kernel.types.{StringType => KernelStringType}
import io.delta.kernel.utils.{CloseableIterable => KernelCloseableIterable, DataFileStatus => KernelDataFileStatus}

// scalastyle:on import.ordering.noEmptyLine
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.util.{Clock, SystemClock}

/**
 * OptimisticTransaction backed by Delta Kernel, extending
 * [[org.apache.spark.sql.delta.OptimisticTransaction]] with `deltaLog = null` (a
 * guardrail): every DeltaLog reach is either overridden by a
 * Kernel-backed seam or neutralized by skipping registration (e.g. the checkpoint and checksum
 * post-commit hooks), so any missed dependency surfaces loudly instead of silently reading
 * DeltaLog-derived state.
 */
private[v2] class DeltaV2OptimisticTransaction(
    catalogTable: Option[CatalogTable],
    // `deltaV2Snapshot` and `kernelEngine` are intentionally private, which is internal
    // implementation details we do not expose in the public API.
    private val deltaV2Snapshot: DeltaV2Snapshot,
    private val kernelEngine: KernelEngine)
  extends OptimisticTransaction(
    null.asInstanceOf[DeltaLog],
    catalogTable,
    deltaV2Snapshot) {

  /**
   * Opt in to the base null-deltaLog guardrail: this transaction legitimately has no V1 DeltaLog.
   */
  override protected def allowNullDeltaLog: Boolean = true

  /** Kernel-sourced path / conf. */
  override def dataPath: Path = deltaV2Snapshot.dataPath

  override def logPath: Path = deltaV2Snapshot.logPath

  /** No V1 deltaLog to source a Hadoop conf from, so use the session Hadoop conf. */
  // scalastyle:off deltahadoopconfiguration
  override def newDeltaHadoopConf(): Configuration = spark.sessionState.newHadoopConf()
  // scalastyle:on deltahadoopconfiguration

  /** A Kernel-backed transaction maintains no V1 incremental-commit CRC state currently. */
  override protected def computeIncrementalCommitEnabled: Boolean = false
  override protected def computeShouldVerifyIncrementalCommit: Boolean = false

  /**
   * A Kernel-backed transaction has no V1 LogSegment (its snapshot's `logSegment` is null), so
   * seed the pre-commit segment as null.
   */
  override protected def initialPreCommitLogSegment: LogSegment = null

  /**
   * Kernel snapshots have no V1 segment or commits to backfill.
   *
   * Before supporting coordinated commits or FsToCC, add Kernel recovery for interrupted
   * CC->FS downgrades to prevent gaps in the backfilled commit sequence.
   */
  override protected def maybeBackfillOnConstruction(): Unit = ()

  /** A Kernel-backed transaction has no V1 DeltaLog to source a clock from; use a system clock. */
  override def clock: Clock = new SystemClock


  /** No log store implemented, emit empty. */
  override protected def commitLogStoreClassName: String = ""
  override protected[delta] def commitLogStoreClassNameForTag: String = ""


  override def registerPostCommitHook(hook: PostCommitHook): Unit = {
    val isUnsupported =
        hook == ChecksumHook ||
        hook == CheckpointHook ||
        hook == IcebergConverterHook ||
        hook == HudiConverterHook
    if (!isUnsupported) {
      super.registerPostCommitHook(hook)
    }
  }

  /**
   * The per-table commit lock serializes concurrent same-table commits within one driver JVM; V1
   * borrows it from the DeltaLog `snapshotLock`, for which a Kernel-backed transaction has no
   * equivalent yet. Left disabled until the connector decides where to surface it.
   */
  override private[delta] def isCommitLockEnabled: Boolean = false


  /**
   * V1 checkpointing is disabled for a Kernel-backed transaction because there is no equivalent
   * Kernel implementation yet.
   */
  override protected def isCheckpointNeeded(
      committedVersion: Long, postCommitSnapshot: Snapshot): Boolean = false

  /**
   * Return None because a [[DeltaV2Snapshot]]'s checkpoint provider is unimplemented.
   */
  override protected def amtCheckpointProviderOpt: Option[AMTCheckpointProvider] = None

  /**
   * Commit-stats telemetry: the table id is sourced from the Kernel snapshot.
   */
  override protected def commitTableId: String = snapshot.metadata.id

  /**
   * Kernel validated the table's protocol when it loaded the snapshot; protocol-CHANGING commits
   * are a kernel wrapper gap and must fail loudly.
   */
  override protected def validateProtocolWrite(protocol: Protocol): Unit = {
    if (protocol != snapshot.protocol) {
      throw new UnsupportedOperationException(
        "DeltaV2OptimisticTransaction cannot commit protocol changes yet (kernel wrapper gap)")
    }
  }

  /**
   * Resolves the post-commit snapshot after a successful commit-file write.
   */
  override protected def resolvePostCommitSnapshot(
      committedVersion: Long,
      commitOpt: Option[Commit],
      newChecksumOpt: Option[VersionChecksum],
      catalogTableOpt: Option[CatalogTable],
      amtCheckpointWrittenInCommitOpt: Option[Checkpoint],
      isIdempotentRetry: Boolean): Snapshot = {
    // TODO: Use Kernel's incremental snapshot load API to build the post-commit snapshot from the
    // pre-commit snapshot plus this commit, instead of a full reload that replays the log. It is
    // supported in Rust but not yet exposed on the Java wrapper.
    val kernelPostCommitSnapshot = KernelTable
      .forPath(kernelEngine, dataPath.toString)
      .getLatestSnapshot(kernelEngine)
      .asInstanceOf[KernelSnapshotImpl]
    require(
      kernelPostCommitSnapshot.getVersion >= committedVersion,
      s"Kernel reload returned version ${kernelPostCommitSnapshot.getVersion}, older than the " +
        s"just-committed version $committedVersion")
    new DeltaV2Snapshot(kernelPostCommitSnapshot)
  }

  /**
   * Commit-IO seam: write the commit through Kernel.
   *
   * Translates staged [[Action]]s into Kernel action rows and commits through `Transaction.commit`,
   * returning a [[Commit]] at the version Kernel reports.
   */
  override protected[interop] def writeCommitFile(
      attemptVersion: Long,
      jsonActions: Iterator[String],
      currentTransactionInfo: CurrentTransactionInfo)
      : (Option[VersionChecksum], Commit, CurrentTransactionInfo) = {
    val actions = currentTransactionInfo.finalActionsToCommit
    val addFiles = new ArrayBuffer[AddFile]()
    actions.foreach {
      case a: AddFile => addFiles += a
      case _: CommitInfo => // Kernel generates its own; V1 operation provenance is an JNR gap.
      case other =>
        throw new UnsupportedOperationException(
          "DeltaV2OptimisticTransaction only supports AddFile actions yet; cannot commit action " +
            s"${other.getClass.getSimpleName} (kernel wrapper gap)")
    }

    val kernelSnapshotForCommit = KernelTable
      .forPath(kernelEngine, dataPath.toString)
      .getLatestSnapshot(kernelEngine)
    if (kernelSnapshotForCommit.getVersion != attemptVersion - 1) {
      throw new FileAlreadyExistsException(
        s"Cannot commit version $attemptVersion through Kernel: the log is already at version " +
          s"${kernelSnapshotForCommit.getVersion}; deferring to V1 conflict resolution")
    }

    val kernelTxn = kernelSnapshotForCommit
      .buildUpdateTableTransaction("DeltaV2OptimisticTransaction", KernelOperation.WRITE)
      .build(kernelEngine)
    try {
      val kernelTxnState = kernelTxn.getTransactionState(kernelEngine)
      val kernelCommitResult = kernelTxn.commit(
        kernelEngine,
        kernelAppendActionsIterable(
          addFiles, kernelTxnState, kernelSnapshotForCommit, dataPath.toString))
      val committedVersion = kernelCommitResult.getVersion
      val deltaFile = FileNames.unsafeDeltaFile(logPath, committedVersion)
      val fs = deltaFile.getFileSystem(newDeltaHadoopConf())
      val fileStatus = fs.getFileStatus(deltaFile)
      (None, new Commit(committedVersion, fileStatus, fileStatus.getModificationTime),
        currentTransactionInfo)
    } finally {
    }
  }

  /**
   * Translates staged [[AddFile]]s into iterable Kernel append-action rows.
   */
  private def kernelAppendActionsIterable(
      addFiles: Iterable[AddFile],
      kernelTxnState: KernelRow,
      kernelSnapshot: KernelSnapshot,
      dataPathStr: String): KernelCloseableIterable[KernelRow] = {
    val kernelSchema = kernelSnapshot.getSchema
    val partitionColNames = kernelSnapshot.getPartitionColumnNames.asScala
    val partitionColumnDataTypesByName = kernelSchema.fields().asScala
      .filter(f => partitionColNames.exists(_.equalsIgnoreCase(f.getName)))
      .map(f => f.getName -> f.getDataType).toMap

    def generateKernelWriteContext(
        partitionValues: Map[String, String]): KernelDataWriteContext = {
      val kernelLiteralPartitionValues: java.util.Map[String, KernelLiteral] =
        partitionValues.map { case (name, value) =>
          val kernelDataType =
            partitionColumnDataTypesByName.getOrElse(name, KernelStringType.STRING)
          name -> KernelPartitionUtils.literalForPartitionValue(kernelDataType, value)
        }.asJava
      KernelTransaction.getWriteContext(kernelEngine, kernelTxnState, kernelLiteralPartitionValues)
    }

    def kernelDataFileStatusFor(addFile: AddFile): KernelDataFileStatus =
      new KernelDataFileStatus(
        DeltaFileOperations.absolutePath(dataPathStr, addFile.path).toString,
        addFile.size,
        addFile.modificationTime,
        Option(addFile.stats).map(KernelDataFileStatistics.deserializeFromJson(_, kernelSchema))
          .getOrElse(Optional.empty[KernelDataFileStatistics]()))

    new KernelCloseableIterable[KernelRow] {
      override def iterator() = {
        val writeContextByPartition =
          new HashMap[Map[String, String], KernelDataWriteContext]()
        KernelUtils.toCloseableIterator(addFiles.iterator.asJava).flatMap {
          (addFile: AddFile) =>
            val kernelWriteContext = writeContextByPartition.getOrElseUpdate(
              addFile.partitionValues, generateKernelWriteContext(addFile.partitionValues))
            KernelTransaction.generateAppendActions(
              kernelEngine,
              kernelTxnState,
              KernelUtils.singletonCloseableIterator(kernelDataFileStatusFor(addFile)),
              kernelWriteContext)
        }
      }

      override def close(): Unit = {}
    }
  }
}
