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

import java.nio.file.FileAlreadyExistsException

import org.apache.spark.sql.delta.actions.{Action, InMemoryLogReplay}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, FileNames}
import org.apache.spark.sql.delta.util.FileNames.{CompactedDeltaFile, DeltaFile}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession

/**
 * Utilities for creating log compaction files. A log compaction file aggregates the actions
 * from a range of commit files `[startVersion, endVersion]` into a single JSON file by following
 * the same action reconciliation rules as a checkpoint, but without full state reconstruction.
 *
 * Per the Delta protocol, log compaction files:
 *  - Reside in `_delta_log` and are named `<startVersion>.<endVersion>.compacted.json`.
 *  - Contain the reconciled actions for the commit range (one action per line), without
 *    `commitInfo` actions (these are stripped during reconciliation, same as checkpoints).
 *  - Are optional: writers may produce them and readers may consume them. They do not require any
 *    protocol or table-feature upgrade; readers that do not understand them simply ignore them.
 *  - Replace the corresponding individual commit files during snapshot construction on the read
 *    path, speeding it up while keeping the checkpoint interval high.
 */
object LogCompaction extends DeltaLogging {

  /** opType under which all log compaction telemetry is recorded. */
  private[delta] val OP_TYPE = "delta.logCompaction"

  // Possible values of `LogCompactionMetrics.status`.
  private[delta] val STATUS_COMPLETED = "completed"
  private[delta] val STATUS_SKIPPED = "skipped"

  // Possible values of `LogCompactionMetrics.skipReason` (only set when status is "skipped").
  private[delta] val SKIP_REASON_TARGET_EXISTS = "targetAlreadyExists"
  private[delta] val SKIP_REASON_CONCURRENT_WRITE = "concurrentlyCreated"
  private[delta] val SKIP_REASON_WINDOW_TOO_LARGE = "windowTooLarge"

  /**
   * Creates a log compaction file for the table covering commits `[startVersion, endVersion]`
   * (both inclusive). The resulting file contains the reconciled actions for the range (following
   * the same rules as checkpoint creation) without `commitInfo`.
   *
   * The individual commit files in the range are read using the path resolution of the provided
   * `snapshot`, so this works for both regular tables and coordinated-commits / catalog-managed
   * tables (where recent commits may live under `_delta_log/_staged_commits`).
   *
   * This is a no-op if a compaction file for the exact `[startVersion, endVersion]` range already
   * exists (idempotent): the common case is short-circuited by an existence check that avoids
   * redundant reconciliation, and the write itself uses `overwrite = false` so a concurrent writer
   * that produced the same (deterministic) file cannot be clobbered.
   *
   * It is also a no-op if the combined size of the window's commit files exceeds
   * `deltaLog.minorCompaction.maxWindowSizeBytes`: reconciliation happens in a single in-memory log
   * replay on the driver, so this bounds the driver memory a single compaction can consume.
   *
   * Each invocation emits a `delta.logCompaction.stats` telemetry event (see
   * [[LogCompactionMetrics]]) capturing the outcome (completed / skipped with a reason), duration,
   * number of commits and actions, and the resulting file size.
   *
   * @param deltaLog The [[DeltaLog]] instance for the table.
   * @param snapshot A snapshot at or after `endVersion`, used to resolve commit file paths.
   * @param startVersion The start version of the compaction range (inclusive).
   * @param endVersion The end version of the compaction range (inclusive).
   */
  def compact(
      deltaLog: DeltaLog,
      snapshot: Snapshot,
      startVersion: Long,
      endVersion: Long): Unit = {
    require(
      endVersion > startVersion,
      s"endVersion ($endVersion) must be greater than startVersion ($startVersion)")

    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val compactedFilePath =
      FileNames.compactedDeltaFile(deltaLog.logPath, startVersion, endVersion)
    val fs = deltaLog.logPath.getFileSystem(hadoopConf)
    val startTimeMs = System.currentTimeMillis()
    def elapsedMs: Long = System.currentTimeMillis() - startTimeMs

    // Idempotency fast path: skip the (potentially expensive) reconciliation and write if the
    // compaction file for this exact range already exists, e.g. because a concurrent writer
    // already produced it. This avoids recomputing the file in the common case; the write below
    // additionally closes the residual race window atomically via overwrite = false.
    if (fs.exists(compactedFilePath)) {
      logInfo(
        log"Skipping log compaction; file already exists " +
        log"${MDC(DeltaLogKeys.PATH, compactedFilePath)}")
      recordCompactionStats(deltaLog, LogCompactionMetrics(
        startVersion = startVersion,
        endVersion = endVersion,
        status = STATUS_SKIPPED,
        durationMs = elapsedMs,
        skipReason = Some(SKIP_REASON_TARGET_EXISTS)))
      return
    }

    // Driver-memory guard: the reconciliation below replays the whole window in a single
    // in-memory log replay on the driver (unlike checkpointing and snapshot construction, which
    // reconcile distributedly). A window containing very large commits could therefore pressure
    // driver memory, so skip compaction when the combined size of the window's commit files
    // exceeds the configured threshold. The commits remain available as individual delta files and
    // are subsumed by the next checkpoint; readers transparently fall back to them.
    val windowSizeBytes = windowCommitSizeBytes(snapshot, startVersion, endVersion)
    val maxWindowSizeBytes =
      SparkSession.active.conf.get(DeltaSQLConf.DELTALOG_MINOR_COMPACTION_MAX_WINDOW_SIZE)
    if (maxWindowSizeBytes > 0 && windowSizeBytes > maxWindowSizeBytes) {
      logInfo(
        log"Skipping log compaction for versions " +
        log"[${MDC(DeltaLogKeys.START_VERSION, startVersion)}, " +
        log"${MDC(DeltaLogKeys.END_VERSION, endVersion)}]; window size " +
        log"${MDC(DeltaLogKeys.NUM_BYTES, windowSizeBytes)} exceeds the configured maximum " +
        log"${MDC(DeltaLogKeys.MAX_SIZE, maxWindowSizeBytes)}")
      recordCompactionStats(deltaLog, LogCompactionMetrics(
        startVersion = startVersion,
        endVersion = endVersion,
        status = STATUS_SKIPPED,
        durationMs = elapsedMs,
        skipReason = Some(SKIP_REASON_WINDOW_TOO_LARGE),
        windowSizeBytes = windowSizeBytes))
      return
    }

    val fileProvider = DeltaCommitFileProvider(snapshot)
    val logReplay = new InMemoryLogReplay(
      minFileRetentionTimestamp = None,
      minSetTransactionRetentionTimestamp = None,
      // A compaction file incrementally replaces its commit range, so (unlike a checkpoint) it
      // must retain `removed = true` DomainMetadata tombstones to suppress earlier adds.
      retainDomainMetadataTombstones = true)

    (startVersion to endVersion).foreach { version =>
      val file = fileProvider.deltaFile(version)
      // `readAsIterator` returns a ClosableIterator backed by an open input stream. Keep a
      // reference to it (rather than chaining `.map`, which discards the close handle) and close
      // it explicitly so the stream is released even if reconciliation is interrupted mid-file.
      val actions = deltaLog.store.readAsIterator(file, hadoopConf)
      try {
        logReplay.append(version, actions.map(Action.fromJson))
      } finally {
        actions.close()
      }
    }

    // Count the reconciled actions as a side effect while the store consumes the iterator below.
    var numActions = 0L
    val actionsToWrite = logReplay.checkpoint.map { action =>
      numActions += 1
      action.json
    }

    // Write with overwrite = false so that, in the residual race where another writer produced the
    // same compaction file between the existence check above and this write, we don't clobber it.
    // The reconciled content for a given [startVersion, endVersion] range is deterministic, so an
    // already-present file is equivalent; a FileAlreadyExistsException is therefore treated as a
    // successful no-op rather than an error.
    try {
      deltaLog.store.write(
        path = compactedFilePath,
        actions = actionsToWrite,
        overwrite = false,
        hadoopConf = hadoopConf)
    } catch {
      case _: FileAlreadyExistsException =>
        logInfo(
          log"Skipping log compaction; file already exists " +
          log"${MDC(DeltaLogKeys.PATH, compactedFilePath)}")
        recordCompactionStats(deltaLog, LogCompactionMetrics(
          startVersion = startVersion,
          endVersion = endVersion,
          status = STATUS_SKIPPED,
          durationMs = elapsedMs,
          skipReason = Some(SKIP_REASON_CONCURRENT_WRITE)))
        return
    }

    recordCompactionStats(deltaLog, LogCompactionMetrics(
      startVersion = startVersion,
      endVersion = endVersion,
      status = STATUS_COMPLETED,
      durationMs = elapsedMs,
      numCommitsCompacted = endVersion - startVersion + 1,
      numActions = numActions,
      compactedFileSizeBytes = fs.getFileStatus(compactedFilePath).getLen,
      windowSizeBytes = windowSizeBytes))

    logInfo(
      log"Created log compaction file " +
      log"${MDC(DeltaLogKeys.PATH, compactedFilePath)} " +
      log"for versions [${MDC(DeltaLogKeys.START_VERSION, startVersion)}, " +
      log"${MDC(DeltaLogKeys.END_VERSION, endVersion)}]")
  }

  /**
   * Returns the combined size, in bytes, of the commit files for versions
   * `[startVersion, endVersion]` as seen by `snapshot`'s log segment. Used to bound the driver
   * memory consumed by reconciling the window. The sizes come from the already-listed
   * [[org.apache.hadoop.fs.FileStatus]] entries, so this adds no extra file-system calls.
   *
   * Best-effort: this assumes `snapshot.logSegment` covers `[startVersion, endVersion]`, which
   * holds on the post-commit-hook path (the window is always entirely after the checkpoint and is
   * represented by individual commit files in the segment). If called directly with a snapshot
   * whose segment does not cover the range, in-range entries may be missing and the result will
   * under-count (possibly `0`) - so the size guard fails open (compaction proceeds) rather than
   * ever wrongly tripping.
   */
  private def windowCommitSizeBytes(
      snapshot: Snapshot,
      startVersion: Long,
      endVersion: Long): Long = {
    snapshot.logSegment.deltas.iterator.collect {
      case DeltaFile(fileStatus, version) if version >= startVersion && version <= endVersion =>
        fileStatus.getLen
      case CompactedDeltaFile(fileStatus, lo, hi) if lo >= startVersion && hi <= endVersion =>
        fileStatus.getLen
    }.sum
  }

  /** Emits a single log compaction telemetry event. */
  private def recordCompactionStats(deltaLog: DeltaLog, metrics: LogCompactionMetrics): Unit =
    recordDeltaEvent(deltaLog, opType = s"$OP_TYPE.stats", data = metrics)
}

/**
 * Telemetry for a single [[LogCompaction.compact]] invocation, emitted as the JSON blob of the
 * `delta.logCompaction.stats` event.
 *
 * @param startVersion The (inclusive) start version of the compaction range.
 * @param endVersion The (inclusive) end version of the compaction range.
 * @param status `completed` if a file was written, `skipped` if it already existed.
 * @param durationMs Wall-clock time spent in `compact`, in milliseconds.
 * @param skipReason When `status == skipped`, why it was skipped (see `SKIP_REASON_*`).
 * @param numCommitsCompacted Number of commit files reconciled into the compaction file.
 * @param numActions Number of actions written to the compaction file.
 * @param compactedFileSizeBytes Size of the written compaction file, in bytes.
 * @param windowSizeBytes Combined size of the window's commit files, in bytes (the value the
 *                        driver-memory guard checks).
 */
case class LogCompactionMetrics(
    startVersion: Long,
    endVersion: Long,
    status: String,
    durationMs: Long,
    skipReason: Option[String] = None,
    numCommitsCompacted: Long = 0L,
    numActions: Long = 0L,
    compactedFileSizeBytes: Long = 0L,
    windowSizeBytes: Long = 0L)
