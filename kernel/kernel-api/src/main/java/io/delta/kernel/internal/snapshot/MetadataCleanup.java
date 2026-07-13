/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.snapshot;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.checkpoints.CheckpointInstance;
import io.delta.kernel.internal.checkpoints.Checkpointer;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.Clock;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCleanup {
  private static final Logger logger = LoggerFactory.getLogger(MetadataCleanup.class);

  private MetadataCleanup() {}

  /**
   * Delete the Delta log files (delta, checkpoint, and checksum files) that are expired according
   * to the table metadata retention settings. While deleting the log files, it makes sure that time
   * travel continues to work for all unexpired table versions.
   *
   * <p>The algorithm mirrors Delta Spark's {@code listExpiredDeltaLogs} and {@code
   * BufferingLogDeletionIterator}:
   *
   * <ol>
   *   <li>Read {@code _last_checkpoint}. If absent, return immediately — never delete without a
   *       checkpoint.
   *   <li>Compute {@code maxVersion = lastCheckpoint.version - 1}. Files at or beyond the last
   *       checkpoint version are never eligible for deletion.
   *   <li>List all commit, checkpoint, and checksum files in the log directory.
   *   <li>Pass the listing through {@link BufferingLogDeletionIterator}, which:
   *       <ul>
   *         <li>Adjusts non-monotonic file modification timestamps to preserve time-travel
   *             correctness.
   *         <li>Buffers files in a staging area and only promotes them to the delete queue when a
   *             complete checkpoint that supersedes them is encountered.
   *         <li>Gates each file on {@code effectiveMtime <= fileCutOffTime && version <=
   *             maxVersion}.
   *       </ul>
   *   <li>Delete every file yielded by the iterator.
   * </ol>
   *
   * @param engine {@link Engine} instance to use for file operations
   * @param clock {@link Clock} instance to get the current time (useful in testing)
   * @param tablePath Table location
   * @param retentionMillis Log file retention period in milliseconds
   * @return number of log files deleted
   * @throws IOException if an error occurs while deleting the log files
   */
  public static long cleanupExpiredLogs(
      Engine engine, Clock clock, Path tablePath, long retentionMillis) throws IOException {
    checkArgument(retentionMillis >= 0, "Retention period must be non-negative");

    Path logPath = new Path(tablePath, "_delta_log");
    String tableName = tablePath.getName();

    // Mirror Spark's listExpiredDeltaLogs: short-circuit when no checkpoint exists.
    Optional<Long> lastCheckpoint =
        new Checkpointer(logPath).readLastCheckpointFile(engine).map(x -> x.version);
    if (!lastCheckpoint.isPresent()) {
      logger.info(
          "[tableName={}] No checkpoint found, skipping log cleanup to avoid deleting "
              + "log files without a covering checkpoint.",
          tableName);
      return 0L;
    }

    // Files at or after the last checkpoint version are never candidates for deletion.
    long maxVersion = lastCheckpoint.get() - 1;
    long fileCutOffTime = clock.getTimeMillis() - retentionMillis;

    logger.info(
        "[tableName={}] Starting the deletion of log files older than {} (maxVersion={})",
        tableName,
        fileCutOffTime,
        maxVersion);

    // List only the file types we care about, mirroring Spark's filter in listExpiredDeltaLogs.
    CloseableIterator<FileStatus> allFiles =
        wrapEngineExceptionThrowsIO(
            () -> engine.getFileSystemClient().listFrom(FileNames.listingPrefix(logPath, 0)),
            "Listing log files for cleanup in %s",
            logPath);

    long numDeleted = 0;
    try (CloseableIterator<FileStatus> expiredLogs =
        new BufferingLogDeletionIterator(filteredIterator(allFiles), fileCutOffTime, maxVersion)) {
      while (expiredLogs.hasNext()) {
        String path = expiredLogs.next().getPath();
        if (wrapEngineExceptionThrowsIO(
            () -> engine.getFileSystemClient().delete(path),
            "Failed to delete the log file as part of the metadata cleanup %s",
            path)) {
          numDeleted++;
        }
      }
    }

    logger.info(
        "[tableName={}] Deleted {} log files older than {}", tableName, numDeleted, fileCutOffTime);
    return numDeleted;
  }

  /**
   * Returns the version of a commit, checkpoint, or checksum file. Mirrors Spark's {@code
   * getDeltaFileChecksumOrCheckpointVersion}.
   */
  static long getFileVersion(String path) {
    return FileNames.getFileVersion(new Path(path));
  }

  /**
   * Wraps the raw listing iterator to pass through only commit, checkpoint, and checksum files,
   * skipping all other file types. Mirroring Spark's {@code listExpiredDeltaLogs} filter: {@code
   * isCheckpointFile || isDeltaFile || isChecksumFile}.
   */
  private static CloseableIterator<FileStatus> filteredIterator(CloseableIterator<FileStatus> raw) {
    return new CloseableIterator<FileStatus>() {
      private FileStatus next = null;

      @Override
      public boolean hasNext() {
        while (next == null && raw.hasNext()) {
          FileStatus candidate = raw.next();
          String path = candidate.getPath();
          if (FileNames.isCommitFile(path)
              || FileNames.isCheckpointFile(path)
              || FileNames.isChecksumFile(path)) {
            next = candidate;
          }
        }
        return next != null;
      }

      @Override
      public FileStatus next() {
        FileStatus result = next;
        next = null;
        return result;
      }

      @Override
      public void close() throws IOException {
        raw.close();
      }
    };
  }

  /**
   * Mirrors Delta Spark's {@code BufferingLogDeletionIterator} in {@code
   * DeltaHistoryManager.scala}.
   *
   * <p>Wraps a sorted iterator of log {@link FileStatus} objects and yields only those that are
   * safe to delete. Safety is enforced by two invariants:
   *
   * <ol>
   *   <li><b>Checkpoint-gated flush:</b> commit and checksum files accumulate in a staging buffer
   *       ({@code maybeDeleteFiles}). They are only promoted to the output queue ({@code
   *       filesToDelete}) when a complete checkpoint that supersedes them is encountered. A file is
   *       never deleted unless a checkpoint has confirmed it is no longer needed.
   *   <li><b>Timestamp monotonicity:</b> because filesystem clocks can skew, a file's modification
   *       time may not be strictly greater than the previous file's even though its version is
   *       higher. When this is detected, the file's effective timestamp is adjusted to {@code
   *       lastFile.modificationTime + 1}. The adjusted timestamp is used for the cutoff comparison,
   *       preserving time-travel correctness.
   * </ol>
   *
   * <p>A file is eligible to be flushed only when {@code effectiveMtime <= maxTimestamp &&
   * version(file) <= maxVersion}. The {@code maxVersion} cap (= {@code lastCheckpointVersion - 1})
   * provides a hard version ceiling matching Spark's {@code threshold} in {@code
   * listExpiredDeltaLogs}.
   */
  static class BufferingLogDeletionIterator implements CloseableIterator<FileStatus> {

    private final CloseableIterator<FileStatus> underlying;
    private final long maxTimestamp;
    private final long maxVersion;

    /** Output queue: files confirmed safe to delete, dequeued one at a time by callers. */
    private final ArrayDeque<FileStatus> filesToDelete = new ArrayDeque<>();

    /**
     * Staging buffer: files not yet confirmed safe to delete. Flushed to {@code filesToDelete} only
     * when a complete checkpoint is encountered.
     */
    private final List<FileStatus> maybeDeleteFiles = new ArrayList<>();

    /** The most recently consumed file; used for timestamp-monotonicity adjustment. */
    private FileStatus lastFile = null;

    /**
     * Accumulates parts of in-progress multi-part checkpoints. Key: (version, totalParts). Value:
     * list of parts seen so far. Keyed on both to match Spark's {@code checkpointMap} and handle
     * the edge case of two different-arity multipart checkpoints at the same version.
     */
    private final Map<Long, Map<Integer, List<FileStatus>>> checkpointPartBuffer = new HashMap<>();

    BufferingLogDeletionIterator(
        CloseableIterator<FileStatus> underlying, long maxTimestamp, long maxVersion) {
      this.underlying = underlying;
      this.maxTimestamp = maxTimestamp;
      this.maxVersion = maxVersion;
      // Seed lastFile and maybeDeleteFiles with the first file, mirroring Spark's init().
      if (underlying.hasNext()) {
        lastFile = underlying.next();
        maybeDeleteFiles.add(lastFile);
      }
    }

    /**
     * Returns true if {@code file} can be deleted: its effective modification time is at or before
     * the cutoff and its version is at or below {@code maxVersion}.
     */
    private boolean shouldDeleteFile(FileStatus file) {
      return file.getModificationTime() <= maxTimestamp
          && getFileVersion(file.getPath()) <= maxVersion;
    }

    /**
     * Returns true if {@code file} requires a timestamp adjustment. This occurs when the file has a
     * higher version than {@code lastFile} but a modification time that is not strictly greater,
     * indicating clock skew. Mirrors Spark's {@code needsTimeAdjustment}.
     */
    private boolean needsTimeAdjustment(FileStatus file) {
      return lastFile != null
          && getFileVersion(lastFile.getPath()) < getFileVersion(file.getPath())
          && lastFile.getModificationTime() >= file.getModificationTime();
    }

    /**
     * Promotes the staging buffer to the output queue if the last buffered file satisfies {@code
     * shouldDeleteFile}. Always clears the buffer. Mirrors Spark's {@code flushBuffer}.
     */
    private void flushBuffer() {
      if (!maybeDeleteFiles.isEmpty()
          && shouldDeleteFile(maybeDeleteFiles.get(maybeDeleteFiles.size() - 1))) {
        filesToDelete.addAll(maybeDeleteFiles);
      }
      maybeDeleteFiles.clear();
    }

    /**
     * Advances the underlying iterator until the next complete checkpoint is processed, populating
     * {@code filesToDelete} as appropriate. Mirrors Spark's {@code queueFilesInBuffer}.
     *
     * <p>Three cases per file:
     *
     * <ul>
     *   <li>Needs timestamp adjustment: create an adjusted copy, append to buffer, keep advancing.
     *   <li>Complete checkpoint (single-part, V2, or completed multi-part): flush the buffer then
     *       append the checkpoint file(s) to the now-empty buffer and stop advancing.
     *   <li>Regular commit or checksum: append to buffer, keep advancing.
     * </ul>
     */
    private void queueFilesInBuffer() {
      boolean continueBuffering = true;
      while (continueBuffering && underlying.hasNext()) {
        FileStatus currentFile = underlying.next();

        if (needsTimeAdjustment(currentFile)) {
          // Adjust the effective timestamp to lastFile.mtime + 1 to preserve time-travel
          // anchor correctness. The adjusted FileStatus replaces the original in the buffer.
          currentFile =
              FileStatus.of(
                  currentFile.getPath(), currentFile.getSize(), lastFile.getModificationTime() + 1);
          maybeDeleteFiles.add(currentFile);
        } else if (FileNames.isCheckpointFile(currentFile.getPath()) && currentFile.getSize() > 0) {
          CheckpointInstance ci = new CheckpointInstance(currentFile.getPath());

          if (!ci.numParts.isPresent() || ci.format == CheckpointInstance.CheckpointFormat.V2) {
            // Single-part or V2 checkpoint: flush immediately.
            flushBuffer();
            maybeDeleteFiles.add(currentFile);
            continueBuffering = false;
          } else {
            // Multi-part checkpoint: accumulate until all parts are present.
            // Key on (version, expectedParts) mirroring Spark's (version -> numParts) map key.
            long version = ci.version;
            int expectedParts = ci.numParts.get();
            List<FileStatus> parts =
                checkpointPartBuffer
                    .computeIfAbsent(version, v -> new HashMap<>())
                    .computeIfAbsent(expectedParts, n -> new ArrayList<>());
            parts.add(currentFile);
            if (parts.size() == expectedParts) {
              flushBuffer();
              maybeDeleteFiles.addAll(parts);
              checkpointPartBuffer.getOrDefault(version, new HashMap<>()).remove(expectedParts);
              continueBuffering = false;
            }
          }
        } else {
          // Regular commit or checksum file: accumulate in the staging buffer.
          maybeDeleteFiles.add(currentFile);
        }

        lastFile = currentFile;
      }
    }

    @Override
    public boolean hasNext() {
      if (filesToDelete.isEmpty()) {
        queueFilesInBuffer();
      }
      return !filesToDelete.isEmpty();
    }

    @Override
    public FileStatus next() {
      if (filesToDelete.isEmpty()) {
        throw new java.util.NoSuchElementException("BufferingLogDeletionIterator is exhausted");
      }
      return filesToDelete.poll();
    }

    @Override
    public void close() throws IOException {
      underlying.close();
    }
  }
}
