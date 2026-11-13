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

package io.delta.flink.table.postcommit;

import io.delta.flink.table.AbstractKernelTable;
import io.delta.flink.table.ExceptionUtils;
import io.delta.flink.table.TableEventListener;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumUtils;
import io.delta.kernel.internal.checksum.ChecksumWriter;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.metrics.FileSizeHistogramResult;
import io.delta.kernel.metrics.TransactionReport;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Post-commit listener that generate checksum for the snapshot */
public class ChecksumListener implements TableEventListener {

  public static final Logger LOG = LoggerFactory.getLogger(ChecksumListener.class);

  @Override
  public void onPostCommit(
      AbstractKernelTable source, Snapshot snapshot, TransactionReport report) {
    if (source.getConf().isChecksumEnabled()) {
      // Write checksum asynchronously
      source.executeWithTiming(
          "postcommit.checksum",
          () ->
              snapshot
                  .getStatistics()
                  .getChecksumWriteMode()
                  .ifPresent(ExceptionUtils.wrap(mode -> writeChecksum(source, snapshot, mode))));
    }
  }

  /**
   * Writes the checksum unless its file-size histogram disagrees with its own file count or size,
   * which Databricks rejects (DELTA_TXN_LOG_FAILED_INTEGRITY). SIMPLE copies the in-memory CRC and
   * FULL builds on the last checksum file, so both can carry a skew; then recompute from the log.
   */
  private static void writeChecksum(
      AbstractKernelTable source, Snapshot snapshot, Snapshot.ChecksumWriteMode mode)
      throws IOException {
    Engine engine = source.getEngine();
    SnapshotImpl impl = (SnapshotImpl) snapshot;
    LogSegment logSegment = impl.getLogSegment();
    Optional<CRCInfo> inMemory =
        mode == Snapshot.ChecksumWriteMode.SIMPLE ? impl.getCurrentCrcInfo() : Optional.empty();
    CRCInfo crc =
        inMemory.isPresent() ? inMemory.get() : ChecksumUtils.computeChecksum(engine, logSegment);
    ChecksumWriter writer = new ChecksumWriter(logSegment.getLogPath());
    if (crc.getFileSizeHistogram().map(h -> isConsistent(crc, h)).orElse(true)) {
      writer.writeCheckSum(engine, crc);
      return;
    }
    LOG.error(
        "Table {}: {} checksum at version {} has a file-size histogram that disagrees with"
            + " numFiles/tableSizeBytes; recomputing it from the full log",
        source.getId(),
        mode,
        crc.getVersion());
    writer.writeCheckSum(
        engine,
        ChecksumUtils.computeChecksum(engine, withoutChecksum(logSegment))
            .withInCommitTimestamp(crc.getInCommitTimestamp()));
    // The cached snapshot still holds the skewed CRC; the next commit would build on it again.
    source.invalidateCachedSnapshot();
  }

  /** Without a checksum to build on, computeChecksum replays the whole log. */
  private static LogSegment withoutChecksum(LogSegment segment) {
    return new LogSegment(
        segment.getLogPath(),
        segment.getVersion(),
        segment.getDeltas(),
        segment.getCompactions(),
        segment.getCheckpoints(),
        segment.getDeltaFileAtEndVersion(),
        Optional.empty(),
        segment.getMaxPublishedDeltaVersion());
  }

  private static boolean isConsistent(CRCInfo crc, FileSizeHistogram histogram) {
    FileSizeHistogramResult h = histogram.captureFileSizeHistogramResult();
    return Arrays.stream(h.getFileCounts()).sum() == crc.getNumFiles()
        && Arrays.stream(h.getTotalBytes()).sum() == crc.getTableSizeBytes();
  }
}
