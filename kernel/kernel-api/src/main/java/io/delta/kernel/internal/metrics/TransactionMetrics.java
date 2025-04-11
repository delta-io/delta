/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metrics;

import io.delta.kernel.internal.stats.FileSizeHistogram;
import io.delta.kernel.metrics.FileSizeHistogramResult;
import io.delta.kernel.metrics.TransactionMetricsResult;
import java.util.Optional;

/**
 * Stores the metrics for an ongoing transaction. These metrics are updated and recorded throughout
 * the transaction using this class.
 *
 * <p>At report time, we create an immutable {@link TransactionMetricsResult} from an instance of
 * {@link TransactionMetrics} to capture the metrics collected during the transaction. The {@link
 * TransactionMetricsResult} interface exposes getters for any metrics collected in this class.
 */
public class TransactionMetrics {

  /** @return a fresh TransactionMetrics object with a default fileSizeHistogram (with 0 counts) */
  public static TransactionMetrics forNewTable() {
    return new TransactionMetrics(Optional.of(FileSizeHistogram.createDefaultHistogram()));
  }

  /** @return a fresh TransactionMetrics object with an initial fileSizeHistogram as provided */
  public static TransactionMetrics withExistingFileSizeHistogram(
      Optional<FileSizeHistogram> fileSizeHistogram) {
    return new TransactionMetrics(fileSizeHistogram);
  }

  public final Timer totalCommitTimer = new Timer();

  public final Counter commitAttemptsCounter = new Counter();

  private final Counter addFilesCounter = new Counter();

  private final Counter removeFilesCounter = new Counter();

  public final Counter totalActionsCounter = new Counter();

  private final Counter addFilesSizeInBytesCounter = new Counter();

  private final Counter removeFilesSizeInBytesCounter = new Counter();

  private Optional<FileSizeHistogram> fileSizeHistogram;

  private TransactionMetrics(Optional<FileSizeHistogram> fileSizeHistogram) {
    this.fileSizeHistogram = fileSizeHistogram;
  }

  /**
   * Updates the metrics for a seen AddFile with size {@code addFileSize}. Specifically, updates
   * addFilesCounter, addFilesSizeInBytesCounter, and fileSizeHistogram. Note, it does NOT increment
   * totalActionsCounter, this needs to be done separately.
   */
  public void updateForAddFile(long addFileSize) {
    addFilesCounter.increment();
    addFilesSizeInBytesCounter.increment(addFileSize);
    fileSizeHistogram.ifPresent(histogram -> histogram.insert(addFileSize));
  }

  /**
   * Updates the metrics for a seen RemoveFile with size {@code removeFileSize}. Specifically,
   * updates removeFilesCounter, removeFilesSizeInBytesCounter, and fileSizeHistogram. Note, it does
   * NOT increment totalActionsCounter, this needs to be done separately.
   */
  public void updateForRemoveFile(long removeFileSize) {
    removeFilesCounter.increment();
    removeFilesSizeInBytesCounter.increment(removeFileSize);
    fileSizeHistogram.ifPresent(histogram -> histogram.remove(removeFileSize));
  }

  /**
   * Resets any action metrics for a failed commit to prepare them for retrying. Specifically,
   *
   * <ul>
   *   <li>Resets addFilesCounter, removeFilesCounter, totalActionsCounter,
   *       addFilesSizeInBytesCounter, and removeFilesSizeInBytesCounter to 0
   *   <li>Sets fileSizeHistogram to be empty since we don't know the updated distribution after the
   *       conflicting txn committed
   * </ul>
   *
   * Action counters / fileSizeHistogram may be partially incremented if an action iterator is not
   * read to completion (i.e. if an exception interrupts a file write). This allows us to reset the
   * counters so that we can increment them correctly from 0 on a retry.
   */
  public void resetActionMetricsForRetry() {
    addFilesCounter.reset();
    addFilesSizeInBytesCounter.reset();
    removeFilesCounter.reset();
    totalActionsCounter.reset();
    removeFilesSizeInBytesCounter.reset();
    // For now, on retry we set fileSizeHistogram = Optional.empty() because we don't know the
    // correct state of fileSizeHistogram after conflicting transaction has committed
    fileSizeHistogram = Optional.empty();
  }

  public TransactionMetricsResult captureTransactionMetricsResult() {
    return new TransactionMetricsResult() {

      final long totalCommitDurationNs = totalCommitTimer.totalDurationNs();
      final long numCommitAttempts = commitAttemptsCounter.value();
      final long numAddFiles = addFilesCounter.value();
      final long totalAddFilesSizeInBytes = addFilesSizeInBytesCounter.value();
      final long numRemoveFiles = removeFilesCounter.value();
      final long numTotalActions = totalActionsCounter.value();
      final long totalRemoveFileSizeInBytes = removeFilesSizeInBytesCounter.value();
      final Optional<FileSizeHistogramResult> fileSizeHistogramResult =
          fileSizeHistogram.map(FileSizeHistogram::captureFileSizeHistogramResult);

      @Override
      public long getTotalCommitDurationNs() {
        return totalCommitDurationNs;
      }

      @Override
      public long getNumCommitAttempts() {
        return numCommitAttempts;
      }

      @Override
      public long getNumAddFiles() {
        return numAddFiles;
      }

      @Override
      public long getNumRemoveFiles() {
        return numRemoveFiles;
      }

      @Override
      public long getNumTotalActions() {
        return numTotalActions;
      }

      @Override
      public long getTotalAddFilesSizeInBytes() {
        return totalAddFilesSizeInBytes;
      }

      @Override
      public long getTotalRemoveFilesSizeInBytes() {
        return totalRemoveFileSizeInBytes;
      }

      @Override
      public Optional<FileSizeHistogramResult> getFileSizeHistogram() {
        return fileSizeHistogramResult;
      }
    };
  }

  @Override
  public String toString() {
    return String.format(
        "TransactionMetrics(totalCommitTimer=%s, commitAttemptsCounter=%s, addFilesCounter=%s, "
            + "removeFilesCounter=%s, totalActionsCounter=%s, totalAddFilesSizeInBytes=%s,"
            + "totalRemoveFilesSizeInBytes=%s, fileSizeHistogram=%s)",
        totalCommitTimer,
        commitAttemptsCounter,
        addFilesCounter,
        removeFilesCounter,
        totalActionsCounter,
        addFilesSizeInBytesCounter,
        removeFilesSizeInBytesCounter,
        fileSizeHistogram);
  }
}
