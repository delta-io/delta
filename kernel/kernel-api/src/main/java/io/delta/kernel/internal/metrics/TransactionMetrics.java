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

import io.delta.kernel.metrics.TransactionMetricsResult;

/**
 * Stores the metrics for an ongoing transaction. These metrics are updated and recorded throughout
 * the transaction using this class.
 *
 * <p>At report time, we create an immutable {@link TransactionMetricsResult} from an instance of
 * {@link TransactionMetrics} to capture the metrics collected during the transaction. The {@link
 * TransactionMetricsResult} interface exposes getters for any metrics collected in this class.
 */
public class TransactionMetrics {

  public final Timer totalCommitTimer = new Timer();

  public final Counter commitAttemptsCounter = new Counter();

  public final Counter addFilesCounter = new Counter();

  public final Counter removeFilesCounter = new Counter();

  public final Counter totalActionsCounter = new Counter();

  public final Counter addFilesSizeInBytesCounter = new Counter();

  // TODO: add removeFilesSizeInBytesCounter (and to TransactionMetricsResult)

  /**
   * Resets the action counters (addFilesCounter, removeFilesCounter and totalActionsCounter) to 0.
   * Action counters may be partially incremented if an action iterator is not read to completion
   * (i.e. if an exception interrupts a file write). This allows us to reset the counters so that we
   * can increment them correctly from 0 on a retry.
   */
  public void resetActionCounters() {
    addFilesCounter.reset();
    addFilesSizeInBytesCounter.reset();
    removeFilesCounter.reset();
    totalActionsCounter.reset();
  }

  public TransactionMetricsResult captureTransactionMetricsResult() {
    return new TransactionMetricsResult() {

      final long totalCommitDurationNs = totalCommitTimer.totalDurationNs();
      final long numCommitAttempts = commitAttemptsCounter.value();
      final long numAddFiles = addFilesCounter.value();
      final long totalAddFilesSizeInBytes = addFilesSizeInBytesCounter.value();
      final long numRemoveFiles = removeFilesCounter.value();
      final long numTotalActions = totalActionsCounter.value();

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
    };
  }

  @Override
  public String toString() {
    return String.format(
        "TransactionMetrics(totalCommitTimer=%s, commitAttemptsCounter=%s, addFilesCounter=%s, "
            + "removeFilesCounter=%s, totalActionsCounter=%s, totalAddFilesSizeInBytes=%s)",
        totalCommitTimer,
        commitAttemptsCounter,
        addFilesCounter,
        removeFilesCounter,
        totalActionsCounter,
        addFilesSizeInBytesCounter);
  }
}
