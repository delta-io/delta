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
package io.delta.kernel.metrics;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Optional;

/** Stores the metrics results for a {@link TransactionReport} */
@JsonPropertyOrder({
  "totalCommitDurationNs",
  "numCommitAttempts",
  "numAddFiles",
  "numRemoveFiles",
  "numTotalActions",
  "totalAddFilesSizeInBytes",
  "totalRemoveFilesSizeInBytes",
  "numDeletionVectorsDelta",
  "numDeletedRecordsDelta"
})
public interface TransactionMetricsResult {

  /** @return the total duration (ns) this transaction spent committing or trying to commit */
  long getTotalCommitDurationNs();

  /** @return the total number of commit attempts this transaction made */
  long getNumCommitAttempts();

  /**
   * @return the number of add files committed in this transaction. For a failed transaction this
   *     metric may be incomplete.
   */
  long getNumAddFiles();

  /**
   * @return the number of remove files committed in this transaction. For a failed transaction this
   *     metric may be incomplete.
   */
  long getNumRemoveFiles();

  /**
   * @return the total number of delta actions committed in this transaction. For a failed
   *     transaction this metric may be incomplete.
   */
  long getNumTotalActions();

  /**
   * @return the sum of size of added files committed in this transaction. For a failed transaction
   *     this metric may be incomplete.
   */
  long getTotalAddFilesSizeInBytes();

  /**
   * @return the sum of size of removed files committed in this transaction. For a failed
   *     transaction this metric may be incomplete.
   */
  long getTotalRemoveFilesSizeInBytes();

  /**
   * @return the net change in the number of deletion vectors committed in this transaction: the
   *     number in add files minus the number in remove files. Can be negative. For a failed
   *     transaction this metric may be incomplete.
   */
  long getNumDeletionVectorsDelta();

  /**
   * @return the net change in records invalidated by deletion vectors committed in this
   *     transaction: add-file DV cardinalities minus remove-file DV cardinalities. Can be negative.
   *     For a failed transaction this metric may be incomplete.
   */
  long getNumDeletedRecordsDelta();

  /**
   * @return the file size histogram information for the table version committed in this
   *     transaction. For a failed transaction this metric may be incomplete.
   */
  @JsonIgnore
  Optional<FileSizeHistogramResult> getTableFileSizeHistogram();
}
