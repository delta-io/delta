/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.util;

import static io.delta.kernel.internal.TableConfig.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.CommitInfo;
import io.delta.kernel.internal.actions.Metadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class InCommitTimestampUtils {

  /**
   * Returns the updated {@link Metadata} with inCommitTimestamp enablement related info (version
   * and timestamp) correctly set. This is done only 1. If this transaction enables
   * inCommitTimestamp. 2. If the commit version is not 0. This is because we only need to persist
   * the enablement info if there are non-ICT commits in the Delta log. Note: This function must
   * only be called after transaction conflicts have been resolved.
   */
  public static Optional<Metadata> getUpdatedMetadataWithICTEnablementInfo(
      Engine engine,
      long inCommitTimestamp,
      SnapshotImpl readSnapshot,
      Metadata metadata,
      long commitVersion) {
    if (didCurrentTransactionEnableICT(engine, metadata, readSnapshot) && commitVersion != 0) {
      Map<String, String> enablementTrackingProperties = new HashMap<>();
      enablementTrackingProperties.put(
          IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey(), Long.toString(commitVersion));
      enablementTrackingProperties.put(
          IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey(), Long.toString(inCommitTimestamp));

      Metadata newMetadata = metadata.withMergedConfiguration(enablementTrackingProperties);
      return Optional.of(newMetadata);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Tries to extract the inCommitTimestamp from the commitInfo action in the given ColumnarBatch.
   * When inCommitTimestamp is enabled, the commitInfo action is always the first action in the
   * delta file. This function assumes that this batch is the leading batch of a single delta file
   * and attempts to extract the commitInfo action from the first row. If the commitInfo action is
   * not present or does not contain an inCommitTimestamp, this function returns an empty Optional.
   */
  public static Optional<Long> tryExtractInCommitTimestamp(
      ColumnarBatch firstActionsBatchFromSingleDelta) {
    final int commitInfoOrdinal =
        firstActionsBatchFromSingleDelta.getSchema().indexOf("commitInfo");
    if (commitInfoOrdinal == -1) {
      return Optional.empty();
    }
    ColumnVector commitInfoVector =
        firstActionsBatchFromSingleDelta.getColumnVector(commitInfoOrdinal);
    // CommitInfo is always the first action in the batch when inCommitTimestamp is enabled.
    int expectedRowIdOfCommitInfo = 0;
    CommitInfo commitInfo =
        CommitInfo.fromColumnVector(commitInfoVector, expectedRowIdOfCommitInfo);
    return commitInfo != null ? commitInfo.getInCommitTimestamp() : Optional.empty();
  }

  /** Returns true if the current transaction implicitly/explicitly enables ICT. */
  private static boolean didCurrentTransactionEnableICT(
      Engine engine, Metadata currentTransactionMetadata, SnapshotImpl readSnapshot) {
    // If ICT is currently enabled, and the read snapshot did not have ICT enabled,
    // then the current transaction must have enabled it.
    // In case of a conflict, any winning transaction that enabled it after
    // our read snapshot would have caused a metadata conflict abort
    // (see {@link ConflictChecker.handleMetadata}), so we know that
    // all winning transactions' ICT enablement status must match the read snapshot.
    //
    // WARNING: To ensure that this function returns true if ICT is enabled during the first
    // commit, we explicitly handle the case where the readSnapshot.version is -1.
    boolean isICTCurrentlyEnabled =
        IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(currentTransactionMetadata);
    boolean wasICTEnabledInReadSnapshot =
        readSnapshot.getVersion() != -1
            && IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(readSnapshot.getMetadata());
    return isICTCurrentlyEnabled && !wasICTEnabledInReadSnapshot;
  }

  /**
   * Finds the greatest lower bound of the target value in the range [lowerBoundInclusive,
   * upperBoundExclusive) using binary search. The indexToValueMapper function is used to map the
   * index to the corresponding value. Note that this function assumes that the values are sorted in
   * ascending order and that the greatestLowerBound exists in the range.
   * @param target The target value to find the greatest lower bound for.
   * @param lowerBoundInclusive The lower bound of the search range (inclusive).
   * @param upperBoundInclusive The upper bound of the search range (inclusive).
   * @param indexToValueMapper A function that maps an index to its corresponding value.
   * @return A tuple containing the index and the value of the greatest lower bound.
   */
  public static Tuple2<Long, Long> greatestLowerBound(
      long target,
      long lowerBoundInclusive,
      long upperBoundInclusive,
      Function<Long, Long> indexToValueMapper) {
    long start = lowerBoundInclusive;
    long end = upperBoundInclusive;
    Tuple2<Long, Long> result = null;
    while (start <= end) {
      long curIndex = start + (end - start) / 2;
      long curValue = indexToValueMapper.apply(curIndex);
      if (curValue == target) {
        return new Tuple2<>(curIndex, curValue);
      } else if (curValue < target) {
        result = new Tuple2<>(curIndex, curValue);
        start = curIndex + 1;
      } else {
        end = curIndex - 1;
      }
    }
    return result;
  }

  public static Tuple2<Long, Long> getNarrowSearchBoundsUsingExponentialSearch(
          long target,
          long lowerBound,
          long upperBound,
          Function<Long, Long> indexToValueMapper) {
    long lowerBoundIdx = lowerBound;
    long upperBoundIdx = upperBound;
    long curIdx = lowerBound;
    for (long i = 1;
         curIdx < upperBound;
         curIdx = Math.round(lowerBound + (Math.pow(2, ++i) - 1))) {
      long curValue = indexToValueMapper.apply(curIdx);
      if (curValue > target) {
        upperBoundIdx = curIdx;
        break;
      } else {
        lowerBoundIdx = curIdx;
      }
    }
    return new Tuple2<>(lowerBoundIdx, upperBoundIdx);
  }

  public static Tuple2<Long, Long> getNarrowSearchBoundsUsingExponentialSearchExpandingLeft(
      long target,
      long lowerBound,
      long upperBound,
      Function<Long, Long> indexToValueMapper) {
    long lowerBoundIdx = lowerBound;
    long upperBoundIdx = upperBound;
    long curIdx = upperBound;
    for (long i = 1;
        curIdx > lowerBound;
        curIdx = Math.round(upperBound - (Math.pow(2, ++i) - 1))) {
      long curValue = indexToValueMapper.apply(curIdx);
      if (curValue <= target) {
        lowerBoundIdx = curIdx;
        break;
      } else {
        upperBoundIdx = curIdx;
      }
    }
    return new Tuple2<>(lowerBoundIdx, upperBoundIdx);
  }
}
