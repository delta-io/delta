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

package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.Optional;
import java.util.Set;

/**
 * Represents a range of contiguous commits in a Delta Lake table with a defined start and end
 * version. Supports operation on the range of commits, such as reading the delta actions committed
 * in each commit in the version range.
 *
 * <p>Commit ranges are created using a {@link CommitRangeBuilder}, which supports specifying the
 * start and end boundaries of the range. The boundaries can be defined using either versions or
 * timestamps.
 *
 * @since 3.4.0
 */
@Evolving
public interface CommitRange {

  /**
   * Returns the starting version number (inclusive) of this commit range.
   *
   * @return the starting version number of the commit range
   */
  long getStartVersion();

  /**
   * Returns the ending version number (inclusive) of this commit range.
   *
   * @return the ending version number of the commit range
   */
  long getEndVersion();

  /**
   * Returns the original query boundary used to define the start boundary of this commit range.
   *
   * <p>The boundary indicates whether the range was defined using a specific version number or a
   * timestamp.
   *
   * @return an {@link Optional} containing the start boundary, or empty if the range was created
   *     with default start parameters (version 0)
   */
  Optional<CommitRangeBuilder.CommitBoundary> getQueryStartBoundary();

  /**
   * Returns the original query boundary used to define the end boundary of this commit range, if
   * available.
   *
   * <p>The boundary indicates whether the range was defined using a specific version number or a
   * timestamp.
   *
   * @return an {@link Optional} containing the end boundary, or empty if the range was created with
   *     default end parameters (latest version)
   */
  Optional<CommitRangeBuilder.CommitBoundary> getQueryEndBoundary();

  /**
   * Returns an iterator of the requested actions for the commits in this commit range.
   *
   * <p>For the returned columnar batches:
   *
   * <ul>
   *   <li>Each row within the same batch is guaranteed to have the same commit version
   *   <li>The batch commit versions are monotonically increasing
   *   <li>The top-level columns include "version", "timestamp", and the actions requested in
   *       actionSet. "version" and "timestamp" are the first and second columns in the schema,
   *       respectively. The remaining columns are based on the actions requested and each have the
   *       schema found in {@code DeltaAction.schema}.
   * </ul>
   *
   * <p>The iterator must be closed after use to release any underlying resources.
   *
   * @param engine the {@link Engine} to use for reading the Delta log files
   * @param startSnapshot the snapshot for startVersion, required to ensure the table is readable by
   *     Kernel at startVersion
   * @param actionSet the set of action types to include in the results. Only actions of these types
   *     will be returned in the iterator
   * @return a {@link CloseableIterator} over columnar batches containing the requested actions
   *     within this commit range
   * @throws IllegalArgumentException if startSnapshot.getVersion() != startVersion
   * @throws KernelException if the version range contains a version with reader protocol that is
   *     unsupported by Kernel
   */
  CloseableIterator<ColumnarBatch> getActions(
      Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet);

  /**
   * Returns an iterator of commits in this commit range, where each commit is represented as a
   * {@link CommitActions} object.
   * @param engine the {@link Engine} to use for reading the Delta log files
   * @param startSnapshot the snapshot for startVersion, required to ensure the table is readable by
   *     Kernel at startVersion
   * @param actionSet the set of action types to include in the results. Only actions of these types
   *     will be returned in each commit's actions iterator
   * @return a {@link CloseableIterator} over {@link CommitActions}, one per commit version in this
   *     range
   * @throws IllegalArgumentException if startSnapshot.getVersion() != startVersion
   * @throws KernelException if the version range contains a version with reader protocol that is
   *     unsupported by Kernel
   */
  CloseableIterator<CommitActions> getCommits(
      Engine engine, Snapshot startSnapshot, Set<DeltaLogActionUtils.DeltaAction> actionSet);
}
