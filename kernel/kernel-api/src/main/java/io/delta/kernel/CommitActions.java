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
import io.delta.kernel.utils.CloseableIterator;

/**
 * Represents all actions from a single commit version in a Delta Lake table.
 *
 * <p>This class groups together all the actions (e.g., add, remove, metadata, protocol) that were
 * part of a single atomic commit, along with the commit's version and timestamp.
 *
 * <p>The actions iterator must be closed after use to release any underlying resources.
 *
 * @since 3.4.0
 */
@Evolving
public interface CommitActions extends AutoCloseable {

  /**
   * Returns the commit version number.
   *
   * @return the version number of this commit
   */
  long getVersion();

  /**
   * Returns the commit timestamp in milliseconds since Unix epoch.
   *
   * @return the timestamp of this commit
   */
  long getTimestamp();

  /**
   * Returns an iterator over the action batches for this commit.
   *
   * <p>Each {@link ColumnarBatch} contains actions from this commit only. The schema of each batch
   * includes the requested action columns (e.g., "add", "remove") as specified in the original
   * {@link CommitRange#getCommits} call.
   *
   * <p>Note: All rows within all batches have the same version (returned by {@link #getVersion()}).
   *
   * @return a {@link CloseableIterator} over columnar batches containing this commit's actions
   */
  CloseableIterator<ColumnarBatch> getActions();

  /**
   * Closes this CommitActions and releases any underlying resources.
   *
   * <p>This will also close the actions iterator if it hasn't been fully consumed.
   */
  @Override
  void close() throws Exception;
}
