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
 * Represents all actions from a single commit version in a table.
 *
 * <p><b>Important:</b> Each iterator returned by {@link #getActions()} must be closed after use to
 * release underlying resources.
 *
 * @since 4.1.0
 */
@Evolving
public interface CommitActions {

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
   * <p>Each {@link ColumnarBatch} contains actions from this commit only.
   *
   * <p>Note: All rows within all batches have the same version (returned by {@link #getVersion()}).
   *
   * <p>This method can be called multiple times, and each call returns a new iterator over the same
   * set of batches. This supports use cases like two-pass processing (e.g., validation pass
   * followed by processing pass).
   *
   * <p><b>Callers are responsible for closing each iterator returned by this method.</b> Each
   * iterator must be closed after use to release underlying resources.
   *
   * @return a {@link CloseableIterator} over columnar batches containing this commit's actions
   */
  CloseableIterator<ColumnarBatch> getActions();
}
