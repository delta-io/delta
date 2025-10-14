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

package io.delta.kernel.internal.commitrange;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Implementation of {@link CommitActions}.
 *
 * <p>This class wraps a pre-computed actions iterator along with the commit's version and
 * timestamp.
 */
public class CommitActionsImpl implements CommitActions {

  private final long version;
  private final long timestamp;
  private final CloseableIterator<ColumnarBatch> actionsIter;

  private boolean closed = false;

  /**
   * Creates a CommitActions with the given version, timestamp, and actions iterator.
   *
   * @param version the commit version
   * @param timestamp the commit timestamp
   * @param actionsIter the iterator over actions for this commit
   */
  public CommitActionsImpl(
      long version, long timestamp, CloseableIterator<ColumnarBatch> actionsIter) {
    this.version = version;
    this.timestamp = timestamp;
    this.actionsIter = requireNonNull(actionsIter, "actionsIter cannot be null");
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public synchronized CloseableIterator<ColumnarBatch> getActions() {
    if (closed) {
      throw new IllegalStateException(
          String.format("CommitActions for version %d is already closed", version));
    }
    return actionsIter;
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      closed = true;
      actionsIter.close();
    }
  }
}
