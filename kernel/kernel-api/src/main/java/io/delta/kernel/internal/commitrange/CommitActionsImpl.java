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
import java.util.function.Supplier;

/**
 * Implementation of {@link CommitActions}.
 *
 * <p>This implementation uses lazy loading: the commit file is only read when {@link #getActions()}
 * is called for the first time.
 */
public class CommitActionsImpl implements CommitActions {

  private final long version;
  private final long timestamp;
  private final Supplier<CloseableIterator<ColumnarBatch>> actionsSupplier;

  // Lazily created on first getActions() call
  private CloseableIterator<ColumnarBatch> actionsIter = null;
  private boolean closed = false;

  /**
   * Creates a CommitActions with a supplier that will be invoked lazily to create the actions
   * iterator.
   *
   * @param version the commit version
   * @param timestamp the commit timestamp
   * @param actionsSupplier supplier that creates the actions iterator when called
   */
  public CommitActionsImpl(
      long version, long timestamp, Supplier<CloseableIterator<ColumnarBatch>> actionsSupplier) {
    this.version = version;
    this.timestamp = timestamp;
    this.actionsSupplier = requireNonNull(actionsSupplier, "actionsSupplier cannot be null");
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
    if (actionsIter == null) {
      // Lazy: create iterator only on first call
      actionsIter = actionsSupplier.get();
    }
    return actionsIter;
  }

  @Override
  public synchronized void close() throws Exception {
    if (!closed) {
      closed = true;
      if (actionsIter != null) {
        // Only close if the iterator was created
        actionsIter.close();
      }
    }
  }
}
