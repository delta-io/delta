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

import io.delta.kernel.CommitActions;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;

/** Implementation of {@link CommitActions}. */
public class CommitActionsImpl implements CommitActions {

  private final long version;
  private final long timestamp;
  private final CloseableIterator<ColumnarBatch> actions;

  public CommitActionsImpl(long version, long timestamp, CloseableIterator<ColumnarBatch> actions) {
    this.version = version;
    this.timestamp = timestamp;
    this.actions = actions;
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
  public CloseableIterator<ColumnarBatch> getActions() {
    return actions;
  }

  @Override
  public void close() throws Exception {
    actions.close();
  }
}
