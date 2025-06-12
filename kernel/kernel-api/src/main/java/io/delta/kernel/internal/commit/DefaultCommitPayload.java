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

package io.delta.kernel.internal.commit;

import io.delta.kernel.commit.CommitPayload;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;

public class DefaultCommitPayload implements CommitPayload {

  private final String logPath;
  private final long commitVersion;
  private final CloseableIterator<Row> finalizedActions;

  public DefaultCommitPayload(
      String logPath, long commitVersion, CloseableIterator<Row> finalizedActions) {
    this.logPath = logPath;
    this.commitVersion = commitVersion;
    this.finalizedActions = finalizedActions;
  }

  @Override
  public String getLogPath() {
    return logPath;
  }

  @Override
  public long getCommitVersion() {
    return commitVersion;
  }

  @Override
  public CloseableIterator<Row> getFinalizedActions() {
    return finalizedActions;
  }

  @Override
  public String toString() {
    return String.format(
        "DefaultCommitPayload{logPath='%s', commitVersion=%d}", logPath, commitVersion);
  }
}
