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
package io.delta.kernel.internal.hook;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.annotation.VisibleForTesting;
import io.delta.kernel.internal.compaction.LogCompactionWriter;
import io.delta.kernel.internal.fs.Path;
import java.io.IOException;

/**
 * A post-commit hook that performs inline log compaction. It merges commit JSON files over a
 * compaction interval into a single compacted JSON file.
 */
public class LogCompactionHook implements PostCommitHook {

  private final Path dataPath;
  private final Path logPath;
  private final long startVersion;
  private final long commitVersion;
  private final long minFileRetentionTimestampMillis;

  public LogCompactionHook(
      Path dataPath,
      Path logPath,
      long startVersion,
      long commitVersion,
      long minFileRetentionTimestampMillis) {
    this.dataPath = requireNonNull(dataPath, "dataPath cannot be null");
    this.logPath = requireNonNull(logPath, "logPath cannot be null");
    this.startVersion = startVersion;
    this.commitVersion = commitVersion;
    this.minFileRetentionTimestampMillis = minFileRetentionTimestampMillis;
  }

  @Override
  public void threadSafeInvoke(Engine engine) throws IOException {
    LogCompactionWriter compactionWriter =
        new LogCompactionWriter(
            dataPath, logPath, startVersion, commitVersion, minFileRetentionTimestampMillis);
    compactionWriter.writeLogCompactionFile(engine);
  }

  @Override
  public PostCommitHookType getType() {
    return PostCommitHookType.LOG_COMPACTION;
  }

  @VisibleForTesting
  public Path getDataPath() {
    return dataPath;
  }

  @VisibleForTesting
  public Path getLogPath() {
    return logPath;
  }

  @VisibleForTesting
  public long getStartVersion() {
    return startVersion;
  }

  @VisibleForTesting
  public long getCommitVersion() {
    return commitVersion;
  }

  @VisibleForTesting
  public long getMinFileRetentionTimestampMillis() {
    return minFileRetentionTimestampMillis;
  }
}
