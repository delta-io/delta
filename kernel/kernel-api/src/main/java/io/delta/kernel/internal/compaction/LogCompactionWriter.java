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
package io.delta.kernel.internal.compaction;

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.lang.ListUtils.getLast;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility for writing out log compactions. */
public class LogCompactionWriter {

  private static final Logger logger = LoggerFactory.getLogger(LogCompactionWriter.class);

  private final Path dataPath;
  private final Path logPath;
  private final long startVersion;
  private final long endVersion;
  // We need to know after what time we can cleanup remove tombstones. This is pulled from the table
  // metadata, which we have at hook creation time in TransactionImpl, so we just store it here so
  // we can use it when we run this hook
  private final long minFileRetentionTimestampMillis;

  public LogCompactionWriter(
      Path dataPath,
      Path logPath,
      long startVersion,
      long endVersion,
      long minFileRetentionTimestampMillis) {
    this.dataPath = requireNonNull(dataPath);
    this.logPath = requireNonNull(logPath);
    this.startVersion = startVersion;
    this.endVersion = endVersion;
    this.minFileRetentionTimestampMillis = minFileRetentionTimestampMillis;
  }

  public void writeLogCompactionFile(Engine engine) throws IOException {
    Path compactedPath = FileNames.logCompactionPath(logPath, startVersion, endVersion);

    logger.info(
        "Writing log compaction file for versions {} to {} to path: {}",
        startVersion,
        endVersion,
        compactedPath);

    final long startTimeMillis = System.currentTimeMillis();
    final List<FileStatus> deltas =
        DeltaLogActionUtils.listDeltaLogFilesAsIter(
                engine,
                Collections.singleton(DeltaLogFileType.COMMIT),
                dataPath,
                startVersion,
                Optional.of(endVersion),
                false /* mustBeRecreatable */)
            .toInMemoryList();

    logger.info(
        "{}: Took {}ms to list commit files for log compaction",
        dataPath,
        System.currentTimeMillis() - startTimeMillis);

    if (deltas.size() != (endVersion - startVersion + 1)) {
      throw new IllegalArgumentException(
          String.format(
              "Asked to compact between versions %d and %d, but found %d delta files",
              startVersion, endVersion, deltas.size()));
    }

    final long lastCommitTimestamp = getLast(deltas).getModificationTime();

    LogSegment segment =
        new LogSegment(dataPath, endVersion, deltas, emptyList(), lastCommitTimestamp);
    CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(engine, segment, minFileRetentionTimestampMillis);
    wrapEngineExceptionThrowsIO(
        () -> {
          try (CloseableIterator<Row> rows = Utils.intoRows(checkpointIterator)) {
            engine.getJsonHandler().writeJsonFileAtomically(compactedPath.toString(), rows, false);
          }
          logger.info("Successfully wrote log compaction file `{}`", compactedPath);
          return null;
        },
        "Writing log compaction file `%s`",
        compactedPath);
  }

  /** Utility to determine if log compaction should run for the given commit version. */
  public static boolean shouldCompact(long commitVersion, long compactionInterval) {
    // commits start at 0, so we add one to the commit version to check if we've hit the interval
    return commitVersion > 0 && ((commitVersion + 1) % compactionInterval == 0);
  }
}
