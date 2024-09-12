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
package io.delta.kernel.internal.snapshot;

import static io.delta.kernel.internal.TableConfig.ENABLE_EXPIRED_LOG_CLEANUP;
import static io.delta.kernel.internal.TableConfig.LOG_RETENTION;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataCleanup {
  private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

  public MetadataCleanup() {}

  public static void doLogCleanup(Engine engine, Path tablePath, Metadata metadata)
      throws IOException {
    Boolean enableExpireLogCleanup = ENABLE_EXPIRED_LOG_CLEANUP.fromMetadata(engine, metadata);
    if (!enableExpireLogCleanup) {
      logger.info(
          "{}: Log cleanup is disabled. Skipping the deletion of expired log files", tablePath);
      return;
    }

    List<String> potentialLogFilesToDelete = new ArrayList<>();

    long retentionMillis = LOG_RETENTION.fromMetadata(engine, metadata);
    long fileCutOffTime = System.currentTimeMillis() - retentionMillis;
    logger.info("{}: Starting the deletion of log files older than {}", tablePath, fileCutOffTime);
    long numDeleted = 0;
    try (CloseableIterator<FileStatus> files = listExpiredDeltaLogs(engine, tablePath)) {
      while (files.hasNext()) {
        FileStatus nextFile = files.next();
        if (nextFile.getModificationTime() > fileCutOffTime) {
          if (!potentialLogFilesToDelete.isEmpty()) {
            logger.info(
                "{}: Skipping deletion of expired log files {}, because there is no checkpoint "
                    + "file that indicates that the log files are no longer needed. ",
                tablePath,
                potentialLogFilesToDelete.size());
          }
          break;
        }

        if (FileNames.isCheckpointFile(nextFile.getPath())) {
          // we have encountered a checkpoint file, now we can delete all the log files
          // in `potentialLogFilesToDelete` list
          for (String logFile : potentialLogFilesToDelete) {
            if (engine.getFileSystemClient().delete(logFile)) {
              numDeleted++;
            }
          }
        } else if (FileNames.isCommitFile(nextFile.getPath())) {
          // Add it the potential delta log files to delete list. We can't delete these
          // files until we encounter a checkpoint later that indicates that the log files
          // are no longer needed.
          potentialLogFilesToDelete.add(nextFile.getPath());
        }
      }
    }
    logger.info("{}: Deleted {} log files older than {}", tablePath, numDeleted, fileCutOffTime);
  }

  private static CloseableIterator<FileStatus> listExpiredDeltaLogs(Engine engine, Path tablePath)
      throws IOException {
    Path logPath = new Path(tablePath, "_delta_log");
    return engine.getFileSystemClient().listFrom(FileNames.listingPrefix(logPath, 0));
  }
}
