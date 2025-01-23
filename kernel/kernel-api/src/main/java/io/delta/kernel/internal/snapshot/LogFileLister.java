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

package io.delta.kernel.internal.snapshot;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.logging.KernelLogger;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.Preconditions;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to list delta and checkpoint files in the _delta_log directory during Snapshot
 * creation.
 */
class LogFileLister {
  private static final Logger LOGGER_IMPL = LoggerFactory.getLogger(LogFileLister.class);

  private final Path dataPath;
  private final Path logPath;
  private final KernelLogger logger;

  LogFileLister(Path dataPath, Path logPath) {
    this.dataPath = dataPath;
    this.logPath = logPath;
    this.logger = new KernelLogger(LOGGER_IMPL, dataPath.toString());
  }

  /////////////////
  // Public APIs //
  /////////////////

  /**
   * Returns the delta files and checkpoint files starting from the given {@code startVersion} in
   * increasing version order.
   *
   * @param startVersion the version to start. Inclusive.
   * @param versionToLoad the optional parameter to set the max version we should return. Inclusive.
   *     Must be >= startVersion if provided. It's usually used to load a table snapshot for a
   *     specific version. If no delta or checkpoint files exist below the versionToLoad and at
   *     least one delta file exists, throws an exception that the state is not reconstructable.
   * @return Some array of files found (possibly empty, if no usable commit files are present), or
   *     None if the listing returned no files at all.
   */
  final Optional<List<FileStatus>> listDeltaAndCheckpointFiles(
      Engine engine, long startVersion, Optional<Long> versionToLoad) {
    versionToLoad.ifPresent(
        v ->
            Preconditions.checkArgument(
                v >= startVersion,
                "versionToLoad=%s provided is less than startVersion=%s",
                v,
                startVersion));
    logger.debug("startVersion: {}, versionToLoad: {}", startVersion, versionToLoad);

    return listFromOrNone(engine, startVersion)
        .map(
            fileStatusesIter -> {
              final List<FileStatus> output = new ArrayList<>();

              while (fileStatusesIter.hasNext()) {
                final FileStatus fileStatus = fileStatusesIter.next();
                final String fileName = Path.getName(fileStatus.getPath());

                // Pick up all checkpoint and delta files
                if (!FileNames.isDeltaCommitOrCheckpointFile(fileName)) {
                  continue;
                }

                // Checkpoint files of 0 size are invalid but may be ignored silently when read,
                // hence we drop them so that we never pick up such checkpoints.
                if (FileNames.isCheckpointFile(fileName) && fileStatus.getSize() == 0) {
                  continue;
                }
                // Take files until the version we want to load
                final boolean versionWithinRange =
                    versionToLoad
                        .map(v -> FileNames.getFileVersion(new Path(fileStatus.getPath())) <= v)
                        .orElse(true);

                if (!versionWithinRange) {
                  // If we haven't taken any files yet and the first file we see is greater
                  // than the versionToLoad then the versionToLoad is not reconstructable
                  // from the existing logs
                  if (output.isEmpty()) {
                    long earliestVersion =
                        DeltaHistoryManager.getEarliestRecreatableCommit(engine, logPath);
                    throw DeltaErrors.versionToLoadBeforeFirstAvailableCommit(
                        dataPath.toString(), versionToLoad.get(), earliestVersion);
                  }
                  break;
                }
                output.add(fileStatus);
              }

              return output;
            });
  }

  ////////////////////////////
  // Private Helper Methods //
  ////////////////////////////

  /** Get an iterator of files in the _delta_log directory starting with the startVersion. */
  private CloseableIterator<FileStatus> listFrom(Engine engine, long startVersion)
      throws IOException {
    return DeltaErrors.wrapEngineExceptionThrowsIO(
        () -> engine.getFileSystemClient().listFrom(FileNames.listingPrefix(logPath, startVersion)),
        "Listing from %s",
        FileNames.listingPrefix(logPath, startVersion));
  }

  /**
   * Returns an iterator containing a list of files found in the _delta_log directory starting with
   * the startVersion. Returns None if no files are found or the directory is missing.
   */
  private Optional<CloseableIterator<FileStatus>> listFromOrNone(Engine engine, long startVersion) {
    // LIST the directory, starting from the provided lower bound (treat missing dir as empty).
    // NOTE: "empty/missing" is _NOT_ equivalent to "contains no useful commit files."
    try {
      CloseableIterator<FileStatus> results = listFrom(engine, startVersion);
      if (results.hasNext()) {
        return Optional.of(results);
      } else {
        return Optional.empty();
      }
    } catch (FileNotFoundException e) {
      return Optional.empty();
    } catch (IOException io) {
      throw new UncheckedIOException("Failed to list the files in delta log", io);
    }
  }
}
