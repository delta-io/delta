/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.replay;

import static io.delta.kernel.internal.replay.VersionStats.fromColumnarBatch;
import static io.delta.kernel.internal.util.FileNames.checksumFile;
import static io.delta.kernel.internal.util.FileNames.checksumVersion;
import static io.delta.kernel.internal.util.FileNames.isChecksumFile;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility method to load protocol and metadata from the Delta log checksum files. */
public class ChecksumReader {
  private static final Logger logger = LoggerFactory.getLogger(ChecksumReader.class);

  /**
   * Load the protocol and metadata from the checksum file at the given version. If the checksum
   * file is not found at the given version, it will try to find the latest checksum file that is
   * created after the lower bound version or within the last 100 versions.
   *
   * @param engine the engine to use for reading the checksum file
   * @param logPath the path to the Delta log
   * @param readVersion the version to read the checksum file from
   * @param lowerBoundOpt the lower bound version to search for the checksum file
   * @return Optional {@link VersionStats} containing the protocol and metadata, and the version of
   *     the checksum file. If the checksum file is not found, it will return an empty
   */
  public static Optional<VersionStats> getVersionStats(
      Engine engine, Path logPath, long readVersion, Optional<Long> lowerBoundOpt) {

    // First try to load the CRC at given version. If not found or failed to read then try to
    // find the latest CRC file that is created after the lower bound version or within the
    // last 100 versions.
    Path crcFilePath = checksumFile(logPath, readVersion);
    Optional<VersionStats> versionStatsOpt = readChecksumFile(engine, crcFilePath);
    if (versionStatsOpt.isPresent()
        ||
        // we don't expect any more checksum files as it is the first version
        readVersion == 0) {
      return versionStatsOpt;
    }

    // Try to list the last 100 CRC files and see if we can find a CRC that we can use
    long lowerBound = Math.max(lowerBoundOpt.orElse(0L) + 1, Math.max(0, readVersion - 100));

    Path listFrom = checksumFile(logPath, lowerBound);
    try (CloseableIterator<FileStatus> crcFiles =
        engine.getFileSystemClient().listFrom(listFrom.toString())) {

      List<FileStatus> crcFilesList = new ArrayList<>();
      crcFiles
          .filter(file -> isChecksumFile(new Path(file.getPath())))
          .takeWhile(file -> checksumVersion(new Path(file.getPath())) <= readVersion)
          .forEachRemaining(crcFilesList::add);

      // pick the last file which is the latest version that has the CRC file
      if (crcFilesList.isEmpty()) {
        logger.warn("No checksum files found in the range {} to {}", lowerBound, readVersion);
        return Optional.empty();
      }

      FileStatus latestCRCFile = crcFilesList.get(crcFilesList.size() - 1);
      return readChecksumFile(engine, new Path(latestCRCFile.getPath()));
    } catch (Exception e) {
      logger.warn("Failed to list checksum files from {}", listFrom, e);
      return Optional.empty();
    }
  }

  private static Optional<VersionStats> readChecksumFile(Engine engine, Path filePath) {
    try (CloseableIterator<ColumnarBatch> iter =
        engine
            .getJsonHandler()
            .readJsonFiles(
                singletonCloseableIterator(FileStatus.of(filePath.toString())),
                VersionStats.FULL_SCHEMA,
                Optional.empty())) {
      // We do this instead of iterating through the rows or using `getSingularRow` so we
      // can use the existing fromColumnVector methods in Protocol, Metadata, Format etc
      if (!iter.hasNext()) {
        logger.warn("Checksum file is empty: {}", filePath);
        return Optional.empty();
      }

      ColumnarBatch batch = iter.next();
      if (batch.getSize() != 1) {
        String msg = "Expected exactly one row in the checksum file {}, found {} rows";
        logger.warn(msg, filePath, batch.getSize());
        return Optional.empty();
      }

      long crcVersion = FileNames.checksumVersion(filePath);

      VersionStats versionStats = fromColumnarBatch(engine, crcVersion, batch, 0 /* rowId */);
      if (versionStats.getMetadata() == null || versionStats.getProtocol() == null) {
        logger.warn("Invalid checksum file missing protocol and/or metadata: {}", filePath);
        return Optional.empty();
      }
      return Optional.of(versionStats);
    } catch (Exception e) {
      // This can happen when the version does not have a checksum file
      logger.warn("Failed to read checksum file {}", filePath, e);
      return Optional.empty();
    }
  }
}
