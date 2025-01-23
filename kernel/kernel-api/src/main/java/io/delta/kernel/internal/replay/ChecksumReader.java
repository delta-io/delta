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

import static io.delta.kernel.internal.replay.CRCInfo.fromColumnarBatch;
import static io.delta.kernel.internal.util.FileNames.checksumFile;
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
   * Load the CRCInfo from the checksum file at the given version. If the checksum file is not found
   * at the given version, it will try to find the latest checksum file that is created after the
   * lower bound version or within the last 100 versions.
   *
   * @param engine the engine to use for reading the checksum file
   * @param logPath the path to the Delta log
   * @param targetedVersion the target version to read the checksum file from
   * @param lowerBound the exclusive lower bound version to search for the checksum file
   * @return Optional {@link CRCInfo} containing the protocol and metadata, and the version of the
   *     checksum file. If the checksum file is not found, it will return an empty
   */
  public static Optional<CRCInfo> getCRCInfo(
      Engine engine, Path logPath, long targetedVersion, long lowerBound) {
    logger.info("Loading CRC file for version {} with lower bound {}", targetedVersion, lowerBound);
    // First try to load the CRC at given version. If not found or failed to read then try to
    // find the latest CRC file that is created after the lower bound version or within the last 100
    // versions if no lower bound is provided.
    Path crcFilePath = checksumFile(logPath, targetedVersion);
    Optional<CRCInfo> crcInfoOpt = readChecksumFile(engine, crcFilePath);
    if (crcInfoOpt.isPresent()
        ||
        // we don't expect any more checksum files as it is the first version
        targetedVersion == 0) {
      return crcInfoOpt;
    }
    logger.info(
        "CRC file for version {} not found, attempt to loading version up to {}",
        targetedVersion,
        lowerBound);

    Path lowerBoundFilePath = checksumFile(logPath, lowerBound + 1);
    try (CloseableIterator<FileStatus> crcFiles =
        engine.getFileSystemClient().listFrom(lowerBoundFilePath.toString())) {
      List<FileStatus> crcFilesList = new ArrayList<>();
      crcFiles.filter(file -> isChecksumFile(file.getPath())).forEachRemaining(crcFilesList::add);

      // pick the last file which is the latest version that has the CRC file
      if (crcFilesList.isEmpty()) {
        logger.warn("No checksum files found in the range {} to {}", lowerBound, targetedVersion);
        return Optional.empty();
      }

      FileStatus latestCRCFile = crcFilesList.get(crcFilesList.size() - 1);
      return readChecksumFile(engine, new Path(latestCRCFile.getPath()));
    } catch (Exception e) {
      logger.warn("Failed to list checksum files from {}", lowerBoundFilePath, e);
      return Optional.empty();
    }
  }

  private static Optional<CRCInfo> readChecksumFile(Engine engine, Path filePath) {
    try (CloseableIterator<ColumnarBatch> iter =
        engine
            .getJsonHandler()
            .readJsonFiles(
                singletonCloseableIterator(FileStatus.of(filePath.toString())),
                CRCInfo.FULL_SCHEMA,
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

      return fromColumnarBatch(engine, crcVersion, batch, 0 /* rowId */, filePath.toString());
    } catch (Exception e) {
      // This can happen when the version does not have a checksum file
      logger.warn("Failed to read checksum file {}", filePath, e);
      return Optional.empty();
    }
  }
}
