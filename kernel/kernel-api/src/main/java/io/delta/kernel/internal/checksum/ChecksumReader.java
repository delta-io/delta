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
package io.delta.kernel.internal.checksum;

import static io.delta.kernel.internal.util.FileNames.*;
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

  public static Optional<CRCInfo> getCRCInfo(Engine engine, FileStatus checkSumFile) {
    try (CloseableIterator<ColumnarBatch> iter =
        engine
            .getJsonHandler()
            .readJsonFiles(
                singletonCloseableIterator(checkSumFile),
                CRCInfo.CRC_FILE_SCHEMA,
                Optional.empty())) {
      // We do this instead of iterating through the rows or using `getSingularRow` so we
      // can use the existing fromColumnVector methods in Protocol, Metadata, Format etc
      if (!iter.hasNext()) {
        logger.warn("Checksum file is empty: {}", checkSumFile.getPath());
        return Optional.empty();
      }

      ColumnarBatch batch = iter.next();
      if (batch.getSize() != 1) {
        String msg = "Expected exactly one row in the checksum file {}, found {} rows";
        logger.warn(msg, checkSumFile.getPath(), batch.getSize());
        return Optional.empty();
      }

      long crcVersion = FileNames.checksumVersion(new Path(checkSumFile.getPath()));

      return CRCInfo.fromColumnarBatch(crcVersion, batch, 0 /* rowId */, checkSumFile.getPath());
    } catch (Exception e) {
      // This can happen when the version does not have a checksum file
      logger.warn("Failed to read checksum file {}", checkSumFile.getPath(), e);
      return Optional.empty();
    }
  }
}
