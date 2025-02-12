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

import static io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO;
import static io.delta.kernel.internal.checksum.CRCInfo.*;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.FileNames;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writers for writing checksum files from a snapshot */
public class ChecksumWriter {

  private static final Logger logger = LoggerFactory.getLogger(ChecksumWriter.class);

  private final Path logPath;

  public ChecksumWriter(Path logPath) {
    this.logPath = requireNonNull(logPath);
  }

  /** Writes a checksum file */
  public void writeCheckSum(Engine engine, CRCInfo crcInfo) throws IOException {
    Path newChecksumPath = FileNames.checksumFile(logPath, crcInfo.getVersion());
    logger.info("Writing checksum file to path: {}", newChecksumPath);
    wrapEngineExceptionThrowsIO(
        () -> {
          engine
              .getJsonHandler()
              .writeJsonFileAtomically(
                  newChecksumPath.toString(),
                  singletonCloseableIterator(buildCheckSumRow(crcInfo)),
                  false /* overwrite */);
          return null;
        },
        "Write checksum file `%s`",
        newChecksumPath);
  }

  private Row buildCheckSumRow(CRCInfo crcInfo) {
    Map<Integer, Object> values = new HashMap<>();
    // Add required fields
    values.put(getSchemaIndex(TABLE_SIZE_BYTES), crcInfo.getTableSizeBytes());
    values.put(getSchemaIndex(NUM_FILES), crcInfo.getNumFiles());
    values.put(getSchemaIndex(NUM_METADATA), 1L);
    values.put(getSchemaIndex(NUM_PROTOCOL), 1L);
    values.put(getSchemaIndex(METADATA), crcInfo.getMetadata().toRow());
    values.put(getSchemaIndex(PROTOCOL), crcInfo.getProtocol().toRow());

    // Add optional fields
    crcInfo.getTxnId().ifPresent(txn -> values.put(getSchemaIndex(TXN_ID), txn));
    return new GenericRow(CRC_FILE_SCHEMA, values);
  }

  private int getSchemaIndex(String fieldName) {
    return CRC_FILE_SCHEMA.indexOf(fieldName);
  }
}
