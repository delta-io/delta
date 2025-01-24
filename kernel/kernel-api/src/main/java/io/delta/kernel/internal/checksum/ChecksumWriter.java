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
import static io.delta.kernel.internal.checksum.ChecksumUtils.CRC_FILE_SCHEMA;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.util.FileNames;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writers for writing checksum files from a snapshot */
public class ChecksumWriter {

  private static final Logger logger = LoggerFactory.getLogger(ChecksumWriter.class);

  private final Path logPath;

  public ChecksumWriter(Path logPath) {
    this.logPath = logPath;
  }

  /**
   * Writes a checksum file in a best-effort manner from a post commit snapshot, write will only
   * process if all required fields, including NumFiles and TableSizeBytes, are collected.
   *
   * @return true if checksum file is successfully written, false otherwise.
   */
  public boolean maybeWriteCheckSum(
      Engine engine, SnapshotHint postCommitSnapshot, Optional<String> txnId) {
    // No sufficient information to write checksum file.
    if (!postCommitSnapshot.getNumFiles().isPresent()
        || !postCommitSnapshot.getTableSizeBytes().isPresent()) {
      logger.warn(
          "Skipping writing checksum due to num_files or total_table_size missing in the snapshot");
      return false;
    }
    Path newChecksumPath = FileNames.checksumFile(logPath, postCommitSnapshot.getVersion());
    try {
      return wrapEngineExceptionThrowsIO(
          () -> {
            engine
                .getJsonHandler()
                .writeJsonFileAtomically(
                    newChecksumPath.toString(),
                    singletonCloseableIterator(buildCheckSumRow(postCommitSnapshot, txnId)),
                    false /* overwrite */);
            return true;
          },
          "Write checksum file `%s`",
          newChecksumPath);
    } catch (IOException io) {
      logger.warn("Write checksum fails with error {}", io.getMessage());
    }
    return false;
  }

  private Row buildCheckSumRow(SnapshotHint snapshot, Optional<String> txnId) {
    checkArgument(snapshot.getTableSizeBytes().isPresent() && snapshot.getNumFiles().isPresent());
    Map<Integer, Object> value = new HashMap<>();
    value.put(CRC_FILE_SCHEMA.indexOf("tableSizeBytes"), snapshot.getTableSizeBytes().getAsLong());
    value.put(CRC_FILE_SCHEMA.indexOf("numFiles"), snapshot.getNumFiles().getAsLong());
    value.put(CRC_FILE_SCHEMA.indexOf("numMetadata"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("numProtocol"), 1L);
    value.put(CRC_FILE_SCHEMA.indexOf("metadata"), snapshot.getMetadata().toRow());
    value.put(CRC_FILE_SCHEMA.indexOf("protocol"), snapshot.getProtocol().toRow());
    txnId.ifPresent(txn -> value.put(CRC_FILE_SCHEMA.indexOf("txnId"), txn));
    return new GenericRow(CRC_FILE_SCHEMA, value);
  }
}
