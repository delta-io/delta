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

import static io.delta.kernel.internal.actions.SingleAction.CHECKPOINT_SCHEMA;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.ChecksumAlreadyExistsException;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DomainMetadata;
import io.delta.kernel.internal.replay.CreateCheckpointIterator;
import io.delta.kernel.internal.stats.FileSizeHistogram;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class ChecksumUtils {

  public static void computeStateAndWriteChecksum(Engine engine, SnapshotImpl snapshot)
      throws IOException {
    // TODO: Optimize using crc after https://github.com/delta-io/delta/pull/4112
    LongAdder tableSizeByte = new LongAdder();
    LongAdder fileCount = new LongAdder();
    FileSizeHistogram fileSizeHistogram = FileSizeHistogram.createDefaultHistogram();
    Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
    ChecksumWriter checksumWriter = new ChecksumWriter(snapshot.getLogPath());

    // TODO: Optimize using crc after https://github.com/delta-io/delta/pull/4112 where
    // snapshot.getLogSegment() could return last available CRC.
    // Set minFileRetentionTimestampMillis to infinite future to skip all removed files.
    try (CreateCheckpointIterator checkpointIterator =
        new CreateCheckpointIterator(
            engine,
            snapshot.getLogSegment(),
            Instant.ofEpochMilli(Long.MAX_VALUE).toEpochMilli())) {
      checkpointIterator.forEachRemaining(
          batch ->
              batch
                  .getRows()
                  .forEachRemaining(
                      row -> {
                        checkState(
                            row.isNullAt(CHECKPOINT_SCHEMA.indexOf("remove")),
                            "unexpected remove row found when setting "
                                + "minFileRetentionTimestampMillis to infinite future");
                        if (!row.isNullAt(CHECKPOINT_SCHEMA.indexOf("add"))) {
                          long addFileSize =
                              new AddFile(row.getStruct(CHECKPOINT_SCHEMA.indexOf("add")))
                                  .getSize();
                          tableSizeByte.add(addFileSize);
                          fileSizeHistogram.insert(addFileSize);
                          fileCount.increment();
                        }
                        if (!row.isNullAt(CHECKPOINT_SCHEMA.indexOf("domainMetadata"))) {
                          DomainMetadata domainMetadata =
                              DomainMetadata.fromRow(
                                  row.getStruct(CHECKPOINT_SCHEMA.indexOf("domainMetadata")));
                          if (!domainMetadataMap.containsKey(domainMetadata.getDomain())) {
                            domainMetadataMap.put(domainMetadata.getDomain(), domainMetadata);
                          }
                        }
                      }));
      checksumWriter.writeCheckSum(
          engine,
          new CRCInfo(
              snapshot.getVersion(),
              snapshot.getMetadata(),
              snapshot.getProtocol(),
              tableSizeByte.longValue(),
              fileCount.longValue(),
              Optional.empty() /* txnId */,
              Optional.of(
                  domainMetadataMap.values().stream()
                      .filter(domainMetadata -> !domainMetadata.isRemoved())
                      .collect(Collectors.toSet())),
              Optional.of(fileSizeHistogram)));

    } catch (FileAlreadyExistsException fileAlreadyExistsException) {
      throw new ChecksumAlreadyExistsException(snapshot.getVersion());
    }
  }
}
