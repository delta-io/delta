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

package io.delta.kernel.internal.replay;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumReader;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static utility class for loading Protocol and Metadata from Delta log files.
 *
 * <p>This class handles the replay of Protocol and Metadata actions from a LogSegment, using CRC
 * information if available for optimization.
 */
public class ProtocolMetadataLogReplay {

  private static final Logger logger = LoggerFactory.getLogger(ProtocolMetadataLogReplay.class);

  /** Read schema when searching for the latest Protocol and Metadata. */
  public static final StructType PROTOCOL_METADATA_READ_SCHEMA =
      new StructType().add("protocol", Protocol.FULL_SCHEMA).add("metaData", Metadata.FULL_SCHEMA);

  /** Result of loading Protocol and Metadata from a LogSegment. */
  public static class Result {
    public final Protocol protocol;
    public final Metadata metadata;

    /**
     * Information about CRC usage:
     * <ul>
     *   <li>Optional.empty() = Never attempted to read CRC file
     *   <li>Optional.of(Optional.empty()) = Attempted to read but failed (exception/invalid)
     *   <li>Optional.of(Optional.of(crcInfo)) = Successfully read and parsed CRC
     * </ul>
     */
    public final Optional<Optional<CRCInfo>> crcInfoUsed;

    public Result(Protocol protocol, Metadata metadata, Optional<Optional<CRCInfo>> crcInfoUsed) {
      this.protocol = protocol;
      this.metadata = metadata;
      this.crcInfoUsed = crcInfoUsed;
    }
  }

  /**
   * Loads the latest Protocol and Metadata from the log files in the given LogSegment.
   *
   * <p>Uses the {@code LogSegment::lastSeenChecksum} to bound how many delta files it reads. That
   * is, we only need to read delta files *newer* than that CRC file to search for any new P & M. If
   * we don't find both the P & M by the time we get to the CRC file version, we can just use the P
   * and M from it.
   *
   * <p>Also validates that the Kernel can read the table at the loaded protocol version.
   */
  public static Result loadProtocolAndMetadata(
      Engine engine, LogSegment logSegment, Path dataPath, SnapshotMetrics snapshotMetrics) {

    final Result result =
        snapshotMetrics.loadProtocolMetadataTotalDurationTimer.time(
            () -> loadProtocolAndMetadataInternal(engine, logSegment, dataPath, snapshotMetrics));

    TableFeatures.validateKernelCanReadTheTable(result.protocol, dataPath.toString());

    logger.info(
        "[{}] Took {}ms to load Protocol and Metadata at version {}",
        dataPath,
        snapshotMetrics.loadProtocolMetadataTotalDurationTimer.totalDurationMs(),
        logSegment.getVersion());

    return result;
  }

  private static Result loadProtocolAndMetadataInternal(
      Engine engine, LogSegment logSegment, Path dataPath, SnapshotMetrics snapshotMetrics) {
    final long snapshotVersion = logSegment.getVersion();
    final Optional<FileStatus> crcFileOpt = logSegment.getLastSeenChecksum();
    final Optional<Long> crcVersionOpt =
        crcFileOpt.map(f -> FileNames.checksumVersion(f.getPath()));

    // Lazy-load the CRC file only once when needed. The result is Optional<CRCInfo>:
    // - Optional.empty() if there is no CRC file in this LogSegment or we failed to read it
    // - Optional.of(crcInfo) if the file exists and was successfully read
    final Lazy<Optional<CRCInfo>> lazyCrcInfo =
        new Lazy<>(
            () -> {
              if (!crcFileOpt.isPresent()) {
                return Optional.empty();
              }
              return snapshotMetrics.loadCrcTotalDurationTimer.time(
                  () -> ChecksumReader.tryReadChecksumFile(engine, crcFileOpt.get()));
            });

    // If CRC is at this exact snapshot version, use it directly
    if (crcVersionOpt.isPresent() && crcVersionOpt.get() == snapshotVersion) {
      final Optional<CRCInfo> crcInfo = lazyCrcInfo.get(); // Read and parse the CRC file!
      if (crcInfo.isPresent()) {
        final Protocol protocol = crcInfo.get().getProtocol();
        final Metadata metadata = crcInfo.get().getMetadata();
        return new Result(protocol, metadata, Optional.of(crcInfo));
      }
    }

    // Otherwise, we need to read log files. The CRC (if present) might still be useful to avoid
    // reading older files.

    long logReadCount = 0;
    Protocol protocol = null;
    Metadata metadata = null;

    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            logSegment.allFilesWithCompactionsReversed(),
            PROTOCOL_METADATA_READ_SCHEMA,
            Optional.empty())) {
      while (reverseIter.hasNext()) {
        final ActionWrapper nextElem = reverseIter.next();
        final long version = nextElem.getVersion();
        logReadCount++;
        // Load this lazily (as needed). We may be able to just use the CRC.
        ColumnarBatch columnarBatch = null;

        if (protocol == null) {
          columnarBatch = nextElem.getColumnarBatch();
          assert (columnarBatch.getSchema().equals(PROTOCOL_METADATA_READ_SCHEMA));

          final ColumnVector protocolVector = columnarBatch.getColumnVector(0);

          for (int i = 0; i < protocolVector.getSize(); i++) {
            if (!protocolVector.isNullAt(i)) {
              protocol = Protocol.fromColumnVector(protocolVector, i);

              if (metadata != null) {
                // Stop since we have found the latest Protocol and Metadata.
                // We didn't need to read CRC, so return Optional.empty()
                return new Result(protocol, metadata, Optional.empty());
              }

              break; // We just found the protocol, exit this for-loop
            }
          }
        }

        if (metadata == null) {
          if (columnarBatch == null) {
            columnarBatch = nextElem.getColumnarBatch();
            assert (columnarBatch.getSchema().equals(PROTOCOL_METADATA_READ_SCHEMA));
          }
          final ColumnVector metadataVector = columnarBatch.getColumnVector(1);

          for (int i = 0; i < metadataVector.getSize(); i++) {
            if (!metadataVector.isNullAt(i)) {
              metadata = Metadata.fromColumnVector(metadataVector, i);

              if (protocol != null) {
                // Stop since we have found the latest Protocol and Metadata.
                // We didn't need to read CRC, so return Optional.empty()
                return new Result(protocol, metadata, Optional.empty());
              }

              break; // We just found the metadata, exit this for-loop
            }
          }
        }

        // Since we haven't returned, then at least one of P or M is null.
        // Note: Suppose the CRC is at version N. We check the CRC eagerly at N + 1 so
        //       that we don't read or open any files at version N.
        if (crcVersionOpt.isPresent() && version == crcVersionOpt.get() + 1) {
          final Optional<CRCInfo> crcInfo = lazyCrcInfo.get();
          if (crcInfo.isPresent()) {
            if (protocol == null) {
              protocol = crcInfo.get().getProtocol();
            }
            if (metadata == null) {
              metadata = crcInfo.get().getMetadata();
            }
            logger.info(
                "{}: Loading Protocol and Metadata read {} logs with CRC at version {}",
                dataPath.toString(),
                logReadCount,
                crcVersionOpt.get());

            return new Result(protocol, metadata, Optional.of(crcInfo));
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Could not close iterator", ex);
    }

    if (protocol == null) {
      throw new IllegalStateException(
          String.format("No protocol found at version %s", logSegment.getVersion()));
    }

    if (metadata == null) {
      throw new IllegalStateException(
          String.format("No metadata found at version %s", logSegment.getVersion()));
    }

    // Return the appropriate CRC state:
    // - If we never computed lazyCrcInfo, return Optional.empty() (never tried)
    // - If we computed it, wrap the result in Optional.of() (tried, may have succeeded or failed)
    return new Result(
        protocol,
        metadata,
        lazyCrcInfo.isPresent() ? Optional.of(lazyCrcInfo.get()) : Optional.empty());
  }
}
