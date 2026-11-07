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
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.FileNames;
import io.delta.kernel.internal.util.InCommitTimestampUtils;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Static utility class for loading Protocol and Metadata from Delta log files. */
public class ProtocolMetadataLogReplay {

  private static final Logger logger = LoggerFactory.getLogger(ProtocolMetadataLogReplay.class);

  /**
   * Minimal CommitInfo sub-schema containing only the {@code inCommitTimestamp} field. Using this
   * instead of {@link io.delta.kernel.internal.actions.CommitInfo#FULL_SCHEMA} avoids reading
   * fields that are not needed during Protocol/Metadata loading.
   *
   * <p>{@code inCommitTimestamp} must be at ordinal 0 here, matching its position in {@code
   * CommitInfo.FULL_SCHEMA}, so that {@link
   * InCommitTimestampUtils#tryExtractInCommitTimestamp(ColumnarBatch)} works correctly with both
   * schemas.
   */
  private static final StructType COMMIT_INFO_ICT_READ_SCHEMA =
      new StructType().add("inCommitTimestamp", LongType.LONG, true /* nullable */);

  /**
   * Read schema used for <b>delta JSON commit files</b> when searching for the latest Protocol,
   * Metadata, and InCommitTimestamp. The {@code commitInfo} field uses a minimal sub-schema to
   * avoid reading unnecessary fields.
   *
   * <p>This schema must NOT be used for Parquet checkpoint files; use {@link
   * #PROTOCOL_METADATA_CHECKPOINT_READ_SCHEMA} for those instead, since Parquet readers may fail on
   * columns that are absent from the file's physical schema.
   */
  public static final StructType PROTOCOL_METADATA_READ_SCHEMA =
      new StructType()
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("commitInfo", COMMIT_INFO_ICT_READ_SCHEMA);

  /**
   * Read schema used for <b>checkpoint files</b> (Parquet or JSON v2 checkpoints) when searching
   * for the latest Protocol and Metadata. Omits {@code commitInfo} because checkpoint files do not
   * contain CommitInfo actions, and some Parquet implementations throw on missing columns.
   */
  private static final StructType PROTOCOL_METADATA_CHECKPOINT_READ_SCHEMA =
      new StructType().add("protocol", Protocol.FULL_SCHEMA).add("metaData", Metadata.FULL_SCHEMA);

  /** Result of loading Protocol, Metadata, and (optionally) InCommitTimestamp from a LogSegment. */
  public static class Result {
    public final Protocol protocol;
    public final Metadata metadata;
    private final long numDeltaFilesRead;
    /**
     * The InCommitTimestamp read from the snapshot version's commit file during P/M loading, if
     * available. Empty when the snapshot version's commit file was not read (e.g. when a CRC file
     * at the exact snapshot version was used), or when the table does not have ICT enabled.
     */
    public final Optional<Long> inCommitTimestampOpt;

    public Result(
        Protocol protocol,
        Metadata metadata,
        long numDeltaFilesRead,
        Optional<Long> inCommitTimestampOpt) {
      this.protocol = protocol;
      this.metadata = metadata;
      this.numDeltaFilesRead = numDeltaFilesRead;
      this.inCommitTimestampOpt = inCommitTimestampOpt;
    }
  }

  /**
   * Loads the latest Protocol and Metadata from the log files in the given LogSegment.
   *
   * <p>Uses the provided lazy CRC loader to bound how many delta files it reads, and to ensure we
   * only read the CRC file if needed.
   *
   * <p>We read delta files in reverse order (newest first) searching for the latest Protocol and
   * Metadata. When we reach the version just before (greater than) the CRC file version (if
   * present), we lazily load the CRC file to fill in any missing Protocol or Metadata, avoiding
   * reading older delta files.
   *
   * <p>Also validates that the Kernel can read the table at the loaded protocol version.
   */
  public static Result loadProtocolAndMetadata(
      Engine engine,
      Path dataPath,
      LogSegment logSegment,
      Lazy<Optional<CRCInfo>> lazyCrcInfo,
      SnapshotMetrics snapshotMetrics) {
    final Result result =
        snapshotMetrics.loadProtocolMetadataTotalDurationTimer.time(
            () -> loadProtocolAndMetadataInternal(engine, logSegment, lazyCrcInfo));

    TableFeatures.validateKernelCanReadTheTable(result.protocol, dataPath.toString());

    logger.info(
        "[{}] Took {}ms to load Protocol and Metadata at version {}, read {} log files",
        dataPath,
        snapshotMetrics.loadProtocolMetadataTotalDurationTimer.totalDurationMs(),
        logSegment.getVersion(),
        result.numDeltaFilesRead);

    return result;
  }

  private static Result loadProtocolAndMetadataInternal(
      Engine engine, LogSegment logSegment, Lazy<Optional<CRCInfo>> lazyCrcInfo) {
    final long snapshotVersion = logSegment.getVersion();
    final Optional<FileStatus> crcFileOpt = logSegment.getLastSeenChecksum();
    final Optional<Long> crcVersionOpt =
        crcFileOpt.map(f -> FileNames.checksumVersion(f.getPath()));

    // If CRC is at this exact snapshot version, use it directly
    if (crcVersionOpt.isPresent() && crcVersionOpt.get() == snapshotVersion) {
      final Optional<CRCInfo> crcInfo = lazyCrcInfo.get();
      if (crcInfo.isPresent()) {
        validateCrcInfoMatchesExpectedVersion(crcInfo.get(), crcVersionOpt.get());

        final Protocol protocol = crcInfo.get().getProtocol();
        final Metadata metadata = crcInfo.get().getMetadata();
        // ICT is not stored in the CRC file; leave it empty so SnapshotImpl reads it lazily.
        return new Result(protocol, metadata, 0 /* logFilesRead */, Optional.empty());
      }
    }

    // Otherwise, we need to read log files. The CRC (if present) might still be useful to avoid
    // reading older files.

    long numDeltaFilesRead = 0;
    Protocol protocol = null;
    Metadata metadata = null;
    // ICT captured from the snapshot version's commit file (the first file we encounter since we
    // iterate in reverse). Empty if that file is not a regular commit file (e.g. log compaction).
    Optional<Long> ictOpt = Optional.empty();
    boolean ictExtractionAttempted = false;

    // Use separate schemas for JSON delta files (which may contain commitInfo) and checkpoint
    // files (which never contain commitInfo and may fail on unexpected columns).
    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            logSegment.allFilesWithCompactionsReversed(),
            PROTOCOL_METADATA_READ_SCHEMA,
            PROTOCOL_METADATA_CHECKPOINT_READ_SCHEMA,
            Optional.empty(),
            Optional.empty())) {
      while (reverseIter.hasNext()) {
        final ActionWrapper nextElem = reverseIter.next();
        final long version = nextElem.getVersion();
        numDeltaFilesRead++;
        // Load this lazily (as needed). We may be able to just use the CRC.
        ColumnarBatch columnarBatch = null;

        // Opportunistically capture the ICT from the first commit file at the snapshot version.
        // CommitInfo (containing ICT) is guaranteed to be the first action in commit files when
        // ICT is enabled, so reading it here avoids a separate cloud read in getTimestamp().
        // We skip log-compaction files because they do not contain CommitInfo actions.
        if (!ictExtractionAttempted
            && version == snapshotVersion
            && !nextElem.isFromCheckpoint()
            && FileNames.isCommitFile(nextElem.getFilePath())) {
          ictExtractionAttempted = true;
          columnarBatch = nextElem.getColumnarBatch();
          ictOpt = InCommitTimestampUtils.tryExtractInCommitTimestamp(columnarBatch);
        }

        if (protocol == null) {
          if (columnarBatch == null) {
            columnarBatch = nextElem.getColumnarBatch();
          }
          // protocol is always at ordinal 0 in both PROTOCOL_METADATA_READ_SCHEMA and
          // PROTOCOL_METADATA_CHECKPOINT_READ_SCHEMA
          assert (columnarBatch.getSchema().at(0).getName().equals("protocol"));

          final ColumnVector protocolVector = columnarBatch.getColumnVector(0);

          for (int i = 0; i < protocolVector.getSize(); i++) {
            if (!protocolVector.isNullAt(i)) {
              protocol = Protocol.fromColumnVector(protocolVector, i);

              if (metadata != null) {
                // Stop since we have found the latest Protocol and Metadata.
                return new Result(protocol, metadata, numDeltaFilesRead, ictOpt);
              }

              break; // We just found the protocol, exit this for-loop
            }
          }
        }

        if (metadata == null) {
          if (columnarBatch == null) {
            columnarBatch = nextElem.getColumnarBatch();
            // metaData is always at ordinal 1 in both read schemas
            assert (columnarBatch.getSchema().at(1).getName().equals("metaData"));
          }
          final ColumnVector metadataVector = columnarBatch.getColumnVector(1);

          for (int i = 0; i < metadataVector.getSize(); i++) {
            if (!metadataVector.isNullAt(i)) {
              metadata = Metadata.fromColumnVector(metadataVector, i);

              if (protocol != null) {
                // Stop since we have found the latest Protocol and Metadata.
                return new Result(protocol, metadata, numDeltaFilesRead, ictOpt);
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
            validateCrcInfoMatchesExpectedVersion(crcInfo.get(), crcVersionOpt.get());

            if (protocol == null) {
              protocol = crcInfo.get().getProtocol();
            }
            if (metadata == null) {
              metadata = crcInfo.get().getMetadata();
            }

            return new Result(protocol, metadata, numDeltaFilesRead, ictOpt);
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

    return new Result(protocol, metadata, numDeltaFilesRead, ictOpt);
  }

  private static void validateCrcInfoMatchesExpectedVersion(CRCInfo crcInfo, long expectedVersion) {
    if (crcInfo.getVersion() != expectedVersion) {
      throw new IllegalStateException(
          String.format(
              "Expected a CRC at version %d but got one at version %d",
              expectedVersion, crcInfo.getVersion()));
    }
  }
}
