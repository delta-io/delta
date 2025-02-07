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

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Collections.max;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Optional;

public class FullLogSegmentReplay extends LogReplay {

  private final Optional<CRCInfo> currentCrcInfo;
  private final Tuple2<Protocol, Metadata> protocolAndMetadata;

  public FullLogSegmentReplay(
      Path dataPath,
      long snapshotVersion,
      Engine engine,
      LogSegment logSegment,
      Optional<SnapshotHint> snapshotHint,
      SnapshotMetrics snapshotMetrics) {
    super(engine, dataPath, logSegment);

    Tuple2<Optional<SnapshotHint>, Optional<CRCInfo>> newerSnapshotHintAndCurrentCrcInfo =
        maybeGetNewerSnapshotHintAndCurrentCrcInfo(
            engine, logSegment, snapshotHint, snapshotVersion);

    this.currentCrcInfo = newerSnapshotHintAndCurrentCrcInfo._2;

    this.protocolAndMetadata =
        snapshotMetrics.loadInitialDeltaActionsTimer.time(
            () ->
                loadTableProtocolAndMetadata(
                    engine, logSegment, newerSnapshotHintAndCurrentCrcInfo._1, snapshotVersion));
  }

  @Override
  public Protocol getProtocol() {
    return protocolAndMetadata._1;
  }

  @Override
  public Metadata getMetadata() {
    return protocolAndMetadata._2;
  }

  @Override
  public Optional<CRCInfo> getCurrentCrcInfo() {
    return currentCrcInfo;
  }

  ////////////////////
  // Helper methods //
  ////////////////////

  /**
   * Calculates the latest snapshot hint before or at the current snapshot version, returns the
   * CRCInfo if checksum file at the current version is read
   */
  private Tuple2<Optional<SnapshotHint>, Optional<CRCInfo>>
      maybeGetNewerSnapshotHintAndCurrentCrcInfo(
          Engine engine,
          LogSegment logSegment,
          Optional<SnapshotHint> snapshotHint,
          long snapshotVersion) {

    // Snapshot hint's version is current.
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == snapshotVersion) {
      return new Tuple2<>(snapshotHint, Optional.empty());
    }

    // Ignore the snapshot hint whose version is larger.
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() > snapshotVersion) {
      snapshotHint = Optional.empty();
    }

    long crcSearchLowerBound =
        max(
            asList(
                // Prefer reading hint over CRC, so start listing from hint's version + 1,
                // if hint is not present, list from version 0.
                snapshotHint.map(SnapshotHint::getVersion).orElse(-1L) + 1,
                logSegment.getCheckpointVersionOpt().orElse(0L),
                // Only find the CRC within 100 versions.
                snapshotVersion - 100,
                0L));
    Optional<CRCInfo> crcInfoOpt =
        ChecksumReader.getCRCInfo(
            engine, logSegment.getLogPath(), snapshotVersion, crcSearchLowerBound);
    if (!crcInfoOpt.isPresent()) {
      return new Tuple2<>(snapshotHint, Optional.empty());
    }
    CRCInfo crcInfo = crcInfoOpt.get();
    checkArgument(
        crcInfo.getVersion() >= crcSearchLowerBound && crcInfo.getVersion() <= snapshotVersion);
    // We found a CRCInfo of a version (a) older than the one we are looking for (snapshotVersion)
    // but (b) newer than the current hint. Use this CRCInfo to create a new hint, and return this
    // crc info if it matches the current version.
    return new Tuple2<>(
        Optional.of(SnapshotHint.fromCrcInfo(crcInfo)),
        crcInfo.getVersion() == snapshotVersion ? crcInfoOpt : Optional.empty());
  }

  /**
   * Returns the latest Protocol and Metadata from the delta files in the `logSegment`. Does *not*
   * validate that this delta-kernel connector understands the table at that protocol.
   *
   * <p>Uses the `snapshotHint` to bound how many delta files it reads. i.e. we only need to read
   * delta files newer than the hint to search for any new P & M. If we don't find them, we can just
   * use the P and/or M from the hint.
   */
  protected Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata(
      Engine engine,
      LogSegment logSegment,
      Optional<SnapshotHint> snapshotHint,
      long snapshotVersion) {

    // Exit early if the hint already has the info we need.
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == snapshotVersion) {
      return new Tuple2<>(snapshotHint.get().getProtocol(), snapshotHint.get().getMetadata());
    }

    // Snapshot hit is not use-able in this case for determine the lower bound.
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() > snapshotVersion) {
      snapshotHint = Optional.empty();
    }

    long crcSearchLowerBound =
        max(
            asList(
                // Prefer reading hint over CRC, so start listing from hint's version + 1.
                snapshotHint.map(SnapshotHint::getVersion).orElse(0L) + 1,
                logSegment.getCheckpointVersionOpt().orElse(0L),
                // Only find the CRC within 100 versions.
                snapshotVersion - 100,
                0L));
    Optional<CRCInfo> crcInfoOpt =
        ChecksumReader.getCRCInfo(
            engine, logSegment.getLogPath(), snapshotVersion, crcSearchLowerBound);
    if (crcInfoOpt.isPresent()) {
      CRCInfo crcInfo = crcInfoOpt.get();
      if (crcInfo.getVersion() == snapshotVersion) {
        // CRC is related to the desired snapshot version. Load protocol and metadata from CRC.
        return new Tuple2<>(crcInfo.getProtocol(), crcInfo.getMetadata());
      }
      checkArgument(
          crcInfo.getVersion() >= crcSearchLowerBound && crcInfo.getVersion() <= snapshotVersion);
      // We found a CRCInfo of a version (a) older than the one we are looking for (snapshotVersion)
      // but (b) newer than the current hint. Use this CRCInfo to create a new hint
      snapshotHint =
          Optional.of(
              new SnapshotHint(crcInfo.getVersion(), crcInfo.getProtocol(), crcInfo.getMetadata()));
    }

    Protocol protocol = null;
    Metadata metadata = null;

    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            logSegment.allLogFilesReversed(),
            PROTOCOL_METADATA_READ_SCHEMA,
            Optional.empty())) {
      while (reverseIter.hasNext()) {
        final ActionWrapper nextElem = reverseIter.next();
        final long version = nextElem.getVersion();

        // Load this lazily (as needed). We may be able to just use the hint.
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
                return new Tuple2<>(protocol, metadata);
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
                TableFeatures.validateReadSupportedTable(
                    protocol, dataPath.toString(), Optional.of(metadata));
                return new Tuple2<>(protocol, metadata);
              }

              break; // We just found the metadata, exit this for-loop
            }
          }
        }

        // Since we haven't returned, at least one of P or M is null.
        // Note: Suppose the hint is at version N. We check the hint eagerly at N + 1 so
        // that we don't read or open any files at version N.
        if (snapshotHint.isPresent() && version == snapshotHint.get().getVersion() + 1) {
          if (protocol == null) {
            protocol = snapshotHint.get().getProtocol();
          }
          if (metadata == null) {
            metadata = snapshotHint.get().getMetadata();
          }
          return new Tuple2<>(protocol, metadata);
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Could not close iterator", ex);
    }

    if (protocol == null) {
      throw new IllegalStateException(
          String.format("No protocol found at version %s", logSegment.getVersion()));
    }

    throw new IllegalStateException(
        String.format("No metadata found at version %s", logSegment.getVersion()));
  }
}
