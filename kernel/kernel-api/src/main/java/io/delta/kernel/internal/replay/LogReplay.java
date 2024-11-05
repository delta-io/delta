/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import static io.delta.kernel.internal.replay.LogReplayUtils.assertLogFilesBelongToTable;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.DeltaErrors;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.TableFeatures;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.snapshot.SnapshotDescriptor;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Optional;

/**
 * Replays a history of actions, resolving them to produce the current state of the table. The
 * protocol for resolution is as follows: - The most recent {@code AddFile} and accompanying
 * metadata for any `(path, dv id)` tuple wins. - {@code RemoveFile} deletes a corresponding
 * AddFile. A {@code RemoveFile} "corresponds" to the AddFile that matches both the parquet file URI
 * *and* the deletion vector's URI (if any). - The most recent {@code Metadata} wins. - The most
 * recent {@code Protocol} version wins. - For each `(path, dv id)` tuple, this class should always
 * output only one {@code FileAction} (either {@code AddFile} or {@code RemoveFile})
 *
 * <p>This class exposes the following public APIs
 *
 * <ul>
 *   <li>{@link #getSnapshotDescriptor}: latest SnapshotDescriptor
 *   <li>{@link #getAddFilesAsColumnarBatches}: return all active (not tombstoned) AddFiles as
 *       {@link ColumnarBatch}s
 * </ul>
 */
public class LogReplay {

  //////////////////////////
  // Static Schema Fields //
  //////////////////////////

  /** Read schema when searching for the latest snapshot descriptor. */
  private static final StructType SNAPSHOT_DESCRIPTOR_LOG_REPLAY_SCHEMA =
      new StructType()
          .add("protocol", Protocol.FULL_SCHEMA)
          .add("metaData", Metadata.FULL_SCHEMA)
          .add("commitInfo", CommitInfo.IN_COMMIT_TIMESTAMP_SCHEMA);

  /** We don't need to read the entire RemoveFile, only the path and dv info */
  private static StructType REMOVE_FILE_SCHEMA =
      new StructType()
          .add("path", StringType.STRING, false /* nullable */)
          .add("deletionVector", DeletionVectorDescriptor.READ_SCHEMA, true /* nullable */);

  /** Read schema when searching for just the transaction identifiers */
  public static final StructType SET_TRANSACTION_READ_SCHEMA =
      new StructType().add("txn", SetTransaction.FULL_SCHEMA);

  private static StructType getAddSchema(boolean shouldReadStats) {
    return shouldReadStats ? AddFile.SCHEMA_WITH_STATS : AddFile.SCHEMA_WITHOUT_STATS;
  }

  public static String SIDECAR_FIELD_NAME = "sidecar";
  public static String ADDFILE_FIELD_NAME = "add";
  public static String REMOVEFILE_FIELD_NAME = "remove";

  public static StructType withSidecarFileSchema(StructType schema) {
    return schema.add(SIDECAR_FIELD_NAME, SidecarFile.READ_SCHEMA);
  }

  public static boolean containsAddOrRemoveFileActions(StructType schema) {
    return schema.fieldNames().contains(ADDFILE_FIELD_NAME)
        || schema.fieldNames().contains(REMOVEFILE_FIELD_NAME);
  }

  /** Read schema when searching for all the active AddFiles */
  public static StructType getAddRemoveReadSchema(boolean shouldReadStats) {
    return new StructType()
        .add(ADDFILE_FIELD_NAME, getAddSchema(shouldReadStats))
        .add(REMOVEFILE_FIELD_NAME, REMOVE_FILE_SCHEMA);
  }

  public static int ADD_FILE_ORDINAL = 0;
  public static int ADD_FILE_PATH_ORDINAL = AddFile.SCHEMA_WITHOUT_STATS.indexOf("path");
  public static int ADD_FILE_DV_ORDINAL = AddFile.SCHEMA_WITHOUT_STATS.indexOf("deletionVector");

  public static int REMOVE_FILE_ORDINAL = 1;
  public static int REMOVE_FILE_PATH_ORDINAL = REMOVE_FILE_SCHEMA.indexOf("path");
  public static int REMOVE_FILE_DV_ORDINAL = REMOVE_FILE_SCHEMA.indexOf("deletionVector");

  ///////////////////////////////////
  // Member fields and constructor //
  ///////////////////////////////////

  private final Path dataPath;
  private final LogSegment logSegment;
  private final SnapshotDescriptor snapshotDescriptor;

  public LogReplay(
      Path logPath,
      Path dataPath,
      long snapshotVersion,
      Engine engine,
      LogSegment logSegment,
      Optional<SnapshotDescriptor> snapshotHint) {
    assertLogFilesBelongToTable(logPath, logSegment.allLogFilesUnsorted());

    this.dataPath = dataPath;
    this.logSegment = logSegment;
    this.snapshotDescriptor =
        loadSnapshotDescriptor(engine, dataPath, snapshotHint, snapshotVersion);
  }

  /////////////////
  // Public APIs //
  /////////////////

  public SnapshotDescriptor getSnapshotDescriptor() {
    return snapshotDescriptor;
  }

  public Optional<Long> getLatestTransactionIdentifier(Engine engine, String applicationId) {
    return loadLatestTransactionVersion(engine, applicationId);
  }

  /**
   * Returns an iterator of {@link FilteredColumnarBatch} representing all the active AddFiles in
   * the table.
   *
   * <p>Statistics are conditionally read for the AddFiles based on {@code shouldReadStats}. The
   * returned batches have schema:
   *
   * <ol>
   *   <li>name: {@code add}
   *       <p>type: {@link AddFile#SCHEMA_WITH_STATS} if {@code shouldReadStats=true}, otherwise
   *       {@link AddFile#SCHEMA_WITHOUT_STATS}
   * </ol>
   */
  public CloseableIterator<FilteredColumnarBatch> getAddFilesAsColumnarBatches(
      Engine engine, boolean shouldReadStats, Optional<Predicate> checkpointPredicate) {
    final CloseableIterator<ActionWrapper> addRemoveIter =
        new ActionsIterator(
            engine,
            logSegment.allLogFilesReversed(),
            getAddRemoveReadSchema(shouldReadStats),
            checkpointPredicate);
    return new ActiveAddFilesIterator(engine, addRemoveIter, dataPath);
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /**
   * Returns the latest {@link SnapshotDescriptor} from the delta files in the `logSegment`.
   *
   * <p>Uses the `snapshotHint` to bound how many delta files it reads. i.e. we only need to read
   * delta files newer than the hint to search for any new P & M. If we don't find them, we can just
   * use the P and/or M from the hint.
   */
  protected SnapshotDescriptor loadSnapshotDescriptor(
      Engine engine,
      Path dataPath,
      Optional<SnapshotDescriptor> snapshotHint,
      long snapshotVersion) {
    // Exit early if the hint already has the info we need
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == snapshotVersion) {
      return snapshotHint.get();
    }

    Protocol protocol = null;
    Metadata metadata = null;
    Optional<Long> ictOpt = Optional.empty();

    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            logSegment.allLogFilesReversed(),
            SNAPSHOT_DESCRIPTOR_LOG_REPLAY_SCHEMA,
            Optional.empty())) {
      while (reverseIter.hasNext()) {
        final ActionWrapper nextElem = reverseIter.next();
        final long version = nextElem.getVersion();

        // Load this lazily (as needed). We may be able to just use the hint.
        ColumnarBatch columnarBatch = null;

        // Note that (1) we don't yet know if ICT is enabled on this table and (2) we only care
        // about getting the ICT from the latest commit whose version matches our desired snapshot
        // version.
        if (version == snapshotVersion) {
          columnarBatch = nextElem.getColumnarBatch();
          assert (columnarBatch.getSchema().equals(SNAPSHOT_DESCRIPTOR_LOG_REPLAY_SCHEMA));
          final ColumnVector commitInfoVector = columnarBatch.getColumnVector(2);

          for (int i = 0; i < commitInfoVector.getSize(); i++) {
            if (!commitInfoVector.isNullAt(i)) {
              ictOpt = CommitInfo.inCommitTimestampFromColumnVector(commitInfoVector, i);
            }

            if (!nextElem.isFromCheckpoint()) {
              // If ICT is enabled and if we are reading from a Delta file (not a checkpoint), then
              // the commitInfo action must be the first action in the commit. If this first commit
              // action is null, then ICT isn't enabled and we can break early.
              break;
            }
          }
        }

        if (protocol == null) {
          columnarBatch = nextElem.getColumnarBatch();
          assert (columnarBatch.getSchema().equals(SNAPSHOT_DESCRIPTOR_LOG_REPLAY_SCHEMA));

          final ColumnVector protocolVector = columnarBatch.getColumnVector(0);

          for (int i = 0; i < protocolVector.getSize(); i++) {
            if (!protocolVector.isNullAt(i)) {
              protocol = Protocol.fromColumnVector(protocolVector, i);

              if (metadata != null) {
                // Stop since we have found the latest Protocol and Metadata.
                validateSnapshotDescriptor(dataPath, protocol, metadata, ictOpt, snapshotVersion);
                return new SnapshotDescriptor(snapshotVersion, protocol, metadata, ictOpt);
              }

              break; // We just found the protocol, exit this for-loop
            }
          }
        }

        if (metadata == null) {
          if (columnarBatch == null) {
            columnarBatch = nextElem.getColumnarBatch();
            assert (columnarBatch.getSchema().equals(SNAPSHOT_DESCRIPTOR_LOG_REPLAY_SCHEMA));
          }
          final ColumnVector metadataVector = columnarBatch.getColumnVector(1);

          for (int i = 0; i < metadataVector.getSize(); i++) {
            if (!metadataVector.isNullAt(i)) {
              metadata = Metadata.fromColumnVector(metadataVector, i);

              if (protocol != null) {
                // Stop since we have found the latest Protocol and Metadata.
                validateSnapshotDescriptor(dataPath, protocol, metadata, ictOpt, snapshotVersion);
                return new SnapshotDescriptor(snapshotVersion, protocol, metadata, ictOpt);
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
          return new SnapshotDescriptor(snapshotVersion, protocol, metadata, ictOpt);
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Could not close iterator", ex);
    }

    if (protocol == null) {
      throw new IllegalStateException(
          String.format("No protocol found at version %s", logSegment.version));
    }

    throw new IllegalStateException(
        String.format("No metadata found at version %s", logSegment.version));
  }

  /**
   * Note: this is called from {@link #loadSnapshotDescriptor}, which is called from the
   * constructor, which has not finished initializing. Do not use member fields declared in the
   * constructor.
   */
  private void validateSnapshotDescriptor(
      Path dataPath,
      Protocol protocol,
      Metadata metadata,
      Optional<Long> ictOpt,
      long snapshotVersion) {
    if (TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(metadata) && !ictOpt.isPresent()) {
      throw DeltaErrors.missingCommitTimestamp(dataPath.toString(), snapshotVersion);
    }
    TableFeatures.validateReadSupportedTable(protocol, dataPath.toString(), Optional.of(metadata));
  }

  private Optional<Long> loadLatestTransactionVersion(Engine engine, String applicationId) {
    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            logSegment.allLogFilesReversed(),
            SET_TRANSACTION_READ_SCHEMA,
            Optional.empty())) {
      while (reverseIter.hasNext()) {
        final ColumnarBatch columnarBatch = reverseIter.next().getColumnarBatch();
        assert (columnarBatch.getSchema().equals(SET_TRANSACTION_READ_SCHEMA));

        final ColumnVector txnVector = columnarBatch.getColumnVector(0);
        for (int rowId = 0; rowId < txnVector.getSize(); rowId++) {
          if (!txnVector.isNullAt(rowId)) {
            SetTransaction txn = SetTransaction.fromColumnVector(txnVector, rowId);
            if (txn != null && applicationId.equals(txn.getAppId())) {
              return Optional.of(txn.getVersion());
            }
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException("Failed to fetch the transaction identifier", ex);
    }

    return Optional.empty();
  }
}
