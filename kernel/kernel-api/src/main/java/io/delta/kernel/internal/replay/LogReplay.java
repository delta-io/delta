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
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.ScanMetrics;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.util.DomainMetadataUtils;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Replays a history of actions, resolving them to produce the current state of the table. The
 * protocol for resolution is as follows: - The most recent {@code AddFile} and accompanying
 * metadata for any `(path, dv id)` tuple wins. - {@code RemoveFile} deletes a corresponding
 * AddFile. A {@code RemoveFile} "corresponds" to the AddFile that matches both the parquet file URI
 * *and* the deletion vector's URI (if any). - The most recent {@code Metadata} wins. - The most
 * recent {@code Protocol} version wins. - For each `(path, dv id)` tuple, this class should always
 * output only one {@code FileAction} (either {@code AddFile} or {@code RemoveFile})
 *
 * <p>This class exposes the following public APIs - {@link #getProtocol()}: latest non-null
 * Protocol - {@link #getMetadata()}: latest non-null Metadata - {@link
 * #getAddFilesAsColumnarBatches}: return all active (not tombstoned) AddFiles as {@link
 * ColumnarBatch}s
 */
public abstract class LogReplay {

  //////////////////////////
  // Static Schema Fields //
  //////////////////////////

  /** Read schema when searching for the latest Protocol and Metadata. */
  public static final StructType PROTOCOL_METADATA_READ_SCHEMA =
      new StructType().add("protocol", Protocol.FULL_SCHEMA).add("metaData", Metadata.FULL_SCHEMA);

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

  /** Read schema when searching for just the domain metadata */
  public static final StructType DOMAIN_METADATA_READ_SCHEMA =
      new StructType().add("domainMetadata", DomainMetadata.FULL_SCHEMA);

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

  protected final Path dataPath;
  protected final LogSegment logSegment;
  private final Lazy<Map<String, DomainMetadata>> domainMetadataMap;

  public LogReplay(Engine engine, Path dataPath, LogSegment logSegment) {
    assertLogFilesBelongToTable(new Path(dataPath, "_delta_log"), logSegment.allLogFilesUnsorted());
    Tuple2<Optional<SnapshotHint>, Optional<CRCInfo>> newerSnapshotHintAndCurrentCrcInfo =
        maybeGetNewerSnapshotHintAndCurrentCrcInfo(
            engine, logSegment, snapshotHint, snapshotVersion);
    this.currentCrcInfo = newerSnapshotHintAndCurrentCrcInfo._2;
    this.dataPath = dataPath;
    this.logSegment = logSegment;
    this.domainMetadataMap = new Lazy<>(() -> loadDomainMetadataMap(engine));
  }

  /////////////////
  // Public APIs //
  /////////////////

  public abstract Protocol getProtocol();

  public abstract Metadata getMetadata();

  public abstract Optional<CRCInfo> getCurrentCrcInfo();

  public Optional<Long> getLatestTransactionIdentifier(Engine engine, String applicationId) {
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

  public Map<String, DomainMetadata> getDomainMetadataMap() {
    return domainMetadataMap.get();
  }

  public long getVersion() {
    return logSegment.getVersion();
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
      Engine engine,
      boolean shouldReadStats,
      Optional<Predicate> checkpointPredicate,
      ScanMetrics scanMetrics) {
    final CloseableIterator<ActionWrapper> addRemoveIter =
        new ActionsIterator(
            engine,
            logSegment.allLogFilesReversed(),
            getAddRemoveReadSchema(shouldReadStats),
            checkpointPredicate);
    return new ActiveAddFilesIterator(engine, addRemoveIter, dataPath, scanMetrics);
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  /**
   * Retrieves a map of domainName to {@link DomainMetadata} from the log files.
   *
   * <p>Loading domain metadata requires an additional round of log replay so this is done lazily
   * only when domain metadata is requested.
   *
   * @param engine The engine used to process the log files.
   * @return A map where the keys are domain names and the values are the corresponding {@link
   *     DomainMetadata} objects.
   * @throws UncheckedIOException if an I/O error occurs while closing the iterator.
   */
  private Map<String, DomainMetadata> loadDomainMetadataMap(Engine engine) {
    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            logSegment.allLogFilesReversed(),
            DOMAIN_METADATA_READ_SCHEMA,
            Optional.empty() /* checkpointPredicate */)) {
      Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
      while (reverseIter.hasNext()) {
        final ColumnarBatch columnarBatch = reverseIter.next().getColumnarBatch();
        assert (columnarBatch.getSchema().equals(DOMAIN_METADATA_READ_SCHEMA));

        final ColumnVector dmVector = columnarBatch.getColumnVector(0);

        // We are performing a reverse log replay. This function ensures that only the first
        // encountered domain metadata for each domain is added to the map.
        DomainMetadataUtils.populateDomainMetadataMap(dmVector, domainMetadataMap);
      }
      return domainMetadataMap;
    } catch (IOException ex) {
      throw new UncheckedIOException("Could not close iterator", ex);
    }
  }
}
