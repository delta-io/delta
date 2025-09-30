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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.checkpoints.SidecarFile;
import io.delta.kernel.internal.checksum.CRCInfo;
import io.delta.kernel.internal.checksum.ChecksumReader;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.lang.Lazy;
import io.delta.kernel.internal.metrics.ScanMetrics;
import io.delta.kernel.internal.metrics.SnapshotMetrics;
import io.delta.kernel.internal.snapshot.LogSegment;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.DomainMetadataUtils;
import io.delta.kernel.internal.util.Tuple2;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replays a history of actions, resolving them to produce the current state of the table. The
 * protocol for resolution is as follows:
 *
 * <ul>
 *   <li>The most recent {@code AddFile} and accompanying metadata for any `(path, dv id)` tuple
 *       wins.
 *   <li>{@code RemoveFile} deletes a corresponding AddFile. A {@code RemoveFile} "corresponds" to
 *       the AddFile that matches both the parquet file URI *and* the deletion vector's URI (if
 *       any).
 *   <li>The most recent {@code Metadata} wins.
 *   <li>The most recent {@code Protocol} version wins.
 *   <li>For each `(path, dv id)` tuple, this class should always output only one {@code *
 *       FileAction} (either {@code AddFile} or {@code RemoveFile})
 * </ul>
 */
public class LogReplay {

  private static final Logger logger = LoggerFactory.getLogger(LogReplay.class);

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

  /** Read schema when searching only for AddFiles */
  public static StructType getAddReadSchema(boolean shouldReadStats) {
    return new StructType().add(ADDFILE_FIELD_NAME, getAddSchema(shouldReadStats));
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
  private final Lazy<LogSegment> lazyLogSegment;
  private final Lazy<Optional<CRCInfo>> lazyLatestCrcInfo;
  private final Lazy<Tuple2<Protocol, Metadata>> lazyProtocolAndMetadata;
  private final Lazy<Map<String, DomainMetadata>> lazyActiveDomainMetadataMap;

  public LogReplay(
      Path dataPath,
      Engine engine,
      Lazy<LogSegment> lazyLogSegment,
      SnapshotMetrics snapshotMetrics) {
    this.dataPath = dataPath;

    this.lazyLogSegment = lazyLogSegment;

    // Lazy loading of CRC info only when needed
    this.lazyLatestCrcInfo =
        new Lazy<>(
            () ->
                snapshotMetrics.loadCrcTotalDurationTimer.time(
                    () ->
                        getLogSegment()
                            .getLastSeenChecksum()
                            .flatMap(crcFile -> ChecksumReader.getCRCInfo(engine, crcFile))));

    // Lazy loading of protocol and metadata only when needed
    this.lazyProtocolAndMetadata = createLazyProtocolAndMetadata(engine, snapshotMetrics);

    // Lazy loading of domain metadata only when needed
    this.lazyActiveDomainMetadataMap =
        new Lazy<>(
            () ->
                loadDomainMetadataMap(engine).entrySet().stream()
                    .filter(entry -> !entry.getValue().isRemoved())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /////////////////
  // Public APIs //
  /////////////////

  public Protocol getProtocol() {
    return lazyProtocolAndMetadata.get()._1;
  }

  public Metadata getMetadata() {
    return lazyProtocolAndMetadata.get()._2;
  }

  public Optional<Long> getLatestTransactionIdentifier(Engine engine, String applicationId) {
    return loadLatestTransactionVersion(engine, applicationId);
  }

  /** Returns map for all active domain metadata. */
  public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
    return lazyActiveDomainMetadataMap.get();
  }

  public long getVersion() {
    return getLogSegment().getVersion();
  }

  /**
   * Returns the CRC info for the current snapshot version if available. Lazily loads and caches the
   * CRC file on first access. Returns empty if no CRC file exists at the snapshot version.
   */
  public Optional<CRCInfo> getCrcInfoAtSnapshotVersion() {
    return lazyLatestCrcInfo.get().filter(crcInfo -> crcInfo.getVersion() == getVersion());
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
      ScanMetrics scanMetrics,
      Optional<PaginationContext> paginationContextOpt) {
    // We do not need to look at any `remove` files from the checkpoints. Skip the column to save
    // I/O. Note that we are still going to process the row groups. Adds and removes are randomly
    // scattered through checkpoint part files, so row group push down is unlikely to be useful.
    final CloseableIterator<ActionWrapper> addRemoveIter =
        new ActionsIterator(
            engine,
            getLogReplayFiles(getLogSegment()),
            getAddRemoveReadSchema(shouldReadStats),
            getAddReadSchema(shouldReadStats),
            checkpointPredicate,
            paginationContextOpt);
    return new ActiveAddFilesIterator(engine, addRemoveIter, dataPath, scanMetrics);
  }

  public LogSegment getLogSegment() {
    return lazyLogSegment.get();
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  // For now we always read log compaction files. Plumb an option through to here if we ever want to
  // make it configurable
  private boolean readLogCompactionFiles = true;

  /**
   * Get the files to use for this log replay, can be configured for example to use or not use log
   * compaction files
   */
  private List<FileStatus> getLogReplayFiles(LogSegment logSegment) {
    if (readLogCompactionFiles) {
      return logSegment.allFilesWithCompactionsReversed();
    } else {
      return logSegment.allLogFilesReversed();
    }
  }

  private Lazy<Tuple2<Protocol, Metadata>> createLazyProtocolAndMetadata(
      Engine engine, SnapshotMetrics snapshotMetrics) {
    return new Lazy<>(
        () -> {
          // Ensure log segment is loaded and valid before starting any protocol/metadata loading
          // timers. This ensures that failures during log segment construction (e.g., invalid
          // version, missing table) occur before protocol/metadata loading metrics are recorded.
          final LogSegment logSegment = getLogSegment();
          final long targetVersion = logSegment.getVersion();

          final Tuple2<Protocol, Metadata> result =
              snapshotMetrics.loadProtocolMetadataTotalDurationTimer.time(
                  () -> {
                    Tuple2<Protocol, Metadata> protocolAndMetadata =
                        loadTableProtocolAndMetadata(
                            engine, logSegment, lazyLatestCrcInfo.get(), targetVersion);

                    TableFeatures.validateKernelCanReadTheTable(
                        protocolAndMetadata._1, dataPath.toString());

                    return protocolAndMetadata;
                  });

          logger.info(
              "[{}] Took {}ms to load Protocol and Metadata at version {}",
              dataPath.toString(),
              snapshotMetrics.loadProtocolMetadataTotalDurationTimer.totalDurationMs(),
              targetVersion);

          return result;
        });
  }

  /**
   * Returns the latest Protocol and Metadata from the delta files in the `logSegment`. Does *not*
   * validate that this delta-kernel connector understands the table at that protocol.
   *
   * <p>Uses the `latestCrcInfo` to bound how many delta files it reads. i.e. we only need to read
   * delta files newer than the latestCrcInfo to search for any new P & M. If we don't find them, we
   * can just use the P and/or M from the latestCrcInfo.
   */
  protected Tuple2<Protocol, Metadata> loadTableProtocolAndMetadata(
      Engine engine, LogSegment logSegment, Optional<CRCInfo> latestCrcInfo, long snapshotVersion) {
    long logReadCount = 0;
    // Exit early if the CRC already has the info we need.
    if (latestCrcInfo.isPresent() && latestCrcInfo.get().getVersion() == snapshotVersion) {
      return new Tuple2<>(latestCrcInfo.get().getProtocol(), latestCrcInfo.get().getMetadata());
    }

    Protocol protocol = null;
    Metadata metadata = null;

    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            getLogReplayFiles(logSegment),
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
                return new Tuple2<>(protocol, metadata);
              }

              break; // We just found the metadata, exit this for-loop
            }
          }
        }

        // Since we haven't returned, at least one of P or M is null.
        // Note: Suppose the CRC is at version N. We check the CRC eagerly at N + 1 so
        // that we don't read or open any files at version N.
        if (latestCrcInfo.isPresent() && version == latestCrcInfo.get().getVersion() + 1) {
          if (protocol == null) {
            protocol = latestCrcInfo.get().getProtocol();
          }
          if (metadata == null) {
            metadata = latestCrcInfo.get().getMetadata();
          }
          logger.info(
              "{}: Loading Protocol and Metadata read {} logs with CRC at version {}",
              dataPath.toString(),
              logReadCount,
              latestCrcInfo.map(crcInfo -> String.valueOf(crcInfo.getVersion())).orElse("N/A"));
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

  private Optional<Long> loadLatestTransactionVersion(Engine engine, String applicationId) {
    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            getLogReplayFiles(getLogSegment()),
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

  /**
   * Loads the domain metadata map, either from CRC info (if available) or from the transaction log.
   *
   * @param engine The engine to use for loading from log when necessary
   * @return A map of domain names to their metadata
   */
  private Map<String, DomainMetadata> loadDomainMetadataMap(Engine engine) {
    long startTimeMillis = System.currentTimeMillis();
    // Case 1: CRC does not exist or does not have domain metadata => read DM from log
    final Optional<CRCInfo> latestCrcInfoOpt = lazyLatestCrcInfo.get();
    if (!latestCrcInfoOpt.isPresent() || !latestCrcInfoOpt.get().getDomainMetadata().isPresent()) {
      Map<String, DomainMetadata> domainMetadataMap =
          loadDomainMetadataMapFromLog(engine, Optional.empty());
      logger.info(
          "{}:No domain metadata available in CRC info,"
              + " loading domain metadata for version {} from logs took {}ms",
          dataPath.toString(),
          getVersion(),
          System.currentTimeMillis() - startTimeMillis);
      return domainMetadataMap;
    }

    final CRCInfo latestCrcInfo = latestCrcInfoOpt.get();

    // Case 2: CRC exists at the snapshot version and has domain metadata => read DM from CRC
    if (latestCrcInfo.getVersion() == getVersion()) {
      Map<String, DomainMetadata> domainMetadataMap =
          latestCrcInfo.getDomainMetadata().get().stream()
              .collect(Collectors.toMap(DomainMetadata::getDomain, Function.identity()));
      logger.info(
          "{}:CRC for version {} found, loading domain metadata from CRC took {}ms",
          dataPath.toString(),
          getVersion(),
          System.currentTimeMillis() - startTimeMillis);
      return domainMetadataMap;
    }

    // Case 3: CRC exists at an *earlier* version and has domain metadata => read DM from CRC and
    //         read DM from log for newer versions
    Map<String, DomainMetadata> finalDomainMetadataMap =
        loadDomainMetadataMapFromLog(engine, Optional.of(latestCrcInfo.getVersion() + 1));
    // Add domains from the CRC that don't exist in the incremental log data
    // - If a domain is updated to the newer versions or removed, it will exist in
    // finalDomainMetadataMap, use the one in the map.
    // - If a domain is only in the CRC file, use the one from CRC.
    latestCrcInfo
        .getDomainMetadata()
        .get()
        .forEach(
            domainMetadataInCrc -> {
              if (!finalDomainMetadataMap.containsKey(domainMetadataInCrc.getDomain())) {
                finalDomainMetadataMap.put(domainMetadataInCrc.getDomain(), domainMetadataInCrc);
              }
            });
    logger.info(
        "{}: Loading domain metadata for version {} from logs with crc version {} took {}ms",
        dataPath.toString(),
        getVersion(),
        latestCrcInfo.getVersion(),
        System.currentTimeMillis() - startTimeMillis);
    return finalDomainMetadataMap;
  }

  /**
   * Retrieves a map of domainName to {@link DomainMetadata} from the log files.
   *
   * <p>Loading domain metadata requires an additional round of log replay so this is done lazily
   * only when domain metadata is requested. We might want to merge this into {@link
   * #loadTableProtocolAndMetadata}.
   *
   * @param engine The engine used to process the log files.
   * @param minLogVersion The minimum log version to read (inclusive). When provided, only reads log
   *     files * starting from this version. When not provided, reads the entire log. * For
   *     incremental loading from crc, this is typically set to (crc version + 1).
   * @return A map where the keys are domain names and the values are the corresponding {@link
   *     DomainMetadata} objects.
   * @throws UncheckedIOException if an I/O error occurs while closing the iterator.
   */
  private Map<String, DomainMetadata> loadDomainMetadataMapFromLog(
      Engine engine, Optional<Long> minLogVersion) {
    long logReadCount = 0;
    try (CloseableIterator<ActionWrapper> reverseIter =
        new ActionsIterator(
            engine,
            getLogReplayFiles(getLogSegment()),
            DOMAIN_METADATA_READ_SCHEMA,
            Optional.empty() /* checkpointPredicate */)) {
      Map<String, DomainMetadata> domainMetadataMap = new HashMap<>();
      while (reverseIter.hasNext()) {
        final ActionWrapper nextElem = reverseIter.next();
        final long version = nextElem.getVersion();
        final ColumnarBatch columnarBatch = nextElem.getColumnarBatch();
        logReadCount++;
        assert (columnarBatch.getSchema().equals(DOMAIN_METADATA_READ_SCHEMA));

        final ColumnVector dmVector = columnarBatch.getColumnVector(0);

        // We are performing a reverse log replay. This function ensures that only the first
        // encountered domain metadata for each domain is added to the map.
        DomainMetadataUtils.populateDomainMetadataMap(dmVector, domainMetadataMap);
        if (minLogVersion.isPresent() && minLogVersion.get() == version) {
          break;
        }
      }
      logger.info(
          "{}: Loading domain metadata from log for version {}, "
              + "read {} actions, using crc version {}",
          dataPath.toString(),
          getVersion(),
          logReadCount,
          minLogVersion.map(String::valueOf).orElse("N/A"));
      return domainMetadataMap;
    } catch (IOException ex) {
      throw new UncheckedIOException("Could not close iterator", ex);
    }
  }
}
