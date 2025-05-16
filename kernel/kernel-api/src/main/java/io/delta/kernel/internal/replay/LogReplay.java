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
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

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
import io.delta.kernel.internal.snapshot.SnapshotHint;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.internal.util.DomainMetadataUtils;
import io.delta.kernel.internal.util.FileNames;
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
  private final LogSegment logSegment;
  private final Tuple2<Protocol, Metadata> protocolAndMetadata;
  private final Lazy<Map<String, DomainMetadata>> activeDomainMetadataMap;
  private final CrcInfoContext crcInfoContext;

  public LogReplay(
      Path logPath,
      Path dataPath,
      Engine engine,
      LogSegment logSegment,
      Optional<SnapshotHint> snapshotHint,
      SnapshotMetrics snapshotMetrics) {

    assertLogFilesBelongToTable(logPath, logSegment.allLogFilesUnsorted());

    // Ignore the snapshot hint whose version is larger than the snapshot version.
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() > logSegment.getVersion()) {
      snapshotHint = Optional.empty();
    }

    this.crcInfoContext = new CrcInfoContext(engine);
    this.dataPath = dataPath;
    this.logSegment = logSegment;
    Optional<SnapshotHint> newerSnapshotHint =
        crcInfoContext.maybeGetNewerSnapshotHintAndUpdateCache(
            engine, logSegment, snapshotHint, logSegment.getVersion());
    this.protocolAndMetadata =
        snapshotMetrics.loadInitialDeltaActionsTimer.time(
            () -> {
              Tuple2<Protocol, Metadata> protocolAndMetadata =
                  loadTableProtocolAndMetadata(
                      engine, logSegment, newerSnapshotHint, logSegment.getVersion());

              TableFeatures.validateKernelCanReadTheTable(
                  protocolAndMetadata._1, dataPath.toString());

              return protocolAndMetadata;
            });
    // Lazy loading of domain metadata only when needed
    this.activeDomainMetadataMap =
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
    return this.protocolAndMetadata._1;
  }

  public Metadata getMetadata() {
    return this.protocolAndMetadata._2;
  }

  public Optional<Long> getLatestTransactionIdentifier(Engine engine, String applicationId) {
    return loadLatestTransactionVersion(engine, applicationId);
  }

  /* Returns map for all active domain metadata. */
  public Map<String, DomainMetadata> getActiveDomainMetadataMap() {
    return activeDomainMetadataMap.get();
  }

  public long getVersion() {
    return logSegment.getVersion();
  }

  /** Returns the crc info for the current snapshot if it is cached */
  public Optional<CRCInfo> getCurrentCrcInfo() {
    return crcInfoContext
        .getLastSeenCrcInfo()
        .filter(crcInfo -> crcInfo.getVersion() == getVersion());
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
    // We do not need to look at any `remove` files from the checkpoints. Skip the column to save
    // I/O. Note that we are still going to process the row groups. Adds and removes are randomly
    // scattered through checkpoint part files, so row group push down is unlikely to be useful.
    final CloseableIterator<ActionWrapper> addRemoveIter =
        new ActionsIterator(
            engine,
            getLogReplayFiles(logSegment),
            getAddRemoveReadSchema(shouldReadStats),
            getAddReadSchema(shouldReadStats),
            checkpointPredicate);
    return new ActiveAddFilesIterator(engine, addRemoveIter, dataPath, scanMetrics);
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
    long logReadCount = 0;
    // Exit early if the hint already has the info we need.
    if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == snapshotVersion) {
      return new Tuple2<>(snapshotHint.get().getProtocol(), snapshotHint.get().getMetadata());
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
          logger.info(
              "{}: Loading Protocol and Metadata read {} logs with snapshot hint at version {}",
              dataPath.toString(),
              logReadCount,
              snapshotHint.map(hint -> String.valueOf(hint.getVersion())).orElse("N/A"));
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
            engine, getLogReplayFiles(logSegment), SET_TRANSACTION_READ_SCHEMA, Optional.empty())) {
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
    // First try to load from CRC info if available
    Optional<CRCInfo> lastSeenCrcInfoOpt = crcInfoContext.getLastSeenCrcInfo();
    if (!lastSeenCrcInfoOpt.isPresent()
        || !lastSeenCrcInfoOpt.get().getDomainMetadata().isPresent()) {
      Map<String, DomainMetadata> domainMetadataMap =
          loadDomainMetadataMapFromLog(engine, Optional.empty());
      logger.info(
          "{}:No domain metadata available in CRC info,"
              + " loading domain metadata for version {} from logs took {}ms",
          dataPath.toString(),
          logSegment.getVersion(),
          System.currentTimeMillis() - startTimeMillis);
      return domainMetadataMap;
    }
    CRCInfo lastSeenCrcInfo = lastSeenCrcInfoOpt.get();
    if (lastSeenCrcInfo.getVersion() == logSegment.getVersion()) {
      Map<String, DomainMetadata> domainMetadataMap =
          lastSeenCrcInfo.getDomainMetadata().get().stream()
              .collect(Collectors.toMap(DomainMetadata::getDomain, Function.identity()));
      logger.info(
          "{}:CRC for version {} found, loading domain metadata from CRC took {}ms",
          dataPath.toString(),
          logSegment.getVersion(),
          System.currentTimeMillis() - startTimeMillis);
      return domainMetadataMap;
    }

    Map<String, DomainMetadata> finalDomainMetadataMap =
        loadDomainMetadataMapFromLog(engine, Optional.of(lastSeenCrcInfo.getVersion() + 1));
    // Add domains from the CRC that don't exist in the incremental log data
    // - If a domain is updated to the newer versions or removed, it will exist in
    // finalDomainMetadataMap, use the one in the map.
    // - If a domain is only in the CRC file, use the one from CRC.
    lastSeenCrcInfo
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
        logSegment.getVersion(),
        lastSeenCrcInfo.getVersion(),
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
            getLogReplayFiles(logSegment),
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
          logSegment.getVersion(),
          logReadCount,
          minLogVersion.map(String::valueOf).orElse("N/A"));
      return domainMetadataMap;
    } catch (IOException ex) {
      throw new UncheckedIOException("Could not close iterator", ex);
    }
  }

  /**
   * Encapsulates CRC-related functionality and state for the LogReplay. This includes caching CRC
   * info and extracting snapshot hints from CRC files.
   *
   * <p>This class uses {@code maybeGetNewerSnapshotHintAndUpdateCache} to calculate a {@code
   * SnapshotHint} and also exposes a {@code getLastSeenCrcInfo} method. Their relationship is:
   *
   * <ul>
   *   <li>We want to find the latest {@code SnapshotHint} to use during log replay for Protocol and
   *       Metadata loading
   *   <li>If we are not provided a SnapshotHint for this version, or are provided a stale hint, we
   *       will try to read the latest seen (by file listing) CRC file (if it exists). If so, we
   *       read it, cache it, and create a newer hint.
   *   <li>Then, when {@code getLastSeenCrcInfo} is called, we will either use the cached CRCInfo
   *       that we have already read, parsed, and cached; or, if it was never cached (because the
   *       hint was sufficiently new) we will read it, parse it, and cache it for the first time
   * </ul>
   */
  private class CrcInfoContext {
    private final Engine engine;
    private Optional<CRCInfo> cachedLastSeenCrcInfo;

    CrcInfoContext(Engine engine) {
      this.engine = requireNonNull(engine);
      this.cachedLastSeenCrcInfo = Optional.empty();
    }

    /** Returns the CRC info persisted in the logSegment's lastSeenChecksum File */
    public Optional<CRCInfo> getLastSeenCrcInfo() {
      if (!cachedLastSeenCrcInfo.isPresent()) {
        cachedLastSeenCrcInfo =
            logSegment
                .getLastSeenChecksum()
                .flatMap(crcFile -> ChecksumReader.getCRCInfo(engine, crcFile));
      }
      return cachedLastSeenCrcInfo;
    }

    /**
     * Attempts to build a newer snapshot hint from CRC that can be used for loading table state
     * more efficiently. When CRC is read, updates the internal cache.
     *
     * @param engine The engine used to read CRC files
     * @param logSegment The log segment containing checksum information
     * @param snapshotHint Existing snapshot hint, if any
     * @param snapshotVersion Target snapshot version
     * @return An updated snapshot hint if a newer CRC file was found, otherwise the original hint
     */
    public Optional<SnapshotHint> maybeGetNewerSnapshotHintAndUpdateCache(
        Engine engine,
        LogSegment logSegment,
        Optional<SnapshotHint> snapshotHint,
        long snapshotVersion) {

      // Snapshot hint's version is current so we could use it in loading P&M.
      // No need to read crc.
      if (snapshotHint.isPresent() && snapshotHint.get().getVersion() == snapshotVersion) {
        return snapshotHint;
      }

      // Prefer reading hint over CRC to save 1 io, only read crc if it is newer than snapshot hint.
      long crcReadLowerBound = snapshotHint.map(SnapshotHint::getVersion).orElse(-1L) + 1;

      Optional<CRCInfo> crcInfoOpt =
          logSegment
              .getLastSeenChecksum()
              .filter(
                  checksum ->
                      FileNames.getFileVersion(new Path(checksum.getPath())) >= crcReadLowerBound)
              .flatMap(checksum -> ChecksumReader.getCRCInfo(engine, checksum));

      if (!crcInfoOpt.isPresent()) {
        return snapshotHint;
      }

      CRCInfo crcInfo = crcInfoOpt.get();
      this.cachedLastSeenCrcInfo = Optional.of(crcInfo);
      checkArgument(
          crcInfo.getVersion() >= crcReadLowerBound && crcInfo.getVersion() <= snapshotVersion);
      // We found a CRCInfo of a version (a) older than the one we are looking for (snapshotVersion)
      // but (b) newer than the current hint. Use this CRCInfo to create a new hint, and return.
      return Optional.of(SnapshotHint.fromCrcInfo(crcInfo));
    }
  }
}
