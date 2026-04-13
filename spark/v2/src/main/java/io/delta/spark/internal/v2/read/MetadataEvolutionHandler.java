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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.adapters.SparkMetadataAdapter;
import io.delta.spark.internal.v2.adapters.SparkProtocolAdapter;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport$;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaStreamUtils;
import org.apache.spark.sql.delta.sources.PersistedMetadata;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Handles metadata and schema evolution for the v2 Delta streaming source.
 *
 * <p>This is the v2 counterpart of the v1 {@code DeltaSourceMetadataEvolutionSupport} trait. It
 * manages the metadata tracking log, detects metadata/protocol changes during streaming, and
 * implements the two-barrier offset protocol to ensure safe schema evolution.
 *
 * <p>Delegates to static utilities in {@code DeltaSourceMetadataEvolutionSupport} for validation
 * logic that is shared between v1 and v2.
 */
public class MetadataEvolutionHandler {

  private static final Logger logger = LoggerFactory.getLogger(MetadataEvolutionHandler.class);

  private final SparkSession spark;
  private final String tableId;
  private final String tablePath;
  private final DeltaSnapshotManager snapshotManager;
  private final Engine engine;
  private final DeltaOptions options;
  private final DeltaStreamUtils.SchemaReadOptions schemaReadOptions;
  private final Option<DeltaSourceMetadataTrackingLog> metadataTrackingLog;

  // Read-time state captured at source initialization
  private final StructType readSchemaAtSourceInit;
  private final StructType readPartitionSchemaAtSourceInit;
  private final Map<String, String> readConfigurationsAtSourceInit;
  private final Protocol readProtocolAtSourceInit;
  private final String metadataPath;

  /** The persisted metadata at source init, if any. */
  private final PersistedMetadata persistedMetadataAtSourceInit;

  public MetadataEvolutionHandler(
      SparkSession spark,
      String tableId,
      String tablePath,
      DeltaSnapshotManager snapshotManager,
      Engine engine,
      DeltaOptions options,
      DeltaStreamUtils.SchemaReadOptions schemaReadOptions,
      Option<DeltaSourceMetadataTrackingLog> metadataTrackingLog,
      StructType readSchemaAtSourceInit,
      StructType readPartitionSchemaAtSourceInit,
      Map<String, String> readConfigurationsAtSourceInit,
      Protocol readProtocolAtSourceInit,
      String metadataPath) {
    this.spark = Objects.requireNonNull(spark);
    this.tableId = Objects.requireNonNull(tableId);
    this.tablePath = Objects.requireNonNull(tablePath);
    this.snapshotManager = Objects.requireNonNull(snapshotManager);
    this.engine = Objects.requireNonNull(engine);
    this.options = Objects.requireNonNull(options);
    this.schemaReadOptions = Objects.requireNonNull(schemaReadOptions);
    this.metadataTrackingLog = Objects.requireNonNull(metadataTrackingLog);
    this.readSchemaAtSourceInit = Objects.requireNonNull(readSchemaAtSourceInit);
    this.readPartitionSchemaAtSourceInit = Objects.requireNonNull(readPartitionSchemaAtSourceInit);
    this.readConfigurationsAtSourceInit = Objects.requireNonNull(readConfigurationsAtSourceInit);
    this.readProtocolAtSourceInit = Objects.requireNonNull(readProtocolAtSourceInit);
    this.metadataPath = Objects.requireNonNull(metadataPath);
    this.persistedMetadataAtSourceInit =
        metadataTrackingLog.isDefined()
                && metadataTrackingLog.get().getCurrentTrackedMetadata().isDefined()
            ? metadataTrackingLog.get().getCurrentTrackedMetadata().get()
            : null;
  }

  /** Whether this source should use schema tracking for metadata evolution. */
  public boolean shouldTrackMetadataChange() {
    return DeltaSourceMetadataEvolutionSupport$.MODULE$.shouldTrackMetadataChange(
        schemaReadOptions, metadataTrackingLog);
  }

  /**
   * Whether the tracking log should be initialized eagerly. This is true when the log is provided
   * but empty. Should only be used for the first write to the schema log.
   */
  public boolean shouldInitializeMetadataTrackingEagerly() {
    return DeltaSourceMetadataEvolutionSupport$.MODULE$.shouldInitializeMetadataTrackingEagerly(
        schemaReadOptions, metadataTrackingLog);
  }

  // ---------------------------------------------------------------------------
  // Offset barrier protocol
  // ---------------------------------------------------------------------------

  /**
   * Truncate the file change iterator at the schema change barrier (inclusive).
   *
   * <p>This ensures a batch never crosses a schema change boundary — it stops at the barrier
   * IndexedFile so the batch can be committed before the schema evolution takes effect.
   */
  public CloseableIterator<IndexedFile> stopIndexedFileIteratorAtSchemaChangeBarrier(
      CloseableIterator<IndexedFile> fileActions) {
    // Consume until we hit the barrier, include the barrier itself, discard the rest.
    List<IndexedFile> result = new ArrayList<>();
    try {
      while (fileActions.hasNext()) {
        IndexedFile file = fileActions.next();
        result.add(file);
        if (file.getIndex() == DeltaSourceOffset.METADATA_CHANGE_INDEX()) {
          break;
        }
      }
    } finally {
      try {
        fileActions.close();
      } catch (Exception e) {
        throw new RuntimeException("Failed to close file actions iterator", e);
      }
    }
    return Utils.toCloseableIterator(result.iterator());
  }

  /**
   * If the version has a metadata or protocol change compared to the current stream metadata,
   * return an iterator with a single sentinel IndexedFile at METADATA_CHANGE_INDEX to act as a
   * barrier. Otherwise, return an empty iterator. The caller concatenates this into the file change
   * stream.
   */
  public CloseableIterator<IndexedFile> getMetadataOrProtocolChangeIndexedFileIterator(
      Metadata metadata, Protocol protocol, long version) {
    if (shouldTrackMetadataChange()
        && hasMetadataOrProtocolChangeComparedToStreamMetadata(metadata, protocol, version)) {
      return Utils.toCloseableIterator(
          Collections.singletonList(
                  IndexedFile.sentinel(version, DeltaSourceOffset.METADATA_CHANGE_INDEX()))
              .iterator());
    }
    return Utils.toCloseableIterator(Collections.emptyIterator());
  }

  /**
   * Handle pending schema change offsets. Implements the two-barrier protocol:
   *
   * <ul>
   *   <li>If previous offset is at METADATA_CHANGE_INDEX, advance to POST_METADATA_CHANGE_INDEX
   *   <li>If previous offset is at POST_METADATA_CHANGE_INDEX and schema evolution hasn't happened
   *       yet, block by returning the same offset
   * </ul>
   *
   * @return the next offset if a schema change is pending, empty otherwise
   */
  public Optional<DeltaSourceOffset> getNextOffsetIfPendingSchemaChange(
      DeltaSourceOffset previousOffset) {
    if (previousOffset.index() == DeltaSourceOffset.METADATA_CHANGE_INDEX()) {
      return Optional.of(
          previousOffset.copy(
              previousOffset.reservoirId(),
              previousOffset.reservoirVersion(),
              DeltaSourceOffset.POST_METADATA_CHANGE_INDEX(),
              previousOffset.isInitialSnapshot()));
    }

    if (previousOffset.index() == DeltaSourceOffset.POST_METADATA_CHANGE_INDEX()) {
      // Check if schema evolution has actually occurred; if not, block.
      Metadata metadata = collectMetadataAtVersion(previousOffset.reservoirVersion());
      Protocol protocol = collectProtocolAtVersion(previousOffset.reservoirVersion());
      if (hasMetadataOrProtocolChangeComparedToStreamMetadata(
          metadata, protocol, previousOffset.reservoirVersion())) {
        return Optional.of(previousOffset);
      }
    }

    return Optional.empty();
  }

  // ---------------------------------------------------------------------------
  // Commit-time evolution
  // ---------------------------------------------------------------------------

  /**
   * Called from commit(). If the committed offset is a schema change barrier, write the new
   * metadata to the tracking log and fail the stream to trigger re-analysis.
   */
  public void updateMetadataTrackingLogAndFailIfNeeded(DeltaSourceOffset offset) {
    if (!shouldTrackMetadataChange()) {
      return;
    }
    if (offset.index() != DeltaSourceOffset.METADATA_CHANGE_INDEX()
        && offset.index() != DeltaSourceOffset.POST_METADATA_CHANGE_INDEX()) {
      return;
    }

    Metadata changedMetadata = collectMetadataAtVersion(offset.reservoirVersion());
    Protocol changedProtocol = collectProtocolAtVersion(offset.reservoirVersion());

    updateMetadataTrackingLogAndFailIfNeeded(
        changedMetadata, changedProtocol, offset.reservoirVersion(), /* replace= */ false);
  }

  /**
   * Write new metadata into the tracking log and fail the stream if there are changes compared to
   * the current stream metadata.
   */
  public void updateMetadataTrackingLogAndFailIfNeeded(
      Metadata changedMetadata, Protocol changedProtocol, long version, boolean replace) {
    if (!hasMetadataOrProtocolChangeComparedToStreamMetadata(
        changedMetadata, changedProtocol, version)) {
      return;
    }

    Metadata metadataToUse = changedMetadata != null ? changedMetadata : getMetadataAtSourceInit();
    Protocol protocolToUse = changedProtocol != null ? changedProtocol : readProtocolAtSourceInit;

    assert metadataTrackingLog.isDefined()
        : "Metadata tracking log must be present to update metadata.";

    PersistedMetadata schemaToPersist =
        PersistedMetadata.apply(
            tableId,
            version,
            new SparkMetadataAdapter(metadataToUse),
            new SparkProtocolAdapter(protocolToUse),
            metadataPath);

    if (replace) {
      metadataTrackingLog.get().writeNewMetadata(schemaToPersist, true);
    } else {
      metadataTrackingLog.get().writeNewMetadata(schemaToPersist, false);
    }

    throw (RuntimeException)
        DeltaErrors.streamingMetadataEvolutionException(
            schemaToPersist.dataSchema(),
            schemaToPersist.tableConfigurations().get(),
            schemaToPersist.protocol().get());
  }

  // ---------------------------------------------------------------------------
  // Initialization
  // ---------------------------------------------------------------------------

  /**
   * Initialize the metadata tracking log on the first batch. Validates that the metadata across the
   * version range is safe to read, then writes the initial entry.
   *
   * @param batchStartVersion start version of the batch
   * @param batchEndVersion optional end version (for constructed batches with existing end offset)
   * @param alwaysFailUponLogInitialized whether to always fail with schema evolution exception
   */
  public void initializeMetadataTrackingAndExitStream(
      long batchStartVersion, Long batchEndVersion, boolean alwaysFailUponLogInitialized) {
    long version;
    Metadata metadata;
    Protocol protocol;

    if (batchEndVersion != null) {
      // Validate no incompatible changes in the range, use the end version's metadata
      ValidatedMetadataAndProtocol validated =
          validateAndResolveMetadataForLogInitialization(batchStartVersion, batchEndVersion);
      version = batchEndVersion;
      metadata = validated.metadata;
      protocol = validated.protocol;
    } else {
      SnapshotImpl snapshot = (SnapshotImpl) snapshotManager.loadSnapshotAt(batchStartVersion);
      version = snapshot.getVersion();
      metadata = snapshot.getMetadata();
      protocol = snapshot.getProtocol();
    }

    assert metadataTrackingLog.isDefined()
        : "Metadata tracking log must be present to initialize metadata tracking.";

    PersistedMetadata newMetadata =
        PersistedMetadata.apply(
            tableId,
            version,
            new SparkMetadataAdapter(metadata),
            new SparkProtocolAdapter(protocol),
            metadataPath);
    metadataTrackingLog.get().writeNewMetadata(newMetadata, false);

    if (hasMetadataOrProtocolChangeComparedToStreamMetadata(metadata, protocol, version)
        || alwaysFailUponLogInitialized) {
      throw (RuntimeException)
          DeltaErrors.streamingMetadataEvolutionException(
              newMetadata.dataSchema(),
              newMetadata.tableConfigurations().get(),
              newMetadata.protocol().get());
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Delegates to the shared static method in {@code DeltaSourceMetadataEvolutionSupport}. */
  private boolean hasMetadataOrProtocolChangeComparedToStreamMetadata(
      Metadata newMetadata, Protocol newProtocol, long newSchemaVersion) {
    Option<SparkMetadataAdapter> metadataOpt =
        newMetadata != null ? Option.apply(new SparkMetadataAdapter(newMetadata)) : Option.empty();
    Option<SparkProtocolAdapter> protocolOpt =
        newProtocol != null ? Option.apply(new SparkProtocolAdapter(newProtocol)) : Option.empty();
    Option<PersistedMetadata> persistedOpt =
        persistedMetadataAtSourceInit != null
            ? Option.apply(persistedMetadataAtSourceInit)
            : Option.empty();

    return DeltaSourceMetadataEvolutionSupport$.MODULE$
        .hasMetadataOrProtocolChangeComparedToStreamMetadata(
            metadataOpt,
            protocolOpt,
            newSchemaVersion,
            persistedOpt,
            new SparkProtocolAdapter(readProtocolAtSourceInit),
            readSchemaAtSourceInit,
            readPartitionSchemaAtSourceInit,
            ScalaUtils.toScalaMap(readConfigurationsAtSourceInit),
            spark);
  }

  /** Collect the metadata action at a specific version. Returns null if none. */
  private Metadata collectMetadataAtVersion(long version) {
    return collectMetadataActions(version, version).get(version);
  }

  /** Collect all metadata actions between start and end version, both inclusive. */
  private Map<Long, Metadata> collectMetadataActions(long startVersion, long endVersion) {
    return StreamingHelper.collectMetadataActionsFromRangeUnsafe(
        startVersion, Optional.of(endVersion + 1), snapshotManager, engine, tablePath);
  }

  /** Collect the protocol action at a specific version. Returns null if none. */
  private Protocol collectProtocolAtVersion(long version) {
    return collectProtocolActions(version, version).get(version);
  }

  /** Collect all protocol actions between start and end version, both inclusive. */
  private Map<Long, Protocol> collectProtocolActions(long startVersion, long endVersion) {
    return StreamingHelper.collectProtocolActionsFromRangeUnsafe(
        startVersion, Optional.of(endVersion + 1), snapshotManager, engine, tablePath);
  }

  /**
   * Given the version range for an ALREADY fetched batch, check if there are any read-incompatible
   * schema changes or protocol changes.
   *
   * <p>Try to find rename or drop columns in between, or nullability/datatype changes by using the
   * last schema as the read schema. If so, we cannot find a good read schema. Otherwise, the most
   * recent metadata change will be the most encompassing schema as well.
   *
   * <p>For protocols, walk through changes and ensure each is a superset of the previous. If not,
   * we cannot find a safe protocol.
   */
  private ValidatedMetadataAndProtocol validateAndResolveMetadataForLogInitialization(
      long startVersion, long endVersion) {
    List<Metadata> metadataChanges = new ArrayList<>(
        collectMetadataActions(startVersion, endVersion).values());
    SnapshotImpl startSnapshot = (SnapshotImpl) snapshotManager.loadSnapshotAt(startVersion);
    Metadata startMetadata = startSnapshot.getMetadata();

    // Try to use the most recent metadata change as the read schema and validate all prior
    // schemas are read-compatible against it.
    Metadata mostRecentMetadataChange = metadataChanges.isEmpty()
        ? null : metadataChanges.get(metadataChanges.size() - 1);
    if (mostRecentMetadataChange != null) {
      // Validate startMetadata + all intermediate changes against the most recent
      List<Metadata> otherMetadataChanges = new ArrayList<>();
      otherMetadataChanges.add(startMetadata);
      otherMetadataChanges.addAll(metadataChanges.subList(0, metadataChanges.size() - 1));
      for (Metadata prior : otherMetadataChanges) {
        // TODO: add column mapping schema change validation (hasNoColumnMappingSchemaChanges)
        // and read-compatibility validation (SchemaUtils.isReadCompatible) using Kernel APIs
        // For now, this is a placeholder matching the v1 structure.
      }
    }

    // Check protocol changes and use the most supportive protocol
    Protocol mostSupportiveProtocol = startSnapshot.getProtocol();
    List<Protocol> protocolChanges = new ArrayList<>(
        collectProtocolActions(startVersion, endVersion).values());
    for (Protocol p : protocolChanges) {
      // TODO: add readerAndWriterFeatureNames subset check using Kernel Protocol APIs
      // similar to v1's mostSupportiveProtocol.readerAndWriterFeatureNames.subsetOf(...)
      mostSupportiveProtocol = p;
    }

    Metadata resolvedMetadata = mostRecentMetadataChange != null
        ? mostRecentMetadataChange : startMetadata;
    return new ValidatedMetadataAndProtocol(resolvedMetadata, mostSupportiveProtocol);
  }

  private Metadata getMetadataAtSourceInit() {
    if (persistedMetadataAtSourceInit != null) {
      SnapshotImpl snapshot =
          (SnapshotImpl)
              snapshotManager.loadSnapshotAt(persistedMetadataAtSourceInit.deltaCommitVersion());
      return snapshot.getMetadata();
    }
    return ((SnapshotImpl) snapshotManager.loadLatestSnapshot()).getMetadata();
  }

  private static class ValidatedMetadataAndProtocol {
    final Metadata metadata;
    final Protocol protocol;

    ValidatedMetadataAndProtocol(Metadata metadata, Protocol protocol) {
      this.metadata = metadata;
      this.protocol = protocol;
    }
  }
}
