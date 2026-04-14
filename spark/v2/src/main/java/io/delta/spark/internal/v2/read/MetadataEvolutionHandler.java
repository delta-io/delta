/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import io.delta.kernel.utils.CloseableIterator.BreakableFilterResult;
import io.delta.spark.internal.v2.adapters.KernelMetadataAdapter;
import io.delta.spark.internal.v2.adapters.KernelProtocolAdapter;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaColumnMapping$;
import org.apache.spark.sql.delta.DeltaErrors;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.TypeWideningMode;
import org.apache.spark.sql.delta.schema.SchemaUtils$;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport$;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaStreamUtils;
import org.apache.spark.sql.delta.sources.PersistedMetadata;
import org.apache.spark.sql.delta.v2.interop.AbstractMetadata;
import org.apache.spark.sql.delta.v2.interop.AbstractProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.immutable.Seq;
import scala.collection.immutable.Seq$;

/**
 * V2 port of V1's {@code DeltaSourceMetadataEvolutionSupport} trait. Handles metadata evolution
 * (schema, table configuration, or protocol changes) for the v2 Delta streaming source.
 *
 * <p>To safely evolve schema mid-stream, this class intercepts streaming at several stages to:
 *
 * <ol>
 *   <li>Capture metadata changes within a stream.
 *   <li>Stop {@code latestOffset} from crossing a metadata change boundary.
 *   <li>Ensure the batch prior to the change can still be served correctly.
 *   <li>Fail the stream if and only if the prior batch was served successfully.
 *   <li>Write the new metadata to the tracking log before the stream fails so the restarted stream
 *       picks up the updated schema.
 * </ol>
 *
 * <p>See the trait-level Scaladoc on {@code
 * org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport} for the full barrier
 * protocol details. Validation logic shared with v1 lives in the {@code
 * DeltaSourceMetadataEvolutionSupport} companion object; this class delegates to those static
 * utilities.
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
  private final Metadata readMetadataAtSourceInit;
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
      Metadata readMetadataAtSourceInit,
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
    this.readMetadataAtSourceInit = Objects.requireNonNull(readMetadataAtSourceInit);
    this.readProtocolAtSourceInit = Objects.requireNonNull(readProtocolAtSourceInit);
    this.metadataPath = Objects.requireNonNull(metadataPath);
    this.persistedMetadataAtSourceInit =
        metadataTrackingLog.isDefined()
                && metadataTrackingLog.get().getCurrentTrackedMetadata().isDefined()
            ? metadataTrackingLog.get().getCurrentTrackedMetadata().get()
            : null;
  }

  /**
   * Whether this source uses the metadata tracking log as its read schema. False when the log is
   * absent/empty or unsafe column-mapping reads are enabled.
   *
   * <p>V2 port of V1's {@code DeltaSourceMetadataEvolutionSupport.shouldTrackMetadataChange}.
   */
  public boolean shouldTrackMetadataChange() {
    return DeltaSourceMetadataEvolutionSupport$.MODULE$.shouldTrackMetadataChange(
        schemaReadOptions, metadataTrackingLog);
  }

  /**
   * Whether the tracking log is provided but still empty, so it should be initialized eagerly on
   * the first batch. Should only be consulted before the first write to the log.
   *
   * <p>V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.shouldInitializeMetadataTrackingEagerly}.
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
    // Lazily include files up to and including the barrier, then break.
    boolean[] sawBarrier = {false};
    return fileActions.breakableFilter(
        file -> {
          if (sawBarrier[0]) {
            return BreakableFilterResult.BREAK;
          }
          if (file.getIndex() == DeltaSourceOffset.METADATA_CHANGE_INDEX()) {
            sawBarrier[0] = true;
          }
          return BreakableFilterResult.INCLUDE;
        });
  }

  /**
   * Returns a single barrier {@link IndexedFile} at {@code METADATA_CHANGE_INDEX} when tracking is
   * on and the given metadata/protocol differ from the init state; empty otherwise.
   *
   * <p>V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.getMetadataOrProtocolChangeIndexedFileIterator}.
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
   * Drives the two-barrier protocol when the previous offset sits on a barrier: advances {@code
   * METADATA_CHANGE_INDEX} to {@code POST_METADATA_CHANGE_INDEX}, blocks at {@code
   * POST_METADATA_CHANGE_INDEX} if the change is still pending, or returns empty when there is no
   * pending schema change.
   *
   * <p>V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.getNextOffsetFromPreviousOffsetIfPendingSchemaChange}.
   */
  public Optional<DeltaSourceOffset> getNextOffsetFromPreviousOffsetIfPendingSchemaChange(
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
   * Called from {@code commit()}: when the committed offset is a schema-change barrier, writes the
   * new metadata to the tracking log and fails the stream to trigger restart under the new schema.
   * No-op for non-barrier offsets or when tracking is disabled.
   *
   * <p>V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.updateMetadataTrackingLogAndFailTheStreamIfNeeded(Offset)}.
   */
  public void updateMetadataTrackingLogAndFailTheStreamIfNeeded(DeltaSourceOffset offset) {
    if (!shouldTrackMetadataChange()) {
      return;
    }
    if (offset.index() != DeltaSourceOffset.METADATA_CHANGE_INDEX()
        && offset.index() != DeltaSourceOffset.POST_METADATA_CHANGE_INDEX()) {
      return;
    }

    Metadata changedMetadata = collectMetadataAtVersion(offset.reservoirVersion());
    Protocol changedProtocol = collectProtocolAtVersion(offset.reservoirVersion());

    updateMetadataTrackingLogAndFailTheStreamIfNeeded(
        changedMetadata, changedProtocol, offset.reservoirVersion(), /* replace= */ false);
  }

  /**
   * Writes the changed metadata/protocol to the tracking log at {@code version} and throws to fail
   * the stream. No-op when the change matches the current init state. With {@code replace=true},
   * the new entry logically replaces the current latest entry instead of being appended.
   *
   * <p>V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.updateMetadataTrackingLogAndFailTheStreamIfNeeded(Option,
   * Option, Long, Boolean)}.
   */
  public void updateMetadataTrackingLogAndFailTheStreamIfNeeded(
      Metadata changedMetadata, Protocol changedProtocol, long version, boolean replace) {
    if (!hasMetadataOrProtocolChangeComparedToStreamMetadata(
        changedMetadata, changedProtocol, version)) {
      return;
    }

    Metadata metadataToUse = changedMetadata != null ? changedMetadata : readMetadataAtSourceInit;
    Protocol protocolToUse = changedProtocol != null ? changedProtocol : readProtocolAtSourceInit;

    PersistedMetadata schemaToPersist =
        PersistedMetadata.apply(
            tableId,
            version,
            new KernelMetadataAdapter(metadataToUse),
            new KernelProtocolAdapter(protocolToUse),
            metadataPath);

    metadataTrackingLog.get().writeNewMetadata(schemaToPersist, /* replaceCurrent= */ replace);

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
   * Initializes an empty tracking log on the first batch with the metadata at {@code
   * batchStartVersion}, or — when {@code batchEndVersion} is given for an already-constructed batch
   * — the most-recent compatible metadata in {@code [start, end]}. Throws to fail the stream if the
   * initialized metadata differs from init or {@code alwaysFailUponLogInitialized} is set.
   *
   * <p>V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.initializeMetadataTrackingAndExitStream}.
   */
  public void initializeMetadataTrackingAndExitStream(
      long batchStartVersion,
      @Nullable Long batchEndVersion,
      boolean alwaysFailUponLogInitialized) {
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

    PersistedMetadata newMetadata =
        PersistedMetadata.apply(
            tableId,
            version,
            new KernelMetadataAdapter(metadata),
            new KernelProtocolAdapter(protocol),
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
    Option<AbstractMetadata> metadataOpt =
        newMetadata != null
            ? Option.apply((AbstractMetadata) new KernelMetadataAdapter(newMetadata))
            : Option.empty();
    Option<AbstractProtocol> protocolOpt =
        newProtocol != null
            ? Option.apply((AbstractProtocol) new KernelProtocolAdapter(newProtocol))
            : Option.empty();
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
            new KernelProtocolAdapter(readProtocolAtSourceInit),
            new KernelMetadataAdapter(readMetadataAtSourceInit),
            spark);
  }

  /** Collect the metadata action at a specific version. Returns null if none. */
  private Metadata collectMetadataAtVersion(long version) {
    return collectMetadataActions(version, version).get(version);
  }

  /** Collect all metadata actions between start and end version, both inclusive. */
  private Map<Long, Metadata> collectMetadataActions(long startVersion, long endVersion) {
    return StreamingHelper.collectMetadataActionsFromRangeUnsafe(
        startVersion, Optional.of(endVersion), snapshotManager, engine, tablePath);
  }

  /** Collect the protocol action at a specific version. Returns null if none. */
  private Protocol collectProtocolAtVersion(long version) {
    return collectProtocolActions(version, version).get(version);
  }

  /** Collect all protocol actions between start and end version, both inclusive. */
  private Map<Long, Protocol> collectProtocolActions(long startVersion, long endVersion) {
    return StreamingHelper.collectProtocolActionsFromRangeUnsafe(
        startVersion, Optional.of(endVersion), snapshotManager, engine, tablePath);
  }

  /**
   * V2 port of V1's {@code
   * DeltaSourceMetadataEvolutionSupport.validateAndResolveMetadataForLogInitialization}.
   */
  private ValidatedMetadataAndProtocol validateAndResolveMetadataForLogInitialization(
      long startVersion, long endVersion) {
    List<Metadata> metadataChanges =
        new ArrayList<>(collectMetadataActions(startVersion, endVersion).values());
    SnapshotImpl startSnapshot = (SnapshotImpl) snapshotManager.loadSnapshotAt(startVersion);
    Metadata startMetadata = startSnapshot.getMetadata();

    // Try to find rename or drop columns in between, or nullability/datatype changes by using
    // the last schema as the read schema. If so, we cannot find a good read schema.
    // Otherwise, the most recent metadata change will be the most encompassing schema as well.
    Metadata mostRecentMetadataChange =
        metadataChanges.isEmpty() ? null : metadataChanges.get(metadataChanges.size() - 1);
    if (mostRecentMetadataChange != null) {
      KernelMetadataAdapter mostRecentAdapter = new KernelMetadataAdapter(mostRecentMetadataChange);
      // Validate startMetadata + all intermediate changes against the most recent
      List<Metadata> otherMetadataChanges = new ArrayList<>();
      otherMetadataChanges.add(startMetadata);
      otherMetadataChanges.addAll(metadataChanges.subList(0, metadataChanges.size() - 1));
      for (Metadata potentialSchemaChangeMetadata : otherMetadataChanges) {
        KernelMetadataAdapter potentialAdapter =
            new KernelMetadataAdapter(potentialSchemaChangeMetadata);
        if (!DeltaColumnMapping$.MODULE$.hasNoColumnMappingSchemaChanges(
                mostRecentAdapter, potentialAdapter, false)
            || !SchemaUtils$.MODULE$.isReadCompatible(
                potentialAdapter.schema(),
                mostRecentAdapter.schema(),
                /* forbidTightenNullability= */ true,
                /* allowMissingColumns= */ false,
                TypeWideningMode.NoTypeWidening$.MODULE$,
                (Seq<String>) Seq$.MODULE$.empty(),
                (Seq<String>) Seq$.MODULE$.empty(),
                /* caseSensitive= */ true,
                /* allowVoidTypeChange= */ false)) {
          throw (RuntimeException)
              DeltaErrors.streamingMetadataLogInitFailedIncompatibleMetadataException(
                  startVersion, endVersion);
        }
      }
    }

    // Check protocol changes and use the most supportive protocol
    Protocol mostSupportiveProtocol = startSnapshot.getProtocol();
    List<Protocol> protocolChanges =
        new ArrayList<>(collectProtocolActions(startVersion, endVersion).values());
    for (Protocol p : protocolChanges) {
      if (p.getReaderAndWriterFeatures()
          .containsAll(mostSupportiveProtocol.getReaderAndWriterFeatures())) {
        mostSupportiveProtocol = p;
      } else {
        // TODO: or use protocol union instead?
        throw (RuntimeException)
            DeltaErrors.streamingMetadataLogInitFailedIncompatibleMetadataException(
                startVersion, endVersion);
      }
    }

    Metadata resolvedMetadata =
        mostRecentMetadataChange != null ? mostRecentMetadataChange : startMetadata;
    return new ValidatedMetadataAndProtocol(resolvedMetadata, mostSupportiveProtocol);
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
