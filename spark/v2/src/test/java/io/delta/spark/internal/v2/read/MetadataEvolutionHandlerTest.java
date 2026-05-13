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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.util.InternalUtils;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.adapters.KernelMetadataAdapter;
import io.delta.spark.internal.v2.adapters.KernelProtocolAdapter;
import io.delta.spark.internal.v2.exception.VersionNotFoundException;
import io.delta.spark.internal.v2.snapshot.DeltaSnapshotManager;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import java.io.File;
import java.util.*;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.DeltaRuntimeException;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataTrackingLog;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaStreamUtils;
import org.apache.spark.sql.delta.sources.PersistedMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/** Unit tests for {@link MetadataEvolutionHandler}. */
public class MetadataEvolutionHandlerTest extends DeltaV2TestBase {

  // ---------------------------------------------------------------------------
  // Shared test fixtures
  // ---------------------------------------------------------------------------

  /** A simple (c1 INT, c2 STRING) schema used as the default for most tests. */
  private static final StructType DEFAULT_KERNEL_SCHEMA =
      new StructType().add("c1", IntegerType.INTEGER).add("c2", StringType.STRING);

  private static final String DEFAULT_SCHEMA_JSON =
      "{\"type\":\"struct\",\"fields\":["
          + "{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
          + "{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";

  /** Default kernel Metadata action used as the handler's init metadata in most tests. */
  private static final Metadata DEFAULT_METADATA =
      new Metadata(
          "test-id",
          Optional.empty(),
          Optional.empty(),
          new Format("parquet", Collections.emptyMap()),
          DEFAULT_SCHEMA_JSON,
          DEFAULT_KERNEL_SCHEMA,
          emptyArrayValue(),
          Optional.empty(),
          VectorUtils.stringStringMapValue(Collections.emptyMap()));

  /** Default kernel Protocol action: reader=1, writer=2, no features. */
  private static final Protocol DEFAULT_PROTOCOL = new Protocol(1, 2);

  private static final scala.collection.immutable.Map<String, String> EMPTY_SCALA_MAP =
      scala.collection.immutable.Map$.MODULE$.empty();

  private static ArrayValue emptyArrayValue() {
    return new ArrayValue() {
      @Override
      public int getSize() {
        return 0;
      }

      @Override
      public ColumnVector getElements() {
        return InternalUtils.singletonStringColumnVector("");
      }
    };
  }

  /**
   * A no-op snapshot manager that throws on any call. Used for tests that only exercise handler
   * logic (tracking state, offset arithmetic, iterator manipulation) without hitting the delta log.
   */
  private static final DeltaSnapshotManager THROWING_SNAPSHOT_MANAGER =
      new DeltaSnapshotManager() {
        @Override
        public Snapshot loadLatestSnapshot() {
          throw new UnsupportedOperationException("not expected in this test");
        }

        @Override
        public Snapshot loadSnapshotAt(long version) {
          throw new UnsupportedOperationException("not expected in this test");
        }

        @Override
        public DeltaHistoryManager.Commit getActiveCommitAtTime(
            long ts, boolean last, boolean recreatable, boolean earliest) {
          throw new UnsupportedOperationException("not expected in this test");
        }

        @Override
        public void checkVersionExists(long version, boolean recreatable, boolean allowOOR)
            throws VersionNotFoundException {
          throw new UnsupportedOperationException("not expected in this test");
        }

        @Override
        public CommitRange getTableChanges(
            Engine engine, long startVersion, Optional<Long> endVersion) {
          throw new UnsupportedOperationException("not expected in this test");
        }
      };

  // ---------------------------------------------------------------------------
  // Builder helpers
  // ---------------------------------------------------------------------------

  private static DeltaStreamUtils.SchemaReadOptions schemaReadOptions(
      boolean allowUnsafeColumnMappingRead) {
    return new DeltaStreamUtils.SchemaReadOptions(
        /* allowUnsafeStreamingReadOnColumnMappingSchemaChanges= */ allowUnsafeColumnMappingRead,
        /* allowUnsafeStreamingReadOnPartitionColumnChanges= */ false,
        /* forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart= */ false,
        /* forceEnableUnsafeReadOnNullabilityChange= */ false,
        /* isStreamingFromColumnMappingTable= */ false,
        /* typeWideningEnabled= */ false,
        /* enableSchemaTrackingForTypeWidening= */ false);
  }

  private DeltaOptions emptyDeltaOptions() {
    return new DeltaOptions(EMPTY_SCALA_MAP, spark.sessionState().conf());
  }

  /**
   * Build a lightweight handler that uses {@link #THROWING_SNAPSHOT_MANAGER}. Suitable for tests
   * that only exercise tracking state, offset arithmetic, or iterator manipulation — NOT for tests
   * that call private helpers like collectMetadataAtVersion.
   */
  private MetadataEvolutionHandler buildLightweightHandler(
      Option<DeltaSourceMetadataTrackingLog> trackingLog,
      DeltaStreamUtils.SchemaReadOptions readOptions) {
    return new MetadataEvolutionHandler(
        spark,
        "test-table-id",
        "/tmp/fake-table",
        THROWING_SNAPSHOT_MANAGER,
        defaultEngine,
        emptyDeltaOptions(),
        readOptions,
        trackingLog,
        DEFAULT_METADATA,
        DEFAULT_PROTOCOL,
        "/tmp/fake-table/_delta_log/_streaming_metadata");
  }

  /**
   * Build a handler backed by a real Delta table on disk, with a real tracking log. The handler's
   * init state (schema, protocol, config) is captured from the table at {@code initVersion}.
   *
   * @param tablePath path to an already-created Delta table
   * @param initVersion the version whose metadata becomes the handler's init state
   * @param seedLogWithInitEntry if true, writes an initial entry to the tracking log at initVersion
   *     so that {@code shouldTrackMetadataChange()} returns true
   */
  private HandlerWithLog buildHandlerWithRealTable(
      String tablePath, long initVersion, boolean seedLogWithInitEntry) {
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(tablePath, spark.sessionState().newHadoopConf());
    SnapshotImpl snapshot = (SnapshotImpl) snapshotManager.loadSnapshotAt(initVersion);
    Metadata tableMetadata = snapshot.getMetadata();
    Protocol tableProtocol = snapshot.getProtocol();
    KernelMetadataAdapter adapter = new KernelMetadataAdapter(tableMetadata);

    String metadataLogPath = tablePath + "/metadata_log";
    DeltaSourceMetadataTrackingLog trackingLog =
        DeltaSourceMetadataTrackingLog.create(
            spark,
            metadataLogPath,
            "test-table-id",
            tablePath,
            EMPTY_SCALA_MAP,
            Option.empty(),
            /* mergeConsecutiveSchemaChanges= */ false,
            /* consecutiveSchemaChangesMerger= */ Option.empty(),
            /* initMetadataLogEagerly= */ true);

    if (seedLogWithInitEntry) {
      PersistedMetadata entry =
          PersistedMetadata.apply(
              "test-table-id",
              initVersion,
              adapter,
              new KernelProtocolAdapter(tableProtocol),
              tablePath + "/_delta_log/_streaming_metadata");
      trackingLog.writeNewMetadata(entry, false);
    }

    MetadataEvolutionHandler handler =
        new MetadataEvolutionHandler(
            spark,
            "test-table-id",
            tablePath,
            snapshotManager,
            defaultEngine,
            emptyDeltaOptions(),
            schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false),
            Option.apply(trackingLog),
            tableMetadata,
            tableProtocol,
            tablePath + "/_delta_log/_streaming_metadata");

    return new HandlerWithLog(handler, trackingLog);
  }

  /**
   * Create a standalone tracking log on disk (not tied to a real table). Useful for tests that only
   * need to control the tracking log state without a real Delta table.
   */
  private DeltaSourceMetadataTrackingLog createStandaloneTrackingLog(
      File logDir, boolean seedWithDefaultEntry) {
    String metadataPath = logDir.getAbsolutePath() + "/metadata_log";
    DeltaSourceMetadataTrackingLog trackingLog =
        DeltaSourceMetadataTrackingLog.create(
            spark,
            metadataPath,
            "test-table-id",
            "/tmp/fake-table",
            EMPTY_SCALA_MAP,
            Option.empty(),
            /* mergeConsecutiveSchemaChanges= */ false,
            /* consecutiveSchemaChangesMerger= */ Option.empty(),
            /* initMetadataLogEagerly= */ true);
    if (seedWithDefaultEntry) {
      PersistedMetadata entry =
          PersistedMetadata.apply(
              "test-table-id",
              0L,
              new KernelMetadataAdapter(DEFAULT_METADATA),
              new KernelProtocolAdapter(DEFAULT_PROTOCOL),
              "/tmp/fake-table/_delta_log/_streaming_metadata");
      trackingLog.writeNewMetadata(entry, false);
    }
    return trackingLog;
  }

  /** Collect all IndexedFiles from a CloseableIterator into a list. */
  private static List<IndexedFile> drainIndexedFiles(CloseableIterator<IndexedFile> iter) {
    List<IndexedFile> result = new ArrayList<>();
    try {
      while (iter.hasNext()) {
        result.add(iter.next());
      }
    } finally {
      try {
        iter.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  /**
   * Assert that a DeltaRuntimeException is a DELTA_STREAMING_METADATA_EVOLUTION error with the
   * expected message parameters (schema, config, protocol).
   */
  private static void assertMetadataEvolutionException(DeltaRuntimeException ex, String context) {
    assertEquals(
        "DELTA_STREAMING_METADATA_EVOLUTION",
        ex.getErrorClass(),
        "Should throw metadata evolution exception " + context);
    java.util.Map<String, String> params = ex.getMessageParameters();
    assertTrue(params.containsKey("schema"), "Missing 'schema' message parameter");
    assertTrue(params.containsKey("config"), "Missing 'config' message parameter");
    assertTrue(params.containsKey("protocol"), "Missing 'protocol' message parameter");
  }

  /** Pairs a handler with its tracking log so tests can verify log writes after handler calls. */
  private static class HandlerWithLog {
    final MetadataEvolutionHandler handler;
    final DeltaSourceMetadataTrackingLog trackingLog;

    HandlerWithLog(MetadataEvolutionHandler handler, DeltaSourceMetadataTrackingLog trackingLog) {
      this.handler = handler;
      this.trackingLog = trackingLog;
    }
  }

  // ---------------------------------------------------------------------------
  // shouldTrackMetadataChange / shouldInitializeMetadataTrackingEagerly
  //
  // These two methods share the same preconditions (tracking log state + unsafe
  // read flag), so we test them together for each configuration.
  // ---------------------------------------------------------------------------

  /** Both should be false when unsafe column mapping reads bypass tracking entirely. */
  @Test
  public void testTrackingState_disabledWhenUnsafeColumnMappingReadAllowed() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));
    assertFalse(handler.shouldTrackMetadataChange());
    assertFalse(handler.shouldInitializeMetadataTrackingEagerly());
  }

  /** Both should be false when no tracking log is provided (Option.empty). */
  @Test
  public void testTrackingState_disabledWhenNoTrackingLog() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));
    assertFalse(handler.shouldTrackMetadataChange());
    assertFalse(handler.shouldInitializeMetadataTrackingEagerly());
  }

  /**
   * When the tracking log exists but is empty: not yet tracking (no persisted metadata to read
   * from), but ready to initialize eagerly on the first batch.
   */
  @Test
  public void testTrackingState_emptyLog_notTrackingButReadyToInitialize(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog emptyLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ false);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(emptyLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));
    assertFalse(handler.shouldTrackMetadataChange());
    assertTrue(handler.shouldInitializeMetadataTrackingEagerly());
  }

  /**
   * When the tracking log has a persisted entry: actively tracking (reads schema from log), and no
   * longer needs eager initialization.
   */
  @Test
  public void testTrackingState_seededLog_trackingActiveAndInitComplete(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));
    assertTrue(handler.shouldTrackMetadataChange());
    assertFalse(handler.shouldInitializeMetadataTrackingEagerly());
  }

  // ---------------------------------------------------------------------------
  // stopIndexedFileIteratorAtSchemaChangeBarrier
  //
  // This method truncates the file action iterator at the METADATA_CHANGE_INDEX
  // sentinel (inclusive) so a batch never crosses a schema change boundary.
  // ---------------------------------------------------------------------------

  /** All files pass through when no barrier sentinel is present. */
  @Test
  public void testStopAtBarrier_allFilesPassThroughWithoutBarrier() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    List<IndexedFile> inputFiles =
        Arrays.asList(
            IndexedFile.sentinel(1L, 0L),
            IndexedFile.sentinel(1L, 1L),
            IndexedFile.sentinel(1L, 2L));

    List<IndexedFile> result =
        drainIndexedFiles(
            handler.stopIndexedFileIteratorAtSchemaChangeBarrier(
                Utils.toCloseableIterator(inputFiles.iterator())));
    assertEquals(3, result.size());
  }

  /** Files before the barrier + the barrier itself are included; files after are discarded. */
  @Test
  public void testStopAtBarrier_includesBarrierAndDiscardsRest() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    long barrierIndex = DeltaSourceOffset.METADATA_CHANGE_INDEX();
    List<IndexedFile> inputFiles =
        Arrays.asList(
            IndexedFile.sentinel(1L, 0L),
            IndexedFile.sentinel(1L, 1L),
            IndexedFile.sentinel(1L, barrierIndex),
            IndexedFile.sentinel(1L, 3L),
            IndexedFile.sentinel(1L, 4L));

    List<IndexedFile> result =
        drainIndexedFiles(
            handler.stopIndexedFileIteratorAtSchemaChangeBarrier(
                Utils.toCloseableIterator(inputFiles.iterator())));
    assertEquals(3, result.size());
    assertEquals(0L, result.get(0).getIndex());
    assertEquals(1L, result.get(1).getIndex());
    assertEquals(barrierIndex, result.get(2).getIndex());
  }

  /** Empty input produces empty output. */
  @Test
  public void testStopAtBarrier_handlesEmptyIterator() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    List<IndexedFile> result =
        drainIndexedFiles(
            handler.stopIndexedFileIteratorAtSchemaChangeBarrier(
                Utils.toCloseableIterator(Collections.<IndexedFile>emptyIterator())));
    assertTrue(result.isEmpty());
  }

  // ---------------------------------------------------------------------------
  // getMetadataOrProtocolChangeIndexedFileIterator
  //
  // Returns a single METADATA_CHANGE_INDEX sentinel if tracking is on AND the
  // incoming metadata/protocol differs from the init state. Empty otherwise.
  // ---------------------------------------------------------------------------

  /** Returns empty even with a different protocol when tracking is disabled (unsafe read). */
  @Test
  public void testChangeIterator_emptyWhenTrackingDisabled() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    Protocol upgradedProtocol = new Protocol(3, 7);
    List<IndexedFile> result =
        drainIndexedFiles(
            handler.getMetadataOrProtocolChangeIndexedFileIterator(
                DEFAULT_METADATA, upgradedProtocol, 1L));
    assertTrue(result.isEmpty());
  }

  /** Returns empty when tracking is on but metadata and protocol match the init state. */
  @Test
  public void testChangeIterator_emptyWhenMetadataAndProtocolUnchanged(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    List<IndexedFile> result =
        drainIndexedFiles(
            handler.getMetadataOrProtocolChangeIndexedFileIterator(
                DEFAULT_METADATA, DEFAULT_PROTOCOL, 1L));
    assertTrue(result.isEmpty());
  }

  /** Returns a barrier sentinel when the protocol version has changed. */
  @Test
  public void testChangeIterator_returnsBarrierOnProtocolVersionChange(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    Protocol upgradedProtocol = new Protocol(3, 7);
    List<IndexedFile> result =
        drainIndexedFiles(
            handler.getMetadataOrProtocolChangeIndexedFileIterator(
                DEFAULT_METADATA, upgradedProtocol, 5L));
    assertEquals(1, result.size());
    assertEquals(5L, result.get(0).getVersion());
    assertEquals(DeltaSourceOffset.METADATA_CHANGE_INDEX(), result.get(0).getIndex());
    assertFalse(result.get(0).hasFileAction());
  }

  /** Returns a barrier sentinel when a column was added to the schema. */
  @Test
  public void testChangeIterator_returnsBarrierOnColumnAdded(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    // Add a c3 column to the default (c1, c2) schema
    StructType schemaWithNewColumn =
        new StructType()
            .add("c1", IntegerType.INTEGER)
            .add("c2", StringType.STRING)
            .add("c3", IntegerType.INTEGER);
    String schemaWithNewColumnJson =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"c1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"c2\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
            + "{\"name\":\"c3\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}";
    Metadata metadataWithNewColumn =
        new Metadata(
            "test-id",
            Optional.empty(),
            Optional.empty(),
            new Format("parquet", Collections.emptyMap()),
            schemaWithNewColumnJson,
            schemaWithNewColumn,
            emptyArrayValue(),
            Optional.empty(),
            VectorUtils.stringStringMapValue(Collections.emptyMap()));

    List<IndexedFile> result =
        drainIndexedFiles(
            handler.getMetadataOrProtocolChangeIndexedFileIterator(
                metadataWithNewColumn, DEFAULT_PROTOCOL, 7L));
    assertEquals(1, result.size());
    assertEquals(7L, result.get(0).getVersion());
    assertEquals(DeltaSourceOffset.METADATA_CHANGE_INDEX(), result.get(0).getIndex());
    assertFalse(result.get(0).hasFileAction());
  }

  /** Returns a barrier sentinel when a delta table configuration was added. */
  @Test
  public void testChangeIterator_returnsBarrierOnDeltaConfigChange(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    // Same schema as default, but with an added delta.* configuration
    Map<String, String> configWithCDF = new HashMap<>();
    configWithCDF.put("delta.enableChangeDataFeed", "true");
    Metadata metadataWithNewConfig =
        new Metadata(
            "test-id",
            Optional.empty(),
            Optional.empty(),
            new Format("parquet", Collections.emptyMap()),
            DEFAULT_SCHEMA_JSON,
            DEFAULT_KERNEL_SCHEMA,
            emptyArrayValue(),
            Optional.empty(),
            VectorUtils.stringStringMapValue(configWithCDF));

    List<IndexedFile> result =
        drainIndexedFiles(
            handler.getMetadataOrProtocolChangeIndexedFileIterator(
                metadataWithNewConfig, DEFAULT_PROTOCOL, 3L));
    assertEquals(1, result.size());
    assertEquals(3L, result.get(0).getVersion());
    assertEquals(DeltaSourceOffset.METADATA_CHANGE_INDEX(), result.get(0).getIndex());
    assertFalse(result.get(0).hasFileAction());
  }

  // ---------------------------------------------------------------------------
  // getNextOffsetFromPreviousOffsetIfPendingSchemaChange
  //
  // Implements the two-barrier protocol:
  //   METADATA_CHANGE_INDEX  -> advance to POST_METADATA_CHANGE_INDEX
  //   POST_METADATA_CHANGE_INDEX -> block (return same offset) if change persists,
  //                                  or empty if change was resolved
  //   any other index        -> empty (no pending change)
  // ---------------------------------------------------------------------------

  /**
   * At METADATA_CHANGE_INDEX, the handler advances to POST_METADATA_CHANGE_INDEX and preserves the
   * reservoirVersion and isInitialSnapshot flag.
   */
  @Test
  public void testPendingSchemaChange_advancesFromBarrierToPostBarrier() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    // isInitialSnapshot = false
    DeltaSourceOffset barrierOffset =
        DeltaSourceOffset.apply(
            "test-reservoir", 5L, DeltaSourceOffset.METADATA_CHANGE_INDEX(), false);
    Optional<DeltaSourceOffset> nextOffset =
        handler.getNextOffsetFromPreviousOffsetIfPendingSchemaChange(barrierOffset);
    assertTrue(nextOffset.isPresent());
    assertEquals(DeltaSourceOffset.POST_METADATA_CHANGE_INDEX(), nextOffset.get().index());
    assertEquals(5L, nextOffset.get().reservoirVersion());
    assertFalse(nextOffset.get().isInitialSnapshot());

    // isInitialSnapshot = true — flag must be preserved
    DeltaSourceOffset initialSnapshotBarrier =
        DeltaSourceOffset.apply(
            "test-reservoir", 3L, DeltaSourceOffset.METADATA_CHANGE_INDEX(), true);
    Optional<DeltaSourceOffset> nextFromSnapshot =
        handler.getNextOffsetFromPreviousOffsetIfPendingSchemaChange(initialSnapshotBarrier);
    assertTrue(nextFromSnapshot.isPresent());
    assertEquals(DeltaSourceOffset.POST_METADATA_CHANGE_INDEX(), nextFromSnapshot.get().index());
    assertTrue(nextFromSnapshot.get().isInitialSnapshot());
  }

  /** A regular (non-barrier) index means no pending schema change — returns empty. */
  @Test
  public void testPendingSchemaChange_emptyForNonBarrierIndex() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    DeltaSourceOffset regularOffset = DeltaSourceOffset.apply("test-reservoir", 5L, 10L, false);
    assertFalse(
        handler.getNextOffsetFromPreviousOffsetIfPendingSchemaChange(regularOffset).isPresent());
  }

  /**
   * At POST_BARRIER, returns empty when metadata at that version matches init (unblocks stream).
   */
  @Test
  public void testPendingSchemaChange_unblockWhenNoActualChange(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ false);

    DeltaSourceOffset postBarrierOffset =
        DeltaSourceOffset.apply(
            "test-reservoir", 0L, DeltaSourceOffset.POST_METADATA_CHANGE_INDEX(), false);
    // Version 0 has the same metadata as init -> unblocks
    assertFalse(
        handlerWithLog
            .handler
            .getNextOffsetFromPreviousOffsetIfPendingSchemaChange(postBarrierOffset)
            .isPresent());
  }

  /** At POST_BARRIER with a still-pending schema change, returns the same offset to block. */
  @Test
  public void testPendingSchemaChange_blocksAtPostBarrierWhenChangeStillPending(
      @TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    // Handler captures version 0 as init, with a seed log entry at v0
    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ true);

    // Evolve at v1 -> schema at v1 differs from init
    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (c3 INT)", tableName));

    DeltaSourceOffset postBarrierAtVersion1 =
        DeltaSourceOffset.apply(
            "test-reservoir", 1L, DeltaSourceOffset.POST_METADATA_CHANGE_INDEX(), false);
    Optional<DeltaSourceOffset> result =
        handlerWithLog.handler.getNextOffsetFromPreviousOffsetIfPendingSchemaChange(
            postBarrierAtVersion1);
    assertTrue(result.isPresent());
    assertEquals(postBarrierAtVersion1.reservoirVersion(), result.get().reservoirVersion());
    assertEquals(postBarrierAtVersion1.index(), result.get().index());
    assertEquals(postBarrierAtVersion1.reservoirId(), result.get().reservoirId());
  }

  // ---------------------------------------------------------------------------
  // updateMetadataTrackingLogAndFailTheStreamIfNeeded
  //
  // Called during commit(). Writes the new metadata to the tracking log and
  // throws DELTA_STREAMING_METADATA_EVOLUTION to trigger stream re-analysis.
  // ---------------------------------------------------------------------------

  /** No-op when tracking is disabled or the offset is not a barrier index. */
  @Test
  public void testUpdateLog_noOpWhenTrackingDisabledOrNonBarrierIndex() {
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.empty(), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ true));

    // Barrier index but tracking is disabled -> no-op
    DeltaSourceOffset barrierOffset =
        DeltaSourceOffset.apply(
            "test-reservoir", 0L, DeltaSourceOffset.METADATA_CHANGE_INDEX(), false);
    handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(barrierOffset);

    // Regular index -> no-op regardless of tracking state
    DeltaSourceOffset regularOffset = DeltaSourceOffset.apply("test-reservoir", 0L, 5L, false);
    handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(regularOffset);
  }

  /**
   * Offset overload: when the barrier version has a schema change (via ALTER TABLE), reads the
   * changed metadata from the log, writes it, and throws.
   */
  @Test
  public void testUpdateLog_throwsWhenBarrierVersionHasSchemaChange(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ true);

    // Evolve the table schema: version 1 has a new column
    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (c3 INT)", tableName));

    DeltaSourceOffset barrierAtVersion1 =
        DeltaSourceOffset.apply(
            "test-reservoir", 1L, DeltaSourceOffset.METADATA_CHANGE_INDEX(), false);

    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handlerWithLog.handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
                    barrierAtVersion1));
    assertMetadataEvolutionException(ex, "when schema changed at barrier version");

    // Verify the new entry was persisted at version 1
    assertTrue(handlerWithLog.trackingLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(
        1L, handlerWithLog.trackingLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
  }

  /**
   * Direct overload: passing a changed protocol throws and writes the new entry to the tracking log
   * at the specified version.
   */
  @Test
  public void testUpdateLog_throwsAndWritesEntryOnDirectProtocolChange(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    Protocol upgradedProtocol = new Protocol(3, 7);
    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
                    null, upgradedProtocol, 5L, /* replace= */ false));
    assertMetadataEvolutionException(ex, "when protocol changed");

    // Verify the new entry was persisted at version 5
    assertTrue(seededLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(5L, seededLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
  }

  /** Direct overload: no-op when metadata and protocol match the init state. */
  @Test
  public void testUpdateLog_noOpOnUnchangedDirectMetadataAndProtocol(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
        DEFAULT_METADATA, DEFAULT_PROTOCOL, 1L, /* replace= */ false);
  }

  /**
   * With replace=true, the new entry's previousMetadataSeqNum is set so getPreviousTrackedMetadata
   * returns empty (logically replacing the seed).
   */
  @Test
  public void testUpdateLog_replaceTrueClearsPreviousEntry(@TempDir File tempDir) {
    DeltaSourceMetadataTrackingLog seededLog =
        createStandaloneTrackingLog(tempDir, /* seedWithDefaultEntry= */ true);
    MetadataEvolutionHandler handler =
        buildLightweightHandler(
            Option.apply(seededLog), schemaReadOptions(/* allowUnsafeColumnMappingRead= */ false));

    Protocol upgradedProtocol = new Protocol(3, 7);
    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
                    null, upgradedProtocol, 5L, /* replace= */ true));
    assertMetadataEvolutionException(ex, "when protocol changed with replace=true");

    assertTrue(seededLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(5L, seededLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
    // The seed at v0 is logically replaced -> previous tracked is empty.
    assertTrue(seededLog.getPreviousTrackedMetadata().isEmpty());
  }

  /** Throw-and-write also fires at POST_METADATA_CHANGE_INDEX, not just METADATA_CHANGE_INDEX. */
  @Test
  public void testUpdateLog_throwsAtPostBarrierIndex(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ true);

    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (c3 INT)", tableName));

    DeltaSourceOffset postBarrierAtVersion1 =
        DeltaSourceOffset.apply(
            "test-reservoir", 1L, DeltaSourceOffset.POST_METADATA_CHANGE_INDEX(), false);

    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handlerWithLog.handler.updateMetadataTrackingLogAndFailTheStreamIfNeeded(
                    postBarrierAtVersion1));
    assertMetadataEvolutionException(ex, "when committing the post-barrier with schema change");

    assertTrue(handlerWithLog.trackingLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(
        1L, handlerWithLog.trackingLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
  }

  // ---------------------------------------------------------------------------
  // initializeMetadataTrackingAndExitStream
  //
  // Called on the first batch. Writes the initial metadata entry to the tracking
  // log. Throws DELTA_STREAMING_METADATA_EVOLUTION if the metadata at the batch
  // version differs from init, or if alwaysFailUponLogInitialized is true.
  // ---------------------------------------------------------------------------

  /**
   * When metadata at the batch version matches init and alwaysFail is false, the entry is written
   * without throwing.
   */
  @Test
  public void testInitialize_writesEntryWithoutThrowingWhenMetadataMatches(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ false);

    handlerWithLog.handler.initializeMetadataTrackingAndExitStream(
        0L, /* batchEndVersion= */ null, /* alwaysFailUponLogInitialized= */ false);

    assertTrue(handlerWithLog.trackingLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(
        0L, handlerWithLog.trackingLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
  }

  /**
   * When alwaysFailUponLogInitialized is true, throws even if metadata matches; entry is still
   * written before throwing.
   */
  @Test
  public void testInitialize_alwaysThrowsWhenAlwaysFailFlagIsSet(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ false);

    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handlerWithLog.handler.initializeMetadataTrackingAndExitStream(
                    0L, /* batchEndVersion= */ null, /* alwaysFailUponLogInitialized= */ true));
    assertMetadataEvolutionException(ex, "when alwaysFailUponLogInitialized is true");

    // Entry should still have been written before throwing
    assertTrue(handlerWithLog.trackingLog.getCurrentTrackedMetadata().isDefined());
  }

  /**
   * When the table schema was evolved (ALTER TABLE ADD COLUMNS) between init and the batch version,
   * the handler writes the new metadata and throws.
   */
  @Test
  public void testInitialize_throwsWhenSchemaEvolvedSinceInit(@TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);

    // Handler captures version 0 as init state
    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ false);

    // Evolve: version 1 has a new column
    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (c3 INT)", tableName));

    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handlerWithLog.handler.initializeMetadataTrackingAndExitStream(
                    1L, /* batchEndVersion= */ null, /* alwaysFailUponLogInitialized= */ false));
    assertMetadataEvolutionException(ex, "when schema evolved since init");

    // Entry should be written at version 1 (the evolved version)
    assertTrue(handlerWithLog.trackingLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(
        1L, handlerWithLog.trackingLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
  }

  /**
   * batchEndVersion overload: when there are no metadata or protocol changes in [start, end],
   * validation succeeds and the entry is written at endVersion.
   */
  @Test
  public void testInitialize_batchEndVersion_succeedsWhenNoMetadataChangeInRange(
      @TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    createEmptyTestTable(tablePath, tableName);
    // INSERT adds an Add action at v1; no metadata or protocol change in [0, 1]
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", tableName));

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ false);

    handlerWithLog.handler.initializeMetadataTrackingAndExitStream(
        0L, /* batchEndVersion= */ 1L, /* alwaysFailUponLogInitialized= */ false);

    assertTrue(handlerWithLog.trackingLog.getCurrentTrackedMetadata().isDefined());
    assertEquals(
        1L, handlerWithLog.trackingLog.getCurrentTrackedMetadata().get().deltaCommitVersion());
  }

  /**
   * batchEndVersion overload: an incompatible schema change (column drop with column mapping)
   * within [start, end] makes validation throw
   * DELTA_STREAMING_SCHEMA_LOG_INIT_FAILED_INCOMPATIBLE_METADATA — there's no schema we can use to
   * safely read the constructed batch.
   */
  @Test
  public void testInitialize_batchEndVersion_throwsOnIncompatibleSchemaChangeInRange(
      @TempDir File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "t_" + UUID.randomUUID().toString().replace('-', '_');
    // Column mapping enables logical column drops (detected as incompatible by validation)
    spark.sql(
        String.format(
            "CREATE TABLE %s (id INT, name STRING, age INT) USING delta LOCATION '%s' "
                + "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')",
            tableName, tablePath));
    spark.sql(String.format("ALTER TABLE %s DROP COLUMN age", tableName));

    HandlerWithLog handlerWithLog =
        buildHandlerWithRealTable(tablePath, 0L, /* seedLogWithInitEntry= */ false);

    DeltaRuntimeException ex =
        assertThrows(
            DeltaRuntimeException.class,
            () ->
                handlerWithLog.handler.initializeMetadataTrackingAndExitStream(
                    0L, /* batchEndVersion= */ 1L, /* alwaysFailUponLogInitialized= */ false));
    assertEquals(
        "DELTA_STREAMING_SCHEMA_LOG_INIT_FAILED_INCOMPATIBLE_METADATA",
        ex.getErrorClass(),
        "Should throw incompatible-metadata error when range contains a column drop");
  }
}
