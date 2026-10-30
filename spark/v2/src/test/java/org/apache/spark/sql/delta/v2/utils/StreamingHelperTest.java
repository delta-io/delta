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
package org.apache.spark.sql.delta.v2.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static io.delta.kernel.internal.actions.AddFile.SCHEMA_WITHOUT_STATS;

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.Protocol;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.exception.VersionNotFoundException;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SnapshotManager;
import org.apache.spark.sql.delta.v2.snapshot.PathBasedSnapshotManager;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;

public class StreamingHelperTest extends DeltaV2TestBase {

  private PathBasedSnapshotManager snapshotManager;

  @Test
  public void testGetAddFileRespectsSelectionAndDataChange() {
    Row addRow = createAddFileRow(true);
    ColumnVector adds =
        VectorUtils.buildColumnVector(
            Arrays.asList(addRow, addRow, addRow, createAddFileRow(false)), SCHEMA_WITHOUT_STATS);
    ColumnarBatch batch = createAddFileBatch(adds);
    ColumnVector selection =
        VectorUtils.buildColumnVector(Arrays.asList(true, false, null, true), BooleanType.BOOLEAN);
    FilteredColumnarBatch selected = new FilteredColumnarBatch(batch, Optional.of(selection));

    Optional<AddFile> addFile = StreamingHelper.getAddFile(selected, 0);
    assertTrue(addFile.isPresent());
    assertEquals("file.parquet", addFile.get().path());
    assertTrue(addFile.get().dataChange());
    assertFalse(StreamingHelper.getAddFile(selected, 1).isPresent());
    assertFalse(StreamingHelper.getAddFile(selected, 2).isPresent());
    assertFalse(StreamingHelper.getAddFile(selected, 3).get().dataChange());

    FilteredColumnarBatch unfiltered = new FilteredColumnarBatch(batch, Optional.empty());
    assertTrue(StreamingHelper.getAddFile(unfiltered, 1).isPresent());
    assertTrue(StreamingHelper.getAddFileWithDataChange(batch, 0).isPresent());
    assertFalse(StreamingHelper.getAddFileWithDataChange(batch, 3).isPresent());

    ColumnVector absentAdd =
        VectorUtils.buildColumnVector(Collections.singletonList(null), SCHEMA_WITHOUT_STATS);
    ColumnarBatch absentBatch = createAddFileBatch(absentAdd);
    assertFalse(
        StreamingHelper.getAddFile(new FilteredColumnarBatch(absentBatch, Optional.empty()), 0)
            .isPresent());
    assertFalse(StreamingHelper.getAddFileWithDataChange(absentBatch, 0).isPresent());
  }

  private static ColumnarBatch createAddFileBatch(ColumnVector adds) {
    return new ColumnarBatch() {
      @Override
      public io.delta.kernel.types.StructType getSchema() {
        return new io.delta.kernel.types.StructType()
            .add("add", SCHEMA_WITHOUT_STATS);
      }

      @Override
      public ColumnVector getColumnVector(int ordinal) {
        return adds;
      }

      @Override
      public int getSize() {
        return adds.getSize();
      }
    };
  }

  private static Row createAddFileRow(boolean dataChange) {
    return new GenericRow(
        SCHEMA_WITHOUT_STATS,
        Map.of(
            SCHEMA_WITHOUT_STATS.indexOf("path"),
            "file.parquet",
            SCHEMA_WITHOUT_STATS.indexOf("partitionValues"),
            VectorUtils.stringStringMapValue(Map.of()),
            SCHEMA_WITHOUT_STATS.indexOf("size"),
            1L,
            SCHEMA_WITHOUT_STATS.indexOf("modificationTime"),
            1L,
            SCHEMA_WITHOUT_STATS.indexOf("dataChange"),
            dataChange));
  }

  @Test
  public void testUnsafeVolatileSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_volatile_snapshot";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.Snapshot deltaSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    assertEquals(0L, deltaSnapshot.version());
    assertEquals(deltaSnapshot.version(), kernelSnapshot.version());
  }

  @Test
  public void testLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_update";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    assertEquals(0L, initialSnapshot.version());

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    org.apache.spark.sql.delta.Snapshot deltaSnapshot =
        deltaLog.update(false, Option.empty(), Option.empty());
    Snapshot updatedSnapshot = snapshotManager.loadLatestSnapshot();
    org.apache.spark.sql.delta.Snapshot cachedSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelcachedSnapshot = snapshotManager.loadLatestSnapshot();

    assertEquals(1L, updatedSnapshot.version());
    assertEquals(deltaSnapshot.version(), updatedSnapshot.version());
    assertEquals(1L, kernelcachedSnapshot.version());
    assertEquals(cachedSnapshot.version(), kernelcachedSnapshot.version());
  }

  @Test
  public void testMultipleLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_multiple_updates";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    assertEquals(0L, snapshotManager.loadLatestSnapshot().version());

    for (int i = 0; i < 3; i++) {
      spark.sql(
          String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, 20 + i, 20 + i));

      org.apache.spark.sql.delta.Snapshot deltaSnapshot =
          deltaLog.update(false, Option.empty(), Option.empty());
      Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();

      long expectedVersion = i + 1;
      assertEquals(expectedVersion, deltaSnapshot.version());
      assertEquals(expectedVersion, kernelSnapshot.version());
    }
  }

  private void setupTableWithDeletedVersions(String testTablePath, String testTableName) {
    createEmptyTestTable(testTablePath, testTableName);
    for (int i = 0; i < 10; i++) {
      spark.sql(
          String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, 100 + i, 100 + i));
    }
    File deltaLogDir = new File(testTablePath, "_delta_log");
    File version0File = new File(deltaLogDir, "00000000000000000000.json");
    File version1File = new File(deltaLogDir, "00000000000000000001.json");
    assertTrue(version0File.exists());
    assertTrue(version1File.exists());
    version0File.delete();
    version1File.delete();
    assertFalse(version0File.exists());
    assertFalse(version1File.exists());
  }

  @Test
  public void testGetActiveCommitAtTime_pastTimestamp(@TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_commit_past";
    setupTableWithDeletedVersions(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Thread.sleep(100);
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    spark.sql(String.format("INSERT INTO %s VALUES (200, 'NewUser')", testTableName));

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                timestamp,
                Option.empty() /* catalogTable */,
                false /* canReturnLastCommit */,
                true /* mustBeRecreatable */,
                false /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        snapshotManager.getActiveCommitAtTime(
            timestamp.getTime(),
            false /* canReturnLastCommit */,
            true /* mustBeRecreatable */,
            false /* canReturnEarliestCommit */);

    assertEquals(deltaCommit.version(), kernelCommit.getVersion());
    assertEquals(deltaCommit.timestamp(), kernelCommit.getTimestamp());
  }

  @Test
  public void testGetActiveCommitAtTime_futureTimestamp_canReturnLast(@TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_commit_future_last";
    setupTableWithDeletedVersions(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Timestamp futureTimestamp = new Timestamp(System.currentTimeMillis() + 10000);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                futureTimestamp,
                Option.empty() /* catalogTable */,
                true /* canReturnLastCommit */,
                true /* mustBeRecreatable */,
                false /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        snapshotManager.getActiveCommitAtTime(
            futureTimestamp.getTime(),
            true /* canReturnLastCommit */,
            true /* mustBeRecreatable */,
            false /* canReturnEarliestCommit */);

    assertEquals(deltaCommit.version(), kernelCommit.getVersion());
    assertEquals(deltaCommit.timestamp(), kernelCommit.getTimestamp());
  }

  @Test
  public void testGetActiveCommitAtTime_futureTimestamp_notMustBeRecreatable(@TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_commit_future_not_recreatable";
    setupTableWithDeletedVersions(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Timestamp futureTimestamp = new Timestamp(System.currentTimeMillis() + 10000);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                futureTimestamp,
                Option.empty() /* catalogTable */,
                true /* canReturnLastCommit */,
                false /* mustBeRecreatable */,
                false /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        snapshotManager.getActiveCommitAtTime(
            futureTimestamp.getTime(),
            true /* canReturnLastCommit */,
            false /* mustBeRecreatable */,
            false /* canReturnEarliestCommit */);

    assertEquals(deltaCommit.version(), kernelCommit.getVersion());
    assertEquals(deltaCommit.timestamp(), kernelCommit.getTimestamp());
  }

  @Test
  public void testGetActiveCommitAtTime_earlyTimestamp_canReturnEarliest(@TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_commit_early";
    setupTableWithDeletedVersions(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Timestamp earlyTimestamp = new Timestamp(0);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                earlyTimestamp,
                Option.empty() /* catalogTable */,
                false /* canReturnLastCommit */,
                true /* mustBeRecreatable */,
                true /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        snapshotManager.getActiveCommitAtTime(
            earlyTimestamp.getTime(),
            false /* canReturnLastCommit */,
            true /* mustBeRecreatable */,
            true /* canReturnEarliestCommit */);

    assertEquals(deltaCommit.version(), kernelCommit.getVersion());
    assertEquals(deltaCommit.timestamp(), kernelCommit.getTimestamp());
  }

  @Test
  public void testGetActiveCommitAtTime_earlyTimestamp_notMustBeRecreatable_canReturnEarliest(
      @TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_commit_early_not_recreatable";
    setupTableWithDeletedVersions(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Timestamp earlyTimestamp = new Timestamp(0);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                earlyTimestamp,
                Option.empty() /* catalogTable */,
                false /* canReturnLastCommit */,
                false /* mustBeRecreatable */,
                true /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        snapshotManager.getActiveCommitAtTime(
            earlyTimestamp.getTime(),
            false /* canReturnLastCommit */,
            false /* mustBeRecreatable */,
            true /* canReturnEarliestCommit */);

    assertEquals(deltaCommit.version(), kernelCommit.getVersion());
    assertEquals(deltaCommit.timestamp(), kernelCommit.getTimestamp());
  }

  private static Stream<Arguments> checkVersionExistsTestCases() {
    return Stream.of(
        Arguments.of(
            "current",
            10L /* versionToCheck */,
            true /* mustBeRecreatable */,
            false /* allowOutOfRange */,
            false /* shouldThrow */),
        Arguments.of(
            "notAllowOutOfRange",
            21L /* versionToCheck */,
            true /* mustBeRecreatable */,
            false /* allowOutOfRange */,
            true /* shouldThrow */),
        Arguments.of(
            "allowOutOfRange",
            21L /* versionToCheck */,
            true /* mustBeRecreatable */,
            true /* allowOutOfRange */,
            false /* shouldThrow */),
        Arguments.of(
            "belowEarliest",
            1L /* versionToCheck */,
            true /* mustBeRecreatable */,
            false /* allowOutOfRange */,
            true /* shouldThrow */),
        Arguments.of(
            "mustBeRecreatable_false",
            2L /* versionToCheck */,
            false /* mustBeRecreatable */,
            false /* allowOutOfRange */,
            false /* shouldThrow */),
        Arguments.of(
            "mustBeRecreatable_true",
            2L /* versionToCheck */,
            true /* mustBeRecreatable */,
            false /* allowOutOfRange */,
            true /* shouldThrow */));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("checkVersionExistsTestCases")
  public void testCheckVersionExists(
      String testName,
      long versionToCheck,
      boolean mustBeRecreatable,
      boolean allowOutOfRange,
      boolean shouldThrow,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_version_" + testName;
    setupTableWithDeletedVersions(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    if (shouldThrow) {
      assertThrows(
          VersionNotFoundException.class,
          () ->
              snapshotManager.checkVersionExists(
                  versionToCheck, mustBeRecreatable, allowOutOfRange));

      assertThrows(
          org.apache.spark.sql.delta.VersionNotFoundException.class,
          () ->
              deltaLog
                  .history()
                  .checkVersionExists(
                      versionToCheck, Option.empty(), mustBeRecreatable, allowOutOfRange));
    } else {
      snapshotManager.checkVersionExists(versionToCheck, mustBeRecreatable, allowOutOfRange);
      deltaLog
          .history()
          .checkVersionExists(versionToCheck, Option.empty(), mustBeRecreatable, allowOutOfRange);
    }
  }

  // ---------------------------------------------------------------------------
  // Metadata and protocol action collectors
  // ---------------------------------------------------------------------------

  // Mocks let these tests inject commit and action read failures.
  @SuppressWarnings("unchecked")
  private static <T> CloseableIterator<T> mockIterator() {
    return mock(CloseableIterator.class, CALLS_REAL_METHODS);
  }

  private DeltaV2SnapshotManager mockSnapshotManager(CloseableIterator<CommitActions> commits) {
    DeltaV2SnapshotManager manager = mock(DeltaV2SnapshotManager.class);
    CommitRange range = mock(CommitRange.class);
    when(manager.getTableChanges(defaultEngine, 7L, Optional.of(7L))).thenReturn(range);
    when(range.getCommitActions(
        defaultEngine, Set.of(DeltaLogActionUtils.DeltaAction.PROTOCOL))).thenReturn(commits);
    return manager;
  }

  @Test
  public void testProtocolCollector_closesEmptyCommitIterator() throws IOException {
    CloseableIterator<CommitActions> commits = mockIterator();

    assertTrue(StreamingHelper.collectProtocolActions(
        7L, Optional.of(7L), mockSnapshotManager(commits), defaultEngine).isEmpty());

    verify(commits).close();
  }

  @Test
  public void testProtocolCollector_preservesIteratorReadFailureWhenClosureFails()
      throws IOException {
    IOException readFailure = new IOException("commit iterator read failed");
    IOException closeFailure = new IOException("commit iterator close failed");
    CloseableIterator<CommitActions> commits = mockIterator();
    when(commits.hasNext()).thenAnswer(ignored -> {
      throw readFailure;
    });
    doThrow(closeFailure).when(commits).close();

    RuntimeException error = assertThrows(
        RuntimeException.class,
        () -> StreamingHelper.collectProtocolActions(
            7L, Optional.of(7L), mockSnapshotManager(commits), defaultEngine));

    assertEquals("Failed to process commits", error.getMessage());
    assertSame(readFailure, error.getCause());
    assertArrayEquals(new Throwable[] {closeFailure}, readFailure.getSuppressed());
    verify(commits).close();
  }

  @Test
  public void testProtocolCollector_reportsCommitVersionWhenMaterializationFails()
      throws IOException {
    IOException readFailure = new IOException("action iterator open failed");
    CommitActions kernelCommit = mock(CommitActions.class);
    when(kernelCommit.getVersion()).thenReturn(7L);
    when(kernelCommit.getActions()).thenAnswer(ignored -> {
      throw readFailure;
    });
    CloseableIterator<CommitActions> commits = mockIterator();
    when(commits.hasNext()).thenReturn(true);
    when(commits.next()).thenReturn(kernelCommit);

    UncheckedIOException error = assertThrows(
        UncheckedIOException.class,
        () -> StreamingHelper.collectProtocolActions(
            7L, Optional.of(7L), mockSnapshotManager(commits), defaultEngine));

    assertEquals("Failed to process commit at version 7", error.getMessage());
    assertSame(readFailure, error.getCause());
    verify(kernelCommit).close();
    verify(commits).close();
  }

  // ---------------------------------------------------------------------------
  // collectMetadataActions
  //
  // Fixture for parameterized cases below: v0 CREATE, v1 INSERT, v2 ALTER ADD COLUMNS.
  // Versions with a Metadata action: {v0, v2}. v1 has none.
  // ---------------------------------------------------------------------------

  private void setupTableWithMetadataChangeAtV2(String testTablePath, String testTableName) {
    createEmptyTestTable(testTablePath, testTableName); // v0
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", testTableName)); // v1
    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (c3 INT)", testTableName)); // v2
  }

  private static Stream<Arguments> collectMetadataTestCases() {
    return Stream.of(
        // scenario, startVersion, endVersionOpt, expectedVersions
        Arguments.of("fullRange_endInclusive", 0L, Optional.of(2L), Set.of(0L, 2L)),
        Arguments.of("nonTrivialStartExcludesV0", 1L, Optional.of(2L), Set.of(2L)),
        Arguments.of("emptyEndReachesLatest", 0L, Optional.empty(), Set.of(0L, 2L)),
        Arguments.of("rangeWithoutMetadataChange", 1L, Optional.of(1L), Set.of()),
        Arguments.of("endBeforeChange", 0L, Optional.of(1L), Set.of(0L)),
        Arguments.of("emptyEndWithNonTrivialStart", 1L, Optional.empty(), Set.of(2L)),
        Arguments.of("singleVersionAtChange", 2L, Optional.of(2L), Set.of(2L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("collectMetadataTestCases")
  public void testCollectMetadataActions(
      String scenario,
      long startVersion,
      Optional<Long> endVersionOpt,
      Set<Long> expectedVersions,
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_metadata_" + scenario;
    setupTableWithMetadataChangeAtV2(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Map<Long, Metadata> result =
        StreamingHelper.collectMetadataActions(
            startVersion, endVersionOpt, snapshotManager, defaultEngine);

    assertArrayEquals(expectedVersions.stream().sorted().toArray(), result.keySet().toArray());
  }

  /** Verifies that the version → Metadata mapping returns the correct Metadata content. */
  @Test
  public void testCollectMetadataActions_returnsCorrectMetadataPerVersion(
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_metadata_content";
    setupTableWithMetadataChangeAtV2(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Map<Long, Metadata> result =
        StreamingHelper.collectMetadataActions(
            0L, Optional.of(2L), snapshotManager, defaultEngine);

    StructType expectedV0Schema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType);
    StructType expectedV2Schema =
        expectedV0Schema.add("c3", DataTypes.IntegerType);
    assertEquals(expectedV0Schema, result.get(0L).schema());
    assertEquals(expectedV2Schema, result.get(2L).schema());
  }

  // ---------------------------------------------------------------------------
  // collectProtocolActions
  //
  // Fixture for parameterized cases below: v0 CREATE, v1 INSERT, v2 SET TBLPROPERTIES (upgrade).
  // Versions with a Protocol action: {v0, v2}. v1 has none.
  // ---------------------------------------------------------------------------

  private void setupTableWithProtocolUpgradeAtV2(String testTablePath, String testTableName) {
    createEmptyTestTable(testTablePath, testTableName); // v0
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", testTableName)); // v1
    // Adding a table feature forces a real protocol upgrade; setting only minReader/minWriter to
    // (3, 7) is normalized back to (1, 2) by Protocol.merge when no features are introduced and
    // produces no Protocol action.
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES " + "('delta.feature.deletionVectors' = 'supported')",
            testTableName)); // v2
  }

  private static Stream<Arguments> collectProtocolTestCases() {
    return Stream.of(
        // scenario, startVersion, endVersionOpt, expectedVersions
        Arguments.of("fullRange_endInclusive", 0L, Optional.of(2L), Set.of(0L, 2L)),
        Arguments.of("nonTrivialStartExcludesV0", 1L, Optional.of(2L), Set.of(2L)),
        Arguments.of("emptyEndReachesLatest", 0L, Optional.empty(), Set.of(0L, 2L)),
        Arguments.of("rangeWithoutProtocolChange", 1L, Optional.of(1L), Set.of()),
        Arguments.of("endBeforeChange", 0L, Optional.of(1L), Set.of(0L)),
        Arguments.of("emptyEndWithNonTrivialStart", 1L, Optional.empty(), Set.of(2L)),
        Arguments.of("singleVersionAtChange", 2L, Optional.of(2L), Set.of(2L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("collectProtocolTestCases")
  public void testCollectProtocolActions(
      String scenario,
      long startVersion,
      Optional<Long> endVersionOpt,
      Set<Long> expectedVersions,
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_protocol_" + scenario;
    setupTableWithProtocolUpgradeAtV2(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Map<Long, Protocol> result =
        StreamingHelper.collectProtocolActions(
            startVersion, endVersionOpt, snapshotManager, defaultEngine);

    assertArrayEquals(expectedVersions.stream().sorted().toArray(), result.keySet().toArray());
  }

  /** Verifies that the version → Protocol mapping returns the correct Protocol content. */
  @Test
  public void testCollectProtocolActions_returnsCorrectProtocolPerVersion(
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_protocol_content";
    setupTableWithProtocolUpgradeAtV2(testTablePath, testTableName);
    snapshotManager = new PathBasedSnapshotManager(testTablePath, createKernelContext());

    Map<Long, Protocol> result =
        StreamingHelper.collectProtocolActions(
            0L, Optional.of(2L), snapshotManager, defaultEngine);

    // v0 default: reader=1, writer=2; v2 upgraded: reader=3, writer=7
    assertEquals(1, result.get(0L).minReaderVersion());
    assertEquals(2, result.get(0L).minWriterVersion());
    assertEquals(3, result.get(2L).minReaderVersion());
    assertEquals(7, result.get(2L).minWriterVersion());
  }
}
