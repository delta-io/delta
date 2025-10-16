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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.spark.SparkDsv2TestBase;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.sources.IndexedFile;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.collection.immutable.Map$;

public class StreamingHelperTest extends SparkDsv2TestBase {

  private StreamingHelper streamingHelper;

  @Test
  public void testUnsafeVolatileSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_volatile_snapshot";
    createEmptyTestTable(testTablePath, testTableName);
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.Snapshot deltaSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelSnapshot = streamingHelper.unsafeVolatileSnapshot();

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    assertEquals(0L, deltaSnapshot.version());
    assertEquals(deltaSnapshot.version(), kernelSnapshot.getVersion());
  }

  @Test
  public void testLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_update";
    createEmptyTestTable(testTablePath, testTableName);
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    Snapshot initialSnapshot = streamingHelper.loadLatestSnapshot();
    assertEquals(0L, initialSnapshot.getVersion());

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    org.apache.spark.sql.delta.Snapshot deltaSnapshot =
        deltaLog.update(false, Option.empty(), Option.empty());
    Snapshot updatedSnapshot = streamingHelper.loadLatestSnapshot();
    org.apache.spark.sql.delta.Snapshot cachedSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelcachedSnapshot = streamingHelper.unsafeVolatileSnapshot();

    assertEquals(1L, updatedSnapshot.getVersion());
    assertEquals(deltaSnapshot.version(), updatedSnapshot.getVersion());
    assertEquals(1L, kernelcachedSnapshot.getVersion());
    assertEquals(cachedSnapshot.version(), kernelcachedSnapshot.getVersion());
  }

  @Test
  public void testMultipleLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_multiple_updates";
    createEmptyTestTable(testTablePath, testTableName);
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    assertEquals(0L, streamingHelper.loadLatestSnapshot().getVersion());

    for (int i = 0; i < 3; i++) {
      spark.sql(
          String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, 20 + i, 20 + i));

      org.apache.spark.sql.delta.Snapshot deltaSnapshot =
          deltaLog.update(false, Option.empty(), Option.empty());
      Snapshot kernelSnapshot = streamingHelper.loadLatestSnapshot();

      long expectedVersion = i + 1;
      assertEquals(expectedVersion, deltaSnapshot.version());
      assertEquals(expectedVersion, kernelSnapshot.getVersion());
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
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());

    Thread.sleep(100);
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    spark.sql(String.format("INSERT INTO %s VALUES (200, 'NewUser')", testTableName));

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                timestamp,
                false /* canReturnLastCommit */,
                true /* mustBeRecreatable */,
                false /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        streamingHelper.getActiveCommitAtTime(
            timestamp,
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
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());

    Timestamp futureTimestamp = new Timestamp(System.currentTimeMillis() + 10000);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                futureTimestamp,
                true /* canReturnLastCommit */,
                true /* mustBeRecreatable */,
                false /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        streamingHelper.getActiveCommitAtTime(
            futureTimestamp,
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
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());

    Timestamp futureTimestamp = new Timestamp(System.currentTimeMillis() + 10000);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                futureTimestamp,
                true /* canReturnLastCommit */,
                false /* mustBeRecreatable */,
                false /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        streamingHelper.getActiveCommitAtTime(
            futureTimestamp,
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
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());

    Timestamp earlyTimestamp = new Timestamp(0);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                earlyTimestamp,
                false /* canReturnLastCommit */,
                true /* mustBeRecreatable */,
                true /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        streamingHelper.getActiveCommitAtTime(
            earlyTimestamp,
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
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());

    Timestamp earlyTimestamp = new Timestamp(0);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.DeltaHistoryManager.Commit deltaCommit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                earlyTimestamp,
                false /* canReturnLastCommit */,
                false /* mustBeRecreatable */,
                true /* canReturnEarliestCommit */);

    DeltaHistoryManager.Commit kernelCommit =
        streamingHelper.getActiveCommitAtTime(
            earlyTimestamp,
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
    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    if (shouldThrow) {
      assertThrows(
          VersionNotFoundException.class,
          () ->
              streamingHelper.checkVersionExists(
                  versionToCheck, mustBeRecreatable, allowOutOfRange));

      assertThrows(
          org.apache.spark.sql.delta.VersionNotFoundException.class,
          () ->
              deltaLog
                  .history()
                  .checkVersionExists(versionToCheck, mustBeRecreatable, allowOutOfRange));
    } else {
      streamingHelper.checkVersionExists(versionToCheck, mustBeRecreatable, allowOutOfRange);
      deltaLog.history().checkVersionExists(versionToCheck, mustBeRecreatable, allowOutOfRange);
    }
  }

  /**
   * Helper method to compare DeltaSource IndexedFile with Kernel IndexedFileAction. Returns the
   * path from AddFile if available.
   */
  private String getAddFilePath(IndexedFile indexedFile) {
    if (indexedFile.add() != null) {
      return indexedFile.add().path();
    }
    return null;
  }

  /** Helper method to get path from Kernel IndexedFileAction */
  private String getAddFilePath(IndexedFileAction action) {
    if (action.getAddFile() != null) {
      Row addFile = action.getAddFile();
      int pathIdx = addFile.getSchema().indexOf("path");
      if (pathIdx >= 0 && !addFile.isNullAt(pathIdx)) {
        return addFile.getString(pathIdx);
      }
    }
    return null;
  }

  /** Helper method to compare two lists of file changes */
  private void compareFileChanges(
      List<IndexedFile> deltaSourceFiles, List<IndexedFileAction> kernelFiles) {
    assertEquals(
        deltaSourceFiles.size(),
        kernelFiles.size(),
        "Number of file changes should match between DeltaSource and Kernel");

    for (int i = 0; i < deltaSourceFiles.size(); i++) {
      IndexedFile deltaFile = deltaSourceFiles.get(i);
      IndexedFileAction kernelFile = kernelFiles.get(i);

      assertEquals(
          deltaFile.version(),
          kernelFile.getVersion(),
          String.format("Version mismatch at index %d", i));

      assertEquals(
          deltaFile.index(), kernelFile.getIndex(), String.format("Index mismatch at index %d", i));

      String deltaPath = getAddFilePath(deltaFile);
      String kernelPath = getAddFilePath(kernelFile);

      if (deltaPath != null || kernelPath != null) {
        assertEquals(deltaPath, kernelPath, String.format("AddFile path mismatch at index %d", i));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getFileChangesParameters")
  public void testGetFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<Long> endVersion,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    // Use unique table name per test instance to avoid conflicts
    // Use testDescription to generate unique table name
    String testTableName =
        "test_file_changes_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions of data
    for (int i = 0; i < 5; i++) {
      spark.sql(String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, i, i));
    }

    streamingHelper = new StreamingHelper(testTablePath, spark.sessionState().newHadoopConf());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.sources.DeltaSource deltaSource =
        createDeltaSource(deltaLog, testTablePath);

    // Get changes using DeltaSource
    scala.Option<org.apache.spark.sql.delta.sources.DeltaSourceOffset> scalaEndOffset =
        scala.Option.empty();
    if (endVersion.isPresent()) {
      scalaEndOffset =
          scala.Option.apply(
              new org.apache.spark.sql.delta.sources.DeltaSourceOffset(
                  deltaLog.tableId(), // reservoirId
                  endVersion.get(), // reservoirVersion
                  Long.MAX_VALUE, // index (use MAX_VALUE for version-only offset)
                  false)); // isInitialSnapshot
    }
    ClosableIterator<IndexedFile> deltaChanges =
        deltaSource.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, scalaEndOffset, true);
    List<IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // Get changes using StreamingHelper
    try (CloseableIterator<IndexedFileAction> kernelChanges =
        streamingHelper.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, endVersion)) {
      List<IndexedFileAction> kernelFilesList = new ArrayList<>();
      while (kernelChanges.hasNext()) {
        kernelFilesList.add(kernelChanges.next());
      }

      // Compare results
      compareFileChanges(deltaFilesList, kernelFilesList);
    }
  }

  /**
   * Provides test parameters for the parameterized getFileChanges test.
   *
   * <p>Each parameter set includes: fromVersion, fromIndex, isInitialSnapshot, endVersion,
   * testDescription
   */
  private static Stream<Arguments> getFileChangesParameters() {
    return Stream.of(
        // Basic cases: fromIndex = -1, isInitialSnapshot = false, no endVersion
        Arguments.of(0L, -1L, false, Optional.empty(), "Basic: from version 0"),
        Arguments.of(1L, -1L, false, Optional.empty(), "Basic: from version 1"),
        Arguments.of(3L, -1L, false, Optional.empty(), "Basic: from middle version"),

        // With fromIndex > -1
        Arguments.of(0L, 0L, false, Optional.empty(), "With fromIndex: version 0, index 0"),
        Arguments.of(1L, 1L, false, Optional.empty(), "With fromIndex: version 1, index 1"),

        // With endVersion
        Arguments.of(0L, -1L, false, Optional.of(2L), "With endVersion: 0 to 2"),
        Arguments.of(1L, -1L, false, Optional.of(3L), "With endVersion: 1 to 3"),
        Arguments.of(2L, -1L, false, Optional.of(4L), "With endVersion: 2 to 4"),

        // With isInitialSnapshot = true
        Arguments.of(0L, -1L, true, Optional.empty(), "InitialSnapshot: version 0"),
        Arguments.of(1L, -1L, true, Optional.empty(), "InitialSnapshot: version 1"),
        Arguments.of(2L, -1L, true, Optional.empty(), "InitialSnapshot: version 2"),

        // InitialSnapshot with fromIndex
        Arguments.of(0L, 0L, true, Optional.empty(), "InitialSnapshot: version 0, index 0"),
        Arguments.of(1L, 1L, true, Optional.empty(), "InitialSnapshot: version 1, index 1"),

        // InitialSnapshot with endVersion
        Arguments.of(0L, -1L, true, Optional.of(2L), "InitialSnapshot with endVersion: 0 to 2"),
        Arguments.of(1L, -1L, true, Optional.of(3L), "InitialSnapshot with endVersion: 1 to 3"),

        // Complex combinations
        Arguments.of(1L, 1L, false, Optional.of(3L), "Complex: v1 idx1 to v3"),
        Arguments.of(2L, 0L, true, Optional.of(4L), "Complex: InitialSnapshot v2 idx0 to v4"));
  }

  /** Helper method to create a DeltaSource for testing */
  private org.apache.spark.sql.delta.sources.DeltaSource createDeltaSource(
      DeltaLog deltaLog, String tablePath) {
    DeltaOptions options = new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
    scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.expressions.Expression> emptySeq =
        scala.collection.JavaConverters.asScalaBuffer(
                new java.util.ArrayList<org.apache.spark.sql.catalyst.expressions.Expression>())
            .toList();
    return new org.apache.spark.sql.delta.sources.DeltaSource(
        spark,
        deltaLog,
        Option.empty(),
        options,
        deltaLog.update(false, Option.empty(), Option.empty()),
        tablePath + "/_checkpoint",
        Option.empty(),
        emptySeq);
  }
}
