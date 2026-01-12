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
package io.delta.spark.internal.v2.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.spark.internal.v2.SparkDsv2TestBase;
import io.delta.spark.internal.v2.exception.VersionNotFoundException;
import java.io.File;
import java.sql.Timestamp;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;

public class PathBasedSnapshotManagerTest extends SparkDsv2TestBase {

  private PathBasedSnapshotManager snapshotManager;

  @Test
  public void testUnsafeVolatileSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_volatile_snapshot";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.Snapshot deltaSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    assertEquals(0L, deltaSnapshot.version());
    assertEquals(deltaSnapshot.version(), kernelSnapshot.getVersion());
  }

  @Test
  public void testLoadLatestSnapshot(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_update";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    Snapshot initialSnapshot = snapshotManager.loadLatestSnapshot();
    assertEquals(0L, initialSnapshot.getVersion());

    spark.sql(String.format("INSERT INTO %s VALUES (4, 'David')", testTableName));

    org.apache.spark.sql.delta.Snapshot deltaSnapshot =
        deltaLog.update(false, Option.empty(), Option.empty());
    Snapshot updatedSnapshot = snapshotManager.loadLatestSnapshot();
    org.apache.spark.sql.delta.Snapshot cachedSnapshot = deltaLog.unsafeVolatileSnapshot();
    Snapshot kernelcachedSnapshot = snapshotManager.loadLatestSnapshot();

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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));

    assertEquals(0L, snapshotManager.loadLatestSnapshot().getVersion());

    for (int i = 0; i < 3; i++) {
      spark.sql(
          String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, 20 + i, 20 + i));

      org.apache.spark.sql.delta.Snapshot deltaSnapshot =
          deltaLog.update(false, Option.empty(), Option.empty());
      Snapshot kernelSnapshot = snapshotManager.loadLatestSnapshot();

      long expectedVersion = i + 1;
      assertEquals(expectedVersion, deltaSnapshot.version());
      assertEquals(expectedVersion, kernelSnapshot.getVersion());
    }
  }

  @Test
  public void testLoadSnapshotAt(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_load_at_version";
    createEmptyTestTable(testTablePath, testTableName);
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

    // Create multiple versions
    for (int i = 0; i < 3; i++) {
      spark.sql(
          String.format("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, 10 + i, 10 + i));
    }

    // Load specific versions
    Snapshot snapshot0 = snapshotManager.loadSnapshotAt(0L);
    assertEquals(0L, snapshot0.getVersion());

    Snapshot snapshot1 = snapshotManager.loadSnapshotAt(1L);
    assertEquals(1L, snapshot1.getVersion());

    Snapshot snapshot2 = snapshotManager.loadSnapshotAt(2L);
    assertEquals(2L, snapshot2.getVersion());

    Snapshot snapshot3 = snapshotManager.loadSnapshotAt(3L);
    assertEquals(3L, snapshot3.getVersion());

    // Note: loadSnapshotAt does not update the cached snapshot
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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

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
    snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());
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
}
