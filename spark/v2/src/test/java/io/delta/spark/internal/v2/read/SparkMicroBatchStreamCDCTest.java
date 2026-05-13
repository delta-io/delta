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

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.CommitActions;
import io.delta.kernel.CommitRange;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.commitrange.CommitRangeImpl;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import io.delta.spark.internal.v2.utils.StreamingHelper;
import java.io.File;
import java.util.*;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset$;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;

/** Tests for SparkMicroBatchStream CDC (Change Data Capture) support and DSv1/DSv2 parity. */
public class SparkMicroBatchStreamCDCTest extends DeltaV2TestBase {

  // TODO(cdf3): Add test for CDF enabled in initial snapshot but disabled in a later commit.

  @Test
  public void testValidateCDFEnabled_throwsWhenNotEnabled(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_not_enabled_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1')", tableName);

    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () -> {
              try (CloseableIterator<IndexedFile> iter =
                  stream.getFileChangesWithRateLimitForCDC(
                      /* fromVersion= */ 0L,
                      /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
                      /* isInitialSnapshot= */ false,
                      /* limits= */ Optional.empty(),
                      /* endOffset= */ Optional.empty())) {
                while (iter.hasNext()) iter.next();
              }
            });
    Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
    assertInstanceOf(AnalysisException.class, cause);
    AnalysisException analysisException = (AnalysisException) cause;
    assertEquals(
        "DELTA_MISSING_CHANGE_DATA",
        analysisException.getErrorClass(),
        "Expected DELTA_MISSING_CHANGE_DATA error class");
  }

  private static final SparkMicroBatchStreamTest.ScenarioSetup CDC_TWO_INSERT_SETUP =
      (tableName, tempDir) -> {
        sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
        sql("INSERT INTO %s VALUES (3, 'User3')", tableName);
      };

  static Stream<Arguments> cdcFileChangesParameters() {
    Optional<Integer> noMaxFiles = Optional.empty();
    Optional<Long> noMaxBytes = Optional.empty();

    return Stream.of(
        // Initial snapshot path (snapshot files + delta logs)
        Arguments.of(
            "Initial snapshot (all inserts)",
            CDC_TWO_INSERT_SETUP,
            /* isInitialSnapshot= */ true,
            noMaxFiles,
            noMaxBytes),
        Arguments.of(
            "Empty table (no data files)",
            (SparkMicroBatchStreamTest.ScenarioSetup) (t, d) -> {},
            /* isInitialSnapshot= */ true,
            noMaxFiles,
            noMaxBytes),
        Arguments.of(
            "Multiple inserts (5 versions)",
            (SparkMicroBatchStreamTest.ScenarioSetup)
                (t, d) -> {
                  for (int i = 1; i <= 5; i++) {
                    sql("INSERT INTO %s VALUES (%d, 'V%d')", t, i, i);
                  }
                },
            /* isInitialSnapshot= */ true,
            noMaxFiles,
            noMaxBytes),
        Arguments.of(
            "Rate limited (maxFiles=1)",
            CDC_TWO_INSERT_SETUP,
            /* isInitialSnapshot= */ true,
            Optional.of(1),
            noMaxBytes),
        Arguments.of(
            "Rate limited (maxBytes=1)",
            CDC_TWO_INSERT_SETUP,
            /* isInitialSnapshot= */ true,
            noMaxFiles,
            Optional.of(1L)),
        // Delta log path only (isInitialSnapshot=false, exercises filterDeltaLogsForCDC directly)
        Arguments.of(
            "Delta log (two inserts)",
            CDC_TWO_INSERT_SETUP,
            /* isInitialSnapshot= */ false,
            noMaxFiles,
            noMaxBytes),
        Arguments.of(
            "Delta log rate limited (maxFiles=1)",
            CDC_TWO_INSERT_SETUP,
            /* isInitialSnapshot= */ false,
            Optional.of(1),
            noMaxBytes),
        Arguments.of(
            "Delta log rate limited (maxBytes=1)",
            CDC_TWO_INSERT_SETUP,
            /* isInitialSnapshot= */ false,
            noMaxFiles,
            Optional.of(1L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cdcFileChangesParameters")
  public void testGetFileChangesForCDC(
      String testDescription,
      SparkMicroBatchStreamTest.ScenarioSetup setup,
      boolean isInitialSnapshot,
      Optional<Integer> maxFiles,
      Optional<Long> maxBytes,
      @TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName =
        "test_cdc_compare_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')", tableName);
    setup.setup(tableName, tempDir);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    long latestVersion = deltaLog.update(false, Option.empty(), Option.empty()).version();
    // For non-initial-snapshot, start from v2 (v0=CREATE, v1=ALTER CDF, v2+=INSERTs)
    long fromVersion = isInitialSnapshot ? latestVersion : 2;

    // DSv1
    scala.collection.immutable.Map<String, String> cdcOptionsMap =
        Map$.MODULE$.<String, String>empty().updated("readChangeFeed", "true");
    DeltaOptions cdcOptions = new DeltaOptions(cdcOptionsMap, spark.sessionState().conf());
    DeltaSource deltaSource = createDeltaSource(deltaLog, tablePath, cdcOptions);
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files = new ArrayList<>();
    try (ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> iter =
        deltaSource.getFileChangesWithRateLimit(
            fromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            isInitialSnapshot,
            ScalaUtils.toScalaOption(createAdmissionLimits(maxFiles, maxBytes)))) {
      while (iter.hasNext()) dsv1Files.add(iter.next());
    }

    // DSv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    List<IndexedFile> dsv2Files = new ArrayList<>();
    try (CloseableIterator<IndexedFile> iter =
        stream.getFileChangesWithRateLimitForCDC(
            fromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            isInitialSnapshot,
            createAdmissionLimits(maxFiles, maxBytes),
            Optional.empty())) {
      while (iter.hasNext()) dsv2Files.add(iter.next());
    }

    if (isInitialSnapshot) {
      assertCDCFileChangesMatch(dsv1Files, dsv2Files);
    } else {
      SparkMicroBatchStreamTest.compareFileChanges(dsv1Files, dsv2Files);
    }
  }

  /**
   * Tests that version-level takeWhile stops processing commits once rate limit capacity is
   * exhausted. With maxFiles=1 and two commits, only the first commit's files should be returned.
   */
  @Test
  public void testGetFileChangesForCDC_versionLevelTakeWhile(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_takewhile_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'V1')", tableName);
    sql("INSERT INTO %s VALUES (2, 'V2')", tableName);
    // v0=CREATE, v1=ALTER CDF, v2=INSERT, v3=INSERT

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    deltaLog.update(false, Option.empty(), Option.empty());

    // Unlimited: DSv1/DSv2 parity, should see data files from both commits (v2 and v3)
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Unlimited =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.empty());
    List<IndexedFile> dsv2Unlimited =
        getDsv2FileChanges(
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.empty());
    SparkMicroBatchStreamTest.compareFileChanges(dsv1Unlimited, dsv2Unlimited);

    Set<Long> unlimitedVersions = new HashSet<>();
    for (IndexedFile f : dsv2Unlimited) {
      if (f.hasFileAction()) unlimitedVersions.add(f.getVersion());
    }
    assertTrue(
        unlimitedVersions.size() >= 2,
        "Expected data files from at least 2 versions, got: " + unlimitedVersions);

    // Rate-limited (maxFiles=1): takeWhile(hasCapacity) should stop after first commit
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Limited =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.of(1),
            /* maxBytes= */ Optional.empty());
    List<IndexedFile> dsv2Limited =
        getDsv2FileChanges(
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.of(1),
            /* maxBytes= */ Optional.empty());
    SparkMicroBatchStreamTest.compareFileChanges(dsv1Limited, dsv2Limited);

    Set<Long> limitedVersions = new HashSet<>();
    for (IndexedFile f : dsv2Limited) {
      if (f.hasFileAction()) limitedVersions.add(f.getVersion());
    }
    assertEquals(
        1,
        limitedVersions.size(),
        "With maxFiles=1, version-level takeWhile should stop after first commit. "
            + "Got versions: "
            + limitedVersions);
  }

  /**
   * Tests that partial admission on a CDC commit doesn't skip files. Without the conditional END
   * sentinel fix, the END sentinel after partial admission would advance the offset past the
   * remaining files.
   */
  @Test
  public void testGetFileChangesForCDC_partialCommitAdmission(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_partial_admit_" + System.nanoTime();
    // Partition by name so each distinct value produces a separate file in one commit.
    createEmptyPartitionedTestTable(tablePath, tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')", tableName);
    sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    long latestVersion = deltaLog.update(false, Option.empty(), Option.empty()).version();
    long insertVersion = latestVersion;

    // Read all files without rate limit for reference
    List<IndexedFile> dsv2All =
        getDsv2FileChanges(
            tablePath,
            insertVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.empty());
    long totalDataFiles = dsv2All.stream().filter(f -> f.getCDCDataFile() != null).count();
    assertTrue(totalDataFiles > 1, "Partitioned insert should produce multiple files");

    // Read with maxFiles=1: should get partial results
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Call1 =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            insertVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.of(1),
            /* maxBytes= */ Optional.empty());
    List<IndexedFile> dsv2Call1 =
        getDsv2FileChanges(
            tablePath,
            insertVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.of(1),
            /* maxBytes= */ Optional.empty());
    SparkMicroBatchStreamTest.compareFileChanges(dsv1Call1, dsv2Call1);

    // Verify no END sentinel in partial result
    boolean dsv2HasEnd =
        dsv2Call1.stream()
            .anyMatch(
                f -> f.getIndex() == DeltaSourceOffset.END_INDEX() && f.getCDCDataFile() == null);
    assertFalse(dsv2HasEnd, "Partial admission should not produce END sentinel");
  }

  /**
   * Tests that a batch-rejected explicit CDC commit exhausts the admission budget, preventing
   * subsequent commits from being admitted. AdmissionLimits.admit(Seq) consumes the budget even on
   * reject, so the version-level takeWhile(hasCapacity) stops the iteration.
   */
  @Test
  public void testGetFileChangesForCDC_batchRejectExhaustsBudget(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_batch_reject_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'A')", tableName); // v2
    sql("DELETE FROM %s WHERE id = 1", tableName); // v3: explicit CDC
    sql("INSERT INTO %s VALUES (2, 'B')", tableName); // v4

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    deltaLog.update(false, Option.empty(), Option.empty());

    // Probe file sizes to craft a byte budget that admits v2 but rejects v3's batch.
    List<IndexedFile> probeFiles =
        getDsv2FileChanges(
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.empty());

    long v2Size = 0;
    long v3BatchSize = 0;
    for (IndexedFile f : probeFiles) {
      if (f.getVersion() == 2 && f.hasFileAction()) v2Size += f.getFileSize();
      if (f.getVersion() == 3 && f.hasFileAction()) v3BatchSize += f.getFileSize();
    }
    assertTrue(v3BatchSize > 0, "DELETE should produce data files");

    // Budget enough for v2 but 1 byte short for v3's batch.
    long maxBytes = v2Size + v3BatchSize - 1;

    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.of(maxBytes));
    List<IndexedFile> dsv2Files =
        getDsv2FileChanges(
            tablePath,
            /* fromVersion= */ 2L,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.of(maxBytes));
    SparkMicroBatchStreamTest.compareFileChanges(dsv1Files, dsv2Files);

    long v2DataFiles =
        dsv2Files.stream().filter(f -> f.getVersion() == 2 && f.hasFileAction()).count();
    assertTrue(v2DataFiles > 0, "Version 2 should be admitted");

    long v3DataFiles =
        dsv2Files.stream().filter(f -> f.getVersion() == 3 && f.hasFileAction()).count();
    assertEquals(0, v3DataFiles, "Version 3 should be batch-rejected");

    long v4DataFiles =
        dsv2Files.stream().filter(f -> f.getVersion() == 4 && f.hasFileAction()).count();
    assertEquals(0, v4DataFiles, "Version 4 should not be admitted after batch-reject");
  }

  private static final Set<DeltaAction> CDC_ACTION_SET =
      Set.of(
          DeltaAction.ADD,
          DeltaAction.REMOVE,
          DeltaAction.METADATA,
          DeltaAction.CDC,
          DeltaAction.COMMITINFO);

  @Test
  public void testProcessCommit_inferredInsert(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_insert_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);

    long commitVersion = 2;
    SparkMicroBatchStream stream = createStream(tablePath);
    CommitActions commit = getCommitActions(tablePath, commitVersion);

    List<IndexedFile> files;
    try (CloseableIterator<IndexedFile> iter =
        stream.processCommitToIndexedFilesForCDC(
            commit,
            /* startVersion= */ commitVersion,
            /* limits= */ Optional.empty(),
            /* endOffsetOpt= */ Optional.empty(),
            /* lastProcessedVersion= */ commitVersion,
            /* lastProcessedIndex= */ DeltaSourceOffset.BASE_INDEX())) {
      files = collectAll(iter);
    }

    // BEGIN sentinel is filtered (at start boundary). Only data files + END remain.
    assertTrue(files.size() >= 2, "Expected at least data + END, got " + files.size());

    IndexedFile end = files.get(files.size() - 1);
    assertEquals(commitVersion, end.getVersion());
    assertEquals(DeltaSourceOffset.END_INDEX(), end.getIndex());
    assertNull(end.getCDCDataFile());

    List<IndexedFile> dataFiles = new ArrayList<>();
    for (IndexedFile f : files) {
      if (f.getCDCDataFile() != null) dataFiles.add(f);
    }
    assertFalse(dataFiles.isEmpty(), "Expected at least one data file");

    for (IndexedFile f : dataFiles) {
      assertEquals("insert", f.getCDCDataFile().getChangeType(), "Change type should be 'insert'");
      assertTrue(
          f.getCDCDataFile().getCommitTimestamp() > 0, "Commit timestamp should be positive");
      assertEquals(commitVersion, f.getVersion());
    }
  }

  @Test
  public void testProcessCommit_explicitCDCFromDelete(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_delete_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
    sql("DELETE FROM %s WHERE id = 1", tableName);

    long commitVersion = 3;
    SparkMicroBatchStream stream = createStream(tablePath);
    CommitActions commit = getCommitActions(tablePath, commitVersion);

    List<IndexedFile> files;
    try (CloseableIterator<IndexedFile> iter =
        stream.processCommitToIndexedFilesForCDC(
            commit,
            /* startVersion= */ commitVersion,
            /* limits= */ Optional.empty(),
            /* endOffsetOpt= */ Optional.empty(),
            /* lastProcessedVersion= */ commitVersion,
            /* lastProcessedIndex= */ DeltaSourceOffset.BASE_INDEX())) {
      files = collectAll(iter);
    }

    // BEGIN sentinel is filtered (at start boundary). Only data files + END remain.
    assertTrue(files.size() >= 2, "Expected at least data + END, got " + files.size());

    List<IndexedFile> dataFiles = new ArrayList<>();
    for (IndexedFile f : files) {
      if (f.getCDCDataFile() != null) dataFiles.add(f);
    }
    assertFalse(dataFiles.isEmpty(), "Expected at least one data file from DELETE");

    for (IndexedFile f : dataFiles) {
      assertTrue(f.isAddCDCFile(), "Data file should be an explicit CDC file");
      assertNotNull(f.getCDCDataFile(), "CDCDataFile should be present");
      assertTrue(f.getCDCDataFile().isAddCDCFile(), "Should be an explicit CDC file");
      assertEquals(commitVersion, f.getVersion());
    }
  }

  @Test
  public void testProcessCommit_noopMergeSkipsInferredFiles(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_noop_merge_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);

    // Spark optimizes away truly no-op MERGEs (no commit produced), so we write a
    // synthetic commit that simulates what a copy-on-write no-op MERGE looks like:
    // AddFile + RemoveFile (file rewrite) with CommitInfo showing zero rows changed.
    long commitVersion = 3;
    writeSyntheticNoopMergeCommit(tablePath, commitVersion);

    SparkMicroBatchStream stream = createStream(tablePath);

    // Verify the commit has file actions (the synthetic commit has Add + Remove).
    CommitActions rawCommit = getCommitActions(tablePath, commitVersion);
    int addFileCount = 0;
    try (CloseableIterator<ColumnarBatch> iter = rawCommit.getActions()) {
      while (iter.hasNext()) {
        ColumnarBatch batch = iter.next();
        for (int rowId = 0; rowId < batch.getSize(); rowId++) {
          if (StreamingHelper.getAddFileWithDataChange(batch, rowId).isPresent()) {
            addFileCount++;
          }
        }
      }
    }
    assertTrue(addFileCount > 0, "Synthetic commit should have AddFile actions");

    // Process through CDC — no-op MERGE should discard all inferred files.
    CommitActions commit = getCommitActions(tablePath, commitVersion);
    List<IndexedFile> files;
    try (CloseableIterator<IndexedFile> iter =
        stream.processCommitToIndexedFilesForCDC(
            commit,
            /* startVersion= */ commitVersion,
            /* limits= */ Optional.empty(),
            /* endOffsetOpt= */ Optional.empty(),
            /* lastProcessedVersion= */ commitVersion,
            /* lastProcessedIndex= */ DeltaSourceOffset.BASE_INDEX())) {
      files = collectAll(iter);
    }

    // BEGIN sentinel filtered (at start boundary). Only END sentinel remains.
    assertEquals(1, files.size(), "Expected only END sentinel for no-op merge");
    assertEquals(DeltaSourceOffset.END_INDEX(), files.get(0).getIndex());
    assertNull(files.get(0).getCDCDataFile());
  }

  /**
   * Tests that inferred CDC preserves delta-log action order for index assignment. A synthetic
   * UPDATE commit writes RemoveFile before AddFile (in that order in the log JSON); DSv1 preserves
   * that order via zipWithIndex. DSv2 must assign indices in the same order to maintain
   * offset/checkpoint compatibility.
   *
   * <p>Regression test: a prior implementation collected AddFile and RemoveFile into separate
   * lists, then emitted all adds before all removes, which reordered indices vs DSv1.
   */
  @Test
  public void testProcessCommit_inferredCDCPreservesDeltaLogActionOrder(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_inferred_order_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);

    // Write a synthetic UPDATE commit with RemoveFile BEFORE AddFile and non-zero metrics
    // (so shouldSkip=false). This exercises the inferred CDC path (no AddCDCFile) with both
    // action types, where action order determines index assignment.
    long commitVersion = 3;
    writeSyntheticInferredCDCCommit(tablePath, commitVersion);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    deltaLog.update(false, Option.empty(), Option.empty());

    // DSv1/DSv2 parity: index assignment must match DSv1's zipWithIndex ordering
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            commitVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.empty());
    List<IndexedFile> dsv2Files =
        getDsv2FileChanges(
            tablePath,
            commitVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* maxFiles= */ Optional.empty(),
            /* maxBytes= */ Optional.empty());
    SparkMicroBatchStreamTest.compareFileChanges(dsv1Files, dsv2Files);

    // Verify ordering: RemoveFile (delete) before AddFile (insert) in delta log
    List<IndexedFile> dataFiles = new ArrayList<>();
    for (IndexedFile f : dsv2Files) {
      if (f.getCDCDataFile() != null) dataFiles.add(f);
    }
    assertEquals(2, dataFiles.size(), "Expected exactly 2 data files (one remove + one add)");

    assertEquals(
        "delete",
        dataFiles.get(0).getCDCDataFile().getChangeType(),
        "First data file should be 'delete' (RemoveFile appears first in delta log)");
    assertEquals(
        "insert",
        dataFiles.get(1).getCDCDataFile().getChangeType(),
        "Second data file should be 'insert' (AddFile appears second in delta log)");
  }

  @Test
  public void testProcessCommit_endOffsetAtBaseIndex(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_baseindex_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1')", tableName);

    long commitVersion = 2;
    SparkMicroBatchStream stream = createStream(tablePath);
    CommitActions commit = getCommitActions(tablePath, commitVersion);

    DeltaSourceOffset endOffset =
        DeltaSourceOffset$.MODULE$.apply(
            "test-reservoir",
            commitVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false);

    List<IndexedFile> files;
    try (CloseableIterator<IndexedFile> iter =
        stream.processCommitToIndexedFilesForCDC(
            commit,
            /* startVersion= */ commitVersion,
            /* limits= */ Optional.empty(),
            Optional.of(endOffset),
            /* lastProcessedVersion= */ commitVersion,
            /* lastProcessedIndex= */ DeltaSourceOffset.BASE_INDEX())) {
      files = collectAll(iter);
    }

    assertEquals(2, files.size(), "Expected exactly BEGIN + END sentinels");
    assertEquals(DeltaSourceOffset.BASE_INDEX(), files.get(0).getIndex());
    assertEquals(DeltaSourceOffset.END_INDEX(), files.get(1).getIndex());
    assertNull(files.get(0).getCDCDataFile());
    assertNull(files.get(1).getCDCDataFile());
  }

  /**
   * Tests that resuming a rate-limited initial snapshot CDC read makes progress. Without the
   * boundary pre-filter fix, already-processed files would consume the admission budget on the
   * second call, causing the stream to stall.
   */
  @Test
  public void testGetFileChangesForCDC_snapshotResumeWithRateLimit(@TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_snapshot_resume_" + System.nanoTime();
    createCDFEnabledTable(tablePath, tableName);
    // Insert multiple rows so the snapshot has multiple files
    sql("INSERT INTO %s VALUES (1, 'A')", tableName);
    sql("INSERT INTO %s VALUES (2, 'B')", tableName);
    sql("INSERT INTO %s VALUES (3, 'C')", tableName);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    long latestVersion = deltaLog.update(false, Option.empty(), Option.empty()).version();
    Optional<Integer> maxFiles = Optional.of(1);

    // Call 1: from BASE_INDEX, should get 1 file
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Call1 =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            latestVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ true,
            maxFiles,
            /* maxBytes= */ Optional.empty());
    List<IndexedFile> dsv2Call1 =
        getDsv2FileChanges(
            tablePath,
            latestVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ true,
            maxFiles,
            /* maxBytes= */ Optional.empty());
    assertCDCFileChangesMatch(dsv1Call1, dsv2Call1);
    long dsv1DataCount1 = dsv1Call1.stream().filter(f -> f.add() != null).count();
    assertTrue(dsv1DataCount1 > 0, "Call 1 should return at least 1 data file");

    // Find the last data file index from call 1
    long lastIndex1 =
        dsv2Call1.stream()
            .filter(IndexedFile::hasFileAction)
            .mapToLong(IndexedFile::getIndex)
            .max()
            .orElse(DeltaSourceOffset.BASE_INDEX());

    // Call 2: resume from lastIndex1, should get the next file (not stall)
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Call2 =
        getDsv1FileChanges(
            deltaLog,
            tablePath,
            latestVersion,
            lastIndex1,
            /* isInitialSnapshot= */ true,
            maxFiles,
            /* maxBytes= */ Optional.empty());
    List<IndexedFile> dsv2Call2 =
        getDsv2FileChanges(
            tablePath,
            latestVersion,
            lastIndex1,
            /* isInitialSnapshot= */ true,
            maxFiles,
            /* maxBytes= */ Optional.empty());
    assertCDCFileChangesMatch(dsv1Call2, dsv2Call2);
    long dsv2DataCount2 = dsv2Call2.stream().filter(IndexedFile::hasFileAction).count();
    assertTrue(dsv2DataCount2 > 0, "Call 2 should return new data files (not stall)");
  }

  /**
   * Helper: get DSv1 CDC file changes for given offset. Creates a fresh {@link
   * DeltaSource.AdmissionLimits} internally so the stateful budget is not shared across calls.
   */
  private List<org.apache.spark.sql.delta.sources.IndexedFile> getDsv1FileChanges(
      DeltaLog deltaLog,
      String tablePath,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<Integer> maxFiles,
      Optional<Long> maxBytes)
      throws Exception {
    scala.collection.immutable.Map<String, String> cdcOptionsMap =
        Map$.MODULE$.<String, String>empty().updated("readChangeFeed", "true");
    DeltaOptions cdcOptions = new DeltaOptions(cdcOptionsMap, spark.sessionState().conf());
    DeltaSource deltaSource = createDeltaSource(deltaLog, tablePath, cdcOptions);
    List<org.apache.spark.sql.delta.sources.IndexedFile> files = new ArrayList<>();
    try (ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> iter =
        deltaSource.getFileChangesWithRateLimit(
            fromVersion,
            fromIndex,
            isInitialSnapshot,
            ScalaUtils.toScalaOption(createAdmissionLimits(maxFiles, maxBytes)))) {
      while (iter.hasNext()) files.add(iter.next());
    }
    return files;
  }

  /**
   * Helper: get DSv2 CDC file changes for given offset. Creates a fresh {@link
   * DeltaSource.AdmissionLimits} internally so the stateful budget is not shared across calls.
   */
  private List<IndexedFile> getDsv2FileChanges(
      String tablePath,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<Integer> maxFiles,
      Optional<Long> maxBytes)
      throws Exception {
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    List<IndexedFile> files = new ArrayList<>();
    try (CloseableIterator<IndexedFile> iter =
        stream.getFileChangesWithRateLimitForCDC(
            fromVersion,
            fromIndex,
            isInitialSnapshot,
            createAdmissionLimits(maxFiles, maxBytes),
            Optional.empty())) {
      while (iter.hasNext()) files.add(iter.next());
    }
    return files;
  }

  private static void sql(String query, Object... args) {
    DeltaV2TestBase.spark.sql(String.format(query, args));
  }

  private DeltaOptions emptyDeltaOptions() {
    return new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
  }

  private void createCDFEnabledTable(String tablePath, String tableName) {
    createEmptyTestTable(tablePath, tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')", tableName);
  }

  private Optional<DeltaSource.AdmissionLimits> createAdmissionLimits(
      Optional<Integer> maxFiles, Optional<Long> maxBytes) {
    Option<Object> scalaMaxFiles = ScalaUtils.toScalaOption(maxFiles.map(i -> (Object) i));
    Option<Object> scalaMaxBytes = ScalaUtils.toScalaOption(maxBytes.map(l -> (Object) l));

    if (scalaMaxFiles.isEmpty() && scalaMaxBytes.isEmpty()) {
      return Optional.empty();
    }
    DeltaOptions options = emptyDeltaOptions();
    return Optional.of(new DeltaSource.AdmissionLimits(options, scalaMaxFiles, scalaMaxBytes));
  }

  private DeltaSource createDeltaSource(DeltaLog deltaLog, String tablePath, DeltaOptions options) {
    Seq<Expression> emptySeq = JavaConverters.asScalaBuffer(new ArrayList<Expression>()).toList();
    Snapshot snapshot =
        deltaLog.update(
            /* isForce= */ false, /* timestamp= */ Option.empty(), /* version= */ Option.empty());
    return new DeltaSource(
        spark,
        deltaLog,
        /* catalogTableOpt= */ Option.empty(),
        options,
        /* snapshotAtSourceInit= */ snapshot,
        /* metadataPath= */ tablePath + "/_checkpoint",
        /* metadataTrackingLog= */ Option.empty(),
        /* filters= */ emptySeq);
  }

  private SparkMicroBatchStream createTestStreamWithDefaults(
      PathBasedSnapshotManager snapshotManager, Configuration hadoopConf, DeltaOptions options) {
    io.delta.kernel.Snapshot snapshot = snapshotManager.loadLatestSnapshot();
    StructType tableSchema =
        io.delta.spark.internal.v2.utils.SchemaUtils.convertKernelSchemaToSparkSchema(
            snapshot.getSchema());
    return new SparkMicroBatchStream(
        snapshotManager,
        snapshot,
        hadoopConf,
        spark,
        options,
        /* tablePath= */ "",
        /* dataSchema= */ tableSchema,
        /* partitionSchema= */ new StructType(),
        /* readDataSchema= */ new StructType(),
        /* ddlOrderedReadOutputSchema= */ new StructType(),
        /* dataFilters= */ new org.apache.spark.sql.sources.Filter[0],
        /* scalaOptions= */ scala.collection.immutable.Map$.MODULE$.empty());
  }

  private SparkMicroBatchStream createStream(String tablePath) {
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    return createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
  }

  private CommitActions getCommitActions(String tablePath, long version) {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);

    CommitRange commitRange =
        snapshotManager.getTableChanges(engine, version, Optional.of(version));

    try (CloseableIterator<CommitActions> iter =
        StreamingHelper.getCommitActionsFromRangeUnsafe(
            engine, (CommitRangeImpl) commitRange, tablePath, CDC_ACTION_SET)) {
      assertTrue(iter.hasNext(), "Expected at least one commit at version " + version);
      return iter.next();
    } catch (Exception e) {
      throw new RuntimeException("Failed to get commit actions at version " + version, e);
    }
  }

  private static List<IndexedFile> collectAll(CloseableIterator<IndexedFile> iter) {
    List<IndexedFile> result = new ArrayList<>();
    while (iter.hasNext()) {
      result.add(iter.next());
    }
    return result;
  }

  /**
   * Writes a synthetic delta log commit that simulates a copy-on-write no-op MERGE: the commit has
   * AddFile + RemoveFile actions (file rewrite) but CommitInfo metrics show zero rows changed.
   *
   * <p>We write this directly because Spark optimizes away truly no-op MERGEs (skips the commit
   * when no data changes).
   */
  private void writeSyntheticNoopMergeCommit(String tablePath, long version) throws Exception {
    String logDir = tablePath + "/_delta_log";
    String filename = String.format("%020d.json", version);
    java.nio.file.Path commitFile = java.nio.file.Paths.get(logDir, filename);

    String commitInfo =
        "{\"commitInfo\":{"
            + "\"timestamp\":"
            + System.currentTimeMillis()
            + ","
            + "\"operation\":\"MERGE\","
            + "\"operationParameters\":{},"
            + "\"isBlindAppend\":false,"
            + "\"operationMetrics\":{"
            + "\"numTargetRowsInserted\":\"0\","
            + "\"numTargetRowsUpdated\":\"0\","
            + "\"numTargetRowsDeleted\":\"0\","
            + "\"numTargetFilesAdded\":\"1\","
            + "\"numTargetFilesRemoved\":\"1\""
            + "}}}";
    String remove =
        "{\"remove\":{"
            + "\"path\":\"noop-merge-old.parquet\","
            + "\"deletionTimestamp\":"
            + System.currentTimeMillis()
            + ","
            + "\"dataChange\":true,"
            + "\"partitionValues\":{},"
            + "\"size\":100"
            + "}}";
    String add =
        "{\"add\":{"
            + "\"path\":\"noop-merge-new.parquet\","
            + "\"partitionValues\":{},"
            + "\"size\":100,"
            + "\"modificationTime\":"
            + System.currentTimeMillis()
            + ","
            + "\"dataChange\":true"
            + "}}";

    java.nio.file.Files.writeString(commitFile, commitInfo + "\n" + remove + "\n" + add + "\n");
  }

  /**
   * Writes a synthetic UPDATE commit with RemoveFile before AddFile (different paths) and non-zero
   * operation metrics. This produces the inferred CDC path (no AddCDCFile) with both action types
   * where the RemoveFile precedes the AddFile in the delta log.
   */
  private void writeSyntheticInferredCDCCommit(String tablePath, long version) throws Exception {
    String logDir = tablePath + "/_delta_log";
    String filename = String.format("%020d.json", version);
    java.nio.file.Path commitFile = java.nio.file.Paths.get(logDir, filename);

    long ts = System.currentTimeMillis();
    String commitInfo =
        "{\"commitInfo\":{"
            + "\"timestamp\":"
            + ts
            + ","
            + "\"operation\":\"UPDATE\","
            + "\"operationParameters\":{},"
            + "\"isBlindAppend\":false,"
            + "\"operationMetrics\":{"
            + "\"numTargetRowsUpdated\":\"1\","
            + "\"numTargetFilesAdded\":\"1\","
            + "\"numTargetFilesRemoved\":\"1\""
            + "}}}";
    // RemoveFile first, then AddFile — different paths so they are not a same-path DV pair
    String remove =
        "{\"remove\":{"
            + "\"path\":\"update-old.parquet\","
            + "\"deletionTimestamp\":"
            + ts
            + ","
            + "\"dataChange\":true,"
            + "\"partitionValues\":{},"
            + "\"size\":100"
            + "}}";
    String add =
        "{\"add\":{"
            + "\"path\":\"update-new.parquet\","
            + "\"partitionValues\":{},"
            + "\"size\":100,"
            + "\"modificationTime\":"
            + ts
            + ","
            + "\"dataChange\":true"
            + "}}";

    java.nio.file.Files.writeString(commitFile, commitInfo + "\n" + remove + "\n" + add + "\n");
  }

  /**
   * Compare CDC file changes between DSv1 and DSv2. Delegates to {@link
   * SparkMicroBatchStreamTest#compareFileChanges} for version/index/path comparison, then verifies
   * CDC metadata on DSv2 data files.
   */
  private void assertCDCFileChangesMatch(
      List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files, List<IndexedFile> dsv2Files) {
    SparkMicroBatchStreamTest.compareFileChanges(dsv1Files, dsv2Files);

    // CDC metadata (only on DSv2 data files, not sentinels)
    for (int i = 0; i < dsv2Files.size(); i++) {
      IndexedFile d2 = dsv2Files.get(i);
      if (d2.getCDCDataFile() != null) {
        assertNull(d2.getAddFile(), "CDC IndexedFile should not have a raw addFile at index " + i);
        assertEquals(
            "insert", d2.getCDCDataFile().getChangeType(), "changeType mismatch at index " + i);
        assertTrue(
            d2.getCDCDataFile().getCommitTimestamp() > 0,
            "commitTimestamp should be > 0 at index " + i);
      }
    }
  }
}
