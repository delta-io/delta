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
                  stream.getFileChangesForCDC(
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
        Arguments.of(
            "Initial snapshot (all inserts)", CDC_TWO_INSERT_SETUP, noMaxFiles, noMaxBytes),
        Arguments.of(
            "Empty table (no data files)",
            (SparkMicroBatchStreamTest.ScenarioSetup) (t, d) -> {},
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
            noMaxFiles,
            noMaxBytes),
        Arguments.of("Rate limited (maxFiles=1)", CDC_TWO_INSERT_SETUP, Optional.of(1), noMaxBytes),
        Arguments.of(
            "Rate limited (maxBytes=1)", CDC_TWO_INSERT_SETUP, noMaxFiles, Optional.of(1L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cdcFileChangesParameters")
  public void testGetFileChangesForCDC(
      String testDescription,
      SparkMicroBatchStreamTest.ScenarioSetup setup,
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

    // DSv1
    scala.collection.immutable.Map<String, String> cdcOptionsMap =
        Map$.MODULE$.<String, String>empty().updated("readChangeFeed", "true");
    DeltaOptions cdcOptions = new DeltaOptions(cdcOptionsMap, spark.sessionState().conf());
    DeltaSource deltaSource = createDeltaSource(deltaLog, tablePath, cdcOptions);
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files = new ArrayList<>();
    try (ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> iter =
        deltaSource.getFileChangesWithRateLimit(
            latestVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ true,
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
        stream.getFileChangesForCDC(
            /* fromVersion= */ latestVersion,
            /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ true,
            /* limits= */ createAdmissionLimits(maxFiles, maxBytes),
            /* endOffset= */ Optional.empty())) {
      while (iter.hasNext()) dsv2Files.add(iter.next());
    }

    assertCDCFileChangesMatch(dsv1Files, dsv2Files);
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
            commit, /* startVersion= */ commitVersion, /* endOffsetOpt= */ Optional.empty())) {
      files = collectAll(iter);
    }

    assertTrue(files.size() >= 3, "Expected at least BEGIN + data + END, got " + files.size());

    IndexedFile begin = files.get(0);
    assertEquals(commitVersion, begin.getVersion());
    assertEquals(DeltaSourceOffset.BASE_INDEX(), begin.getIndex());
    assertNull(begin.getCDCDataFile());

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
      assertNotNull(f.getCDCDataFile(), "Data file should have a CDCDataFile");
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
            commit, /* startVersion= */ commitVersion, /* endOffsetOpt= */ Optional.empty())) {
      files = collectAll(iter);
    }

    assertTrue(files.size() >= 3, "Expected at least BEGIN + data + END, got " + files.size());

    List<IndexedFile> dataFiles = new ArrayList<>();
    for (IndexedFile f : files) {
      if (f.getCDCDataFile() != null) dataFiles.add(f);
    }
    assertFalse(dataFiles.isEmpty(), "Expected at least one data file from DELETE");

    for (IndexedFile f : dataFiles) {
      assertTrue(f.isAddCDCFile(), "Data file should be a CDC file");
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
            commit, /* startVersion= */ commitVersion, /* endOffsetOpt= */ Optional.empty())) {
      files = collectAll(iter);
    }

    assertEquals(2, files.size(), "Expected only BEGIN + END sentinels for no-op merge");
    assertEquals(DeltaSourceOffset.BASE_INDEX(), files.get(0).getIndex());
    assertEquals(DeltaSourceOffset.END_INDEX(), files.get(1).getIndex());
    assertNull(files.get(0).getCDCDataFile());
    assertNull(files.get(1).getCDCDataFile());
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
            commit, /* startVersion= */ commitVersion, Optional.of(endOffset))) {
      files = collectAll(iter);
    }

    assertEquals(2, files.size(), "Expected exactly BEGIN + END sentinels");
    assertEquals(DeltaSourceOffset.BASE_INDEX(), files.get(0).getIndex());
    assertEquals(DeltaSourceOffset.END_INDEX(), files.get(1).getIndex());
    assertNull(files.get(0).getCDCDataFile());
    assertNull(files.get(1).getCDCDataFile());
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
        /* checkpointLocation= */ "",
        /* dataSchema= */ tableSchema,
        /* partitionSchema= */ new StructType(),
        /* readDataSchema= */ new StructType(),
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
