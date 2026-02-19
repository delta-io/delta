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

import io.delta.kernel.utils.CloseableIterator;
import io.delta.spark.internal.v2.DeltaV2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.Some;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map$;
import scala.collection.immutable.Seq;

/**
 * Tests for CDC (Change Data Capture) support in SparkMicroBatchStream.
 *
 * <p>Tests the getFileChangesForCDC code path, which is routed to when readChangeFeed=true in
 * DeltaOptions. Uses DSv1-vs-DSv2 comparison to verify correctness.
 */
public class SparkMicroBatchStreamCDCTest extends DeltaV2TestBase {

  // ================================================================================================
  // Helper methods
  // ================================================================================================

  /** Functional interface for setting up test scenarios. */
  @FunctionalInterface
  interface ScenarioSetup {
    void setup(String tableName, File tempDir) throws Exception;
  }

  private static void sql(String query, Object... args) {
    DeltaV2TestBase.spark.sql(String.format(query, args));
  }

  private void enableCDC(String tableName) {
    sql("ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')", tableName);
  }

  private DeltaOptions emptyDeltaOptions() {
    return new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
  }

  private DeltaOptions createCDCDeltaOptions() {
    scala.collection.immutable.Map<String, String> scalaMap =
        Map$.MODULE$.<String, String>empty().updated("readChangeFeed", "true");
    return new DeltaOptions(scalaMap, spark.sessionState().conf());
  }

  private SparkMicroBatchStream createTestStreamWithDefaults(
      PathBasedSnapshotManager snapshotManager, Configuration hadoopConf, DeltaOptions options) {
    return new SparkMicroBatchStream(
        snapshotManager,
        snapshotManager.loadLatestSnapshot(),
        hadoopConf,
        spark,
        options,
        /* tablePath= */ "",
        /* dataSchema= */ new StructType(),
        /* partitionSchema= */ new StructType(),
        /* readDataSchema= */ new StructType(),
        /* dataFilters= */ new org.apache.spark.sql.sources.Filter[0],
        /* scalaOptions= */ scala.collection.immutable.Map$.MODULE$.empty());
  }

  private DeltaSource createDeltaSource(DeltaLog deltaLog, String tablePath, DeltaOptions options) {
    Seq<Expression> emptySeq = JavaConverters.asScalaBuffer(new ArrayList<Expression>()).toList();
    Snapshot snapshot = deltaLog.update(false, Option.empty(), Option.empty());
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

  /** Convenience wrapper: create AdmissionLimits with just maxFiles. */
  private Optional<DeltaSource.AdmissionLimits> createLimits(int maxFiles) {
    Option<Object> maxFilesOpt = new Some<>((Object) maxFiles);
    Option<Object> maxBytesOpt = Option.empty();
    return Optional.of(
        new DeltaSource.AdmissionLimits(emptyDeltaOptions(), maxFilesOpt, maxBytesOpt));
  }

  /** Collect all DSv1 IndexedFiles from getFileChangesWithRateLimit. */
  private List<org.apache.spark.sql.delta.sources.IndexedFile> collectDSv1CDCFiles(
      DeltaSource deltaSource,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits)
      throws Exception {
    List<org.apache.spark.sql.delta.sources.IndexedFile> files = new ArrayList<>();
    ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> iter =
        deltaSource.getFileChangesWithRateLimit(
            fromVersion, fromIndex, isInitialSnapshot, ScalaUtils.toScalaOption(limits));
    while (iter.hasNext()) {
      files.add(iter.next());
    }
    iter.close();
    return files;
  }

  /** Collect all DSv2 IndexedFiles from getFileChangesWithRateLimit. */
  private List<IndexedFile> collectDSv2CDCFiles(
      SparkMicroBatchStream stream,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits)
      throws Exception {
    List<IndexedFile> files = new ArrayList<>();
    try (CloseableIterator<IndexedFile> iter =
        stream.getFileChangesWithRateLimit(fromVersion, fromIndex, isInitialSnapshot, limits)) {
      while (iter.hasNext()) {
        files.add(iter.next());
      }
    }
    return files;
  }

  /**
   * Compare CDC file changes between DSv1 and DSv2. Filters to data files only (files with a file
   * action) because sentinel files (BEGIN/END markers for offset tracking) may differ in count
   * between DSv1 and DSv2 when admission limits are in play. Compares version, index, shouldSkip,
   * and file action path (add/remove/cdc). DSv1 IndexedFile has no changeType or commitTimestamp,
   * so those are skipped.
   */
  private void compareCDCFileChanges(
      List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files, List<IndexedFile> dsv2Files) {
    // Filter to data files only — sentinel files (no file action) are implementation details
    // for offset tracking and may differ in count between DSv1 and DSv2.
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1DataFiles =
        dsv1Files.stream().filter(f -> f.getFileAction() != null).collect(Collectors.toList());
    List<IndexedFile> dsv2DataFiles =
        dsv2Files.stream().filter(IndexedFile::hasFileAction).collect(Collectors.toList());

    assertEquals(dsv1DataFiles.size(), dsv2DataFiles.size(), "CDC data file count mismatch");
    for (int i = 0; i < dsv1DataFiles.size(); i++) {
      var d1 = dsv1DataFiles.get(i);
      var d2 = dsv2DataFiles.get(i);
      assertEquals(d1.version(), d2.getVersion(), "version mismatch at " + i);
      assertEquals(d1.index(), d2.getIndex(), "index mismatch at " + i);
      assertEquals(d1.shouldSkip(), d2.isShouldSkip(), "shouldSkip mismatch at " + i);

      // Compare file action path
      String d1Path =
          d1.add() != null
              ? d1.add().path()
              : d1.remove() != null
                  ? d1.remove().path()
                  : d1.cdc() != null ? d1.cdc().path() : null;
      String d2Path =
          d2.getAddFile() != null
              ? d2.getAddFile().getPath()
              : d2.getRemoveFile() != null
                  ? d2.getRemoveFile().getPath()
                  : d2.getCdcFile() != null ? d2.getCdcFile().getPath() : null;
      assertEquals(d1Path, d2Path, "path mismatch at " + i);
    }
  }

  // ================================================================================================
  // Test 1: validateCDFEnabled throws when CDF is not enabled (DSv2-only)
  // ================================================================================================

  @Test
  public void testValidateCDFEnabled_throwsWhenNotEnabled(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_cdc_not_enabled_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    sql("INSERT INTO %s VALUES (1, 'User1')", tableName);

    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, createCDCDeltaOptions());

    // Calling getFileChangesWithRateLimit with readChangeFeed=true on a non-CDC table should throw
    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                collectDSv2CDCFiles(
                    stream,
                    /* fromVersion= */ 0L,
                    /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
                    /* isInitialSnapshot= */ false,
                    /* limits= */ Optional.empty()));
    // The DeltaAnalysisException is wrapped in RuntimeException
    Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
    String msg = cause.getMessage().toLowerCase();
    assertTrue(
        msg.contains("change data") || msg.contains("cdc"),
        "Exception should mention change data feed: " + cause.getMessage());
  }

  // ================================================================================================
  // Test 2: DSv1-vs-DSv2 comparison for getFileChangesForCDC
  // ================================================================================================

  static Stream<Arguments> cdcFileChangesParameters() {
    return Stream.of(
        Arguments.of(
            "Initial snapshot (all inserts)",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
                  sql("INSERT INTO %s VALUES (3, 'User3')", tableName);
                },
            /* useLatestVersionAsFrom= */ true,
            /* fromVersion= */ -1L, // ignored when useLatestVersionAsFrom=true
            /* isInitialSnapshot= */ true),
        Arguments.of(
            "Insert-only incremental (inferred CDC)",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
                  sql("INSERT INTO %s VALUES (3, 'User3')", tableName);
                },
            /* useLatestVersionAsFrom= */ false,
            /* fromVersion= */ 1L,
            /* isInitialSnapshot= */ false),
        Arguments.of(
            "Explicit CDC from UPDATE and DELETE",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2'), (3, 'User3')", tableName);
                  sql("UPDATE %s SET name = 'UpdatedUser1' WHERE id = 1", tableName);
                  sql("DELETE FROM %s WHERE id = 2", tableName);
                },
            /* useLatestVersionAsFrom= */ false,
            /* fromVersion= */ 1L,
            /* isInitialSnapshot= */ false),
        Arguments.of(
            "No-op MERGE (shouldSkip=true)",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
                  sql(
                      "MERGE INTO %s t USING (SELECT 999 AS id, 'Ghost' AS name) s "
                          + "ON t.id = s.id "
                          + "WHEN MATCHED THEN UPDATE SET t.name = s.name",
                      tableName);
                },
            /* useLatestVersionAsFrom= */ false,
            /* fromVersion= */ 1L,
            /* isInitialSnapshot= */ false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cdcFileChangesParameters")
  public void testGetFileChangesForCDC(
      String testDescription,
      ScenarioSetup setup,
      boolean useLatestVersionAsFrom,
      long fromVersion,
      boolean isInitialSnapshot,
      @TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName =
        "test_cdc_compare_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    enableCDC(tableName);
    setup.setup(tableName, tempDir);

    // Resolve fromVersion if needed
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    long resolvedFromVersion =
        useLatestVersionAsFrom
            ? deltaLog.update(false, Option.empty(), Option.empty()).version()
            : fromVersion;

    // DSv1
    DeltaSource deltaSource = createDeltaSource(deltaLog, tablePath, createCDCDeltaOptions());
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files =
        collectDSv1CDCFiles(
            deltaSource,
            resolvedFromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            isInitialSnapshot,
            Optional.empty());

    // DSv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, createCDCDeltaOptions());
    List<IndexedFile> dsv2Files =
        collectDSv2CDCFiles(
            stream,
            resolvedFromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            isInitialSnapshot,
            Optional.empty());

    // Compare
    assertFalse(dsv1Files.isEmpty(), "DSv1 should return files for: " + testDescription);
    compareCDCFileChanges(dsv1Files, dsv2Files);
  }

  // ================================================================================================
  // Test 3: DSv1-vs-DSv2 comparison for getFileChangesWithRateLimit with admission limits
  // ================================================================================================

  static Stream<Arguments> cdcRateLimitingParameters() {
    return Stream.of(
        Arguments.of(
            "Explicit CDC batch admission (maxFiles=1)",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2'), (3, 'User3')", tableName);
                  sql("UPDATE %s SET name = 'UpdatedAll' WHERE id <= 3", tableName);
                },
            /* maxFiles= */ 1),
        Arguments.of(
            "Inferred CDC per-file admission (maxFiles=2)",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
                  sql("INSERT INTO %s VALUES (3, 'User3'), (4, 'User4')", tableName);
                  sql("INSERT INTO %s VALUES (5, 'User5'), (6, 'User6')", tableName);
                },
            /* maxFiles= */ 2),
        Arguments.of(
            "Version-level termination (maxFiles=1)",
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1')", tableName);
                  sql("INSERT INTO %s VALUES (2, 'User2')", tableName);
                  sql("INSERT INTO %s VALUES (3, 'User3')", tableName);
                },
            /* maxFiles= */ 1));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cdcRateLimitingParameters")
  public void testGetFileChangesWithRateLimitForCDC(
      String testDescription, ScenarioSetup setup, int maxFiles, @TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName =
        "test_cdc_ratelimit_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    enableCDC(tableName);
    setup.setup(tableName, tempDir);

    long fromVersion = 1L; // after enableCDC ALTER TABLE
    boolean isInitialSnapshot = false;

    // DSv1
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, tablePath, createCDCDeltaOptions());
    Optional<DeltaSource.AdmissionLimits> dsv1Limits = createLimits(maxFiles);
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files =
        collectDSv1CDCFiles(
            deltaSource,
            fromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            isInitialSnapshot,
            dsv1Limits);

    // DSv2 — need fresh AdmissionLimits (stateful)
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, createCDCDeltaOptions());
    Optional<DeltaSource.AdmissionLimits> dsv2Limits = createLimits(maxFiles);
    List<IndexedFile> dsv2Files =
        collectDSv2CDCFiles(
            stream, fromVersion, DeltaSourceOffset.BASE_INDEX(), isInitialSnapshot, dsv2Limits);

    // Compare
    compareCDCFileChanges(dsv1Files, dsv2Files);
  }
}
