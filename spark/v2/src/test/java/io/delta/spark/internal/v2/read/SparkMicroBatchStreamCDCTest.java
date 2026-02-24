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

  /** Functional interface for setting up test scenarios (inserts, updates, etc.). */
  @FunctionalInterface
  interface ScenarioSetup {
    void setup(String tableName) throws Exception;
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

  /** Collect all DSv2 IndexedFiles from getFileChangesForCDC directly. */
  private List<IndexedFile> collectDSv2CDCFiles(
      SparkMicroBatchStream stream,
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<DeltaSource.AdmissionLimits> limits)
      throws Exception {
    List<IndexedFile> files = new ArrayList<>();
    try (CloseableIterator<IndexedFile> iter =
        stream.getFileChangesForCDC(
            fromVersion, fromIndex, isInitialSnapshot, limits, /* endOffset= */ Optional.empty())) {
      while (iter.hasNext()) {
        files.add(iter.next());
      }
    }
    return files;
  }

  /**
   * Compare CDC file changes between DSv1 and DSv2. Compares ALL IndexedFiles including BEGIN/END
   * sentinels (both implementations produce identical sentinel structure). For each file, compares
   * version, index, and AddFile path. DSv1 IndexedFile has no changeType or commitTimestamp, so
   * those are verified separately via {@link #assertCDCMetadata}.
   */
  private void compareCDCFileChanges(
      List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files, List<IndexedFile> dsv2Files) {
    assertEquals(
        dsv1Files.size(),
        dsv2Files.size(),
        String.format(
            "CDC file count mismatch (including sentinels): dsv1=%d, dsv2=%d",
            dsv1Files.size(), dsv2Files.size()));

    for (int i = 0; i < dsv1Files.size(); i++) {
      var d1 = dsv1Files.get(i);
      var d2 = dsv2Files.get(i);
      assertEquals(d1.version(), d2.getVersion(), "version mismatch at index " + i);
      assertEquals(d1.index(), d2.getIndex(), "index mismatch at index " + i);

      // Compare AddFile path (null for sentinels)
      String d1Path = d1.add() != null ? d1.add().path() : null;
      String d2Path = d2.getAddFile() != null ? d2.getAddFile().getPath() : null;
      assertEquals(d1Path, d2Path, "add path mismatch at index " + i);
    }
  }

  /**
   * Assert CDC-specific metadata on DSv2 data files. These fields don't exist on DSv1's
   * IndexedFile, so they can't be cross-compared — we verify them independently.
   */
  private void assertCDCMetadata(List<IndexedFile> dsv2Files) {
    List<IndexedFile> dataFiles =
        dsv2Files.stream().filter(IndexedFile::hasFileAction).collect(Collectors.toList());
    for (int i = 0; i < dataFiles.size(); i++) {
      IndexedFile f = dataFiles.get(i);
      assertEquals("insert", f.getChangeType(), "changeType mismatch at data file " + i);
      assertTrue(f.getCommitTimestamp() > 0, "commitTimestamp should be > 0 at data file " + i);
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
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());

    // Calling getFileChangesForCDC on a non-CDC table should throw
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
    // DeltaAnalysisException is a checked exception, wrapped in RuntimeException
    Throwable cause = exception.getCause() != null ? exception.getCause() : exception;
    String msg = cause.getMessage().toLowerCase();
    assertTrue(
        msg.contains("change data") || msg.contains("cdc"),
        "Exception should mention change data feed: " + cause.getMessage());
  }

  // ================================================================================================
  // Test 2: DSv1-vs-DSv2 comparison for getFileChangesForCDC
  // ================================================================================================

  /** Two inserts across two commits (3 rows total). Reused by multiple scenarios. */
  private static final ScenarioSetup TWO_INSERT_SETUP =
      tableName -> {
        sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
        sql("INSERT INTO %s VALUES (3, 'User3')", tableName);
      };

  // All scenarios are initial-snapshot CDC reads (cdf1 scope).
  // Parameters: (description, setup, maxFiles, maxBytes)
  static Stream<Arguments> cdcFileChangesParameters() {
    Optional<Integer> noMaxFiles = Optional.empty();
    Optional<Long> noMaxBytes = Optional.empty();

    return Stream.of(
        Arguments.of("Initial snapshot (all inserts)", TWO_INSERT_SETUP, noMaxFiles, noMaxBytes),
        Arguments.of(
            "Empty table (no data files)", (ScenarioSetup) t -> {}, noMaxFiles, noMaxBytes),
        Arguments.of(
            "Multiple inserts (5 versions)",
            (ScenarioSetup)
                t -> {
                  for (int i = 1; i <= 5; i++) {
                    sql("INSERT INTO %s VALUES (%d, 'V%d')", t, i, i);
                  }
                },
            noMaxFiles,
            noMaxBytes),
        Arguments.of("Rate limited (maxFiles=1)", TWO_INSERT_SETUP, Optional.of(1), noMaxBytes),
        Arguments.of("Rate limited (maxBytes=1)", TWO_INSERT_SETUP, noMaxFiles, Optional.of(1L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cdcFileChangesParameters")
  public void testGetFileChangesForCDC(
      String testDescription,
      ScenarioSetup setup,
      Optional<Integer> maxFiles,
      Optional<Long> maxBytes,
      @TempDir File tempDir)
      throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    String tableName =
        "test_cdc_compare_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    enableCDC(tableName);
    setup.setup(tableName);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    long latestVersion = deltaLog.update(false, Option.empty(), Option.empty()).version();

    // DSv1 — AdmissionLimits is stateful (admit() mutates), so create separate instances
    DeltaSource deltaSource = createDeltaSource(deltaLog, tablePath, createCDCDeltaOptions());
    List<org.apache.spark.sql.delta.sources.IndexedFile> dsv1Files =
        collectDSv1CDCFiles(
            deltaSource,
            latestVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ true,
            createAdmissionLimits(maxFiles, maxBytes));

    // DSv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    List<IndexedFile> dsv2Files =
        collectDSv2CDCFiles(
            stream,
            latestVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ true,
            createAdmissionLimits(maxFiles, maxBytes));

    compareCDCFileChanges(dsv1Files, dsv2Files);
    assertCDCMetadata(dsv2Files);
  }
}
