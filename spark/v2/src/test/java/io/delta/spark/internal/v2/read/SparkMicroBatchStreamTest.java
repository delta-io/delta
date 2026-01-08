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
import io.delta.spark.internal.v2.SparkDsv2TestBase;
import io.delta.spark.internal.v2.snapshot.PathBasedSnapshotManager;
import io.delta.spark.internal.v2.utils.ScalaUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.ReadLimit;
import org.apache.spark.sql.delta.CheckpointInstance;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.sources.ReadMaxBytes;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.util.JsonUtils;
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

public class SparkMicroBatchStreamTest extends SparkDsv2TestBase {

  /**
   * Helper method to create a minimal SparkMicroBatchStream instance for tests that only check for
   * UnsupportedOperationException.
   */
  private SparkMicroBatchStream createTestStream(File tempDir) {
    String tablePath = tempDir.getAbsolutePath();
    String tableName = "test_unsupported_" + System.nanoTime();
    createEmptyTestTable(tablePath, tableName);
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager = new PathBasedSnapshotManager(tablePath, hadoopConf);
    return createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
  }

  private DeltaOptions emptyDeltaOptions() {
    return new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
  }

  @Test
  public void testLatestOffset_throwsUnsupportedOperationException(@TempDir File tempDir) {
    SparkMicroBatchStream microBatchStream = createTestStream(tempDir);
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> microBatchStream.latestOffset());
  }

  @Test
  public void testInitialOffset_throwsUnsupportedOperationException(@TempDir File tempDir) {
    SparkMicroBatchStream microBatchStream = createTestStream(tempDir);
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.initialOffset());
    assertEquals(
        "initialOffset with initial snapshot is not supported yet", exception.getMessage());
  }

  @Test
  public void testDeserializeOffset_ValidJson(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    SparkMicroBatchStream stream = createTestStream(tempDir);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    String tableId = deltaLog.tableId();
    DeltaSourceOffset expected = new DeltaSourceOffset(tableId, 5L, 10L, false);
    String json = org.apache.spark.sql.delta.util.JsonUtils.mapper().writeValueAsString(expected);

    Offset result = stream.deserializeOffset(json);
    DeltaSourceOffset actual = (DeltaSourceOffset) result;

    assertEquals(expected.reservoirId(), actual.reservoirId());
    assertEquals(expected.reservoirVersion(), actual.reservoirVersion());
    assertEquals(expected.index(), actual.index());
    assertEquals(expected.isInitialSnapshot(), actual.isInitialSnapshot());
  }

  @Test
  public void testDeserializeOffset_MismatchedTableId(@TempDir File tempDir) throws Exception {
    SparkMicroBatchStream stream = createTestStream(tempDir);

    // Create offset with wrong tableId
    String wrongTableId = "wrong-table-id";
    DeltaSourceOffset offset =
        new DeltaSourceOffset(
            wrongTableId,
            /* reservoirVersion= */ 1L,
            /* index= */ 0L,
            /* isInitialSnapshot= */ false);
    String json = JsonUtils.mapper().writeValueAsString(offset);
    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> stream.deserializeOffset(json));

    assertTrue(
        exception
            .getMessage()
            .contains("streaming query was reading from an unexpected Delta table"));
  }

  @Test
  public void testDeserializeOffset_InvalidJson(@TempDir File tempDir) {
    SparkMicroBatchStream stream = createTestStream(tempDir);
    String invalidJson = "{this is not valid json}";
    assertThrows(RuntimeException.class, () -> stream.deserializeOffset(invalidJson));
  }

  @Test
  public void testDeserializeOffset_WithInitialSnapshot(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    SparkMicroBatchStream stream = createTestStream(tempDir);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    String tableId = deltaLog.tableId();
    long baseIndex = DeltaSourceOffset.BASE_INDEX();
    DeltaSourceOffset expected =
        new DeltaSourceOffset(
            tableId, /* reservoirVersion= */ 0L, baseIndex, /* isInitialSnapshot= */ true);
    String json = org.apache.spark.sql.delta.util.JsonUtils.mapper().writeValueAsString(expected);

    Offset result = stream.deserializeOffset(json);
    DeltaSourceOffset actual = (DeltaSourceOffset) result;

    assertTrue(actual.isInitialSnapshot());
    assertEquals(0L, actual.reservoirVersion());
    assertEquals(baseIndex, actual.index());
  }

  @Test
  public void testCommit_NoOp(@TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    SparkMicroBatchStream stream = createTestStream(tempDir);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(tablePath));
    String tableId = deltaLog.tableId();
    DeltaSourceOffset offset = new DeltaSourceOffset(tableId, 1L, 0L, false);

    assertDoesNotThrow(() -> stream.commit(offset));
  }

  @Test
  public void testStop_NoOp(@TempDir File tempDir) {
    SparkMicroBatchStream stream = createTestStream(tempDir);
    assertDoesNotThrow(() -> stream.stop());
  }

  // ================================================================================================
  // Tests for initialOffset parity between DSv1 and DSv2
  // ================================================================================================

  @ParameterizedTest
  @MethodSource("initialOffsetParameters")
  public void testInitialOffset_firstBatchParity(
      String startingVersion,
      ReadLimitConfig limitConfig,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_initial_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 10,
        /* includeEmptyVersion= */ false);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    ReadLimit readLimit = limitConfig.toReadLimit();
    DeltaOptions options;
    if (startingVersion == null) {
      options = emptyDeltaOptions();
    } else {
      scala.collection.immutable.Map<String, String> scalaMap =
          Map$.MODULE$.<String, String>empty().updated("startingVersion", startingVersion);
      options = new DeltaOptions(scalaMap, spark.sessionState().conf());
    }

    // DSv1
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath, options);
    // DSv1 sources don't have an initialOffset() method.
    // Batch 0 is called with startOffset=null.
    Offset dsv1Offset = deltaSource.latestOffset(/* startOffset= */ null, readLimit);

    // DSv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, options);
    Offset initialOffset = stream.initialOffset();
    Offset dsv2Offset = stream.latestOffset(initialOffset, readLimit);

    compareOffsets(dsv1Offset, dsv2Offset, testDescription);
  }

  /** Provides test parameters for the initialOffset parity test. */
  private static Stream<Arguments> initialOffsetParameters() {
    return Stream.of(
        Arguments.of("0", ReadLimitConfig.noLimit(), "NoLimit1"),
        Arguments.of("1", ReadLimitConfig.noLimit(), "NoLimit2"),
        Arguments.of("3", ReadLimitConfig.noLimit(), "NoLimit3"),
        Arguments.of("latest", ReadLimitConfig.noLimit(), "LatestNoLimit"),
        Arguments.of("latest", ReadLimitConfig.maxFiles(1000), "LatestMaxFiles"),
        Arguments.of("latest", ReadLimitConfig.maxBytes(1000), "LatestMaxBytes"),
        Arguments.of("0", ReadLimitConfig.maxFiles(5), "MaxFiles1"),
        Arguments.of("1", ReadLimitConfig.maxFiles(10), "MaxFiles2"),
        Arguments.of("0", ReadLimitConfig.maxBytes(1000), "MaxBytes1"),
        Arguments.of("1", ReadLimitConfig.maxBytes(2000), "MaxBytes2"));
  }

  // ================================================================================================
  // Tests for getFileChanges parity between DSv1 and DSv2
  // ================================================================================================

  /**
   * Parameterized test that verifies parity between DSv1 DeltaSource.getFileChanges and DSv2
   * SparkMicroBatchStream.getFileChanges using Delta Kernel APIs.
   *
   * <p>TODO(#5319): consider adding a test similar to SparkGoldenTableTest.java.
   *
   * <p>TODO(#5318): add tests for ccv2 tables once we fully support them.
   */
  @ParameterizedTest
  @MethodSource("getFileChangesParameters")
  public void testGetFileChanges(
      long fromVersion,
      long fromIndex,
      boolean isInitialSnapshot,
      Optional<Long> endVersion,
      Optional<Long> endIndex,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    // Use unique table name per test instance to avoid conflicts
    String testTableName =
        "test_file_changes_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions of data (versions 1-5, version 0 is the CREATE TABLE)
    // Insert 100 rows per commit to potentially trigger multiple batches
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 100,
        /* includeEmptyVersion= */ false);

    // dsv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);

    Option<DeltaSourceOffset> scalaEndOffset = Option.empty();
    if (endVersion.isPresent()) {
      long offsetIndex = endIndex.orElse(DeltaSourceOffset.END_INDEX());
      scalaEndOffset =
          Option.apply(
              new DeltaSourceOffset(
                  deltaLog.tableId(), endVersion.get(), offsetIndex, isInitialSnapshot));
    }
    ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
        deltaSource.getFileChanges(
            fromVersion,
            fromIndex,
            isInitialSnapshot,
            scalaEndOffset,
            /* verifyMetadataAction= */ true);
    List<org.apache.spark.sql.delta.sources.IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // dsv2 SparkMicroBatchStream
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    Optional<DeltaSourceOffset> endOffsetOption = ScalaUtils.toJavaOptional(scalaEndOffset);
    try (CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, endOffsetOption)) {
      List<IndexedFile> kernelFilesList = new ArrayList<>();
      while (kernelChanges.hasNext()) {
        kernelFilesList.add(kernelChanges.next());
      }
      compareFileChanges(deltaFilesList, kernelFilesList);
    }
  }

  /** Provides test parameters for the parameterized getFileChanges test. */
  private static Stream<Arguments> getFileChangesParameters() {
    boolean notInitialSnapshot = false;
    long BASE_INDEX = DeltaSourceOffset.BASE_INDEX();
    long END_INDEX = DeltaSourceOffset.END_INDEX();
    Optional<Long> noEndVersion = Optional.empty();
    Optional<Long> noEndIndex = Optional.empty();

    // Arguments: (fromVersion, fromIndex, isInitialSnapshot, endVersion, endIndex, testDescription)
    return Stream.of(
        // With FromVersion: start with BASE_INDEX, no endVersion
        Arguments.of(
            0L, BASE_INDEX, notInitialSnapshot, noEndVersion, noEndIndex, "With FromVersion 1"),
        Arguments.of(
            3L, BASE_INDEX, notInitialSnapshot, noEndVersion, noEndIndex, "With FromVersion 2"),

        // With FromIndex: start with specific fromIndex, no endVersion
        Arguments.of(0L, 0L, notInitialSnapshot, noEndVersion, noEndIndex, "With FromIndex 1"),
        Arguments.of(1L, 5L, notInitialSnapshot, noEndVersion, noEndIndex, "With FromIndex 2"),

        // With EndVersion
        Arguments.of(
            1L, BASE_INDEX, notInitialSnapshot, Optional.of(3L), noEndIndex, "With EndVersion 1"),
        Arguments.of(
            1L,
            BASE_INDEX,
            notInitialSnapshot,
            Optional.of(2L),
            Optional.of(5L),
            "With EndVersion 2"),
        Arguments.of(
            1L,
            5L,
            notInitialSnapshot,
            Optional.of(3L),
            Optional.of(END_INDEX),
            "With EndVersion 3"),
        Arguments.of(
            1L,
            END_INDEX,
            notInitialSnapshot,
            Optional.of(2L),
            Optional.of(END_INDEX),
            "With EndVersion 4"),

        // Empty Range
        Arguments.of(
            2L, 50L, notInitialSnapshot, Optional.of(2L), Optional.of(40L), "Empty Range"));
  }

  // ================================================================================================
  // Tests for getFileChangesWithRateLimit parity between DSv1 and DSv2
  // ================================================================================================

  /**
   * Test that verifies parity between DSv1 DeltaSource.getFileChangesWithRateLimit and DSv2
   * SparkMicroBatchStream.getFileChangesWithRateLimit.
   *
   * <p>TODO(#5318): test initial snapshot once we fully support it.
   */
  @ParameterizedTest
  @MethodSource("getFileChangesWithRateLimitParameters")
  public void testGetFileChangesWithRateLimit(
      Optional<Integer> maxFiles,
      Optional<Long> maxBytes,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_rate_limit_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions with 10 rows each (versions 1-5)
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 10,
        /* includeEmptyVersion= */ false);

    // dsv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);
    DeltaOptions options = emptyDeltaOptions();

    Optional<DeltaSource.AdmissionLimits> dsv1Limits =
        createAdmissionLimits(deltaSource, maxFiles, maxBytes);

    ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
        deltaSource.getFileChangesWithRateLimit(
            /* fromVersion= */ 0L,
            /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            ScalaUtils.toScalaOption(dsv1Limits));
    List<org.apache.spark.sql.delta.sources.IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // dsv2 SparkMicroBatchStream
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    // We need a separate AdmissionLimits object for DSv2 because the method is stateful.
    Optional<DeltaSource.AdmissionLimits> dsv2Limits =
        createAdmissionLimits(deltaSource, maxFiles, maxBytes);

    try (CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChangesWithRateLimit(
            /* fromVersion= */ 0L,
            /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            dsv2Limits)) {
      List<IndexedFile> kernelFilesList = new ArrayList<>();
      while (kernelChanges.hasNext()) {
        kernelFilesList.add(kernelChanges.next());
      }
      compareFileChanges(deltaFilesList, kernelFilesList);
    }
  }

  /** Provides test parameters for the parameterized getFileChangesWithRateLimit test. */
  private static Stream<Arguments> getFileChangesWithRateLimitParameters() {
    Optional<Integer> noMaxFiles = Optional.empty();
    Optional<Long> noMaxBytes = Optional.empty();

    return Stream.of(
        // No rate limits
        Arguments.of(noMaxFiles, noMaxBytes, "No limits"),
        // MaxFiles only
        Arguments.of(Optional.of(5), noMaxBytes, "MaxFiles"),
        // MaxBytes only
        Arguments.of(noMaxFiles, Optional.of(5000L), "MaxBytes"),
        // Both limits
        Arguments.of(Optional.of(10), Optional.of(10000L), "MaxFiles and MaxBytes"));
  }

  private void compareFileChanges(
      List<org.apache.spark.sql.delta.sources.IndexedFile> deltaSourceFiles,
      List<IndexedFile> kernelFiles) {
    assertEquals(
        deltaSourceFiles.size(),
        kernelFiles.size(),
        String.format(
            "Number of file changes should match between dsv1 (%d) and dsv2 (%d)",
            deltaSourceFiles.size(), kernelFiles.size()));

    for (int i = 0; i < deltaSourceFiles.size(); i++) {
      org.apache.spark.sql.delta.sources.IndexedFile deltaFile = deltaSourceFiles.get(i);
      IndexedFile kernelFile = kernelFiles.get(i);

      assertEquals(
          deltaFile.version(),
          kernelFile.getVersion(),
          String.format(
              "Version mismatch at index %d: dsv1=%d, dsv2=%d",
              i, deltaFile.version(), kernelFile.getVersion()));

      assertEquals(
          deltaFile.index(),
          kernelFile.getIndex(),
          String.format(
              "Index mismatch at index %d: dsv1=%d, dsv2=%d",
              i, deltaFile.index(), kernelFile.getIndex()));

      // Sentinel files have null AddFile and null RemoveFile.
      String deltaPath = deltaFile.add() != null ? deltaFile.add().path() : null;
      String kernelPath =
          kernelFile.getAddFile() != null ? kernelFile.getAddFile().getPath() : null;

      if (deltaPath != null || kernelPath != null) {
        assertEquals(
            deltaPath,
            kernelPath,
            String.format(
                "AddFile path mismatch at index %d: dsv1=%s, dsv2=%s", i, deltaPath, kernelPath));
      }
    }
  }

  // ================================================================================================
  // Tests for commits with no data file changes
  // ================================================================================================

  /**
   * Parameterized test that verifies both DSv1 and DSv2 handle commits with no ADD or REMOVE
   * actions correctly. Such commits only contain METADATA, PROTOCOL, or other non-data changes.
   */
  @ParameterizedTest
  @MethodSource("emptyVersionScenarios")
  public void testGetFileChanges_emptyVersions(
      ScenarioSetup scenarioSetup,
      List<Long> expectedEmptyVersions,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_empty_versions_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Execute the scenario-specific setup
    scenarioSetup.setup(testTableName, tempDir);

    // Read from version 0 (start of the table) to capture all changes
    long fromVersion = 0L;
    long fromIndex = DeltaSourceOffset.BASE_INDEX();
    boolean isInitialSnapshot = false;
    Option<DeltaSourceOffset> endOffset = Option.empty();

    // Test DSv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);

    ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
        deltaSource.getFileChanges(
            fromVersion, fromIndex, isInitialSnapshot, endOffset, /* verifyMetadataAction= */ true);
    List<org.apache.spark.sql.delta.sources.IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // Test DSv2 SparkMicroBatchStream
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    try (CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChanges(
            fromVersion, fromIndex, isInitialSnapshot, ScalaUtils.toJavaOptional(endOffset))) {
      List<IndexedFile> kernelFilesList = new ArrayList<>();
      while (kernelChanges.hasNext()) {
        kernelFilesList.add(kernelChanges.next());
      }

      // Compare results
      compareFileChanges(deltaFilesList, kernelFilesList);
    }
  }

  /** Provides test scenarios with various types of empty versions (no ADD/REMOVE actions). */
  private static Stream<Arguments> emptyVersionScenarios() {
    return Stream.of(
        Arguments.of(
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
                  sql("ALTER TABLE %s SET TBLPROPERTIES ('test.property' = 'value1')", tableName);
                  sql("INSERT INTO %s VALUES (3, 'User3')", tableName);
                },
            Arrays.asList(2L),
            "Single metadata-only version"),
        Arguments.of(
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1')", tableName);
                  sql("ALTER TABLE %s SET TBLPROPERTIES ('p1' = 'v1')", tableName);
                  sql("ALTER TABLE %s SET TBLPROPERTIES ('p2' = 'v2')", tableName);
                  sql("ALTER TABLE %s SET TBLPROPERTIES ('p3' = 'v3')", tableName);
                },
            Arrays.asList(2L),
            "Multiple consecutive metadata-only versions"));
  }

  @Test
  public void testGetFileChanges_lazyLoading(@TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_lazy_loading_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 3 INSERT versions (versions 1-3 with ADD files only)
    sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", testTableName);
    sql("INSERT INTO %s VALUES (3, 'User3'), (4, 'User4')", testTableName);
    sql("INSERT INTO %s VALUES (5, 'User5'), (6, 'User6')", testTableName);

    // Version 4: DELETE operation that will create a REMOVE file
    sql("DELETE FROM %s WHERE id = 1", testTableName);

    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());

    // Get file changes from version 0 (which will include versions 1-4)
    CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChanges(
            /* fromVersion= */ 0L,
            /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* endOffset= */ Optional.empty());

    try {
      // Partially consume the iterator: only read files from versions 1-2
      // With lazy loading, this should succeed without hitting the REMOVE file error in version 4
      List<IndexedFile> partialFiles = new ArrayList<>();
      int filesRead = 0;
      int targetVersion = 2; // Only read up to version 2

      while (kernelChanges.hasNext()) {
        IndexedFile file = kernelChanges.next();
        partialFiles.add(file);
        filesRead++;

        // Stop after we've passed version 2's END sentinel
        // Each version has: BEGIN sentinel + actual files + END sentinel
        if (file.getVersion() == targetVersion
            && file.getIndex() == DeltaSourceOffset.END_INDEX()) {
          break;
        }
      }

      // If we got here, lazy loading worked - we successfully read versions 1-2 without
      // encountering the REMOVE file error in version 4
      // Version 0 (CREATE TABLE): BEGIN + 1 metadata/protocol action + END = 3 files
      // Version 1 (INSERT): BEGIN + 1 data file + END = 3 files
      // Version 2 (INSERT): BEGIN + 1 data file + END = 3 files
      // Total = 9 files
      assertEquals(9, filesRead, "Should have read exactly 9 IndexedFiles from versions 0-2");

      // Now consume the rest of the iterator - this should hit the REMOVE file in version 4
      assertThrows(
          UnsupportedOperationException.class,
          () -> {
            while (kernelChanges.hasNext()) {
              kernelChanges.next(); // This should throw when it reaches version 4's REMOVE
            }
          },
          "Should throw UnsupportedOperationException when reaching version 4 with REMOVE file");
    } finally {
      try {
        kernelChanges.close();
      } catch (Exception ignored) {
      }
    }
  }

  // ================================================================================================
  // Tests for REMOVE file handling
  // ================================================================================================

  /**
   * Parameterized test that verifies both DSv1 and DSv2 throw UnsupportedOperationException when
   * encountering REMOVE actions (from DELETE, UPDATE, MERGE operations).
   */
  @ParameterizedTest
  @MethodSource("removeFileScenarios")
  public void testGetFileChanges_onRemoveFile_throwError(
      ScenarioSetup scenarioSetup, String testDescription, @TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_remove_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Execute the scenario-specific setup (which will generate REMOVE actions)
    scenarioSetup.setup(testTableName, tempDir);

    // Try to read from version 0, which should include commits with REMOVE actions
    long fromVersion = 0L;
    long fromIndex = DeltaSourceOffset.BASE_INDEX();
    boolean isInitialSnapshot = false;
    Option<DeltaSourceOffset> endOffset = Option.empty();

    // Test DSv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);

    AtomicInteger dsv1SuccessfulCalls = new AtomicInteger(0);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
              deltaSource.getFileChanges(
                  fromVersion,
                  fromIndex,
                  isInitialSnapshot,
                  endOffset,
                  /* verifyMetadataAction= */ true);
          try {
            while (deltaChanges.hasNext()) {
              deltaChanges.next(); // Should throw when hitting REMOVE file
              dsv1SuccessfulCalls.incrementAndGet();
            }
          } finally {
            deltaChanges.close();
          }
        },
        String.format("DSv1 should throw on REMOVE for scenario: %s", testDescription));

    // Test DSv2 SparkMicroBatchStream
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());

    AtomicInteger dsv2SuccessfulCalls = new AtomicInteger(0);
    assertThrows(
        UnsupportedOperationException.class,
        () -> {
          CloseableIterator<IndexedFile> kernelChanges =
              stream.getFileChanges(
                  fromVersion, fromIndex, isInitialSnapshot, ScalaUtils.toJavaOptional(endOffset));
          try {
            while (kernelChanges.hasNext()) {
              kernelChanges.next(); // Should throw when hitting REMOVE file
              dsv2SuccessfulCalls.incrementAndGet();
            }
          } finally {
            kernelChanges.close();
          }
        },
        String.format("DSv2 should throw on REMOVE for scenario: %s", testDescription));

    // Verify both threw at the exact same point
    assertEquals(
        dsv1SuccessfulCalls.get(),
        dsv2SuccessfulCalls.get(),
        String.format(
            "DSv1 and DSv2 should throw after the same number of next() calls for scenario: %s. "
                + "DSv1=%d, DSv2=%d",
            testDescription, dsv1SuccessfulCalls.get(), dsv2SuccessfulCalls.get()));
  }

  /** Provides test scenarios that generate REMOVE actions through various DML operations. */
  private static Stream<Arguments> removeFileScenarios() {
    return Stream.of(
        // Simple DELETE scenario
        Arguments.of(
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", tableName);
                  sql("INSERT INTO %s VALUES (3, 'User3'), (4, 'User4')", tableName);
                  sql("DELETE FROM %s WHERE id = 1", tableName);
                },
            "DELETE: Simple delete"),

        // Many ADDs followed by REMOVE
        Arguments.of(
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  // Create 10 versions with ADDs (50 rows each)
                  for (int i = 0; i < 10; i++) {
                    StringBuilder values = new StringBuilder();
                    for (int j = 0; j < 50; j++) {
                      if (j > 0) values.append(", ");
                      int id = i * 50 + j;
                      values.append(String.format("(%d, 'User%d')", id, id));
                    }
                    sql("INSERT INTO %s VALUES %s", tableName, values);
                  }
                  sql("DELETE FROM %s WHERE id < 100", tableName);
                },
            "DELETE: Many ADDs (10 versions) followed by REMOVE"),

        // UPDATE scenario (generates REMOVE + ADD pairs)
        Arguments.of(
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql(
                      "INSERT INTO %s VALUES (1, 'User1'), (2, 'User2'), (3, 'User3'), (4,"
                          + " 'User4'), (5, 'User5')",
                      tableName);
                  sql("INSERT INTO %s VALUES (6, 'User6'), (7, 'User7'), (8, 'User8')", tableName);
                  sql("UPDATE %s SET name = 'UpdatedUser' WHERE id <= 3", tableName);
                },
            "UPDATE: Update multiple rows (generates REMOVE + ADD)"),

        // MERGE scenario (generates REMOVE + ADD for matched, ADD for not matched)
        Arguments.of(
            (ScenarioSetup)
                (tableName, tempDir) -> {
                  sql("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2'), (3, 'User3')", tableName);

                  // Create a source table for MERGE
                  String sourceTableName = "merge_source_" + System.nanoTime();
                  sql(
                      "CREATE TABLE %s (id INT, name STRING) USING delta LOCATION '%s'",
                      sourceTableName, tempDir.getAbsolutePath() + "_source");
                  sql("INSERT INTO %s VALUES (2, 'UpdatedUser2'), (4, 'User4')", sourceTableName);

                  // Perform MERGE operation
                  sql(
                      "MERGE INTO %s AS target USING %s AS source ON target.id = source.id WHEN"
                          + " MATCHED THEN UPDATE SET target.name = source.name WHEN NOT MATCHED"
                          + " THEN INSERT (id, name) VALUES (source.id, source.name)",
                      tableName, sourceTableName);

                  sql("DROP TABLE IF EXISTS %s", sourceTableName);
                },
            "MERGE: Matched (REMOVE+ADD) and not matched (ADD)"));
  }

  @Test
  public void testGetFileChanges_startingVersionAfterCheckpointAndLogCleanup(@TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_checkpoint_cleanup_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Insert 5 versions
    for (int i = 1; i <= 5; i++) {
      sql("INSERT INTO %s VALUES (%d, 'User%d')", testTableName, i, i);
    }

    // Create checkpoint at version 5
    DeltaLog.forTable(spark, new Path(testTablePath)).checkpoint();

    // Delete 0.json to simulate log cleanup
    Path logPath = new Path(testTablePath, "_delta_log");
    Path logFile0 = new Path(logPath, "00000000000000000000.json");
    File file0 = new File(logFile0.toUri().getPath());
    if (file0.exists()) {
      file0.delete();
    }

    // Now test with startingVersion=1
    Configuration hadoopConf = spark.sessionState().newHadoopConf();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());

    // Get file changes from version 1 onwards
    try (CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChanges(
            /* fromVersion= */ 1L,
            /* fromIndex= */ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false,
            /* endOffset= */ Optional.empty())) {

      List<IndexedFile> kernelFilesList = new ArrayList<>();
      while (kernelChanges.hasNext()) {
        kernelFilesList.add(kernelChanges.next());
      }

      // Filter to get only actual data files (addFile != null)
      long actualFileCount = kernelFilesList.stream().filter(f -> f.getAddFile() != null).count();

      // Should be able to read 5 data files from versions 1-5
      assertEquals(
          5,
          actualFileCount,
          "Should read 5 data files from versions 1-5 even though version 0 log is deleted");
    }
  }

  // ================================================================================================
  // Tests for latestOffset parity between DSv1 and DSv2
  // ================================================================================================

  /**
   * Parameterized test that verifies parity between DSv1 DeltaSource.latestOffset and DSv2
   * SparkMicroBatchStream.latestOffset.
   */
  @ParameterizedTest
  @MethodSource("latestOffsetParameters")
  public void testLatestOffset_notInitialSnapshot(
      Long startVersion,
      Long startIndex,
      ReadLimitConfig limitConfig,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_latest_offset_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 10,
        /* includeEmptyVersion= */ true);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    String tableId = deltaLog.tableId();
    Offset startOffset =
        new DeltaSourceOffset(tableId, startVersion, startIndex, /* isInitialSnapshot= */ false);
    ReadLimit readLimit = limitConfig.toReadLimit();

    // dsv1
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);
    Offset v1EndOffset = deltaSource.latestOffset(startOffset, readLimit);

    // dsv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    Offset v2EndOffset = stream.latestOffset(startOffset, readLimit);

    compareOffsets(v1EndOffset, v2EndOffset, testDescription);
  }

  /** Provides test parameters for the parameterized latestOffset test. */
  private static Stream<Arguments> latestOffsetParameters() {
    long BASE_INDEX = DeltaSourceOffset.BASE_INDEX();
    long END_INDEX = DeltaSourceOffset.END_INDEX();

    // TODO(#5318): Add tests for initial offset & latestOffset(null, ReadLimit)
    return Stream.of(
        // No limits
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.noLimit(),
            "NoLimits1"),
        Arguments.of(
            /* startVersion= */ 2L, /* startIndex= */ 5L, ReadLimitConfig.noLimit(), "NoLimits2"),
        Arguments.of(
            /* startVersion= */ 3L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.noLimit(),
            "NoLimits3"),
        Arguments.of(
            /* startVersion= */ 5L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.noLimit(),
            "NoLimits4"),

        // Max files
        Arguments.of(
            /* startVersion= */ 3L,
            /* startIndex= */ 5L,
            ReadLimitConfig.maxFiles(10),
            "MaxFiles1"),
        Arguments.of(
            /* startVersion= */ 4L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(5),
            "MaxFiles2"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.maxFiles(1),
            "MaxFiles3"),
        Arguments.of(
            /* startVersion= */ 5L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.maxFiles(1),
            "MaxFiles4"),

        // Max bytes
        Arguments.of(
            /* startVersion= */ 3L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxBytes(1000),
            "MaxBytes1"),
        Arguments.of(
            /* startVersion= */ 3L,
            /* startIndex= */ 5L,
            ReadLimitConfig.maxBytes(1000),
            "MaxBytes2"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.maxBytes(1000),
            "MaxBytes3"),
        Arguments.of(
            /* startVersion= */ 5L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.maxBytes(1000),
            "MaxBytes4"));
  }

  /**
   * Parameterized test that verifies sequential batch advancement produces identical offset
   * sequences for DSv1 and DSv2. This simulates real streaming where latestOffset is called
   * multiple times, each using the previous offset as the starting point.
   */
  @ParameterizedTest
  @MethodSource("sequentialBatchAdvancementParameters")
  public void testLatestOffset_sequentialBatchAdvancement(
      long startVersion,
      long startIndex,
      ReadLimitConfig limitConfig,
      int numIterations,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_sequential_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 10,
        /* includeEmptyVersion= */ true);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    String tableId = deltaLog.tableId();

    DeltaSourceOffset startOffset =
        new DeltaSourceOffset(tableId, startVersion, startIndex, /* isInitialSnapshot= */ false);

    // dsv1
    ReadLimit readLimit = limitConfig.toReadLimit();
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);
    List<Offset> dsv1Offsets =
        advanceOffsetSequenceDsv1(deltaSource, startOffset, numIterations, readLimit);

    // dsv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    List<Offset> dsv2Offsets =
        advanceOffsetSequenceDsv2(stream, startOffset, numIterations, readLimit);

    compareOffsetSequence(dsv1Offsets, dsv2Offsets, testDescription);
  }

  /** Provides test parameters for sequential batch advancement test. */
  private static Stream<Arguments> sequentialBatchAdvancementParameters() {
    long BASE_INDEX = DeltaSourceOffset.BASE_INDEX();
    long END_INDEX = DeltaSourceOffset.END_INDEX();

    return Stream.of(
        // No limits
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits1"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits2"),
        Arguments.of(
            /* startVersion= */ 4L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits3"),
        Arguments.of(
            /* startVersion= */ 4L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits4"),

        // Max files
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(5),
            /* numIterations= */ 5,
            "MaxFiles1"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(5),
            /* numIterations= */ 3,
            "MaxFiles2"),
        Arguments.of(
            /* startVersion= */ 4L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.maxFiles(1),
            /* numIterations= */ 10,
            "MaxFiles3"),
        // Max bytes
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxBytes(1000),
            /* numIterations= */ 3,
            "MaxBytes1"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ 5L,
            ReadLimitConfig.maxBytes(1000),
            /* numIterations= */ 3,
            "MaxBytes2"),
        Arguments.of(
            /* startVersion= */ 4L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.maxBytes(1000),
            /* numIterations= */ 3,
            "MaxBytes3"));
  }

  /**
   * Parameterized test that verifies behavior when calling latestOffset but no new data is
   * available (we're already at the latest version).
   */
  @ParameterizedTest
  @MethodSource("noNewDataAtLatestVersionParameters")
  public void testLatestOffset_noNewDataAtLatestVersion(
      long startIndex,
      Long expectedVersionOffset,
      Long expectedIndex,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_no_new_data_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 1,
        /* includeEmptyVersion= */ false);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    String tableId = deltaLog.tableId();
    long latestVersion =
        deltaLog
            .update(
                /* isForce= */ false,
                /* timestamp= */ scala.Option.empty(),
                /* version= */ scala.Option.empty())
            .version();

    DeltaSourceOffset startOffset =
        new DeltaSourceOffset(tableId, latestVersion, startIndex, /* isInitialSnapshot= */ false);
    ReadLimit readLimit = ReadLimit.allAvailable();

    // dsv1
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);
    org.apache.spark.sql.connector.read.streaming.Offset dsv1Offset =
        deltaSource.latestOffset(startOffset, readLimit);

    // dsv2
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    Offset dsv2Offset = stream.latestOffset(startOffset, readLimit);

    compareOffsets(dsv1Offset, dsv2Offset, testDescription);

    // Verify expected offset
    if (expectedVersionOffset == null) {
      assertNull(
          dsv1Offset,
          String.format(
              "Test: %s | Expected null offset but got: %s", testDescription, dsv1Offset));
    } else {
      assertNotNull(
          dsv1Offset,
          String.format("Test: %s | Expected non-null offset but got null", testDescription));
      DeltaSourceOffset dsv1DeltaOffset = (DeltaSourceOffset) dsv1Offset;
      long expectedVersion = latestVersion + expectedVersionOffset;
      assertEquals(
          expectedVersion,
          dsv1DeltaOffset.reservoirVersion(),
          String.format(
              "Test: %s | Expected version: %d, Actual version: %d",
              testDescription, expectedVersion, dsv1DeltaOffset.reservoirVersion()));
      assertEquals(
          expectedIndex,
          dsv1DeltaOffset.index(),
          String.format(
              "Test: %s | Expected index: %d, Actual index: %d",
              testDescription, expectedIndex, dsv1DeltaOffset.index()));
    }
  }

  /** Provides test parameters for no new data at latest version test. */
  private static Stream<Arguments> noNewDataAtLatestVersionParameters() {
    long BASE_INDEX = DeltaSourceOffset.BASE_INDEX();
    long END_INDEX = DeltaSourceOffset.END_INDEX();

    // Arguments: (startIndex, expectedVersionOffset, expectedIndex, testDescription)
    // expectedVersionOffset is relative to latestVersion (null means expect null offset)
    return Stream.of(
        Arguments.of(BASE_INDEX, 1L, BASE_INDEX, "Latest version BASE_INDEX, no new data"),
        Arguments.of(END_INDEX, 0L, END_INDEX, "Latest version END_INDEX, no new data"),
        Arguments.of(0L, 1L, BASE_INDEX, "Latest version index=0, no new data"));
  }

  // ================================================================================================
  // Tests for availableNow parity between DSv1 and DSv2
  // ================================================================================================

  @ParameterizedTest
  @MethodSource("availableNowParameters")
  public void testAvailableNow_SequentialBatchAdvancement(
      Long startVersion,
      Long startIndex,
      ReadLimitConfig limitConfig,
      int numIterations,
      String testDescription,
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_availableNow_sequential"
            + Math.abs(testDescription.hashCode())
            + "_"
            + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 10,
        /* includeEmptyVersion= */ true);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    String tableId = deltaLog.tableId();

    DeltaSourceOffset startOffset =
        new DeltaSourceOffset(tableId, startVersion, startIndex, /* isInitialSnapshot= */ false);
    ReadLimit readLimit = limitConfig.toReadLimit();

    // dsv1 source
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);
    // Enable availableNow
    deltaSource.prepareForTriggerAvailableNow();
    // Advance through multiple batches using dsv1, collecting offset after each batch
    List<Offset> dsv1Offsets =
        advanceOffsetSequenceDsv1(deltaSource, startOffset, numIterations, readLimit);

    // dsv2 source
    Configuration hadoopConf = new Configuration();
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, hadoopConf);
    SparkMicroBatchStream stream =
        createTestStreamWithDefaults(snapshotManager, hadoopConf, emptyDeltaOptions());
    // Enable availableNow
    stream.prepareForTriggerAvailableNow();
    // Advance through multiple batches using dsv2, collecting offset after each batch
    List<Offset> dsv2Offsets =
        advanceOffsetSequenceDsv2(stream, startOffset, numIterations, readLimit);

    // Ensure dsv1 and dsv2 produce identical offset sequences
    compareOffsetSequence(dsv1Offsets, dsv2Offsets, testDescription);
  }

  private static Stream<Arguments> availableNowParameters() {
    long BASE_INDEX = DeltaSourceOffset.BASE_INDEX();
    long END_INDEX = DeltaSourceOffset.END_INDEX();

    return Stream.of(
        // No limits respects availableNow
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits1"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits2"),
        Arguments.of(
            /* startVersion= */ 4L,
            /* startIndex= */ END_INDEX,
            ReadLimitConfig.noLimit(),
            /* numIterations= */ 3,
            "NoLimits3"),

        // Max files respects availableNow
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(1),
            /* numIterations= */ 10,
            "MaxFiles1"),
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(1000),
            /* numIterations= */ 3,
            "MaxFiles2"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(2),
            /* numIterations= */ 10,
            "MaxFiles3"),
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxFiles(0),
            /* numIterations= */ 3,
            "MaxFiles4"),

        // Max bytes respects availableNow
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxBytes(1),
            /* numIterations= */ 100,
            "MaxBytes1"),
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxBytes(1000000), // ensure larger than total file size
            /* numIterations= */ 3,
            "MaxBytes2"),
        Arguments.of(
            /* startVersion= */ 1L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxBytes(1000),
            /* numIterations= */ 100,
            "MaxBytes3"),
        Arguments.of(
            /* startVersion= */ 0L,
            /* startIndex= */ BASE_INDEX,
            ReadLimitConfig.maxBytes(0),
            /* numIterations= */ 3,
            "MaxBytes4"));
  }

  // ================================================================================================
  // Tests for planInputPartitions
  // ================================================================================================

  @ParameterizedTest
  @MethodSource("planInputPartitionsParameters")
  public void testPlanInputPartitions_dataParity(
      long fromVersion,
      long toVersion,
      Optional<Integer> maxFiles,
      Optional<Long> maxBytes,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_plan_partitions_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 10,
        /* includeEmptyVersion= */ true);

    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSourceOffset startOffset =
        new DeltaSourceOffset(
            deltaLog.tableId(),
            fromVersion,
            DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot= */ false);
    DeltaSourceOffset planPartitionsEndOffset =
        new DeltaSourceOffset(
            deltaLog.tableId(),
            toVersion,
            DeltaSourceOffset.END_INDEX(),
            /* isInitialSnapshot= */ false);

    // Ground truth: Read directly from Delta table
    List<Row> expectedRows = new ArrayList<>();
    Dataset<Row> toVersionData =
        spark.read().format("delta").option("versionAsOf", toVersion).load(testTablePath);
    if (fromVersion > 0) {
      Dataset<Row> beforeFromVersionData =
          spark.read().format("delta").option("versionAsOf", fromVersion - 1).load(testTablePath);
      toVersionData = toVersionData.except(beforeFromVersionData);
    }
    expectedRows.addAll(toVersionData.collectAsList());

    // DSv2: planInputPartitions + createReaderFactory
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, spark.sessionState().newHadoopConf());

    org.apache.spark.sql.delta.Snapshot deltaSnapshot = deltaLog.unsafeVolatileSnapshot();
    StructType dataSchema = deltaSnapshot.metadata().schema();
    StructType partitionSchema = deltaSnapshot.metadata().partitionSchema();

    SparkMicroBatchStream stream =
        new SparkMicroBatchStream(
            snapshotManager,
            snapshotManager.loadLatestSnapshot(),
            spark.sessionState().newHadoopConf(),
            spark,
            emptyDeltaOptions(),
            testTablePath,
            dataSchema,
            partitionSchema,
            dataSchema,
            new org.apache.spark.sql.sources.Filter[0],
            Map$.MODULE$.empty());

    InputPartition[] partitions = stream.planInputPartitions(startOffset, planPartitionsEndOffset);
    PartitionReaderFactory readerFactory = stream.createReaderFactory();

    // Simulates how Spark calls the reader factory and reads the data
    List<Row> dsv2Rows = new ArrayList<>();
    for (InputPartition partition : partitions) {
      if (readerFactory.supportColumnarReads(partition)) {
        PartitionReader<org.apache.spark.sql.vectorized.ColumnarBatch> reader =
            readerFactory.createColumnarReader(partition);
        while (reader.next()) {
          org.apache.spark.sql.vectorized.ColumnarBatch batch = reader.get();
          // Convert ColumnarBatch to Rows
          org.apache.spark.sql.catalyst.expressions.UnsafeProjection projection =
              org.apache.spark.sql.catalyst.expressions.UnsafeProjection.create(dataSchema);
          for (int rowId = 0; rowId < batch.numRows(); rowId++) {
            InternalRow internalRow = batch.getRow(rowId);
            Row row = convertInternalRowToRow(internalRow, dataSchema);
            dsv2Rows.add(row);
          }
        }
        reader.close();
      } else {
        PartitionReader<InternalRow> reader = readerFactory.createReader(partition);
        while (reader.next()) {
          InternalRow internalRow = reader.get();
          // Convert InternalRow to Row for comparison using the dataSchema we already have
          Row row = convertInternalRowToRow(internalRow, dataSchema);
          dsv2Rows.add(row);
        }
        reader.close();
      }
    }

    // Compare results
    compareDataResults(expectedRows, dsv2Rows, testDescription);
  }

  /** Provides test parameters for the planInputPartitions data parity test. */
  private static Stream<Arguments> planInputPartitionsParameters() {
    Optional<Integer> noMaxFiles = Optional.empty();
    Optional<Long> noMaxBytes = Optional.empty();

    return Stream.of(
        // Basic version range tests
        Arguments.of(
            /* fromVersion= */ 1L,
            /* toVersion= */ 2L,
            noMaxFiles,
            noMaxBytes,
            "Single version (1 to 2)"),
        Arguments.of(
            /* fromVersion= */ 1L,
            /* toVersion= */ 3L,
            noMaxFiles,
            noMaxBytes,
            "Multiple versions (1 to 3)"),
        Arguments.of(
            /* fromVersion= */ 0L,
            /* toVersion= */ 5L,
            noMaxFiles,
            noMaxBytes,
            "From version 0 to 5"),
        Arguments.of(
            /* fromVersion= */ 2L,
            /* toVersion= */ 4L,
            noMaxFiles,
            noMaxBytes,
            "Mid-range versions (2 to 4)"),

        // Rate limiting tests
        Arguments.of(
            /* fromVersion= */ 1L,
            /* toVersion= */ 5L,
            Optional.of(5),
            noMaxFiles,
            "With maxFiles limit"),
        Arguments.of(
            /* fromVersion= */ 1L,
            /* toVersion= */ 5L,
            noMaxFiles,
            Optional.of(5000L),
            "With maxBytes limit"),
        Arguments.of(
            /* fromVersion= */ 1L,
            /* toVersion= */ 5L,
            Optional.of(10),
            Optional.of(10000L),
            "With both limits"),

        // Edge cases
        Arguments.of(
            /* fromVersion= */ 3L,
            /* toVersion= */ 3L,
            noMaxFiles,
            noMaxBytes,
            "Same version (3 to 3)"));
  }

  /**
   * Helper method to convert InternalRow to Row for comparison.
   *
   * @param internalRow The InternalRow to convert
   * @param schema The schema of the row
   * @return A Row object
   */
  private Row convertInternalRowToRow(InternalRow internalRow, StructType schema) {
    // Use Spark's built-in conversion from InternalRow to Row
    scala.collection.Seq<Object> seq = internalRow.toSeq(schema);
    Object[] values = scala.collection.JavaConverters.seqAsJavaList(seq).toArray();
    return new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(values, schema);
  }

  /**
   * Helper method to compare data results between expected (ground truth) and DSv2.
   *
   * @param expectedRows Rows from ground truth (batch read from Delta table)
   * @param dsv2Rows Rows from DSv2's planInputPartitions()
   * @param testDescription Description of the test case for error messages
   */
  private void compareDataResults(
      List<Row> expectedRows, List<Row> dsv2Rows, String testDescription) {
    assertEquals(
        expectedRows.size(),
        dsv2Rows.size(),
        String.format(
            "[%s] Number of rows should match: Expected=%d, DSv2=%d",
            testDescription, expectedRows.size(), dsv2Rows.size()));

    // Sort both lists for consistent comparison (order may differ due to partitioning)
    Comparator<Row> rowComparator =
        (r1, r2) -> {
          // Compare by id field (first column)
          int id1 = r1.getInt(0);
          int id2 = r2.getInt(0);
          return Integer.compare(id1, id2);
        };

    List<Row> sortedExpected =
        expectedRows.stream().sorted(rowComparator).collect(Collectors.toList());
    List<Row> sortedDsv2 = dsv2Rows.stream().sorted(rowComparator).collect(Collectors.toList());

    // Compare each row
    for (int i = 0; i < sortedExpected.size(); i++) {
      Row expectedRow = sortedExpected.get(i);
      Row dsv2Row = sortedDsv2.get(i);

      assertEquals(
          expectedRow.length(),
          dsv2Row.length(),
          String.format(
              "[%s] Row %d length mismatch: Expected=%d, DSv2=%d",
              testDescription, i, expectedRow.length(), dsv2Row.length()));

      // Compare each field
      for (int fieldIdx = 0; fieldIdx < expectedRow.length(); fieldIdx++) {
        Object expectedValue = expectedRow.get(fieldIdx);
        Object dsv2Value = dsv2Row.get(fieldIdx);

        // Convert both values to strings for comparison to handle UTF8String vs String
        String expectedStr = expectedValue == null ? null : expectedValue.toString();
        String dsv2Str = dsv2Value == null ? null : dsv2Value.toString();

        assertEquals(
            expectedStr,
            dsv2Str,
            String.format(
                "[%s] Row %d, field %d mismatch: Expected=%s, DSv2=%s",
                testDescription, i, fieldIdx, expectedStr, dsv2Str));
      }
    }
  }

  // ================================================================================================
  // Tests for getStartingVersion parity between DSv1 and DSv2
  // ================================================================================================

  /**
   * Parameterized test that verifies parity between DSv1 DeltaSource.getStartingVersion and DSv2
   * SparkMicroBatchStream.getStartingVersion.
   */
  @ParameterizedTest
  @MethodSource("getStartingVersionParameters")
  public void testGetStartingVersion(
      String startingVersion, Optional<Long> expectedVersion, @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_starting_version_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions (version 0 = CREATE TABLE, versions 1-5 = INSERTs)
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 1,
        /* includeEmptyVersion= */ false);

    testAndCompareStartingVersion(
        testTablePath, startingVersion, expectedVersion, "startingVersion=" + startingVersion);
  }

  /** Provides test parameters for the parameterized getStartingVersion test. */
  private static Stream<Arguments> getStartingVersionParameters() {
    return Stream.of(
        Arguments.of(/* startingVersion= */ "0", /* expectedVersion= */ Optional.of(0L)),
        Arguments.of(/* startingVersion= */ "1", /* expectedVersion= */ Optional.of(1L)),
        Arguments.of(/* startingVersion= */ "3", /* expectedVersion= */ Optional.of(3L)),
        Arguments.of(/* startingVersion= */ "5", /* expectedVersion= */ Optional.of(5L)),
        Arguments.of(/* startingVersion= */ "latest", /* expectedVersion= */ Optional.of(6L)),
        Arguments.of(/* startingVersion= */ null, /* expectedVersion= */ Optional.empty()));
  }

  /**
   * Test that verifies both DSv1 and DSv2 handle the case where no DeltaOptions are provided. DSv1
   * receives an empty DeltaOptions (no parameters), while DSv2 receives Optional.empty(). This
   * tests the equivalence between these two approaches.
   */
  @Test
  public void testGetStartingVersion_noOptions(@TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_no_options_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions (version 0 = CREATE TABLE, versions 1-5 = INSERTs)
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 1,
        /* includeEmptyVersion= */ false);

    // dsv1
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaOptions emptyOptions = emptyDeltaOptions();
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath, emptyOptions);
    scala.Option<Object> dsv1Result = deltaSource.getStartingVersion();

    // dsv2
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, new Configuration());
    SparkMicroBatchStream dsv2Stream =
        createTestStreamWithDefaults(snapshotManager, new Configuration(), emptyDeltaOptions());
    Optional<Long> dsv2Result = dsv2Stream.getStartingVersion();

    compareStartingVersionResults(dsv1Result, dsv2Result, Optional.empty(), "No options provided");
  }

  /** Test that verifies both DSv1 and DSv2 handle negative startingVersion values identically. */
  @Test
  public void testGetStartingVersion_negativeVersion_throwsError(@TempDir File tempDir)
      throws Exception {
    // Negative values are rejected during DeltaOptions parsing, before getStartingVersion is
    // called.
    assertThrows(IllegalArgumentException.class, () -> createDeltaOptions("-1"));
  }

  /**
   * Parameterized test that verifies both DSv1 and DSv2 handle the protocol validation behavior
   * identically with the validation flag on/off.
   *
   * <p>When protocol validation is enabled, validateProtocolAt is called and must succeed. When
   * disabled, the code immediately falls back to checkVersionExists without protocol validation.
   */
  @ParameterizedTest
  @MethodSource("protocolValidationParameters")
  public void testGetStartingVersion_protocolValidationFlag(
      boolean enableProtocolValidation,
      String startingVersion,
      String testDescription,
      @TempDir File tempDir)
      throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName =
        "test_protocol_fallback_" + Math.abs(testDescription.hashCode()) + "_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 5 versions (version 0 = CREATE TABLE, versions 1-5 = INSERTs)
    insertVersions(
        testTableName,
        /* numVersions= */ 5,
        /* rowsPerVersion= */ 1,
        /* includeEmptyVersion= */ false);

    // Test with protocol validation enabled/disabled
    String configKey = DeltaSQLConf.FAST_DROP_FEATURE_STREAMING_ALWAYS_VALIDATE_PROTOCOL().key();
    try {
      spark.conf().set(configKey, String.valueOf(enableProtocolValidation));
      testAndCompareStartingVersion(
          testTablePath,
          startingVersion,
          Optional.of(Long.parseLong(startingVersion)),
          testDescription);
    } finally {
      spark.conf().unset(configKey);
    }
  }

  /** Provides test parameters for protocol validation scenarios. */
  private static Stream<Arguments> protocolValidationParameters() {
    return Stream.of(
        Arguments.of(
            /* enableProtocolValidation= */ true,
            /* startingVersion= */ "2",
            "Protocol validation enabled"),
        Arguments.of(
            /* enableProtocolValidation= */ false,
            /* startingVersion= */ "3",
            "Protocol validation disabled"));
  }

  // TODO(#5320): Add test for unsupported table feature
  // Test case where protocol validation encounters an unsupported table feature and throws
  // (does NOT fall back to checkVersionExists). This is difficult to test reliably as it
  // requires creating a table with features that Kernel doesn't support, which Spark SQL
  // validates upfront. This scenario is tested through integration tests.

  /**
   * Test case where protocol validation fails with a non-feature exception (snapshot cannot be
   * recreated), but checkVersionExists succeeds (commit logically exists).
   *
   * <p>Scenario: After creating a checkpoint at version 10, old log files 0-5 are deleted
   * (simulating log cleanup by timestamp). This makes version 7 non-recreatable (it exists between
   * the deleted logs and the checkpoint). Protocol validation fails when trying to build snapshot
   * at version 7, but checkVersionExists succeeds because the commit still logically exists.
   */
  @Test
  public void testGetStartingVersion_protocolValidationNonFeatureExceptionFallback(
      @TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_non_recreatable_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create 10 versions (version 0 = CREATE TABLE, versions 1-10 = INSERTs)
    insertVersions(
        testTableName,
        /* numVersions= */ 10,
        /* rowsPerVersion= */ 1,
        /* includeEmptyVersion= */ false);

    // Create checkpoint at version 10
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    Snapshot snapshotV10 =
        deltaLog.getSnapshotAt(
            10, Option.<CheckpointInstance>empty(), Option.<CatalogTable>empty(), false);
    deltaLog.checkpoint(snapshotV10, Option.<CatalogTable>empty());

    // Simulate log cleanup by timestamp: delete logs 0-5
    // This makes version 7 non-recreatable while allowing DeltaLog to load the latest snapshot
    Path logPath = new Path(testTablePath, "_delta_log");
    for (long version = 0; version <= 5; version++) {
      Path logFile = new Path(logPath, String.format("%020d.json", version));
      File file = new File(logFile.toUri().getPath());
      if (file.exists()) {
        file.delete();
      }
    }

    // Test with startingVersion=7 (a version that's no longer recreatable but logically exists)
    String startingVersion = "7";

    // dsv1
    DeltaLog freshDeltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource =
        createDeltaSource(freshDeltaLog, testTablePath, createDeltaOptions(startingVersion));
    scala.Option<Object> dsv1Result = deltaSource.getStartingVersion();

    // dsv2
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, new Configuration());
    SparkMicroBatchStream dsv2Stream =
        createTestStreamWithDefaults(
            snapshotManager, new Configuration(), createDeltaOptions(startingVersion));
    Optional<Long> dsv2Result = dsv2Stream.getStartingVersion();

    compareStartingVersionResults(
        dsv1Result,
        dsv2Result,
        Optional.of(Long.parseLong(startingVersion)),
        "Protocol validation fallback with non-recreatable version");
  }

  // ================================================================================================

  // Helper methods
  // ================================================================================================

  /** Functional interface for setting up test scenarios. */
  @FunctionalInterface
  interface ScenarioSetup {
    /**
     * Set up the test scenario by executing SQL statements.
     *
     * @param tableName The name of the test table
     * @param tempDir The temporary directory for this test
     */
    void setup(String tableName, File tempDir) throws Exception;
  }

  static class ReadLimitConfig {
    private final Optional<Integer> maxFiles;
    private final Optional<Long> maxBytes;

    private ReadLimitConfig(Optional<Integer> maxFiles, Optional<Long> maxBytes) {
      this.maxFiles = maxFiles;
      this.maxBytes = maxBytes;
    }

    static ReadLimitConfig noLimit() {
      return new ReadLimitConfig(Optional.empty(), Optional.empty());
    }

    static ReadLimitConfig maxFiles(int files) {
      return new ReadLimitConfig(Optional.of(files), Optional.empty());
    }

    static ReadLimitConfig maxBytes(long bytes) {
      return new ReadLimitConfig(Optional.empty(), Optional.of(bytes));
    }

    ReadLimit toReadLimit() {
      if (maxFiles.isPresent()) {
        return ReadLimit.maxFiles(maxFiles.get());
      } else if (maxBytes.isPresent()) {
        return new ReadMaxBytes(maxBytes.get());
      } else {
        return ReadLimit.allAvailable();
      }
    }
  }

  private void compareOffsets(Offset dsv1Offset, Offset dsv2Offset, String testDescription) {
    if (dsv1Offset == null && dsv2Offset == null) {
      return; // Both null is valid (no data case)
    }

    // Both should be non-null or both should be null
    if (dsv1Offset == null || dsv2Offset == null) {
      throw new AssertionError(
          String.format(
              "Offset mismatch for test '%s': DSv1=%s, DSv2=%s",
              testDescription, dsv1Offset, dsv2Offset));
    }

    DeltaSourceOffset dsv1DeltaOffset = (DeltaSourceOffset) dsv1Offset;
    DeltaSourceOffset dsv2DeltaOffset = (DeltaSourceOffset) dsv2Offset;

    assertEquals(
        dsv1DeltaOffset.reservoirVersion(),
        dsv2DeltaOffset.reservoirVersion(),
        String.format(
            "Version mismatch for test '%s': DSv1=%d, DSv2=%d",
            testDescription,
            dsv1DeltaOffset.reservoirVersion(),
            dsv2DeltaOffset.reservoirVersion()));

    assertEquals(
        dsv1DeltaOffset.index(),
        dsv2DeltaOffset.index(),
        String.format(
            "Index mismatch for test '%s': DSv1=%d, DSv2=%d",
            testDescription, dsv1DeltaOffset.index(), dsv2DeltaOffset.index()));

    assertEquals(
        dsv1DeltaOffset.isInitialSnapshot(),
        dsv2DeltaOffset.isInitialSnapshot(),
        String.format(
            "isInitialSnapshot mismatch for test '%s': DSv1=%b, DSv2=%b",
            testDescription,
            dsv1DeltaOffset.isInitialSnapshot(),
            dsv2DeltaOffset.isInitialSnapshot()));
  }

  /** Helper method to execute SQL with String.format. */
  private static void sql(String query, Object... args) {
    SparkDsv2TestBase.spark.sql(String.format(query, args));
  }

  /**
   * Helper method to insert multiple versions of data into a test table.
   *
   * @param tableName The name of the table to insert into
   * @param numVersions The number of versions (commits) to create
   * @param rowsPerVersion The number of rows to insert per version
   * @param includeEmptyVersion Whether to include an empty version (metadata-only change) at
   *     version 1
   */
  private void insertVersions(
      String tableName, int numVersions, int rowsPerVersion, boolean includeEmptyVersion) {
    for (int i = 0; i < numVersions; i++) {
      if (i == 1 && includeEmptyVersion) {
        sql("ALTER TABLE %s SET TBLPROPERTIES ('test.property' = 'value')", tableName);
      } else {
        StringBuilder values = new StringBuilder();
        for (int j = 0; j < rowsPerVersion; j++) {
          if (j > 0) values.append(", ");
          int id = i * rowsPerVersion + j;
          values.append(String.format("(%d, 'User%d')", id, id));
        }
        sql("INSERT INTO %s VALUES %s", tableName, values.toString());
      }
    }
  }

  private Optional<DeltaSource.AdmissionLimits> createAdmissionLimits(
      DeltaSource deltaSource, Optional<Integer> maxFiles, Optional<Long> maxBytes) {
    Option<Object> scalaMaxFiles = ScalaUtils.toScalaOption(maxFiles.map(i -> (Object) i));
    Option<Object> scalaMaxBytes = ScalaUtils.toScalaOption(maxBytes.map(l -> (Object) l));

    if (scalaMaxFiles.isEmpty() && scalaMaxBytes.isEmpty()) {
      return Optional.empty();
    }
    DeltaOptions options = emptyDeltaOptions();
    return Optional.of(new DeltaSource.AdmissionLimits(options, scalaMaxFiles, scalaMaxBytes));
  }

  /** Helper method to format a DSv1 IndexedFile for debugging. */
  private String formatIndexedFile(org.apache.spark.sql.delta.sources.IndexedFile file) {
    return String.format(
        "IndexedFile(version=%d, index=%d, hasAdd=%b)",
        file.version(), file.index(), file.add() != null);
  }

  /** Helper method to format a DSv2 IndexedFile for debugging. */
  private String formatKernelIndexedFile(IndexedFile file) {
    return String.format(
        "IndexedFile(version=%d, index=%d, hasAdd=%b)",
        file.getVersion(), file.getIndex(), file.getAddFile() != null);
  }

  private List<Offset> advanceOffsetSequenceDsv1(
      DeltaSource deltaSource, Offset startOffset, int numIterations, ReadLimit limit) {
    List<Offset> offsets = new ArrayList<>();
    offsets.add(startOffset);

    Offset currentOffset = startOffset;
    for (int i = 0; i < numIterations; i++) {
      Offset nextOffset = deltaSource.latestOffset(currentOffset, limit);
      offsets.add(nextOffset);
      currentOffset = nextOffset;
    }
    return offsets;
  }

  private List<Offset> advanceOffsetSequenceDsv2(
      SparkMicroBatchStream stream, Offset startOffset, int numIterations, ReadLimit limit) {
    List<Offset> offsets = new ArrayList<>();
    offsets.add(startOffset);

    Offset currentOffset = startOffset;
    for (int i = 0; i < numIterations; i++) {
      Offset nextOffset = stream.latestOffset(currentOffset, limit);
      offsets.add(nextOffset);
      currentOffset = nextOffset;
    }
    return offsets;
  }

  private void compareOffsetSequence(
      List<Offset> dsv1Offsets, List<Offset> dsv2Offsets, String testDescription) {
    assertEquals(
        dsv1Offsets.size(),
        dsv2Offsets.size(),
        String.format(
            "Offset sequence length mismatch for test '%s': DSv1=%d, DSv2=%d",
            testDescription, dsv1Offsets.size(), dsv2Offsets.size()));

    for (int i = 0; i < dsv1Offsets.size(); i++) {
      compareOffsets(
          dsv1Offsets.get(i),
          dsv2Offsets.get(i),
          String.format("%s (iteration %d)", testDescription, i));
    }
  }

  private DeltaSource createDeltaSource(DeltaLog deltaLog, String tablePath) {
    DeltaOptions options = emptyDeltaOptions();
    return createDeltaSource(deltaLog, tablePath, options);
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

  /** Helper method to create a SparkMicroBatchStream with default empty values for testing. */
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

  /** Helper method to create DeltaOptions with startingVersion for testing. */
  private DeltaOptions createDeltaOptions(String startingVersionValue) {
    if (startingVersionValue == null) {
      // Empty options
      return emptyDeltaOptions();
    } else {
      // Create Scala Map with startingVersion
      scala.collection.immutable.Map<String, String> scalaMap =
          Map$.MODULE$.<String, String>empty().updated("startingVersion", startingVersionValue);
      return new DeltaOptions(scalaMap, spark.sessionState().conf());
    }
  }

  /** Helper method to test and compare getStartingVersion results from DSv1 and DSv2. */
  private void testAndCompareStartingVersion(
      String testTablePath,
      String startingVersion,
      Optional<Long> expectedVersion,
      String testDescription)
      throws Exception {
    // DSv1: Create DeltaSource and get starting version
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource =
        createDeltaSource(deltaLog, testTablePath, createDeltaOptions(startingVersion));
    scala.Option<Object> dsv1Result = deltaSource.getStartingVersion();

    // DSv2: Create SparkMicroBatchStream and get starting version
    PathBasedSnapshotManager snapshotManager =
        new PathBasedSnapshotManager(testTablePath, new Configuration());
    SparkMicroBatchStream dsv2Stream =
        createTestStreamWithDefaults(
            snapshotManager, new Configuration(), createDeltaOptions(startingVersion));
    Optional<Long> dsv2Result = dsv2Stream.getStartingVersion();

    compareStartingVersionResults(dsv1Result, dsv2Result, expectedVersion, testDescription);
  }

  /** Helper method to compare getStartingVersion results from DSv1 and DSv2. */
  private void compareStartingVersionResults(
      scala.Option<Object> dsv1Result,
      Optional<Long> dsv2Result,
      Optional<Long> expectedVersion,
      String testDescription) {

    Optional<Long> dsv1Optional;
    if (dsv1Result.isEmpty()) {
      dsv1Optional = Optional.empty();
    } else {
      dsv1Optional = Optional.of((Long) dsv1Result.get());
    }

    assertEquals(
        dsv1Optional,
        dsv2Result,
        String.format("DSv1 and DSv2 getStartingVersion should match for %s", testDescription));

    assertEquals(
        expectedVersion,
        dsv2Result,
        String.format("DSv2 getStartingVersion should match for %s", testDescription));
  }
}
