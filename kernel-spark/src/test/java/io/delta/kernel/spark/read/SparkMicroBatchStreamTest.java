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
package io.delta.kernel.spark.read;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.spark.SparkDsv2TestBase;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.sources.DeltaSource;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
import scala.collection.immutable.Map$;

public class SparkMicroBatchStreamTest extends SparkDsv2TestBase {

  private SparkMicroBatchStream microBatchStream;

  @BeforeEach
  void setUp() {
    microBatchStream = new SparkMicroBatchStream(null, new Configuration());
  }

  @Test
  public void testLatestOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.latestOffset());
    assertEquals("latestOffset is not supported", exception.getMessage());
  }

  @Test
  public void testPlanInputPartitions_throwsUnsupportedOperationException() {
    Offset start = null;
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> microBatchStream.planInputPartitions(start, end));
    assertEquals("planInputPartitions is not supported", exception.getMessage());
  }

  @Test
  public void testCreateReaderFactory_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.createReaderFactory());
    assertEquals("createReaderFactory is not supported", exception.getMessage());
  }

  @Test
  public void testInitialOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.initialOffset());
    assertEquals("initialOffset is not supported", exception.getMessage());
  }

  @Test
  public void testDeserializeOffset_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class, () -> microBatchStream.deserializeOffset("{}"));
    assertEquals("deserializeOffset is not supported", exception.getMessage());
  }

  @Test
  public void testCommit_throwsUnsupportedOperationException() {
    Offset end = null;
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.commit(end));
    assertEquals("commit is not supported", exception.getMessage());
  }

  @Test
  public void testStop_throwsUnsupportedOperationException() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> microBatchStream.stop());
    assertEquals("stop is not supported", exception.getMessage());
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
    insertVersions(testTableName, /* numVersions= */ 5, /* rowsPerVersion= */ 100);

    // dsv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);

    scala.Option<DeltaSourceOffset> scalaEndOffset = scala.Option.empty();
    if (endVersion.isPresent()) {
      long offsetIndex = endIndex.orElse(DeltaSourceOffset.END_INDEX());
      scalaEndOffset =
          scala.Option.apply(
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
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath, new Configuration());
    Option<DeltaSourceOffset> endOffsetOption = scalaEndOffset;
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
    insertVersions(testTableName, /* numVersions= */ 5, /* rowsPerVersion= */ 10);

    // dsv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);
    DeltaOptions options = new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());

    scala.Option<Object> scalaMaxFiles =
        maxFiles.isPresent() ? scala.Option.apply(maxFiles.get()) : scala.Option.empty();
    scala.Option<Object> scalaMaxBytes =
        maxBytes.isPresent() ? scala.Option.apply(maxBytes.get()) : scala.Option.empty();
    Option<DeltaSource.AdmissionLimits> dsv1Limits =
        deltaSource.createAdmissionLimits(scalaMaxFiles, scalaMaxBytes);

    ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
        deltaSource.getFileChangesWithRateLimit(
            /*fromVersion=*/ 0L,
            /* fromIndex=*/ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot=*/ false,
            dsv1Limits);
    List<org.apache.spark.sql.delta.sources.IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // dsv2 SparkMicroBatchStream
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath, new Configuration());
    // We need a separate AdmissionLimits object for DSv2 because the method is stateful.
    scala.Option<DeltaSource.AdmissionLimits> dsv2Limits =
        deltaSource.createAdmissionLimits(scalaMaxFiles, scalaMaxBytes);

    try (CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChangesWithRateLimit(
            /*fromVersion=*/ 0L,
            /* fromIndex=*/ DeltaSourceOffset.BASE_INDEX(),
            /* isInitialSnapshot=*/ false,
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
  public void testGetFileChanges_EmptyVersions(
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
    scala.Option<DeltaSourceOffset> endOffset = scala.Option.empty();

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
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath, new Configuration());
    try (CloseableIterator<IndexedFile> kernelChanges =
        stream.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, endOffset)) {
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

  // ================================================================================================
  // Tests for REMOVE file handling
  // ================================================================================================

  /**
   * Parameterized test that verifies both DSv1 and DSv2 throw UnsupportedOperationException when
   * encountering REMOVE actions (from DELETE, UPDATE, MERGE operations).
   */
  @ParameterizedTest
  @MethodSource("removeFileScenarios")
  public void testGetFileChanges_OnRemoveFile_throwError(
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
    scala.Option<DeltaSourceOffset> endOffset = scala.Option.empty();

    // Test DSv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    DeltaSource deltaSource = createDeltaSource(deltaLog, testTablePath);

    UnsupportedOperationException dsv1Exception =
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
              // Consume the iterator to trigger validation
              while (deltaChanges.hasNext()) {
                // Exception is thrown by .next() when it encounters a REMOVE
                deltaChanges.next();
              }
              deltaChanges.close();
            },
            String.format("DSv1 should throw on REMOVE for scenario: %s", testDescription));

    // Test DSv2 SparkMicroBatchStream
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath, new Configuration());
    UnsupportedOperationException dsv2Exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              CloseableIterator<IndexedFile> kernelChanges =
                  stream.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, endOffset);
              try {
                // Consume the iterator to trigger validation (if not already triggered)
                while (kernelChanges.hasNext()) {
                  kernelChanges.next();
                }
                kernelChanges.close();
              } finally {
                // Make sure to close the iterator even if exception occurs
                if (kernelChanges != null) {
                  try {
                    kernelChanges.close();
                  } catch (Exception ignored) {
                  }
                }
              }
            },
            String.format("DSv2 should throw on REMOVE for scenario: %s", testDescription));

    // TODO(#5318): Add precise exception point verification when DSv2 implements
    // lazy loading. Currently, DSv1 uses lazy loading (throws during .next() iteration after
    // processing ADD files) while DSv2 uses eager loading (throws during getFileChanges() before
    // iteration begins). Once DSv2 implements lazy loading, both should throw at exactly the same
    // point.
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
                      "INSERT INTO %s VALUES (1, 'User1'), (2, 'User2'), (3, 'User3'), (4, 'User4'), (5, 'User5')",
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
                      "MERGE INTO %s AS target USING %s AS source ON target.id = source.id "
                          + "WHEN MATCHED THEN UPDATE SET target.name = source.name "
                          + "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name)",
                      tableName, sourceTableName);

                  sql("DROP TABLE IF EXISTS %s", sourceTableName);
                },
            "MERGE: Matched (REMOVE+ADD) and not matched (ADD)"));
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
   */
  private void insertVersions(String tableName, int numVersions, int rowsPerVersion) {
    for (int i = 0; i < numVersions; i++) {
      StringBuilder values = new StringBuilder();
      for (int j = 0; j < rowsPerVersion; j++) {
        if (j > 0) values.append(", ");
        int id = i * rowsPerVersion + j;
        values.append(String.format("(%d, 'User%d')", id, id));
      }
      sql("INSERT INTO %s VALUES %s", tableName, values.toString());
    }
  }

  /** Helper method to create a DeltaSource instance for testing. */
  private DeltaSource createDeltaSource(DeltaLog deltaLog, String tablePath) {
    DeltaOptions options = new DeltaOptions(Map$.MODULE$.empty(), spark.sessionState().conf());
    scala.collection.immutable.Seq<org.apache.spark.sql.catalyst.expressions.Expression> emptySeq =
        scala.collection.JavaConverters.asScalaBuffer(
                new java.util.ArrayList<org.apache.spark.sql.catalyst.expressions.Expression>())
            .toList();
    org.apache.spark.sql.delta.Snapshot snapshot =
        deltaLog.update(false, Option.empty(), Option.empty());
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
}
