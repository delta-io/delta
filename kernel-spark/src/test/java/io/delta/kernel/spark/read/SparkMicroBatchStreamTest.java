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
import io.delta.kernel.spark.read.IndexedFile;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
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

  private String formatIndexedFile(org.apache.spark.sql.delta.sources.IndexedFile file) {
    return String.format(
        "IndexedFile(version=%d, index=%d, hasAdd=%b)",
        file.version(), file.index(), file.add() != null);
  }

  private String formatKernelIndexedFile(IndexedFile file) {
    return String.format(
        "IndexedFile(version=%d, index=%d, hasAdd=%b)",
        file.getVersion(), file.getIndex(), file.getAddFile() != null);
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

  /**
   * Parameterized test that verifies parity between DSv1 DeltaSource.getFileChanges and DSv2
   * SparkMicroBatchStream.getFileChanges using Delta Kernel APIs.
   *
   * <p>TODO(#5319): consider adding a test similar to SparkGoldenTableTest.java.
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
    for (int i = 0; i < 5; i++) {
      StringBuilder insertValues = new StringBuilder();
      for (int j = 0; j < 100; j++) {
        if (j > 0) insertValues.append(", ");
        int id = i * 100 + j;
        insertValues.append(String.format("(%d, 'User%d')", id, id));
      }
      spark.sql(String.format("INSERT INTO %s VALUES %s", testTableName, insertValues.toString()));
    }
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath, new Configuration());

    // dsv1 DeltaSource
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.sources.DeltaSource deltaSource =
        createDeltaSource(deltaLog, testTablePath);

    scala.Option<DeltaSourceOffset> scalaEndOffset = scala.Option.empty();
    if (endVersion.isPresent()) {
      long offsetIndex = endIndex.orElse(DeltaSourceOffset.END_INDEX());
      scalaEndOffset =
          scala.Option.apply(
              new DeltaSourceOffset(
                  deltaLog.tableId(), endVersion.get(), offsetIndex, isInitialSnapshot));
    }
    ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
        deltaSource.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, scalaEndOffset);
    List<org.apache.spark.sql.delta.sources.IndexedFile> deltaFilesList = new ArrayList<>();
    while (deltaChanges.hasNext()) {
      deltaFilesList.add(deltaChanges.next());
    }
    deltaChanges.close();

    // dsv2 SparkMicroBatchStream
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
    boolean isInitialSnapshot = false;
    long BASE_INDEX = DeltaSourceOffset.BASE_INDEX();
    return Stream.of(
        // Arguments: fromVersion, fromIndex, isInitialSnapshot, endVersion, endIndex,
        // testDescription
        // Basic cases: fromIndex = BASE_INDEX, no endVersion
        Arguments.of(
            0L,
            BASE_INDEX,
            isInitialSnapshot,
            Optional.empty(),
            Optional.empty(),
            "Basic: from v0"),
        Arguments.of(
            3L,
            BASE_INDEX,
            isInitialSnapshot,
            Optional.empty(),
            Optional.empty(),
            "Basic: from v3"),

        // With fromIndex > BASE_INDEX
        Arguments.of(
            0L, 0L, isInitialSnapshot, Optional.empty(), Optional.empty(), "from: v0 id:0"),
        Arguments.of(
            1L, 5L, isInitialSnapshot, Optional.empty(), Optional.empty(), "from: v1 id:5"),

        // With endVersion
        Arguments.of(
            1L, BASE_INDEX, isInitialSnapshot, Optional.of(3L), Optional.empty(), "v1 to v3"),
        Arguments.of(
            0L, BASE_INDEX, isInitialSnapshot, Optional.of(2L), Optional.of(5L), "v0 to v2 id:5"),
        Arguments.of(
            1L, 5L, isInitialSnapshot, Optional.of(3L), Optional.of(10L), "v1 id:5 to v3 id:10"),

        // Same version start and end (narrow range within single version)
        Arguments.of(2L, 50L, isInitialSnapshot, Optional.of(2L), Optional.of(40L), "empty range"));
  }

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

  @Test
  public void testGetFileChanges_OnRemoveFile_throwError(@TempDir File tempDir) throws Exception {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_delete_error_" + System.nanoTime();
    createEmptyTestTable(testTablePath, testTableName);

    // Create initial data (version 1)
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'User1'), (2, 'User2')", testTableName));

    // Add more data (version 2)
    spark.sql(String.format("INSERT INTO %s VALUES (3, 'User3'), (4, 'User4')", testTableName));

    // Delete some data (version 3) - this creates RemoveFile actions with dataChange=true
    spark.sql(String.format("DELETE FROM %s WHERE id = 1", testTableName));

    // Try to read from version 0, which should include the REMOVE commit at version 3
    long fromVersion = 0L;
    long fromIndex = DeltaSourceOffset.BASE_INDEX();
    boolean isInitialSnapshot = false;
    scala.Option<DeltaSourceOffset> endOffset = scala.Option.empty();

    // Test DSv1 DeltaSource - should throw exception when encountering REMOVE
    DeltaLog deltaLog = DeltaLog.forTable(spark, new Path(testTablePath));
    org.apache.spark.sql.delta.sources.DeltaSource deltaSource =
        createDeltaSource(deltaLog, testTablePath);

    UnsupportedOperationException dsv1Exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              ClosableIterator<org.apache.spark.sql.delta.sources.IndexedFile> deltaChanges =
                  deltaSource.getFileChanges(
                      fromVersion,
                      fromIndex,
                      isInitialSnapshot,
                      endOffset);
              // Consume the iterator to trigger validation
              while (deltaChanges.hasNext()) {
                org.apache.spark.sql.delta.sources.IndexedFile file = deltaChanges.next();
                System.out.println(
                    String.format(
                        "  DSv1 Read: version=%d, index=%d, hasAdd=%b",
                        file.version(), file.index(), file.add() != null));
              }
              deltaChanges.close();
            });

    // Test DSv2 SparkMicroBatchStream - should throw the same exception
    SparkMicroBatchStream stream = new SparkMicroBatchStream(testTablePath, new Configuration());

    UnsupportedOperationException dsv2Exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> {
              try (CloseableIterator<IndexedFile> kernelChanges =
                  stream.getFileChanges(fromVersion, fromIndex, isInitialSnapshot, endOffset)) {
                // Consume the iterator to trigger validation
                while (kernelChanges.hasNext()) {
                  IndexedFile file = kernelChanges.next();
                  System.out.println(
                      String.format(
                          "  DSv2 Read: version=%d, index=%d, hasAdd=%b",
                          file.getVersion(), file.getIndex(), file.getAddFile() != null));
                }
              }
            });
  }
}
