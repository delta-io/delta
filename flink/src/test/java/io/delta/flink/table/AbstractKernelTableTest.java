/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.flink.table;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.junit.jupiter.api.Assertions.*;

import dev.failsafe.function.CheckedConsumer;
import io.delta.flink.TestHelper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for AbstractKernelTable. */
class AbstractKernelTableTest extends TestHelper {

  /**
   * Helper method to create a test table with default configuration.
   *
   * @param schema table schema
   * @param partitionCols partition columns
   * @param callback callback invoked with the created table
   */
  private void withTestTable(
      StructType schema,
      List<String> partitionCols,
      CheckedConsumer<LocalFileSystemTable> callback) {
    withTestTable(schema, partitionCols, Collections.emptyMap(), callback);
  }

  /**
   * Helper method to create a test table with custom configuration.
   *
   * @param schema table schema
   * @param partitionCols partition columns
   * @param tableConfig table configuration
   * @param callback callback invoked with the created table
   */
  private void withTestTable(
      StructType schema,
      List<String> partitionCols,
      Map<String, String> tableConfig,
      CheckedConsumer<LocalFileSystemTable> callback) {
    withTempDir(
        dir -> {
          LocalFileSystemTable table =
              new LocalFileSystemTable(dir.toURI(), tableConfig, schema, partitionCols);
          table.open();

          callback.accept(table);
        });
  }

  @Test
  void testNormalizeURI() {
    assertEquals(
        "file:///var/char/good/",
        AbstractKernelTable.normalize(URI.create("file:/var/char/good")).toString());
    assertEquals(
        "file:///var/char/good/",
        AbstractKernelTable.normalize(URI.create("/var/char/good")).toString());
    assertEquals(
        "file:///var/char/good/",
        AbstractKernelTable.normalize(URI.create("file:///var/char/good")).toString());
    assertEquals(
        "s3://host/var/", AbstractKernelTable.normalize(URI.create("s3://host/var")).toString());
  }

  @Test
  void testTableIsSerializable() throws Exception {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);

    withTestTable(
        schema,
        Collections.emptyList(),
        table -> {
          byte[] serialized = InstantiationUtil.serializeObject(table);
          AbstractKernelTable copy =
              InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
          assertNotNull(copy);
        });
  }

  @Test
  void testTableStoredConfIntoDeltaLogs() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);

    Map<String, String> tableConfig = new HashMap<>();
    tableConfig.put("delta.enableDeletionVectors", "true");
    tableConfig.put("showme", "themoney");
    tableConfig.put("something", "fornothing");

    withTestTable(
        schema,
        Collections.emptyList(),
        tableConfig,
        table -> {
          table.commit(
              CloseableIterable.inMemoryIterable(
                  singletonCloseableIterator(dummyAddFileRow(schema, 1, Collections.emptyMap()))),
              "app",
              100L,
              Collections.emptyMap());

          Snapshot snapshot = table.snapshot().get();
          assertEquals("true", snapshot.getTableProperties().get("delta.enableDeletionVectors"));
          assertFalse(snapshot.getTableProperties().containsKey("showme"));
          assertFalse(snapshot.getTableProperties().containsKey("something"));
          assertTrue(
              ((SnapshotImpl) snapshot).getProtocol().getWriterFeatures().contains("v2Checkpoint"));
        });
  }

  @Test
  void testCommitToEmptyTableWithoutPartition() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

    withTestTable(
        schema,
        Collections.emptyList(),
        table -> {
          List<Row> actions =
              IntStream.range(0, 5)
                  .mapToObj(
                      i ->
                          dummyAddFileRow(
                              schema, 10 + i, Map.of("part", Literal.ofString("p" + i))))
                  .collect(Collectors.toList());

          CloseableIterable<Row> dataActions =
              new CloseableIterable<Row>() {
                @Override
                public CloseableIterator<Row> iterator() {
                  return Utils.toCloseableIterator(actions.iterator());
                }

                @Override
                public void close() {
                  // Nothing to close
                }
              };

          table.commit(dataActions, "a", 100, Collections.emptyMap());

          // The target table should have one version
          verifyTableContent(
              table.getTablePath().toString(),
              (version, addFiles, properties) -> {
                assertEquals(1L, version);
                // There should be 5 files to scan
                List<AddFile> actionsList = new ArrayList<>();
                addFiles.forEach(actionsList::add);
                assertEquals(5, actionsList.size());
                long sum = actionsList.stream().mapToLong(af -> af.getNumRecords().get()).sum();
                assertEquals(60, sum);
              });
        });
  }

  @Test
  void testCommitToEmptyTableWithPartition() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          List<Row> actions =
              IntStream.range(0, 5)
                  .mapToObj(
                      i ->
                          dummyAddFileRow(
                              schema, 10 + i, Map.of("part", Literal.ofString("p" + i))))
                  .collect(Collectors.toList());

          CloseableIterable<Row> dataActions =
              new CloseableIterable<Row>() {
                @Override
                public CloseableIterator<Row> iterator() {
                  return Utils.toCloseableIterator(actions.iterator());
                }

                @Override
                public void close() {
                  // Nothing to close
                }
              };

          table.commit(dataActions, "a", 100, Collections.emptyMap());

          // The target table should have one version
          verifyTableContent(
              dir.toString(),
              (version, addFiles, properties) -> {
                assertEquals(1L, version);
                // There should be 5 files to scan
                List<AddFile> actionsList = new ArrayList<>();
                addFiles.forEach(actionsList::add);
                assertEquals(5, actionsList.size());

                Set<String> partitionValues =
                    actionsList.stream()
                        .map(af -> af.getPartitionValues().getValues().getString(0))
                        .collect(Collectors.toSet());
                assertEquals(Set.of("p0", "p1", "p2", "p3", "p4"), partitionValues);

                long sum = actionsList.stream().mapToLong(af -> af.getNumRecords().get()).sum();
                assertEquals(60, sum);
              });
        });
  }

  @Test
  void testCommitToExistingTableWithoutPartition() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          createNonEmptyTable(
              DefaultEngine.create(new Configuration()),
              tablePath,
              schema,
              Collections.emptyList(),
              30);
          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();

          List<Row> actions =
              IntStream.range(0, 5)
                  .mapToObj(i -> dummyAddFileRow(schema, 10 + i, Collections.emptyMap()))
                  .collect(Collectors.toList());

          CloseableIterable<Row> dataActions =
              CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions.iterator()));

          table.commit(dataActions, "a", 100, Collections.emptyMap());

          // The target table should have one version
          verifyTableContent(
              dir.toString(),
              (version, addFiles, properties) -> {
                assertEquals(1L, version);
                // There should be 6 files to scan
                List<AddFile> actionsList = new ArrayList<>();
                addFiles.forEach(actionsList::add);
                assertEquals(6, actionsList.size());
                long sum = actionsList.stream().mapToLong(af -> af.getNumRecords().get()).sum();
                assertEquals(90, sum);
              });
        });
  }

  @Test
  void testCommitToExistingTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);
          createNonEmptyTable(
              DefaultEngine.create(new Configuration()), tablePath, schema, List.of("part"), 30);

          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          List<Row> actions =
              IntStream.range(0, 5)
                  .mapToObj(
                      i ->
                          dummyAddFileRow(
                              schema, 10 + i, Map.of("part", Literal.ofString("p" + i))))
                  .collect(Collectors.toList());

          CloseableIterable<Row> dataActions =
              CloseableIterable.inMemoryIterable(Utils.toCloseableIterator(actions.iterator()));

          table.commit(dataActions, "a", 100, Collections.emptyMap());

          // The target table should have one version
          verifyTableContent(
              dir.toString(),
              (version, addFiles, properties) -> {
                assertEquals(1L, version);
                // There should be 6 files to scan
                List<AddFile> actionsList = new ArrayList<>();
                addFiles.forEach(actionsList::add);
                assertEquals(6, actionsList.size());

                Set<String> partitionValues =
                    actionsList.stream()
                        .map(af -> af.getPartitionValues().getValues().getString(0))
                        .collect(Collectors.toSet());
                assertTrue(partitionValues.containsAll(Set.of("p0", "p1", "p2", "p3", "p4")));

                long sum = actionsList.stream().mapToLong(af -> af.getNumRecords().get()).sum();
                assertEquals(90, sum);
              });
        });
  }

  @Test
  void testRefreshOnEmptyTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();
          table.refresh();
          assertTrue(table.snapshot().isPresent());
          Snapshot snapshot = table.snapshot().get();
          assertEquals(0, snapshot.getVersion());
        });
  }

  @Test
  void testRefreshOnExistingTable() {
    withTempDir(
        dir -> {
          String tablePath = dir.getAbsolutePath();
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

          createNonEmptyTable(
              DefaultEngine.create(new Configuration()), tablePath, schema, List.of("part"), 30);
          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  URI.create(tablePath), Collections.emptyMap(), schema, List.of("part"));
          table.open();

          table.refresh();
          assertEquals(0, table.snapshot().get().getVersion());
        });
  }

  @Test
  void testCloseCancelOngoingOperations() {
    withTempDir(
        dir -> {
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
          createNonEmptyTable(engine, dir.getAbsolutePath(), schema);

          int[] callCounter = {0};

          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  dir.toURI(), Collections.emptyMap(), schema, Collections.emptyList()) {

                @Override
                protected Snapshot loadLatestSnapshot() {
                  Snapshot snapshot = super.loadLatestSnapshot();
                  callCounter[0]++;
                  if (callCounter[0] >= 2) {
                    for (int i = 0; i < 50; i++) {
                      try {
                        Thread.sleep(100);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                      }
                    }
                  }
                  return snapshot;
                }
              };
          table.open();

          // With cache, load will not be called again
          table.setCacheManager(new SnapshotCacheManager.NoCacheManager());

          // this thread will refresh the table
          Thread thread1 =
              new Thread(
                  () -> {
                    table.refresh();
                  });
          thread1.start();

          // If we do not call close, the refresh will take ~5s to stop
          long wcstart = System.currentTimeMillis();
          while (thread1.isAlive()) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
          long elapse = System.currentTimeMillis() - wcstart;
          assertTrue(elapse >= 4500);

          // this thread will refresh the table
          Thread thread2 =
              new Thread(
                  () -> {
                    try {
                      table.refresh();
                    } catch (Exception e) {
                      // Ignore the InterruptException
                    }
                  });
          thread2.start();
          // If we call close, the refresh was interrupted quickly
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          wcstart = System.currentTimeMillis();
          table.close();
          while (thread2.isAlive()) {
            try {
              Thread.sleep(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
          elapse = System.currentTimeMillis() - wcstart;
          assertTrue(elapse < 200);
        });
  }

  @Test
  void testRetryConcurrencyException() {
    withTempDir(
        dir -> {
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
          createNonEmptyTable(engine, dir.getAbsolutePath(), schema);

          int[] retryCounter = {0};
          int[] loadCounter = {0};

          LocalFileSystemTable testHadoopTable =
              new LocalFileSystemTable(
                  dir.toURI(), Collections.emptyMap(), schema, Collections.emptyList()) {

                @Override
                protected Snapshot loadLatestSnapshot() {
                  loadCounter[0]++;
                  Snapshot result = super.loadLatestSnapshot();
                  if (loadCounter[0] == 2) {
                    throw new ConcurrentModificationException();
                  }
                  return result;
                }

                @Override
                public void reloadSnapshot() {
                  // This should be called once
                  retryCounter[0]++;
                }
              };
          testHadoopTable.open();
          // Disable cache for retry to work
          testHadoopTable.setCacheManager(new SnapshotCacheManager.NoCacheManager());

          testHadoopTable.commit(
              CloseableIterable.inMemoryIterable(
                  Utils.singletonCloseableIterator(dummyAddFileRow(schema, 4, Map.of()))),
              "a",
              1000L,
              Collections.emptyMap());

          assertEquals(1, retryCounter[0]);
          assertEquals(3, loadCounter[0]);
        });
  }

  @Test
  void testRetryCredentialExceptionToSucceed() {
    withTempDir(
        dir -> {
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
          createNonEmptyTable(engine, dir.getAbsolutePath(), schema);

          int[] retryCounter = {0};
          int[] loadCounter = {0};

          LocalFileSystemTable testHadoopTable =
              new LocalFileSystemTable(
                  dir.toURI(), Collections.emptyMap(), schema, Collections.emptyList()) {

                @Override
                protected Snapshot loadLatestSnapshot() {
                  loadCounter[0]++;
                  Snapshot result = super.loadLatestSnapshot();
                  if (loadCounter[0] == 2) {
                    throw new RuntimeException(new AccessDeniedException(""));
                  }
                  return result;
                }

                @Override
                public void refreshCredential() {
                  // This should be called once
                  retryCounter[0]++;
                }
              };
          testHadoopTable.open();
          testHadoopTable.setCacheManager(new SnapshotCacheManager.NoCacheManager());
          testHadoopTable.refresh();
          assertEquals(1, retryCounter[0]);
        });
  }

  @Test
  void testRetryCredentialExceptionToExceedMaxAttempts() {
    withTempDir(
        dir -> {
          Engine engine = DefaultEngine.create(new Configuration());
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
          createNonEmptyTable(engine, dir.getAbsolutePath(), schema);

          int[] retryCounter = {0};
          int[] loadCounter = {0};

          LocalFileSystemTable testHadoopTable =
              new LocalFileSystemTable(
                  dir.toURI(), Collections.emptyMap(), schema, Collections.emptyList()) {

                @Override
                protected Snapshot loadLatestSnapshot() {
                  loadCounter[0]++;
                  Snapshot result = super.loadLatestSnapshot();
                  if (loadCounter[0] >= 2) {
                    throw new RuntimeException(new AccessDeniedException(""));
                  }
                  return result;
                }

                @Override
                public void refreshCredential() {
                  // This should be called three times
                  retryCounter[0]++;
                }
              };
          testHadoopTable.open();
          // Disable cache for retry to work
          testHadoopTable.setCacheManager(new SnapshotCacheManager.NoCacheManager());

          Exception e = assertThrows(Exception.class, () -> testHadoopTable.refresh());
          assertTrue(
              ExceptionUtils.recursiveCheck(ex -> ex instanceof AccessDeniedException).test(e));
          assertEquals(3, retryCounter[0]);
        });
  }

  @Test
  void testWriteResultHasProperStats() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);

    withTestTable(
        schema,
        Collections.emptyList(),
        table -> {
          int numColumns = 2;
          ColumnVector[] columnVectors = new ColumnVector[numColumns];

          List<List<?>> dataBuffer = List.of(List.of(1, "Jack"), List.of(2, "Amy"));

          for (int colIdx = 0; colIdx < numColumns; colIdx++) {
            var colDataType = schema.at(colIdx).getDataType();
            columnVectors[colIdx] = new DataColumnVectorView(dataBuffer, colIdx, colDataType);
          }

          CloseableIterator<FilteredColumnarBatch> data =
              Utils.singletonCloseableIterator(
                  new FilteredColumnarBatch(
                      new DefaultColumnarBatch(dataBuffer.size(), schema, columnVectors),
                      Optional.empty()));

          CloseableIterator<Row> result = table.writeParquet("", data, Collections.emptyMap());

          result.toInMemoryList().stream()
              .map(r -> new AddFile(r.getStruct(SingleAction.ADD_FILE_ORDINAL)))
              .forEach(
                  file -> {
                    assertFalse(file.getStatsJson().isEmpty());
                    assertEquals(
                        Optional.of(
                            "{\"numRecords\":2,\"minValues\":{\"id\":1,\"name\":\"Amy\"},"
                                + "\"maxValues\":{\"id\":2,\"name\":\"Jack\"},\"nullCount\":{\"id\":0,\"name\":0}}"),
                        file.getStatsJson());
                  });
        });
  }

  @Test
  public void testGenerateChecksum() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  dir.toURI(), Collections.emptyMap(), schema, Collections.emptyList());
          table.open();

          for (int i = 0; i < 10; i++) {
            table.commit(
                CloseableIterable.inMemoryIterable(
                    Utils.singletonCloseableIterator(dummyAddFileRow(schema, 10, Map.of()))),
                "a",
                1000L + i,
                Collections.emptyMap());

            String checksumPath =
                String.format("%s/_delta_log/%020d.crc", dir.getAbsolutePath(), i);
            File checksumFile = new File(checksumPath);
            // Async creation, wait for file to appear
            for (int j = 0; j < 100; j++) {
              if (checksumFile.exists()) {
                break;
              }
              Thread.sleep(100);
            }
            assertTrue(checksumFile.exists(), checksumPath);
          }
        });
  }

  @Test
  public void testPublishAndCheckpoint() {
    withTempDir(
        dir -> {
          StructType schema =
              new StructType().add("id", IntegerType.INTEGER).add("name", StringType.STRING);
          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  dir.toURI(),
                  Map.of(TableConf.CHECKPOINT_FREQUENCY.key(), "1.0"),
                  schema,
                  Collections.emptyList());
          table.open();

          for (int i = 0; i < 10; i++) {
            table.commit(
                CloseableIterable.inMemoryIterable(
                    Utils.singletonCloseableIterator(dummyAddFileRow(schema, 10, Map.of()))),
                "a",
                1000L + i,
                Collections.emptyMap());

            String checkpointPath =
                String.format("%s/_delta_log/%020d.checkpoint.parquet", dir.getAbsolutePath(), i);
            File checkpointFile = new File(checkpointPath);
            // Async creation, wait for file to appear
            for (int j = 0; j < 100; j++) {
              if (checkpointFile.exists()) {
                break;
              }
              Thread.sleep(100);
            }
            assertTrue(checkpointFile.exists(), checkpointPath);
            // Ensure cache is updated
            var cachedSnapshot = table.snapshot().get();
            assertEquals(i + 1, cachedSnapshot.getVersion());
          }
        });
  }
}
