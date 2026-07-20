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

import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_ID_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_MODE_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;
import static org.junit.jupiter.api.Assertions.*;

import dev.failsafe.function.CheckedConsumer;
import io.delta.flink.TestHelper;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.util.InstantiationUtil;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** JUnit 6 test suite for AbstractKernelTable. */
class AbstractKernelTableTest extends TestHelper {

  private static final class BarrierLocalFileSystemTable extends LocalFileSystemTable {
    private final CyclicBarrier snapshotBarrier;
    private final AtomicBoolean blockNextSnapshot = new AtomicBoolean();

    private BarrierLocalFileSystemTable(
        URI tablePath, Map<String, String> conf, StructType schema, CyclicBarrier snapshotBarrier) {
      super(tablePath, conf, schema, List.of());
      this.snapshotBarrier = snapshotBarrier;
    }

    void blockNextSnapshot() {
      blockNextSnapshot.set(true);
    }

    @Override
    protected Optional<Snapshot> loadLatestSnapshotUncached() {
      Optional<Snapshot> snapshot = super.loadLatestSnapshotUncached();
      if (blockNextSnapshot.compareAndSet(true, false)) {
        try {
          snapshotBarrier.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        } catch (BrokenBarrierException | TimeoutException e) {
          throw new RuntimeException(e);
        }
      }
      return snapshot;
    }
  }

  private static Throwable awaitFailure(Future<?> future) throws Exception {
    try {
      future.get(30, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    }
  }

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
    assertEquals(
        "s3://my_bucket/var/",
        AbstractKernelTable.normalize(URI.create("s3://my_bucket/var")).toString());
    assertEquals(
        "abfss://container@account.dfs.core.windows.net/path/",
        AbstractKernelTable.normalize(
                URI.create("abfss://container@account.dfs.core.windows.net/path"))
            .toString());
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
          assertNull(copy.getSchema());
          assertNull(copy.tableState);
          assertEquals(copy.getId(), table.getId());
          assertEquals(copy.getTablePath(), table.getTablePath());
          assertEquals(copy.getTableUUID(), table.getTableUUID());
          assertEquals(copy.getPartitionColumns(), table.getPartitionColumns());
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
  void testUpdateSchemaPreservesAndAssignsColumnMappingMetadata() {
    StructType initialSchema = new StructType().add("id", IntegerType.INTEGER);
    FieldMetadata userMetadata =
        FieldMetadata.builder().putString("user.metadata", "preserved").build();
    StructType nestedType =
        new StructType().add(new StructField("city", StringType.STRING, true, userMetadata));
    StructType targetSchema =
        new StructType()
            // Deliberately omit existing mapping metadata. The table snapshot remains
            // authoritative.
            .add("id", IntegerType.INTEGER)
            .add(new StructField("name", StringType.STRING, true, userMetadata))
            .add(new StructField("address", nestedType, true, userMetadata));

    withTestTable(
        initialSchema,
        Collections.emptyList(),
        Map.of(COLUMN_MAPPING_MODE_KEY, "name"),
        table -> {
          StructField oldId = table.getSchema().at(0);
          long oldIdMapping = oldId.getMetadata().getLong(COLUMN_MAPPING_ID_KEY);
          String oldPhysicalName = oldId.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY);
          long oldVersion = table.snapshot().orElseThrow().getVersion();

          table.updateSchema(targetSchema);

          StructType updatedSchema = table.getSchema();
          assertEquals(List.of("id", "name", "address"), updatedSchema.fieldNames());
          assertEquals(oldVersion + 1, table.snapshot().orElseThrow().getVersion());
          assertEquals(
              oldIdMapping, updatedSchema.at(0).getMetadata().getLong(COLUMN_MAPPING_ID_KEY));
          assertEquals(
              oldPhysicalName,
              updatedSchema.at(0).getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY));

          StructField name = updatedSchema.at(1);
          assertNotNull(name.getMetadata().getLong(COLUMN_MAPPING_ID_KEY));
          assertNotNull(name.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY));
          assertEquals("preserved", name.getMetadata().getString("user.metadata"));

          StructField nestedCity = ((StructType) updatedSchema.at(2).getDataType()).at(0);
          assertNotNull(nestedCity.getMetadata().getLong(COLUMN_MAPPING_ID_KEY));
          assertNotNull(nestedCity.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY));
          assertEquals("preserved", nestedCity.getMetadata().getString("user.metadata"));

          table.updateSchema(targetSchema);
          assertEquals(oldVersion + 1, table.snapshot().orElseThrow().getVersion());
        });
  }

  @Test
  void testUpdateSchemaPreservesCallerSuppliedColumnMappingMetadata() {
    StructType initialSchema = new StructType().add("id", IntegerType.INTEGER);
    FieldMetadata suppliedMappingMetadata =
        FieldMetadata.builder()
            .putLong(COLUMN_MAPPING_ID_KEY, 999L)
            .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, "caller-physical-name")
            .build();
    StructType targetSchema =
        initialSchema.add(
            new StructField("name", StringType.STRING, true, suppliedMappingMetadata));

    withTestTable(
        initialSchema,
        Collections.emptyList(),
        Map.of(COLUMN_MAPPING_MODE_KEY, "name"),
        table -> {
          table.updateSchema(targetSchema);

          StructField addedField = table.getSchema().at(1);
          assertEquals(999L, addedField.getMetadata().getLong(COLUMN_MAPPING_ID_KEY));
          assertEquals(
              "caller-physical-name",
              addedField.getMetadata().getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY));
        });
  }

  @Test
  void testUpdateSchemaRejectsUnsupportedChanges() {
    StructType nestedType = new StructType().add("value", IntegerType.INTEGER);
    StructType initialSchema =
        new StructType().add("id", IntegerType.INTEGER).add("payload", nestedType);

    withTestTable(
        initialSchema,
        Collections.emptyList(),
        Map.of(COLUMN_MAPPING_MODE_KEY, "name"),
        table -> {
          long initialVersion = table.snapshot().orElseThrow().getVersion();
          List<StructType> invalidTargets =
              List.of(
                  new StructType().add("id", IntegerType.INTEGER),
                  new StructType().add("payload", nestedType).add("id", IntegerType.INTEGER),
                  new StructType().add("id", LongType.LONG).add("payload", nestedType),
                  new StructType()
                      .add(new StructField("id", IntegerType.INTEGER, false))
                      .add("payload", nestedType),
                  new StructType()
                      .add("id", IntegerType.INTEGER)
                      .add("payload", new StructType().add("renamed", IntegerType.INTEGER)),
                  initialSchema.add(new StructField("required", StringType.STRING, false)));

          invalidTargets.forEach(
              target ->
                  assertThrows(IllegalArgumentException.class, () -> table.updateSchema(target)));
          assertEquals(initialVersion, table.snapshot().orElseThrow().getVersion());
        });
  }

  @Test
  void testConcurrentSchemaUpdatesDoNotMergeDifferentTargets() {
    withTempDir(
        dir -> {
          StructType initialSchema = new StructType().add("id", IntegerType.INTEGER);
          Map<String, String> properties =
              Map.of(
                  COLUMN_MAPPING_MODE_KEY,
                  "name",
                  "fs.file.impl",
                  "org.apache.hadoop.fs.RawLocalFileSystem",
                  "fs.file.impl.disable.cache",
                  "true");
          CyclicBarrier snapshotBarrier = new CyclicBarrier(2);
          ExecutorService executor = Executors.newFixedThreadPool(2);

          try (BarrierLocalFileSystemTable first =
                  new BarrierLocalFileSystemTable(
                      dir.toURI(), properties, initialSchema, snapshotBarrier);
              BarrierLocalFileSystemTable second =
                  new BarrierLocalFileSystemTable(
                      dir.toURI(), properties, initialSchema, snapshotBarrier)) {
            first.open();
            second.open();

            StructType sharedTarget = initialSchema.add("name", StringType.STRING);
            first.blockNextSnapshot();
            second.blockNextSnapshot();
            Future<?> firstShared = executor.submit(() -> first.updateSchema(sharedTarget));
            Future<?> secondShared = executor.submit(() -> second.updateSchema(sharedTarget));
            assertNull(awaitFailure(firstShared));
            assertNull(awaitFailure(secondShared));
            assertEquals(1L, first.snapshot().orElseThrow().getVersion());

            StructType emailTarget = sharedTarget.add("email", StringType.STRING);
            StructType phoneTarget = sharedTarget.add("phone", StringType.STRING);
            first.blockNextSnapshot();
            second.blockNextSnapshot();
            Future<?> emailUpdate = executor.submit(() -> first.updateSchema(emailTarget));
            Future<?> phoneUpdate = executor.submit(() -> second.updateSchema(phoneTarget));
            Throwable emailFailure = awaitFailure(emailUpdate);
            Throwable phoneFailure = awaitFailure(phoneUpdate);

            assertEquals(
                1L, Stream.of(emailFailure, phoneFailure).filter(Objects::nonNull).count());
            assertInstanceOf(
                IllegalArgumentException.class, emailFailure == null ? phoneFailure : emailFailure);
            Snapshot latest = first.snapshot().orElseThrow();
            assertEquals(2L, latest.getVersion());
            assertTrue(
                List.of(emailTarget.fieldNames(), phoneTarget.fieldNames())
                    .contains(latest.getSchema().fieldNames()));
          } finally {
            executor.shutdownNow();
          }
        });
  }

  @Test
  void testUpdateSchemaRequiresColumnMappingAndOpenTable() {
    StructType initialSchema = new StructType().add("id", IntegerType.INTEGER);
    StructType targetSchema = initialSchema.add("name", StringType.STRING);

    withTempDir(
        dir -> {
          LocalFileSystemTable unopened =
              new LocalFileSystemTable(dir.toURI(), Map.of(), initialSchema, List.of());
          assertThrows(IllegalStateException.class, () -> unopened.updateSchema(targetSchema));
        });

    withTestTable(
        initialSchema,
        Collections.emptyList(),
        table -> {
          long initialVersion = table.snapshot().orElseThrow().getVersion();
          RuntimeException error =
              assertThrows(RuntimeException.class, () -> table.updateSchema(targetSchema));
          assertTrue(error.getMessage().toLowerCase(Locale.ROOT).contains("column mapping"));
          assertEquals(initialVersion, table.snapshot().orElseThrow().getVersion());
        });
  }

  /**
   * Reads a single merged Hadoop configuration value from the engine produced by {@link
   * AbstractKernelTable#createEngine()}. {@code createEngine} builds the configuration internally
   * and only exposes it through the engine's {@link FileIO}, so this reaches it via {@code
   * DefaultEngine}'s private {@code fileIO} field rather than adding a production-only accessor.
   */
  private static String engineConfValue(Engine engine, String key) throws Exception {
    Field fileIOField = DefaultEngine.class.getDeclaredField("fileIO");
    fileIOField.setAccessible(true);
    FileIO fileIO = (FileIO) fileIOField.get(engine);
    return fileIO.getConf(key).orElse(null);
  }

  @Test
  void testEngineConfigurationPrecedence() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);

    // Customer-provided fs.* options: one overrides a built-in default, one collides with a
    // credential vended by the credential manager, one is unique.
    Map<String, String> tableConfig = new HashMap<>();
    tableConfig.put("fs.s3a.path.style.access", "true"); // built-in default is "false"
    tableConfig.put("fs.s3a.access.key", "customer-key"); // also vended by the credential manager
    tableConfig.put("fs.s3a.endpoint", "customer-endpoint"); // no default, no credential

    withTempDir(
        dir -> {
          LocalFileSystemTable table =
              new LocalFileSystemTable(dir.toURI(), tableConfig, schema, Collections.emptyList());
          // Inject a credential manager that vends a value colliding with a customer fs.* key.
          // The field is protected/transient and only initialized by open() when null, so setting
          // it here needs no change to AbstractKernelTable.
          table.credentialManager =
              new CredentialManager.AmbientCredentialManager() {
                @Override
                Map<String, String> getCredentials() {
                  return Map.of("fs.s3a.access.key", "credential-key");
                }
              };

          // engineConf carries the customer-provided fs.* options verbatim.
          assertEquals(
              Map.of(
                  "fs.s3a.path.style.access", "true",
                  "fs.s3a.access.key", "customer-key",
                  "fs.s3a.endpoint", "customer-endpoint"),
              table.conf.engineConf());

          Engine engine = table.createEngine();
          // Customer value overrides the built-in default.
          assertEquals("true", engineConfValue(engine, "fs.s3a.path.style.access"));
          // Credential manager value overrides the customer value.
          assertEquals("credential-key", engineConfValue(engine, "fs.s3a.access.key"));
          // Customer value with no default/credential is passed through.
          assertEquals("customer-endpoint", engineConfValue(engine, "fs.s3a.endpoint"));
        });
  }

  @Test
  void testCredentialManagerFromConf() {
    StructType schema = new StructType().add("id", IntegerType.INTEGER);
    String key = TableConf.CREDENTIALS_SOURCE.key();

    // Default (no conf) and explicit "uc" both yield a UC-vending manager, not the ambient one.
    withTestTable(
        schema,
        Collections.emptyList(),
        table ->
            assertFalse(
                table.credentialManager instanceof CredentialManager.AmbientCredentialManager));
    withTestTable(
        schema,
        Collections.emptyList(),
        Map.of(key, "uc"),
        table ->
            assertFalse(
                table.credentialManager instanceof CredentialManager.AmbientCredentialManager));

    // "ambient" (case-insensitive) yields the no-op manager that fetches no credentials.
    withTestTable(
        schema,
        Collections.emptyList(),
        Map.of(key, "AMBIENT"),
        table -> {
          assertInstanceOf(
              CredentialManager.AmbientCredentialManager.class, table.credentialManager);
          assertTrue(table.credentialManager.getCredentials().isEmpty());
        });

    // An unknown source fails fast.
    withTempDir(
        dir -> {
          LocalFileSystemTable table =
              new LocalFileSystemTable(
                  dir.toURI(), Map.of(key, "not-a-source"), schema, Collections.emptyList());
          IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, table::open);
          assertTrue(ex.getMessage().contains(key));
        });
  }

  @Test
  void testCreateTableAndCommitWithoutPartition() {
    StructType schema =
        new StructType().add("id", IntegerType.INTEGER).add("part", StringType.STRING);

    withTestTable(
        schema,
        Collections.emptyList(),
        table -> {
          List<Row> actions =
              dummyAddFileRows(schema, 5, (i) -> Map.of("part", Literal.ofString("p" + i)));

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
  void testCreateNewTableAndCommitWithPartition() {
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
              dummyAddFileRows(schema, 5, (i) -> Map.of("part", Literal.ofString("p" + i)));

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

          List<Row> actions = dummyAddFileRows(schema, 5, (i) -> Map.of());

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
              dummyAddFileRows(schema, 5, (i) -> Map.of("part", Literal.ofString("p" + i)));

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
  public void testCheckpoint() {
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
