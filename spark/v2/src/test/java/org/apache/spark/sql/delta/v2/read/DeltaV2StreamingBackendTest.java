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
package org.apache.spark.sql.delta.v2.read;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.CommitFileStatus;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.DeltaOptions;
import org.apache.spark.sql.delta.DeltaSingleCommit;
import org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException;
import org.apache.spark.sql.delta.InvalidProtocolVersionException;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.VersionNotFoundException;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.Metadata;
import org.apache.spark.sql.delta.actions.Protocol;
import org.apache.spark.sql.delta.actions.Protocol$;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSourceMetadataEvolutionSupport$;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.storage.ClosableIterator.IteratorCloseOps;
import org.apache.spark.sql.delta.storage.LogStore;
import org.apache.spark.sql.delta.util.JsonUtils;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SingleCommit$;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManagerCache$;
import org.apache.spark.sql.delta.v2.utils.ScalaUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Option;
import scala.Tuple2;
import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

/** Unit tests for {@link DeltaV2StreamingBackend}. */
public class DeltaV2StreamingBackendTest extends DeltaV2TestBase {

  private final DeltaV2QueryContext queryContext = DeltaV2QueryContext$.MODULE$.empty();

  @Override
  protected boolean clearTableManagerCacheAfterEach() {
    return true;
  }

  private static DeltaOptions defaultOptions() {
    return new DeltaOptions(
        ScalaUtils.toScalaMap(Collections.emptyMap()), spark.sessionState().conf());
  }

  private static String createTable(File dir, int commits) {
    String path = dir.getAbsolutePath();
    for (int i = 0; i < commits; i++) {
      spark.range(1).write().format("delta").mode("append").save(path);
    }
    return path;
  }

  private static DeltaSourceOffset createEndOffset(String tableId, long version) {
    return DeltaSourceOffset.apply(
        tableId, version, DeltaSourceOffset.BASE_INDEX(), /* isInitialSnapshot= */ false);
  }

  private static List<Long> collectCommitVersions(ClosableIterator<DeltaSingleCommit> commits)
      throws IOException {
    try (commits) {
      List<Long> versions = new ArrayList<>();
      while (commits.hasNext()) {
        versions.add(commits.next().version());
      }
      return versions;
    }
  }

  @Test
  public void getSingleCommitIteratorReturnsRequestedVersionsInOrder(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, /* commits= */ 4);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 3L,
            spark,
            defaultOptions());
    DeltaSourceOffset endOffset = createEndOffset(backend.update().metadata().id(), 2L);

    assertEquals(
        List.of(1L, 2L),
        collectCommitVersions(
            backend.getSingleCommitIterator(
                /* startVersion= */ 1L, /* failOnDataLoss= */ true, Option.apply(endOffset))));
  }

  @Test
  public void getSingleCommitIteratorCapsEndOffsetAtLatestVersion(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, /* commits= */ 3);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 2L,
            spark,
            defaultOptions());
    // Finishing version 2 advances the stream's offset to version 3, which does not exist yet.
    DeltaSourceOffset endOffset = createEndOffset(backend.update().metadata().id(), 3L);

    assertEquals(
        List.of(1L, 2L),
        collectCommitVersions(
            backend.getSingleCommitIterator(
                /* startVersion= */ 1L, /* failOnDataLoss= */ true, Option.apply(endOffset))));
  }

  @Test
  public void getSingleCommitIteratorReturnsEmptyWhenStartExceedsCappedEnd(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, /* commits= */ 1);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 0L,
            spark,
            defaultOptions());
    DeltaSourceOffset endOffset = createEndOffset(backend.update().metadata().id(), 1L);

    assertEquals(
        Collections.emptyList(),
        collectCommitVersions(
            backend.getSingleCommitIterator(
                /* startVersion= */ 1L, /* failOnDataLoss= */ true, Option.apply(endOffset))));
  }

  @Test
  public void getSingleCommitIteratorReadsNewCommitsWithEndOffset(@TempDir File tempDir)
      throws Exception {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT().key(),
        "0",
        () -> {
          String path = createTable(tempDir, /* commits= */ 1);
          DeltaV2StreamingBackend backend =
              new DeltaV2StreamingBackend(
                  DeltaV2TableManagerCache$.MODULE$.forTable(
                      spark, path, Collections.emptyMap(), Option.empty()),
                  queryContext,
                  /* snapshotVersionAtSourceInit= */ 0L,
                  spark,
                  defaultOptions());
          DeltaSourceOffset endOffset = createEndOffset(backend.update().metadata().id(), 1L);

          spark.range(1).write().format("delta").mode("append").save(path);

          assertEquals(
              List.of(1L),
              collectCommitVersions(
                  backend.getSingleCommitIterator(
                      /* startVersion= */ 1L,
                      /* failOnDataLoss= */ true,
                      Option.apply(endOffset))));
        });
  }

  @Test
  public void getSingleCommitIteratorFindsNewCommitsWithoutEndOffset(@TempDir File tempDir)
      throws Exception {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT().key(),
        "0",
        () -> {
          String path = createTable(tempDir, /* commits= */ 1);
          DeltaV2StreamingBackend backend =
              new DeltaV2StreamingBackend(
                  DeltaV2TableManagerCache$.MODULE$.forTable(
                      spark, path, Collections.emptyMap(), Option.empty()),
                  queryContext,
                  /* snapshotVersionAtSourceInit= */ 0L,
                  spark,
                  defaultOptions());

          assertEquals(
              Collections.emptyList(),
              collectCommitVersions(
                  backend.getSingleCommitIterator(
                      /* startVersion= */ 1L, /* failOnDataLoss= */ true, Option.empty())));

          spark.range(1).write().format("delta").mode("append").save(path);

          assertEquals(
              List.of(1L),
              collectCommitVersions(
                  backend.getSingleCommitIterator(
                      /* startVersion= */ 1L, /* failOnDataLoss= */ true, Option.empty())));
        });
  }

  @Test
  public void closeCommitsWithFilesClosesFilesAndCommits(@TempDir File tempDir) throws Exception {
    String path = createTable(tempDir, /* commits= */ 1);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 0L,
            spark,
            defaultOptions());
    List<String> closed = new ArrayList<>();

    // The Kernel commits hold open resources, so closing the files also closes them.
    backend
        .closeCommitsWithFiles(
            emptyIteratorRecordingClose(closed, "files"),
            emptyIteratorRecordingClose(closed, "commits"))
        .close();

    assertEquals(List.of("files", "commits"), closed);
  }

  private static <T> ClosableIterator<T> emptyIteratorRecordingClose(
      List<String> closed, String name) {
    return new ClosableIterator<T>() {
      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public T next() {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {
        closed.add(name);
      }
    };
  }

  @Test
  public void unsafeVolatileSnapshotIsAtLeastAsNewAsActiveCommit(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 1);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 0L,
            spark,
            defaultOptions());
    assertEquals(0L, backend.update().version());

    spark.range(1).write().format("delta").mode("append").save(path);
    long commitVersion =
        backend
            .getActiveCommitAtTime(
                new Timestamp(System.currentTimeMillis()),
                /* canReturnLastCommit= */ true,
                /* mustBeRecreatable= */ false,
                /* canReturnEarliestCommit= */ true)
            .version();

    assertEquals(1L, commitVersion);
    assertEquals(1L, backend.unsafeVolatileSnapshot().version());
  }

  @Test
  public void unsafeVolatileTableIdMatchesDeltaLog(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 2);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 1L,
            spark,
            defaultOptions());

    // The V1 backend returns DeltaLog.unsafeVolatileTableId.
    assertEquals(
        DeltaLog.forTable(spark, path).unsafeVolatileTableId(), backend.unsafeVolatileTableId());
  }

  @Test
  public void checkVersionExistsThrowsDeltaVersionNotFound(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 3);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 2L,
            spark,
            defaultOptions());

    VersionNotFoundException e =
        assertThrows(
            VersionNotFoundException.class,
            () ->
                backend.checkVersionExists(
                    /* version= */ 5L,
                    /* mustBeRecreatable= */ false,
                    /* allowOutOfRange= */ false));
    assertEquals("DELTA_VERSION_NOT_FOUND", e.getErrorClass());
    assertEquals(5L, e.userVersion());
    assertEquals(0L, e.earliest());
    assertEquals(2L, e.latest());
  }

  @Test
  public void getSnapshotToValidateProtocolThrowsDeltaUnsupportedFeature(@TempDir File tempDir)
      throws Exception {
    String path = createTable(tempDir, /* commits= */ 1);
    // Commit a protocol with a reader feature that no client supports.
    String feature = "unsupportedReaderFeatureForTest";
    Files.write(
        Paths.get(path, "_delta_log", "00000000000000000001.json"),
        Collections.singletonList(
            "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,"
                + "\"readerFeatures\":[\""
                + feature
                + "\"],\"writerFeatures\":[\""
                + feature
                + "\"]}}"));
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 0L,
            spark,
            defaultOptions());

    DeltaUnsupportedTableFeatureException e =
        assertThrows(
            DeltaUnsupportedTableFeatureException.class,
            () -> backend.getSnapshotToValidateProtocol(/* version= */ 1L));
    assertEquals("DELTA_UNSUPPORTED_FEATURES_FOR_READ", e.getErrorClass());
    assertTrue(e.getMessage().contains(feature), e.getMessage());
  }

  @Test
  public void protocolReadRejectsAProtocolThatThisClientCannotRead(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 1);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 0L,
            spark,
            defaultOptions());
    Snapshot snapshot = backend.update();

    backend.protocolRead(snapshot.protocol(), snapshot);
    InvalidProtocolVersionException versionError =
        assertThrows(
            InvalidProtocolVersionException.class,
            () ->
                backend.protocolRead(
                    Protocol$.MODULE$.apply(Integer.MAX_VALUE, Integer.MAX_VALUE), snapshot));
    // The same path as in V1 errors: the table data path.
    assertEquals(snapshot.dataPath().toString(), versionError.tableNameOrPath());
    Protocol unknownReaderFeature =
        (Protocol)
            Action.fromJson(
                "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,"
                    + "\"readerFeatures\":[\"unknownFeature\"],"
                    + "\"writerFeatures\":[\"unknownFeature\"]}}");
    DeltaUnsupportedTableFeatureException featureError =
        assertThrows(
            DeltaUnsupportedTableFeatureException.class,
            () -> backend.protocolRead(unknownReaderFeature, snapshot));
    assertEquals("DELTA_UNSUPPORTED_FEATURES_FOR_READ", featureError.getErrorClass());
  }

  // ---------------------------------------------------------------------------
  // Metadata and protocol action collectors
  // ---------------------------------------------------------------------------

  private DeltaV2StreamingBackend collectorBackend(DeltaV2TableManager manager, long version) {
    return new DeltaV2StreamingBackend(manager, queryContext, version, spark, defaultOptions());
  }

  private static <T> Map<Long, T> toActionMap(Seq<Tuple2<Object, T>> actions) {
    Map<Long, T> result = new LinkedHashMap<>();
    for (Tuple2<Object, T> action : CollectionConverters.asJava(actions)) {
      result.put((Long) action._1(), action._2());
    }
    return result;
  }

  private DeltaV2StreamingBackend createCollectorBackend(
      String path, String tableName, boolean catalogBacked) throws Exception {
    return createCollectorBackend(path, tableName, catalogBacked, true);
  }

  private DeltaV2StreamingBackend createCollectorBackend(
      String path, String tableName, boolean catalogBacked, boolean failOnDataLoss)
      throws Exception {
    DeltaOptions collectorOptions =
        new DeltaOptions(
            ScalaUtils.toScalaMap(
                Collections.singletonMap("failOnDataLoss", Boolean.toString(failOnDataLoss))),
            spark.sessionState().conf());
    Option<CatalogTable> catalogTableOpt =
        catalogBacked
            ? Option.apply(
                spark.sessionState().catalog().getTableMetadata(new TableIdentifier(tableName)))
            : Option.empty();
    DeltaV2QueryContext collectorQueryContext = DeltaV2QueryContext$.MODULE$.apply(catalogTableOpt);
    DeltaV2TableManager manager =
        DeltaV2TableManagerCache$.MODULE$.forTable(
            spark, path, Collections.emptyMap(), catalogTableOpt);
    return new DeltaV2StreamingBackend(manager, collectorQueryContext, 0L, spark, collectorOptions);
  }

  @ParameterizedTest
  @CsvSource({"metaData,false", "metaData,true", "protocol,false", "protocol,true"})
  void actionCollectorsPreserveDuplicatesAndReturnFirstAtVersion(
      String actionName, boolean catalogBacked, @TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_duplicate_" + actionName + "_" + catalogBacked;
    withTable(
        new String[] {tableName},
        () -> {
          createEmptyTestTable(path, tableName);
          Snapshot snapshot = DeltaLog.forTable(spark, path).snapshot();
          List<String> actions;
          if (actionName.equals("metaData")) {
            ObjectNode metadata =
                (ObjectNode) JsonUtils.mapper().readTree(snapshot.metadata().json());
            ((ObjectNode) metadata.get("metaData")).put("description", "second metadata");
            actions = List.of(snapshot.metadata().json(), metadata.toString());
          } else {
            actions =
                List.of(
                    "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":1}}",
                    "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");
          }
          Files.write(Paths.get(path, "_delta_log", "00000000000000000001.json"), actions);
          DeltaV2StreamingBackend backend = createCollectorBackend(path, tableName, catalogBacked);

          List<? extends Tuple2<Object, ? extends Action>> collected;
          Action first;
          if (actionName.equals("metaData")) {
            collected =
                CollectionConverters.asJava(
                    DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                        backend, 1L, Option.apply(1L)));
            first =
                DeltaSourceMetadataEvolutionSupport$.MODULE$
                    .collectMetadataAtVersion(backend, 1L)
                    .get();
          } else {
            collected =
                CollectionConverters.asJava(
                    DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                        backend, 1L, Option.apply(1L)));
            first =
                DeltaSourceMetadataEvolutionSupport$.MODULE$
                    .collectProtocolAtVersion(backend, 1L)
                    .get();
          }
          assertEquals(2, collected.size());
          for (int i = 0; i < actions.size(); i++) {
            assertEquals(1L, collected.get(i)._1());
            assertEquals(Action.fromJson(actions.get(i)), collected.get(i)._2());
          }
          assertEquals(Action.fromJson(actions.get(0)), first);
        });
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void actionCollectorsHonorDataLossPolicyAndEndVersion(
      boolean catalogBacked, boolean failOnDataLoss, @TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_collector_gaps_" + catalogBacked + "_" + failOnDataLoss;
    withTable(
        new String[] {tableName},
        () ->
            withSQLConf(
                DeltaSQLConf.DELTA_GET_CHANGE_LOG_FILES_FAIL_ON_GAPS_IN_TESTS().key(),
                "false",
                () -> {
                  createEmptyTestTable(path, tableName);
                  for (int i = 0; i < 3; i++) {
                    spark.sql("INSERT INTO " + tableName + " VALUES (1, 'Alice')");
                  }
                  DeltaLog.forTable(spark, path).checkpoint();
                  Files.delete(Paths.get(path, "_delta_log", "00000000000000000002.json"));
                  DeltaV2StreamingBackend backend =
                      createCollectorBackend(path, tableName, catalogBacked, failOnDataLoss);

                  assertEquals(
                      Set.of(0L),
                      toActionMap(
                              DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                                  backend, 0L, Option.apply(1L)))
                          .keySet());
                  assertEquals(
                      Set.of(0L),
                      toActionMap(
                              DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                                  backend, 0L, Option.apply(1L)))
                          .keySet());
                  assertTrue(
                      DeltaSourceMetadataEvolutionSupport$.MODULE$
                          .collectMetadataAtVersion(backend, 1L)
                          .isEmpty());
                  assertTrue(
                      DeltaSourceMetadataEvolutionSupport$.MODULE$
                          .collectProtocolAtVersion(backend, 1L)
                          .isEmpty());
                  if (failOnDataLoss) {
                    // JVM and FFI Kernel backends expose different exception types for gaps.
                    RuntimeException metadataError =
                        assertThrows(
                            RuntimeException.class,
                            () ->
                                DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                                    backend, 0L, Option.apply(3L)));
                    RuntimeException protocolError =
                        assertThrows(
                            RuntimeException.class,
                            () ->
                                DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                                    backend, 0L, Option.apply(3L)));
                    assertTrue(metadataError.getMessage().contains("version 2"));
                    assertTrue(protocolError.getMessage().contains("version 2"));
                  } else {
                    assertEquals(
                        Set.of(0L),
                        toActionMap(
                                DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                                    backend, 0L, Option.apply(3L)))
                            .keySet());
                    assertEquals(
                        Set.of(0L),
                        toActionMap(
                                DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                                    backend, 0L, Option.apply(3L)))
                            .keySet());
                  }
                }));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void actionCollectorsReadMetadataWithoutKernelSchemaConversion(
      boolean catalogBacked, @TempDir File tempDir) throws Exception {
    String path = tempDir.getAbsolutePath();
    String tableName = "test_raw_metadata_" + catalogBacked;
    withTable(
        new String[] {tableName},
        () -> {
          createEmptyTestTable(path, tableName);
          Snapshot snapshot = DeltaLog.forTable(spark, path).snapshot();
          String schema =
              "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"UnsupportedType\","
                  + "\"nullable\":true,\"metadata\":{}}]}";
          ObjectNode metadata =
              (ObjectNode) JsonUtils.mapper().readTree(snapshot.metadata().json());
          ((ObjectNode) metadata.get("metaData")).put("schemaString", schema);
          Files.write(
              Paths.get(path, "_delta_log", "00000000000000000001.json"),
              List.of(snapshot.protocol().json(), metadata.toString()));
          // Keep the latest snapshot valid so collection inspects the historical commit.
          Files.write(
              Paths.get(path, "_delta_log", "00000000000000000002.json"),
              List.of(snapshot.metadata().json(), snapshot.protocol().json()));
          DeltaV2StreamingBackend backend = createCollectorBackend(path, tableName, catalogBacked);

          Map<Long, Metadata> metadataActions =
              toActionMap(
                  DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                      backend, 1L, Option.apply(1L)));
          assertEquals(Set.of(1L), metadataActions.keySet());
          assertEquals(Action.fromJson(metadata.toString()), metadataActions.get(1L));
          assertEquals(
              Map.of(1L, snapshot.protocol()),
              toActionMap(
                  DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                      backend, 1L, Option.apply(1L))));
        });
  }

  // Mocks let these tests inject commit and action read failures.
  @SuppressWarnings("unchecked")
  private static <T> CloseableIterator<T> mockIterator() {
    return mock(CloseableIterator.class, CALLS_REAL_METHODS);
  }

  private DeltaV2TableManager mockTableManager(
      CloseableIterator<CommitFileStatus> commits, boolean failOnDataLoss) {
    return mockTableManager(
        commits,
        failOnDataLoss,
        defaultKernelContext.logStore(),
        defaultKernelContext.materializeHadoopConf());
  }

  private DeltaV2TableManager mockTableManager(
      CloseableIterator<CommitFileStatus> commits,
      boolean failOnDataLoss,
      LogStore logStore,
      Configuration conf) {
    DeltaV2TableManager manager = mock(DeltaV2TableManager.class);
    ClosableIterator<DeltaSingleCommit> v1Commits =
        new IteratorCloseOps<>(ScalaUtils.toClosableIterator(commits))
            .withClose(
                iterator ->
                    iterator.map(file -> DeltaV2SingleCommit$.MODULE$.apply(file, logStore, conf)));
    when(manager.getSingleCommitIterator(
            7L, Option.apply((Object) 7L), queryContext, failOnDataLoss))
        .thenReturn(v1Commits);
    return manager;
  }

  @Test
  public void testProtocolCollector_closesEmptyCommitIterator() throws IOException {
    CloseableIterator<CommitFileStatus> commits = mockIterator();

    assertTrue(
        DeltaSourceMetadataEvolutionSupport$.MODULE$
            .collectProtocolActions(
                collectorBackend(mockTableManager(commits, true), 0L), 7L, Option.apply(7L))
            .isEmpty());

    verify(commits).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void singleVersionCollectorsStopAndCloseIterators(boolean metadata) throws Exception {
    CommitFileStatus kernelCommit =
        new CommitFileStatus(7L, io.delta.kernel.utils.FileStatus.of("/table/7.json", 1L, 0L));
    CloseableIterator<CommitFileStatus> commits = mockIterator();
    when(commits.hasNext())
        .thenReturn(true)
        .thenThrow(new AssertionError("Read commits past the first matching action"));
    when(commits.next()).thenReturn(kernelCommit);
    CloseableIterator<String> actions = mockIterator();
    when(actions.hasNext()).thenReturn(true);
    Action expected =
        metadata
            ? Action.fromJson("{\"metaData\":{\"id\":\"test-id\"}}")
            : Protocol$.MODULE$.apply(1, 2);
    when(actions.next())
        .thenReturn(expected.json())
        .thenThrow(new AssertionError("Read past the first matching action"));
    LogStore store = mock(LogStore.class);
    Configuration conf = defaultKernelContext.materializeHadoopConf();
    when(store.readAsIterator(any(FileStatus.class), same(conf)))
        .thenReturn(ScalaUtils.toClosableIterator(actions));

    withSQLConf(
        DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD().key(),
        "0",
        () -> {
          DeltaV2StreamingBackend backend =
              collectorBackend(mockTableManager(commits, true, store, conf), 0L);
          Action actual =
              metadata
                  ? DeltaSourceMetadataEvolutionSupport$.MODULE$
                      .collectMetadataAtVersion(backend, 7L)
                      .get()
                  : DeltaSourceMetadataEvolutionSupport$.MODULE$
                      .collectProtocolAtVersion(backend, 7L)
                      .get();
          assertEquals(expected, actual);
        });

    verify(actions).next();
    verify(actions).close();
    verify(commits).next();
    verify(commits).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rangeCollectorsReadActionsBeforeAdvancingAndCloseAfterConsumption(boolean metadata)
      throws Exception {
    Action first =
        metadata
            ? Action.fromJson("{\"metaData\":{\"id\":\"first\"}}")
            : Protocol$.MODULE$.apply(1, 2);
    Action second =
        metadata
            ? Action.fromJson("{\"metaData\":{\"id\":\"second\"}}")
            : Protocol$.MODULE$.apply(1, 3);
    List<String> remainingActions = new ArrayList<>(List.of(first.json(), second.json()));
    // Detect descriptor buffering and lazy results that escape the collector's close scope.
    CloseableIterator<CommitFileStatus> commits = mockIterator();
    when(commits.hasNext())
        .thenReturn(true)
        .thenAnswer(
            ignored -> {
              assertTrue(remainingActions.isEmpty(), "Advanced commits before reading actions");
              return false;
            });
    when(commits.next())
        .thenReturn(
            new CommitFileStatus(7L, io.delta.kernel.utils.FileStatus.of("/table/7.json", 1L, 0L)));
    CloseableIterator<String> actions = mockIterator();
    when(actions.hasNext()).thenAnswer(ignored -> !remainingActions.isEmpty());
    when(actions.next()).thenAnswer(ignored -> remainingActions.remove(0));
    LogStore store = mock(LogStore.class);
    Configuration conf = defaultKernelContext.materializeHadoopConf();
    when(store.readAsIterator(any(FileStatus.class), same(conf)))
        .thenReturn(ScalaUtils.toClosableIterator(actions));

    withSQLConf(
        DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD().key(),
        "0",
        () -> {
          DeltaV2StreamingBackend backend =
              collectorBackend(mockTableManager(commits, true, store, conf), 0L);
          Seq<?> result =
              metadata
                  ? DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                      backend, 7L, Option.apply(7L))
                  : DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                      backend, 7L, Option.apply(7L));
          assertTrue(remainingActions.isEmpty(), "Returned before reading all matching actions");
          verify(actions).close();
          verify(commits).close();
          assertEquals(
              List.of(new Tuple2<>(7L, first), new Tuple2<>(7L, second)),
              CollectionConverters.asJava(result));
        });
  }

  @Test
  public void testProtocolCollector_preservesIteratorReadFailureWhenClosureFails()
      throws IOException {
    IOException readFailure = new IOException("commit iterator read failed");
    IOException closeFailure = new IOException("commit iterator close failed");
    CloseableIterator<CommitFileStatus> commits = mockIterator();
    when(commits.hasNext())
        .thenAnswer(
            ignored -> {
              throw readFailure;
            });
    doThrow(closeFailure).when(commits).close();

    IOException error =
        assertThrows(
            IOException.class,
            () ->
                DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                    collectorBackend(mockTableManager(commits, true), 0L), 7L, Option.apply(7L)));

    assertSame(readFailure, error);
    assertArrayEquals(new Throwable[] {closeFailure}, readFailure.getSuppressed());
    verify(commits).close();
  }

  @Test
  public void testProtocolCollector_closesFileIteratorWhenOpeningActionsFails() throws Exception {
    IOException readFailure = new IOException("action iterator open failed");
    CommitFileStatus kernelCommit =
        new CommitFileStatus(
            7L, // version
            io.delta.kernel.utils.FileStatus.of("/table/7.json", 1L, 0L));
    CloseableIterator<CommitFileStatus> commits = mockIterator();
    when(commits.hasNext()).thenReturn(true, false);
    when(commits.next()).thenReturn(kernelCommit);
    LogStore store = mock(LogStore.class);
    Configuration conf = defaultKernelContext.materializeHadoopConf();
    when(store.readAsIterator(any(FileStatus.class), same(conf)))
        .thenAnswer(
            ignored -> {
              throw readFailure;
            });

    withSQLConf(
        DeltaSQLConf.LOG_SIZE_IN_MEMORY_THRESHOLD().key(),
        "0",
        () -> {
          IOException error =
              assertThrows(
                  IOException.class,
                  () ->
                      DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                          collectorBackend(mockTableManager(commits, true, store, conf), 0L),
                          7L,
                          Option.apply(7L)));
          assertSame(readFailure, error);
        });

    verify(commits).close();
  }

  private void setupTableWithMetadataChangeAtV2(String testTablePath, String testTableName) {
    createEmptyTestTable(testTablePath, testTableName); // v0
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", testTableName)); // v1
    spark.sql(String.format("ALTER TABLE %s ADD COLUMNS (c3 INT)", testTableName)); // v2
  }

  private static Stream<Arguments> collectMetadataTestCases() {
    return Stream.of(
        // scenario, startVersion, endVersionOpt, expectedVersions
        Arguments.of("fullRange_endInclusive", 0L, Optional.of(2L), Set.of(0L, 2L)),
        Arguments.of("nonTrivialStartExcludesV0", 1L, Optional.of(2L), Set.of(2L)),
        Arguments.of("emptyEndReachesLatest", 0L, Optional.empty(), Set.of(0L, 2L)),
        Arguments.of("rangeWithoutMetadataChange", 1L, Optional.of(1L), Set.of()),
        Arguments.of("endBeforeChange", 0L, Optional.of(1L), Set.of(0L)),
        Arguments.of("emptyEndWithNonTrivialStart", 1L, Optional.empty(), Set.of(2L)),
        Arguments.of("singleVersionAtChange", 2L, Optional.of(2L), Set.of(2L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("collectMetadataTestCases")
  public void testCollectMetadataActions(
      String scenario,
      long startVersion,
      Optional<Long> endVersionOpt,
      Set<Long> expectedVersions,
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_metadata_" + scenario;
    setupTableWithMetadataChangeAtV2(testTablePath, testTableName);
    DeltaV2TableManager tableManager =
        DeltaV2TableManagerCache$.MODULE$.forTable(
            spark, testTablePath, Collections.emptyMap(), Option.empty());

    Map<Long, Metadata> result =
        toActionMap(
            DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                collectorBackend(tableManager, 2L),
                startVersion,
                ScalaUtils.toScalaOption(endVersionOpt.map(v -> (Object) v))));

    assertArrayEquals(expectedVersions.stream().sorted().toArray(), result.keySet().toArray());
  }

  /** Verifies that the version-to-Metadata mapping returns the correct Metadata content. */
  @Test
  public void testCollectMetadataActions_returnsCorrectMetadataPerVersion(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_metadata_content";
    setupTableWithMetadataChangeAtV2(testTablePath, testTableName);
    DeltaV2TableManager tableManager =
        DeltaV2TableManagerCache$.MODULE$.forTable(
            spark, testTablePath, Collections.emptyMap(), Option.empty());

    Map<Long, Metadata> result =
        toActionMap(
            DeltaSourceMetadataEvolutionSupport$.MODULE$.collectMetadataActions(
                collectorBackend(tableManager, 2L), 0L, Option.apply(2L)));

    StructType expectedV0Schema =
        new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
    StructType expectedV2Schema = expectedV0Schema.add("c3", DataTypes.IntegerType);
    assertEquals(expectedV0Schema, result.get(0L).schema());
    assertEquals(expectedV2Schema, result.get(2L).schema());
  }

  // ---------------------------------------------------------------------------
  // collectProtocolActions
  //
  // Fixture for parameterized cases below: v0 CREATE, v1 INSERT, v2 SET TBLPROPERTIES (upgrade).
  // Versions with a Protocol action: {v0, v2}. v1 has none.
  // ---------------------------------------------------------------------------

  private void setupTableWithProtocolUpgradeAtV2(String testTablePath, String testTableName) {
    createEmptyTestTable(testTablePath, testTableName); // v0
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'Alice')", testTableName)); // v1
    // Adding a table feature forces a real protocol upgrade; setting only minReader/minWriter to
    // (3, 7) is normalized back to (1, 2) by Protocol.merge when no features are introduced and
    // produces no Protocol action.
    spark.sql(
        String.format(
            "ALTER TABLE %s SET TBLPROPERTIES " + "('delta.feature.deletionVectors' = 'supported')",
            testTableName)); // v2
  }

  private static Stream<Arguments> collectProtocolTestCases() {
    return Stream.of(
        // scenario, startVersion, endVersionOpt, expectedVersions
        Arguments.of("fullRange_endInclusive", 0L, Optional.of(2L), Set.of(0L, 2L)),
        Arguments.of("nonTrivialStartExcludesV0", 1L, Optional.of(2L), Set.of(2L)),
        Arguments.of("emptyEndReachesLatest", 0L, Optional.empty(), Set.of(0L, 2L)),
        Arguments.of("rangeWithoutProtocolChange", 1L, Optional.of(1L), Set.of()),
        Arguments.of("endBeforeChange", 0L, Optional.of(1L), Set.of(0L)),
        Arguments.of("emptyEndWithNonTrivialStart", 1L, Optional.empty(), Set.of(2L)),
        Arguments.of("singleVersionAtChange", 2L, Optional.of(2L), Set.of(2L)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("collectProtocolTestCases")
  public void testCollectProtocolActions(
      String scenario,
      long startVersion,
      Optional<Long> endVersionOpt,
      Set<Long> expectedVersions,
      @TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_protocol_" + scenario;
    setupTableWithProtocolUpgradeAtV2(testTablePath, testTableName);
    DeltaV2TableManager tableManager =
        DeltaV2TableManagerCache$.MODULE$.forTable(
            spark, testTablePath, Collections.emptyMap(), Option.empty());

    Map<Long, Protocol> result =
        toActionMap(
            DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                collectorBackend(tableManager, 2L),
                startVersion,
                ScalaUtils.toScalaOption(endVersionOpt.map(v -> (Object) v))));

    assertArrayEquals(expectedVersions.stream().sorted().toArray(), result.keySet().toArray());
  }

  /** Verifies that the version-to-Protocol mapping returns the correct Protocol content. */
  @Test
  public void testCollectProtocolActions_returnsCorrectProtocolPerVersion(@TempDir File tempDir) {
    String testTablePath = tempDir.getAbsolutePath();
    String testTableName = "test_collect_protocol_content";
    setupTableWithProtocolUpgradeAtV2(testTablePath, testTableName);
    DeltaV2TableManager tableManager =
        DeltaV2TableManagerCache$.MODULE$.forTable(
            spark, testTablePath, Collections.emptyMap(), Option.empty());

    Map<Long, Protocol> result =
        toActionMap(
            DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                collectorBackend(tableManager, 2L), 0L, Option.apply(2L)));

    // v0 default: reader=1, writer=2; v2 upgraded: reader=3, writer=7
    assertEquals(1, result.get(0L).minReaderVersion());
    assertEquals(2, result.get(0L).minWriterVersion());
    assertEquals(3, result.get(2L).minReaderVersion());
    assertEquals(7, result.get(2L).minWriterVersion());
  }

  @ParameterizedTest
  @CsvSource({
    "metadata, unsupportedVersion",
    "protocol, unsupportedVersion",
    "metadata, unsupportedReaderFeature",
    "protocol, unsupportedReaderFeature",
    "metadata, writerOnlyFeature",
    "protocol, writerOnlyFeature"
  })
  void collectorsReadHistoricalProtocolsWithoutValidation(
      String actionType, String scenario, @TempDir File tempDir) throws Exception {
    String tablePath = tempDir.getAbsolutePath();
    createEmptyTestTable(tablePath, "collector_protocol_" + System.nanoTime());
    DeltaV2TableManager manager =
        DeltaV2TableManagerCache$.MODULE$.forTable(
            spark, tablePath, Collections.emptyMap(), Option.empty());
    Snapshot snapshot = manager.loadLatestSnapshot(queryContext);
    Protocol protocol;
    if (scenario.equals("unsupportedVersion")) {
      protocol = Protocol$.MODULE$.apply(99, 7);
    } else {
      String readerFeatures =
          scenario.equals("unsupportedReaderFeature") ? "[\"unknownFeature\"]" : "[]";
      protocol =
          (Protocol)
              Action.fromJson(
                  "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":"
                      + readerFeatures
                      + ",\"writerFeatures\":[\"unknownFeature\"]}}");
    }
    Files.writeString(
        new File(tempDir, "_delta_log/00000000000000000001.json").toPath(), protocol.json() + "\n");
    // Keep the latest snapshot readable while collecting the raw historical actions.
    Files.writeString(
        new File(tempDir, "_delta_log/00000000000000000002.json").toPath(),
        snapshot.protocol().json() + "\n");

    DeltaV2StreamingBackend backend = collectorBackend(manager, 0L);
    if (actionType.equals("metadata")) {
      assertTrue(
          DeltaSourceMetadataEvolutionSupport$.MODULE$
              .collectMetadataActions(backend, 1L, Option.apply(1L))
              .isEmpty());
      assertTrue(
          DeltaSourceMetadataEvolutionSupport$.MODULE$
              .collectMetadataAtVersion(backend, 1L)
              .isEmpty());
    } else {
      assertEquals(
          Map.of(1L, protocol),
          toActionMap(
              DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolActions(
                  backend, 1L, Option.apply(1L))));
      assertEquals(
          protocol,
          DeltaSourceMetadataEvolutionSupport$.MODULE$.collectProtocolAtVersion(backend, 1L).get());
    }
  }
}
