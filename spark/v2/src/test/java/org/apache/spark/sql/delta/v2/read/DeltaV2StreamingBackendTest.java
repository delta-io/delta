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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.delta.DeltaSingleCommit;
import org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException;
import org.apache.spark.sql.delta.VersionNotFoundException;
import org.apache.spark.sql.delta.sources.DeltaSQLConf;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;
import org.apache.spark.sql.delta.storage.ClosableIterator;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext$;
import org.apache.spark.sql.delta.v2.interop.DeltaV2SingleCommit;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManagerCache$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Option;

/** Unit tests for {@link DeltaV2StreamingBackend}. */
public class DeltaV2StreamingBackendTest extends DeltaV2TestBase {

  private final DeltaV2QueryContext queryContext = DeltaV2QueryContext$.MODULE$.empty();

  @AfterEach
  void clearTableManagerCache() {
    DeltaV2TableManagerCache$.MODULE$.clearCache();
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
        try (DeltaV2SingleCommit commit = (DeltaV2SingleCommit) commits.next()) {
          versions.add(commit.version());
        }
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
            /* snapshotVersionAtSourceInit= */ 3L);
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
            /* snapshotVersionAtSourceInit= */ 2L);
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
            /* snapshotVersionAtSourceInit= */ 0L);
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
                  /* snapshotVersionAtSourceInit= */ 0L);
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
                  /* snapshotVersionAtSourceInit= */ 0L);

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
  public void unsafeVolatileSnapshotIsAtLeastAsNewAsActiveCommit(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 1);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 0L);
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
  public void checkVersionExistsThrowsDeltaVersionNotFound(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 3);
    DeltaV2StreamingBackend backend =
        new DeltaV2StreamingBackend(
            DeltaV2TableManagerCache$.MODULE$.forTable(
                spark, path, Collections.emptyMap(), Option.empty()),
            queryContext,
            /* snapshotVersionAtSourceInit= */ 2L);

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
            /* snapshotVersionAtSourceInit= */ 0L);

    DeltaUnsupportedTableFeatureException e =
        assertThrows(
            DeltaUnsupportedTableFeatureException.class,
            () -> backend.getSnapshotToValidateProtocol(/* version= */ 1L));
    assertEquals("DELTA_UNSUPPORTED_FEATURES_FOR_READ", e.getErrorClass());
    assertTrue(e.getMessage().contains(feature), e.getMessage());
  }
}
