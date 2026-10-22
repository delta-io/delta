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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Collections;
import org.apache.spark.sql.delta.DeltaUnsupportedTableFeatureException;
import org.apache.spark.sql.delta.VersionNotFoundException;
import org.apache.spark.sql.delta.v2.DeltaV2TestBase;
import org.apache.spark.sql.delta.v2.snapshot.PathBasedSnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link KernelStreamingBackend}. */
public class KernelStreamingBackendTest extends DeltaV2TestBase {

  private static String createTable(File dir, int commits) {
    String path = dir.getAbsolutePath();
    for (int i = 0; i < commits; i++) {
      spark.range(1).write().format("delta").mode("append").save(path);
    }
    return path;
  }

  @Test
  public void unsafeVolatileSnapshotIsAtLeastAsNewAsActiveCommit(@TempDir File tempDir) {
    String path = createTable(tempDir, /* commits= */ 1);
    KernelStreamingBackend backend =
        new KernelStreamingBackend(new PathBasedSnapshotManager(path, defaultKernelContext));
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
    KernelStreamingBackend backend =
        new KernelStreamingBackend(new PathBasedSnapshotManager(path, defaultKernelContext));

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
    KernelStreamingBackend backend =
        new KernelStreamingBackend(new PathBasedSnapshotManager(path, defaultKernelContext));

    DeltaUnsupportedTableFeatureException e =
        assertThrows(
            DeltaUnsupportedTableFeatureException.class,
            () -> backend.getSnapshotToValidateProtocol(/* version= */ 1L));
    assertEquals("DELTA_UNSUPPORTED_FEATURES_FOR_READ", e.getErrorClass());
    assertTrue(e.getMessage().contains(feature), e.getMessage());
  }
}
