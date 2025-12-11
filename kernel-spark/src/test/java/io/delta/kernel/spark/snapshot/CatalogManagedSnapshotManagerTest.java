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
package io.delta.kernel.spark.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.spark.exception.VersionNotFoundException;
import io.delta.kernel.spark.mock.MockManagedCommitClient;
import io.delta.kernel.spark.mock.MockSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CatalogManagedSnapshotManager}.
 *
 * <p>Uses MockManagedCommitClient to validate that the snapshot manager correctly delegates all
 * operations to the catalog client without any catalog-specific logic.
 */
public class CatalogManagedSnapshotManagerTest {

  private MockManagedCommitClient mockClient;
  private CatalogManagedSnapshotManager snapshotManager;
  private Engine engine;

  private static final String TABLE_ID = "catalog.schema.test_table";
  private static final String TABLE_PATH = "/test/table/path";

  @BeforeEach
  public void setUp() {
    mockClient = new MockManagedCommitClient();
    engine = DefaultEngine.create(new Configuration());
    snapshotManager = new CatalogManagedSnapshotManager(mockClient, TABLE_ID, TABLE_PATH, engine);
  }

  @Test
  public void testLoadLatestSnapshot() {
    // Setup: Add snapshots to mock client
    Snapshot snapshot0 = new MockSnapshot(0L);
    Snapshot snapshot1 = new MockSnapshot(1L);
    Snapshot snapshot2 = new MockSnapshot(2L);

    mockClient.addSnapshot(TABLE_ID, 0L, snapshot0);
    mockClient.addSnapshot(TABLE_ID, 1L, snapshot1);
    mockClient.addSnapshot(TABLE_ID, 2L, snapshot2);

    // Act: Load latest snapshot
    Snapshot latest = snapshotManager.loadLatestSnapshot();

    // Assert: Should return the highest version
    assertNotNull(latest);
    assertEquals(2L, latest.getVersion());
  }

  @Test
  public void testLoadSnapshotAt() {
    // Setup: Add snapshots to mock client
    mockClient.addSnapshot(TABLE_ID, 0L, new MockSnapshot(0L));
    mockClient.addSnapshot(TABLE_ID, 1L, new MockSnapshot(1L));
    mockClient.addSnapshot(TABLE_ID, 2L, new MockSnapshot(2L));

    // Act: Load specific versions
    Snapshot snapshot0 = snapshotManager.loadSnapshotAt(0L);
    Snapshot snapshot1 = snapshotManager.loadSnapshotAt(1L);
    Snapshot snapshot2 = snapshotManager.loadSnapshotAt(2L);

    // Assert: Each version should be loaded correctly
    assertEquals(0L, snapshot0.getVersion());
    assertEquals(1L, snapshot1.getVersion());
    assertEquals(2L, snapshot2.getVersion());
  }

  @Test
  public void testLoadSnapshotAt_negativeVersion() {
    // Act & Assert: Negative version should throw exception
    assertThrows(IllegalArgumentException.class, () -> snapshotManager.loadSnapshotAt(-1L));
  }

  @Test
  public void testCheckVersionExists_exists() {
    // Setup: Add snapshot at version 5
    mockClient.addSnapshot(TABLE_ID, 5L, new MockSnapshot(5L));

    // Act & Assert: Should not throw exception
    snapshotManager.checkVersionExists(5L, false, false);
  }

  @Test
  public void testCheckVersionExists_notExists() {
    // Setup: Add snapshots at versions 0, 1, 2
    mockClient.addSnapshot(TABLE_ID, 0L, new MockSnapshot(0L));
    mockClient.addSnapshot(TABLE_ID, 1L, new MockSnapshot(1L));
    mockClient.addSnapshot(TABLE_ID, 2L, new MockSnapshot(2L));

    // Act & Assert: Version 10 doesn't exist, should throw exception
    VersionNotFoundException exception =
        assertThrows(
            VersionNotFoundException.class,
            () -> snapshotManager.checkVersionExists(10L, false, false));

    assertEquals(10L, exception.getUserVersion());
    assertEquals(2L, exception.getLatest());
  }

  @Test
  public void testGetActiveCommitAtTime_notSupported() {
    // Act & Assert: getActiveCommitAtTime should throw UnsupportedOperationException
    long timestamp = System.currentTimeMillis();
    assertThrows(
        UnsupportedOperationException.class,
        () -> snapshotManager.getActiveCommitAtTime(timestamp, false, false, false));
  }

  @Test
  public void testGetTableChanges_notSupported() {
    // Act & Assert: getTableChanges should throw UnsupportedOperationException
    assertThrows(
        UnsupportedOperationException.class,
        () -> snapshotManager.getTableChanges(engine, 0L, java.util.Optional.empty()));
  }
}
