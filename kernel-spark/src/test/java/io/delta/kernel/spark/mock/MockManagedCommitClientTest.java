/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.kernel.spark.mock;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.Snapshot;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests for MockManagedCommitClient.
 *
 * <p>These tests verify the mock's storage/retrieval logic. We use a minimal SnapshotStub that only
 * implements getVersion() since that's all the mock client calls.
 */
public class MockManagedCommitClientTest {

  /** Minimal Snapshot stub for testing - only implements getVersion() */
  private static class SnapshotStub implements Snapshot {
    private final long version;

    SnapshotStub(long version) {
      this.version = version;
    }

    @Override
    public long getVersion() {
      return version;
    }

    // All other methods throw UnsupportedOperationException - they're not used by the mock
    @Override
    public String getPath() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.types.StructType getSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.List<String> getPartitionColumnNames() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getTimestamp(io.delta.kernel.engine.Engine engine) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Optional<String> getDomainMetadata(String domain) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.Map<String, String> getTableProperties() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.statistics.SnapshotStatistics getStatistics() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.ScanBuilder getScanBuilder() {
      throw new UnsupportedOperationException();
    }

    @Override
    public io.delta.kernel.transaction.UpdateTableTransactionBuilder buildUpdateTableTransaction(
        String engineInfo, io.delta.kernel.Operation operation) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void publish(io.delta.kernel.engine.Engine engine) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void writeChecksum(
        io.delta.kernel.engine.Engine engine, Snapshot.ChecksumWriteMode mode) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void testAddAndRetrieveSnapshot() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    Snapshot snapshot = new SnapshotStub(5L);

    client.addSnapshot("table1", 5L, snapshot);

    Snapshot result =
        client.getSnapshot(null, "table1", "/path", Optional.of(5L), Optional.empty());

    assertSame(snapshot, result, "Should return the same snapshot object");
    assertEquals(5L, result.getVersion());
  }

  @Test
  public void testGetLatestVersion() {
    MockManagedCommitClient client = new MockManagedCommitClient();

    client.addSnapshot("table1", 3L, new SnapshotStub(3L));
    client.addSnapshot("table1", 7L, new SnapshotStub(7L));
    client.addSnapshot("table1", 5L, new SnapshotStub(5L));

    assertEquals(7L, client.getLatestVersion("table1"), "Should return highest version number");
  }

  @Test
  public void testVersionExists() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    client.addSnapshot("table1", 5L, new SnapshotStub(5L));

    assertTrue(client.versionExists("table1", 5L), "Added version should exist");
    assertFalse(client.versionExists("table1", 99L), "Non-existent version should not exist");
    assertFalse(client.versionExists("nonexistent", 5L), "Non-existent table should return false");
  }

  @Test
  public void testGetLatestVersion_EmptyTable() {
    MockManagedCommitClient client = new MockManagedCommitClient();

    assertEquals(
        -1L,
        client.getLatestVersion("nonexistent"),
        "Should return -1 for table with no snapshots");
  }

  @Test
  public void testGetSnapshot_TableNotFound() {
    MockManagedCommitClient client = new MockManagedCommitClient();

    assertThrows(
        IllegalArgumentException.class,
        () -> client.getSnapshot(null, "nonexistent", "/path", Optional.of(5L), Optional.empty()),
        "Should throw for non-existent table");
  }

  @Test
  public void testGetSnapshot_VersionNotFound() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    client.addSnapshot("table1", 5L, new SnapshotStub(5L));

    assertThrows(
        IllegalArgumentException.class,
        () -> client.getSnapshot(null, "table1", "/path", Optional.of(99L), Optional.empty()),
        "Should throw for non-existent version");
  }

  @Test
  public void testClose_ClearsState() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    client.addSnapshot("table1", 5L, new SnapshotStub(5L));

    assertTrue(client.versionExists("table1", 5L), "Version should exist before close");

    client.close();

    assertFalse(client.versionExists("table1", 5L), "Version should not exist after close");
  }
}
