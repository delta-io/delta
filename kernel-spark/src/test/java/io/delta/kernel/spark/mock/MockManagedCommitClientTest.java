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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import io.delta.kernel.Snapshot;
import java.util.Optional;
import org.junit.Test;

/** Tests for MockManagedCommitClient to verify the ManagedCommitClient interface contract. */
public class MockManagedCommitClientTest {

  @Test
  public void testAddAndGetSnapshot() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    Snapshot mockSnapshot = mock(Snapshot.class);
    when(mockSnapshot.getVersion()).thenReturn(5L);

    // Add snapshot
    client.addSnapshot("table123", 5L, mockSnapshot);

    // Get snapshot by version
    Snapshot result =
        client.getSnapshot(
            null, // engine not used in mock
            "table123",
            "/path/to/table",
            Optional.of(5L),
            Optional.empty());

    assertEquals(5L, result.getVersion());
    verify(mockSnapshot).getVersion();
  }

  @Test
  public void testGetLatestSnapshot() {
    MockManagedCommitClient client = new MockManagedCommitClient();

    Snapshot snapshot3 = mock(Snapshot.class);
    Snapshot snapshot5 = mock(Snapshot.class);
    Snapshot snapshot7 = mock(Snapshot.class);

    when(snapshot3.getVersion()).thenReturn(3L);
    when(snapshot5.getVersion()).thenReturn(5L);
    when(snapshot7.getVersion()).thenReturn(7L);

    // Add multiple snapshots
    client.addSnapshot("table123", 3L, snapshot3);
    client.addSnapshot("table123", 5L, snapshot5);
    client.addSnapshot("table123", 7L, snapshot7);

    // Get latest version
    long latest = client.getLatestVersion("table123");
    assertEquals(7L, latest);

    // Get latest snapshot (no version specified)
    Snapshot result =
        client.getSnapshot(
            null,
            "table123",
            "/path/to/table",
            Optional.empty(), // no specific version = latest
            Optional.empty());

    assertEquals(7L, result.getVersion());
  }

  @Test
  public void testVersionExists() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    Snapshot mockSnapshot = mock(Snapshot.class);

    client.addSnapshot("table123", 5L, mockSnapshot);

    assertTrue(client.versionExists("table123", 5L));
    assertFalse(client.versionExists("table123", 3L));
    assertFalse(client.versionExists("table123", 7L));
    assertFalse(client.versionExists("nonexistent", 5L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSnapshot_TableNotFound() {
    MockManagedCommitClient client = new MockManagedCommitClient();

    client.getSnapshot(null, "nonexistent", "/path", Optional.of(5L), Optional.empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSnapshot_VersionNotFound() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    Snapshot mockSnapshot = mock(Snapshot.class);

    client.addSnapshot("table123", 5L, mockSnapshot);

    // Try to get non-existent version
    client.getSnapshot(null, "table123", "/path", Optional.of(10L), Optional.empty());
  }

  @Test
  public void testGetLatestVersion_EmptyTable() {
    MockManagedCommitClient client = new MockManagedCommitClient();

    long latest = client.getLatestVersion("nonexistent");
    assertEquals(-1L, latest);
  }

  @Test
  public void testClose() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    Snapshot mockSnapshot = mock(Snapshot.class);

    client.addSnapshot("table123", 5L, mockSnapshot);
    assertTrue(client.versionExists("table123", 5L));

    // Close should clear state
    client.close();
    assertFalse(client.versionExists("table123", 5L));
  }
}
