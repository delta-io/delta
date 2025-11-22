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
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructType;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Tests for MockManagedCommitClient to verify the ManagedCommitClient interface contract. */
public class MockManagedCommitClientTest {

  /** Simple fake Snapshot implementation for testing */
  private static class FakeSnapshot implements Snapshot {
    private final long version;

    FakeSnapshot(long version) {
      this.version = version;
    }

    @Override
    public long getVersion() {
      return version;
    }

    @Override
    public String getPath(Engine engine) {
      return "/fake/path";
    }

    @Override
    public StructType getSchema(Engine engine) {
      return new StructType();
    }

    @Override
    public Row getMetadata(Engine engine) {
      throw new UnsupportedOperationException("Not implemented in fake");
    }

    @Override
    public Row getProtocol(Engine engine) {
      throw new UnsupportedOperationException("Not implemented in fake");
    }

    @Override
    public ColumnarBatch getSetTransactions(Engine engine) {
      throw new UnsupportedOperationException("Not implemented in fake");
    }
  }

  @Test
  public void testGetSnapshot() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    Snapshot snapshot = new FakeSnapshot(5L);
    client.addSnapshot("table1", 5L, snapshot);

    Snapshot result = client.getSnapshot(null, "table1", "/path", Optional.of(5L), Optional.empty());
    assertEquals(5L, result.getVersion());
  }

  @Test
  public void testGetLatestVersion() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    client.addSnapshot("table1", 3L, new FakeSnapshot(3L));
    client.addSnapshot("table1", 7L, new FakeSnapshot(7L));

    assertEquals(7L, client.getLatestVersion("table1"));
  }

  @Test
  public void testVersionExists() {
    MockManagedCommitClient client = new MockManagedCommitClient();
    client.addSnapshot("table1", 5L, new FakeSnapshot(5L));

    assertTrue(client.versionExists("table1", 5L));
    assertFalse(client.versionExists("table1", 99L));
  }
}
