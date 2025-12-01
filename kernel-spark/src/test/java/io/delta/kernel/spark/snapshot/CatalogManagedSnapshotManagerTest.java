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

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.files.ParsedLogData;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

/** Tests for {@link CatalogManagedSnapshotManager}. */
class CatalogManagedSnapshotManagerTest {

  @Test
  void testConstructor_NullHadoopConf_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> new CatalogManagedSnapshotManager(new NoOpClient(), "table-id", "/tmp/path", null),
        "Null hadoopConf should throw NullPointerException");
  }

  @Test
  void testConstructor_NullClient_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> new CatalogManagedSnapshotManager(null, "table-id", "/tmp/path", new Configuration()),
        "Null commitClient should throw NullPointerException");
  }

  private static final class NoOpClient implements ManagedCatalogAdapter {
    @Override
    public Snapshot loadSnapshot(
        Engine engine, Optional<Long> versionOpt, Optional<Long> timestampOpt) {
      throw new UnsupportedOperationException("noop");
    }

    @Override
    public CommitRange loadCommitRange(
        Engine engine,
        Optional<Long> startVersionOpt,
        Optional<Long> startTimestampOpt,
        Optional<Long> endVersionOpt,
        Optional<Long> endTimestampOpt) {
      throw new UnsupportedOperationException("noop");
    }

    @Override
    public List<ParsedLogData> getRatifiedCommits(Optional<Long> endVersionOpt) {
      throw new UnsupportedOperationException("noop");
    }

    @Override
    public long getLatestRatifiedVersion() {
      throw new UnsupportedOperationException("noop");
    }

    @Override
    public void close() {}
  }
}
