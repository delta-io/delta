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

package io.delta.kernel.spark.mock;

import io.delta.kernel.Operation;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.commit.PublishFailedException;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.statistics.SnapshotStatistics;
import io.delta.kernel.transaction.UpdateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Simple mock snapshot for testing. */
public class MockSnapshot implements Snapshot {

  private final long version;
  private final String path;

  public MockSnapshot(long version) {
    this(version, "/tmp/mock");
  }

  public MockSnapshot(long version, String path) {
    this.version = version;
    this.path = path;
  }

  @Override
  public String getPath() {
    return path;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public List<String> getPartitionColumnNames() {
    return Collections.emptyList();
  }

  @Override
  public long getTimestamp(Engine engine) {
    throw new UnsupportedOperationException("Not implemented for mock");
  }

  @Override
  public StructType getSchema() {
    throw new UnsupportedOperationException("Not implemented for mock");
  }

  @Override
  public Optional<String> getDomainMetadata(String domain) {
    return Optional.empty();
  }

  @Override
  public Map<String, String> getTableProperties() {
    return Collections.emptyMap();
  }

  @Override
  public SnapshotStatistics getStatistics() {
    return () -> Optional.empty();
  }

  @Override
  public ScanBuilder getScanBuilder() {
    throw new UnsupportedOperationException("Not implemented for mock");
  }

  @Override
  public UpdateTableTransactionBuilder buildUpdateTableTransaction(
      String engineInfo, Operation operation) {
    throw new UnsupportedOperationException("Not implemented for mock");
  }

  @Override
  public void publish(Engine engine) throws PublishFailedException {
    throw new UnsupportedOperationException("Not implemented for mock");
  }

  @Override
  public void writeChecksum(Engine engine, ChecksumWriteMode mode) throws IOException {
    throw new UnsupportedOperationException("Not implemented for mock");
  }
}
