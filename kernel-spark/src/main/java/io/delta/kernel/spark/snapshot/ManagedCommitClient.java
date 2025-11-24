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

import io.delta.kernel.CommitRange;
import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import java.util.Optional;

/**
 * Catalog-managed commit client that knows how to load snapshots and commit ranges for a specific
 * table.
 */
public interface ManagedCommitClient extends AutoCloseable {

  /** @return catalog-managed table identifier (for logging/telemetry). */
  String getTableId();

  /** @return physical table path used by Delta Kernel. */
  String getTablePath();

  Snapshot loadSnapshot(
      Engine engine, Optional<Long> versionOpt, Optional<Long> timestampOpt);

  CommitRange loadCommitRange(
      Engine engine,
      Optional<Long> startVersionOpt,
      Optional<Long> startTimestampOpt,
      Optional<Long> endVersionOpt,
      Optional<Long> endTimestampOpt);

  @Override
  void close();
}
