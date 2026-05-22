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
package io.delta.spark.internal.v2.read;

import io.delta.kernel.utils.CloseableIterator;

/**
 * Unified interface for serving initial-snapshot files to the streaming engine.
 *
 * <p>Implementations provide snapshot files either from a driver-local sorted list ({@link
 * DriverBasedIndexedFileSnapshotCache}) or from a distributed DataFrame ({@link
 * DataFrameBasedIndexedFileSnapshotCache}).
 *
 * <p>Contract for {@code getFiles(fromIndex)}: return snapshot files starting after {@code
 * fromIndex}. The iterator includes BEGIN/END sentinels for offset tracking. The {@code fromIndex}
 * parameter is a performance hint that implementations use to avoid unnecessary work; callers still
 * apply boundary filtering for correctness.
 */
public interface IndexedFileSnapshotCache extends AutoCloseable {

  /** Returns the snapshot version this source serves. */
  long getVersion();

  /** Returns the commit timestamp of the snapshot version. */
  long getCommitTimestamp();

  /**
   * Returns an iterator of {@link IndexedFile}s (with BEGIN/END sentinels) for the snapshot,
   * skipping files at or before {@code fromIndex} as a performance optimization.
   */
  CloseableIterator<IndexedFile> getFiles(long fromIndex);
}
