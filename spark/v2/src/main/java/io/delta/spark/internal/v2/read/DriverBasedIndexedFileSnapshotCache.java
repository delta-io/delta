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

import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.List;
import org.apache.spark.sql.delta.sources.DeltaSourceOffset;

/**
 * Driver-local implementation of {@link IndexedFileSnapshotCache} that caches the initial snapshot
 * files to avoid re-sorting on repeated access. Backed by an in-memory sorted list of {@link
 * IndexedFile}s.
 *
 * <p>The files list structure is: {@code [BEGIN(BASE_INDEX=-1), file(0), file(1), ...,
 * END(END_INDEX)]}
 *
 * <p>Data file indices are 0-based, so the list offset for a data file with index {@code i} is
 * {@code i + 1} (accounting for the BEGIN sentinel at position 0).
 */
public class DriverBasedIndexedFileSnapshotCache implements IndexedFileSnapshotCache {

  private final long version;
  private final List<IndexedFile> files;
  private final long commitTimestamp;

  public DriverBasedIndexedFileSnapshotCache(
      long version, List<IndexedFile> files, long commitTimestamp) {
    this.version = version;
    this.files = files;
    this.commitTimestamp = commitTimestamp;
  }

  @Override
  public long getVersion() {
    return version;
  }

  @Override
  public long getCommitTimestamp() {
    return commitTimestamp;
  }

  @Override
  public CloseableIterator<IndexedFile> getFiles(long fromIndex) {
    if (fromIndex > DeltaSourceOffset.BASE_INDEX()) {
      // Skip past already-processed files. The list layout is:
      //   position 0: BEGIN sentinel (index = BASE_INDEX = -1)
      //   position 1: first data file (index = 0)
      //   position i+1: data file with index i
      //   last position: END sentinel (index = END_INDEX)
      //
      // To start after fromIndex (exclusive), the first included data file has index
      // fromIndex+1, at list position fromIndex+2. Clamp to files.size() for safety.
      int startPos = Math.min((int) fromIndex + 2, files.size());
      return Utils.toCloseableIterator(files.subList(startPos, files.size()).iterator());
    }
    return Utils.toCloseableIterator(files.iterator());
  }

  @Override
  public void close() {
    // No-op: the list is GC'd with the object.
  }
}
