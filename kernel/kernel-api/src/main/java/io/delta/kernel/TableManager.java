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

package io.delta.kernel;

import io.delta.kernel.annotation.Experimental;
import io.delta.kernel.internal.table.SnapshotBuilderImpl;

/** The entry point to load and create Delta tables. */
@Experimental
public interface TableManager {

  /**
   * Creates a builder for loading a snapshot at the given path.
   *
   * <p>The returned builder can be configured to load the snapshot at a specific version or with
   * additional metadata to optimize the loading process.
   *
   * @param path the file system path to the Delta table
   * @return a {@link SnapshotBuilder} that can be used to load a {@link Snapshot} at the given path
   */
  static SnapshotBuilder loadSnapshot(String path) {
    return new SnapshotBuilderImpl(path);
  }

  // TODO: static CreateTableTransactionBuilder buildCreateTableTransaction(...)
}
