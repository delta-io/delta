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
import io.delta.kernel.internal.table.ResolvedTableBuilderImpl;
import io.delta.kernel.internal.transaction.builder.CreateTableTransactionBuilderImpl;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;

/** The entry point to load and create {@link ResolvedTable}s. */
@Experimental
public interface TableManager {
  // TODO static ResolvedTable forPathAtLatest(Engine engine, String path);
  // TODO static ResolvedTable forPathAtVersion(Engine engine, String path, long version);
  // TODO static ResolvedTable forPathAtTimestamp(Engine engine, String path, long timestamp);

  // TODO: Take in a Committer for write support.

  /**
   * Creates a builder for loading a Delta table at the given path.
   *
   * <p>The returned builder can be configured to load the table at a specific version or with
   * additional metadata to optimize the loading process.
   *
   * @param path the file system path to the Delta table
   * @return a {@link ResolvedTableBuilder} that can be used to load a {@link ResolvedTable} at the
   *     given path
   */
  static ResolvedTableBuilder loadTable(String path) {
    return new ResolvedTableBuilderImpl(path);
  }

  static CreateTableTransactionBuilder createTable(String path, StructType schema) {
    return new CreateTableTransactionBuilderImpl(path, schema);
  }

  // TODO: static CreateTableTransactionBuilder buildCreateTableTransaction(...)
}
