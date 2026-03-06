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
package io.delta.spark.internal.v2.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Cache for the initial snapshot: stores the sorted DataFrame (persisted in Spark's managed
 * memory/disk) instead of a {@code List<IndexedFile>} in JVM heap. Each call to {@code
 * getSnapshotFiles()} creates a fresh iterator via {@code toLocalIterator()}.
 */
class InitialSnapshotCache {
  final long version;
  final Dataset<Row> sortedAddFiles;

  InitialSnapshotCache(long version, Dataset<Row> sortedAddFiles) {
    this.version = version;
    this.sortedAddFiles = sortedAddFiles;
  }

  void close() {
    sortedAddFiles.unpersist();
  }
}
