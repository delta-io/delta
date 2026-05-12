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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/** Cached sorted AddFile DataFrame, keyed by version. Callers synchronize externally. */
public class DataFrameSnapshotCache implements AutoCloseable {

  private final long version;
  private Dataset<Row> sortedAddFiles;

  public DataFrameSnapshotCache(long version, Dataset<Row> sortedAddFiles) {
    this.version = version;
    this.sortedAddFiles = sortedAddFiles;
  }

  public long getVersion() {
    return version;
  }

  public Dataset<Row> getSortedAddFiles() {
    return sortedAddFiles;
  }

  @Override
  public void close() {
    Dataset<Row> df = sortedAddFiles;
    if (df != null) {
      df.unpersist();
      sortedAddFiles = null;
    }
  }
}
