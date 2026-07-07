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
package io.delta.spark.internal.v2.write;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;
import io.delta.spark.internal.v2.read.DeltaScanFile;
import java.util.List;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/** WriteBuilder for Delta DSv2 ReplaceData commits backed by a configured SparkScan. */
class DeltaReplaceDataWriteBuilder implements WriteBuilder {

  private final Engine engine;
  private final String tablePath;
  private final Configuration hadoopConf;
  private final Snapshot initialSnapshot;
  private final Supplier<List<DeltaScanFile>> selectedFilesProvider;
  private final LogicalWriteInfo writeInfo;

  DeltaReplaceDataWriteBuilder(
      Engine engine,
      String tablePath,
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      Supplier<List<DeltaScanFile>> selectedFilesProvider,
      LogicalWriteInfo writeInfo) {
    this.engine = requireNonNull(engine, "engine is null");
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.selectedFilesProvider =
        requireNonNull(selectedFilesProvider, "selectedFilesProvider is null");
    this.writeInfo = requireNonNull(writeInfo, "writeInfo is null");
  }

  @Override
  public Write build() {
    DeltaV2WriteBuilder.validateDataSchema(initialSnapshot, writeInfo.schema());
    return new DeltaReplaceDataBatchWrite(
        engine, hadoopConf, tablePath, initialSnapshot, writeInfo, selectedFilesProvider);
  }
}
