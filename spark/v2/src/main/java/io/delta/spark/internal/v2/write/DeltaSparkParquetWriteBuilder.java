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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** WriteBuilder that prepares batch-write construction inputs for the DSv2 path. */
public class DeltaSparkParquetWriteBuilder implements WriteBuilder {
  private final String tablePath;
  private final Configuration hadoopConf;
  private final Snapshot initialSnapshot;
  private final StructType writeSchema;
  private final String queryId;
  private final Map<String, String> options;
  private final List<String> partitionColumnNames;

  public DeltaSparkParquetWriteBuilder(
      String tablePath,
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      StructType writeSchema,
      String queryId,
      Map<String, String> options,
      List<String> partitionColumnNames) {
    this.tablePath = requireNonNull(tablePath, "table path is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoop conf is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initial snapshot is null");
    this.writeSchema = requireNonNull(writeSchema, "write schema is null");
    this.queryId = requireNonNull(queryId, "query id is null");
    this.options = requireNonNull(options, "options is null");
    this.partitionColumnNames =
        requireNonNull(partitionColumnNames, "partition column names is null");
  }

  @Override
  public DeltaSparkParquetBatchWrite buildForBatch() {
    return new DeltaSparkParquetBatchWrite(
        tablePath,
        hadoopConf,
        initialSnapshot,
        writeSchema,
        queryId,
        options,
        partitionColumnNames);
  }
}
