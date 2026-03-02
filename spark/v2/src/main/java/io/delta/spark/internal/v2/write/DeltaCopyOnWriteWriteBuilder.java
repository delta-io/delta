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

import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.read.SparkScan;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * DSv2 WriteBuilder for Copy-on-Write row-level operations. Builds a {@link
 * DeltaCopyOnWriteBatchWrite} that knows which files were scanned (to generate RemoveFile actions)
 * and writes new data files (to generate AddFile actions).
 */
public class DeltaCopyOnWriteWriteBuilder implements WriteBuilder {

  private final SparkTable table;
  private final LogicalWriteInfo writeInfo;
  private final SparkScan configuredScan;
  private final RowLevelOperation.Command command;

  public DeltaCopyOnWriteWriteBuilder(
      SparkTable table,
      LogicalWriteInfo writeInfo,
      SparkScan configuredScan,
      RowLevelOperation.Command command) {
    this.table = table;
    this.writeInfo = writeInfo;
    this.configuredScan = configuredScan;
    this.command = command;
  }

  @Override
  public BatchWrite buildForBatch() {
    String tablePath = table.getTablePath();
    io.delta.kernel.Snapshot initialSnapshot = table.getInitialSnapshot();
    StructType writeSchema = writeInfo.schema();
    String queryId = writeInfo.queryId();
    Map<String, String> options = writeInfo.options().asCaseSensitiveMap();
    List<String> partitionColumnNames = table.getPartitionColumnNames();

    return new DeltaCopyOnWriteBatchWrite(
        tablePath,
        table.getHadoopConf(),
        initialSnapshot,
        configuredScan,
        writeSchema,
        queryId,
        options,
        partitionColumnNames,
        command);
  }
}
