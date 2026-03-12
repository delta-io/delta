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
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * DSv2 WriteBuilder for the Delta Kernel-based table. Supports append-only batch writes via {@link
 * #buildForBatch()}. Overwrite/truncate are not supported in the first version.
 */
public class DeltaKernelWriteBuilder implements WriteBuilder {

  private static final String ENGINE_INFO = "Spark-Delta-Kernel";

  private final String tablePath;
  private final Configuration hadoopConf;
  private final io.delta.kernel.Snapshot initialSnapshot;
  private final StructType writeSchema;
  private final String queryId;
  private final Map<String, String> options;
  private final List<String> partitionColumnNames;

  public DeltaKernelWriteBuilder(SparkTable table, LogicalWriteInfo writeInfo) {
    this.tablePath = table.getTablePath();
    this.hadoopConf = table.getHadoopConf();
    this.initialSnapshot = table.getInitialSnapshot();
    this.writeSchema = writeInfo.schema();
    this.queryId = writeInfo.queryId();
    this.options = writeInfo.options().asCaseSensitiveMap();
    this.partitionColumnNames = table.getPartitionColumnNames();
  }

  @Override
  public DeltaKernelBatchWrite buildForBatch() {
    return new DeltaKernelBatchWrite(
        tablePath,
        hadoopConf,
        initialSnapshot,
        writeSchema,
        queryId,
        options,
        partitionColumnNames);
  }
}
