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

package io.delta.spark.internal.v2.write

import io.delta.spark.internal.v2.catalog.SparkTable
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}

/**
 * Option B (Spark/Delta Parquet path) WriteBuilder.
 * Uses Spark's Parquet writer for data; Kernel only for commit.
 * See docs/dsv2-context/06-write-path-design-kernel-vs-spark-parquet.md.
 */
class DeltaSparkParquetWriteBuilder(table: SparkTable, writeInfo: LogicalWriteInfo)
    extends WriteBuilder {

  override def buildForBatch(): DeltaSparkParquetBatchWrite = {
    new DeltaSparkParquetBatchWrite(
      tablePath = table.getTablePath,
      hadoopConf = table.getHadoopConf,
      initialSnapshot = table.getInitialSnapshot,
      writeSchema = writeInfo.schema(),
      queryId = writeInfo.queryId(),
      options = writeInfo.options().asCaseSensitiveMap(),
      partitionColumnNames = table.getPartitionColumnNames
    )
  }
}
