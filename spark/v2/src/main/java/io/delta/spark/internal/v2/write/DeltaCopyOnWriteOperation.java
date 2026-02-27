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
import io.delta.spark.internal.v2.read.SparkScanBuilder;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Copy-on-Write implementation of {@link RowLevelOperation} for Delta Lake.
 *
 * <p>Flow: Spark reads affected files via the scan, applies row-level modifications (DELETE /
 * UPDATE / MERGE), and writes the surviving rows back. At commit time, the old files (from the
 * scan) are removed and the new files are added atomically.
 */
public class DeltaCopyOnWriteOperation implements RowLevelOperation {

  private final SparkTable table;
  private final Command command;

  private SparkScan configuredScan;

  public DeltaCopyOnWriteOperation(SparkTable table, RowLevelOperationInfo info) {
    this.table = table;
    this.command = info.command();
  }

  @Override
  public Command command() {
    return command;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkScanBuilder(
        table.name(),
        table.getInitialSnapshot(),
        table.getSnapshotManager(),
        table.getDataSchema(),
        table.getPartitionSchema(),
        mergeOptionsWithTable(options)) {
      @Override
      public Scan build() {
        Scan scan = super.build();
        DeltaCopyOnWriteOperation.this.configuredScan = (SparkScan) scan;
        return scan;
      }
    };
  }

  private CaseInsensitiveStringMap mergeOptionsWithTable(CaseInsensitiveStringMap scanOptions) {
    java.util.Map<String, String> combined = new java.util.HashMap<>(table.getOptions());
    combined.putAll(scanOptions.asCaseSensitiveMap());
    return new CaseInsensitiveStringMap(combined);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new DeltaCopyOnWriteWriteBuilder(table, info, configuredScan, command);
  }
}
