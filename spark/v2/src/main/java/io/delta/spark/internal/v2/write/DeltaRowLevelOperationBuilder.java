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
import io.delta.spark.internal.v2.catalog.DeltaV2Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.expressions.FieldReference;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Builds Delta's copy-on-write row-level operation stack. */
public class DeltaRowLevelOperationBuilder implements RowLevelOperationBuilder {

  private final DeltaV2Table table;
  private final Engine engine;
  private final Configuration hadoopConf;
  private final Snapshot initialSnapshot;
  private final RowLevelOperationInfo info;

  public DeltaRowLevelOperationBuilder(
      DeltaV2Table table,
      Engine engine,
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      RowLevelOperationInfo info) {
    this.table = requireNonNull(table, "table is null");
    this.engine = requireNonNull(engine, "engine is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.info = requireNonNull(info, "info is null");
  }

  @Override
  public RowLevelOperation build() {
    return new CopyOnWriteOperation(table, engine, hadoopConf, initialSnapshot, info);
  }

  private static class CopyOnWriteOperation implements RowLevelOperation {

    private final DeltaV2Table table;
    private final Engine engine;
    private final Configuration hadoopConf;
    private final Snapshot initialSnapshot;
    private final RowLevelOperationInfo info;

    private CopyOnWriteOperation(
        DeltaV2Table table,
        Engine engine,
        Configuration hadoopConf,
        Snapshot initialSnapshot,
        RowLevelOperationInfo info) {
      this.table = requireNonNull(table, "table is null");
      this.engine = requireNonNull(engine, "engine is null");
      this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
      this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
      this.info = requireNonNull(info, "info is null");
    }

    @Override
    public Command command() {
      return info.command();
    }

    @Override
    public String description() {
      return "DeltaCopyOnWrite(" + command() + ")";
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return table.newScanBuilder(options);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo writeInfo) {
      requireNonNull(writeInfo, "writeInfo is null");
      throw new UnsupportedOperationException(
          "Delta copy-on-write write support is introduced in a follow-up change");
    }

    @Override
    public NamedReference[] requiredMetadataAttributes() {
      return new NamedReference[] {FieldReference.apply(FileFormat$.MODULE$.METADATA_NAME())};
    }
  }
}
