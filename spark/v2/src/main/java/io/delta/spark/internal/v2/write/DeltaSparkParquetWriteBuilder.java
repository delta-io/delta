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
package io.delta.spark.internal.v2.write;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Snapshot;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * WriteBuilder for DSv2 batch writes to Delta tables. Mirrors the read-side {@code
 * SparkScanBuilder} pattern: takes table-level state and Spark's {@link LogicalWriteInfo},
 * validates the write schema, and builds a {@link Write} whose {@code toBatch()} produces a {@link
 * DeltaSparkParquetBatchWrite}.
 */
public class DeltaSparkParquetWriteBuilder implements WriteBuilder {

  private final String tablePath;
  private final Configuration hadoopConf;
  private final Snapshot initialSnapshot;
  private final LogicalWriteInfo writeInfo;

  public DeltaSparkParquetWriteBuilder(
      String tablePath,
      Configuration hadoopConf,
      Snapshot initialSnapshot,
      LogicalWriteInfo writeInfo) {
    this.tablePath = requireNonNull(tablePath, "tablePath is null");
    this.hadoopConf = requireNonNull(hadoopConf, "hadoopConf is null");
    this.initialSnapshot = requireNonNull(initialSnapshot, "initialSnapshot is null");
    this.writeInfo = requireNonNull(writeInfo, "writeInfo is null");
  }

  @Override
  public Write build() {
    StructType tableSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(initialSnapshot.getSchema());
    StructType writeSchema = writeInfo.schema();
    validateWriteSchema(writeSchema, tableSchema);

    Map<String, String> options = writeInfo.options().asCaseSensitiveMap();

    return new Write() {
      @Override
      public BatchWrite toBatch() {
        try {
          return new DeltaSparkParquetBatchWrite(hadoopConf, initialSnapshot, options);
        } catch (IOException e) {
          throw new UncheckedIOException("Failed to create batch write", e);
        }
      }
    };
  }

  /**
   * Validates that the write schema is compatible with the table schema. Currently checks that all
   * top-level fields in the write schema exist in the table schema. Full schema evolution and type
   * widening support will be added in follow-up PRs.
   */
  private static void validateWriteSchema(StructType writeSchema, StructType tableSchema) {
    for (org.apache.spark.sql.types.StructField writeField : writeSchema.fields()) {
      if (tableSchema.getFieldIndex(writeField.name()).isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "Field '%s' in write schema does not exist in table schema. " + "Table columns: %s",
                writeField.name(), String.join(", ", tableSchema.fieldNames())));
      }
    }
  }
}
