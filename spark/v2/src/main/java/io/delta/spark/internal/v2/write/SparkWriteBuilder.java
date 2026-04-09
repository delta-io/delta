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

import io.delta.kernel.expressions.Predicate;
import io.delta.spark.internal.v2.catalog.SparkTable;
import io.delta.spark.internal.v2.utils.ExpressionUtils;
import java.util.Optional;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite;
import org.apache.spark.sql.connector.write.SupportsOverwrite;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;

/**
 * DSv2 {@link WriteBuilder} for Delta tables.
 *
 * <p>Created during Spark's analysis phase via {@link SparkTable#newWriteBuilder}. Packages options
 * from the logical write plan and defers all heavy initialization (transaction, committer,
 * OutputWriterFactory) to {@link DeltaWrite#toBatch()}.
 *
 * <p>Implements {@link SupportsOverwrite}, {@link SupportsTruncate}, and {@link
 * SupportsDynamicOverwrite} to support INSERT OVERWRITE semantics.
 */
public class SparkWriteBuilder
    implements WriteBuilder, SupportsOverwrite, SupportsTruncate, SupportsDynamicOverwrite {

  private final SparkTable table;
  private final LogicalWriteInfo info;
  private WriteMode writeMode = WriteMode.APPEND;
  private Predicate replaceWherePredicate = null;

  public SparkWriteBuilder(SparkTable table, LogicalWriteInfo info) {
    this.table = requireNonNull(table, "table is null");
    this.info = requireNonNull(info, "info is null");
  }

  @Override
  public WriteBuilder truncate() {
    this.writeMode = WriteMode.TRUNCATE;
    return this;
  }

  @Override
  public WriteBuilder overwrite(Filter[] filters) {
    this.writeMode = WriteMode.REPLACE_WHERE;
    // Convert Spark Filter[] → single AND-combined Kernel Predicate.
    // Uses the same converter the read path uses for pushdown.
    Predicate combined = null;
    for (Filter f : filters) {
      ExpressionUtils.ConvertedPredicate cp =
          ExpressionUtils.convertSparkFilterToKernelPredicate(f);
      Optional<Predicate> converted = cp.getConvertedPredicate();
      if (converted.isEmpty()) {
        throw new UnsupportedOperationException(
            "Cannot convert overwrite filter to Kernel predicate: " + f);
      }
      if (combined == null) {
        combined = converted.get();
      } else {
        combined = new Predicate("AND", combined, converted.get());
      }
    }
    this.replaceWherePredicate = combined;
    return this;
  }

  @Override
  public WriteBuilder overwriteDynamicPartitions() {
    this.writeMode = WriteMode.DYNAMIC_OVERWRITE;
    return this;
  }

  @Override
  public Write build() {
    return new DeltaWrite(
        table.getInitialSnapshot(),
        table.getTablePath().toString(),
        table.getHadoopConf(),
        table.schema(),
        table.getDataSchema(),
        table.getPartitionSchema(),
        info,
        writeMode,
        replaceWherePredicate);
  }
}
