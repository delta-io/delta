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
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;

/**
 * Builds a {@link RowLevelOperation} for Delta Lake. Currently only supports Copy-on-Write mode for
 * DELETE, UPDATE, and MERGE operations.
 */
public class DeltaRowLevelOperationBuilder implements RowLevelOperationBuilder {

  private final SparkTable table;
  private final RowLevelOperationInfo info;

  public DeltaRowLevelOperationBuilder(SparkTable table, RowLevelOperationInfo info) {
    this.table = table;
    this.info = info;
  }

  @Override
  public RowLevelOperation build() {
    return new DeltaCopyOnWriteOperation(table, info);
  }
}
