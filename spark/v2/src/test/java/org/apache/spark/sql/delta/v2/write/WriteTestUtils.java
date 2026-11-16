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
package org.apache.spark.sql.delta.v2.write;

import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.v2.interop.DeltaV2QueryContext$;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManager;
import org.apache.spark.sql.delta.v2.tablemanager.DeltaV2TableManagerCache$;
import org.apache.spark.sql.delta.v2.utils.ScalaUtils;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Shared helpers for the write-side unit tests. */
final class WriteTestUtils {

  private WriteTestUtils() {}

  /**
   * Builds a {@link LogicalWriteInfo}. Spark's {@code LogicalWriteInfoImpl} is {@code private[sql]}
   * and not reachable from this package, so we implement the interface directly.
   */
  static LogicalWriteInfo logicalWriteInfo(StructType schema, CaseInsensitiveStringMap options) {
    return new LogicalWriteInfo() {
      @Override
      public String queryId() {
        return "test-query-id";
      }

      @Override
      public StructType schema() {
        return schema;
      }

      @Override
      public CaseInsensitiveStringMap options() {
        return options;
      }
    };
  }

  static DeltaV2TableManager tableManager(SparkSession spark, String path) {
    return DeltaV2TableManagerCache$.MODULE$.forTable(
        spark, path, Collections.emptyMap(), ScalaUtils.toScalaOption(Optional.empty()));
  }

  static Snapshot latestSnapshot(DeltaV2TableManager tableManager) {
    return tableManager.loadLatestSnapshot(DeltaV2QueryContext$.MODULE$.empty());
  }

  static PhysicalWriteInfo physicalWriteInfo(int numPartitions) {
    return () -> numPartitions;
  }
}
