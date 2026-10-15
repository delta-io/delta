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

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
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

  static PhysicalWriteInfo physicalWriteInfo(int numPartitions) {
    return () -> numPartitions;
  }
}
