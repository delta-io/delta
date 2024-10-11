/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.table;

import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

public class DeltaPartitionUtil {
  private DeltaPartitionUtil() {}

  // always set the spec ID to 1 because it cannot change in a table. use 0 for the
  // unpartitioned spec
  private static final int SPEC_ID = 1;

  /**
   * Convert a set of partition columns to an Iceberg partition spec.
   *
   * @param schema current table schema
   * @param partitionColumns a list of column names
   * @return an equivalent Iceberg PartitionSpec
   */
  public static PartitionSpec convert(Schema schema, List<String> partitionColumns) {
    if (partitionColumns.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema).withSpecId(SPEC_ID);

    for (String column : partitionColumns) {
      builder.identity(column);
    }

    return builder.build();
  }
}
