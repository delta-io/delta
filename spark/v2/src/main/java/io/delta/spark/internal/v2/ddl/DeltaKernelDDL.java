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
package io.delta.spark.internal.v2.ddl;

import io.delta.kernel.Operation;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.spark.internal.v2.utils.SchemaUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

/**
 * Utility class for metadata-only Delta DDL operations via Kernel.
 *
 * <p>This class encapsulates the logic for creating a Delta table using Kernel APIs, writing only
 * metadata (commit 0) without any data files. It is used by the V2 DDL path in {@code DeltaCatalog}
 * when STRICT mode is enabled.
 */
public class DeltaKernelDDL {

  private static final String ENGINE_INFO = "delta-spark-v2";

  /** Property keys that should not be persisted to the Delta log. */
  private static final Set<String> TRANSIENT_PROPERTY_PREFIXES = Set.of("fs.", "dfs.", "option.");

  private static final Set<String> TRANSIENT_PROPERTY_KEYS =
      Set.of(
          "provider",
          "location",
          "is_managed_location",
          "external",
          "owner",
          "comment",
          "path",
          // catalogManaged feature triggers Kernel's catalog committer validation.
          // For V2 DDL we use filesystem commit; UC registration handles managed semantics.
          "delta.feature.catalogmanaged");

  private DeltaKernelDDL() {}

  /**
   * Creates a metadata-only Delta table via Kernel APIs.
   *
   * <p>This writes commit 0 to {@code _delta_log/00000000000000000000.json} containing only
   * metadata (Protocol, Metadata actions) with no data files.
   *
   * @param engine the Kernel engine to use for the commit
   * @param tableLocation filesystem path for the new table
   * @param sparkSchema the table schema in Spark types
   * @param partitions the partition transforms from DDL
   * @param tableProperties all table properties from the DDL statement
   * @return the commit result from Kernel
   */
  public static TransactionCommitResult createMetadataOnlyTable(
      Engine engine,
      String tableLocation,
      StructType sparkSchema,
      Transform[] partitions,
      Map<String, String> tableProperties) {

    io.delta.kernel.types.StructType kernelSchema =
        SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);

    List<String> partitionColumns = extractPartitionColumns(partitions);

    Map<String, String> filteredProperties = filterProperties(tableProperties);

    return io.delta.kernel.Table.forPath(engine, tableLocation)
        .createTransactionBuilder(engine, ENGINE_INFO, Operation.CREATE_TABLE)
        .withSchema(engine, kernelSchema)
        .withPartitionColumns(engine, partitionColumns)
        .withTableProperties(engine, filteredProperties)
        .build(engine)
        .commit(engine, CloseableIterable.emptyIterable());
  }

  /**
   * Extracts partition column names from Spark's Transform array.
   *
   * @param partitions partition transforms from the DDL statement
   * @return list of partition column names
   */
  static List<String> extractPartitionColumns(Transform[] partitions) {
    List<String> partitionColumns = new ArrayList<>();
    for (Transform partition : partitions) {
      String columnName = partition.references()[0].describe();
      partitionColumns.add(columnName);
    }
    return partitionColumns;
  }

  /**
   * Filters out transient properties that should not be persisted to the Delta log. Removes
   * properties with transient prefixes (fs.*, dfs.*, option.*) and transient keys (provider,
   * location, etc.).
   *
   * @param properties all table properties from the DDL statement
   * @return filtered properties suitable for persisting to the Delta log
   */
  static Map<String, String> filterProperties(Map<String, String> properties) {
    Map<String, String> filtered = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (!isTransientProperty(key)) {
        filtered.put(key, entry.getValue());
      }
    }
    return filtered;
  }

  private static boolean isTransientProperty(String key) {
    String lowerKey = key.toLowerCase(Locale.ROOT);
    if (TRANSIENT_PROPERTY_KEYS.contains(lowerKey)) {
      return true;
    }
    for (String prefix : TRANSIENT_PROPERTY_PREFIXES) {
      if (lowerKey.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
