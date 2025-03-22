/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.data;

import static io.delta.kernel.internal.tablefeatures.TableFeatures.CLUSTERING_W_FEATURE;
import static io.delta.kernel.internal.util.VectorUtils.buildArrayValue;
import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Transaction;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.TableConfig;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class TransactionStateRow extends GenericRow {
  private static final StructType SCHEMA =
      new StructType()
          .add("logicalSchemaString", StringType.STRING)
          .add("physicalSchemaString", StringType.STRING)
          .add("partitionColumns", new ArrayType(StringType.STRING, false /* containsNull */))
          .add(
              "configuration",
              new MapType(StringType.STRING, StringType.STRING, false /* valueContainsNull */))
          .add("tablePath", StringType.STRING)
          .add("writerFeatures", new ArrayType(StringType.STRING, false /* containsNull */));

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, SCHEMA.length())
          .boxed()
          .collect(toMap(i -> SCHEMA.at(i).getName(), i -> i));

  public static TransactionStateRow of(
      Metadata metadata, Protocol protocol, String tablePath, StructType physicalSchema) {
    HashMap<Integer, Object> valueMap = new HashMap<>();
    valueMap.put(COL_NAME_TO_ORDINAL.get("logicalSchemaString"), metadata.getSchemaString());
    valueMap.put(COL_NAME_TO_ORDINAL.get("physicalSchemaString"), physicalSchema.toJson());
    valueMap.put(COL_NAME_TO_ORDINAL.get("partitionColumns"), metadata.getPartitionColumns());
    valueMap.put(COL_NAME_TO_ORDINAL.get("configuration"), metadata.getConfigurationMapValue());
    valueMap.put(COL_NAME_TO_ORDINAL.get("tablePath"), tablePath);
    valueMap.put(
        COL_NAME_TO_ORDINAL.get("writerFeatures"),
        buildArrayValue(new ArrayList<>(protocol.getWriterFeatures()), StringType.STRING));
    return new TransactionStateRow(valueMap);
  }

  private TransactionStateRow(HashMap<Integer, Object> valueMap) {
    super(SCHEMA, valueMap);
  }

  /**
   * Get the logical schema of the table from the transaction state {@link Row} returned by {@link
   * Transaction#getTransactionState(Engine)}}
   *
   * @param transactionState Transaction state state {@link Row}
   * @return Logical schema of the table as {@link StructType}
   */
  public static StructType getLogicalSchema(Row transactionState) {
    String serializedSchema =
        transactionState.getString(COL_NAME_TO_ORDINAL.get("logicalSchemaString"));
    return DataTypeJsonSerDe.deserializeStructType(serializedSchema);
  }

  /**
   * Get the physical schema of the table from the transaction state {@link Row} returned by {@link
   * Transaction#getTransactionState(Engine)}}
   *
   * @param transactionState Transaction state state {@link Row}
   * @return Logical schema of the table as {@link StructType}
   */
  public static StructType getPhysicalSchema(Row transactionState) {
    String serializedSchema =
        transactionState.getString(COL_NAME_TO_ORDINAL.get("physicalSchemaString"));
    return DataTypeJsonSerDe.deserializeStructType(serializedSchema);
  }

  /**
   * Get the configuration from the transaction state {@link Row} returned by {@link
   * Transaction#getTransactionState(Engine)}
   *
   * @param transactionState
   * @return Configuration as a map of key-value pairs.
   */
  public static Map<String, String> getConfiguration(Row transactionState) {
    return VectorUtils.toJavaMap(transactionState.getMap(COL_NAME_TO_ORDINAL.get("configuration")));
  }

  /**
   * Get the iceberg compatibility enabled or not from the transaction state {@link Row} returned by
   * {@link Transaction#getTransactionState(Engine)}
   *
   * @param transactionState Transaction state state {@link Row}
   * @return True if iceberg compatibility is enabled, false otherwise.
   */
  public static boolean isIcebergCompatV2Enabled(Row transactionState) {
    return Boolean.parseBoolean(
        getConfiguration(transactionState)
            .getOrDefault(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey(), "false"));
  }

  /**
   * Get the writerFeatures from the transaction state {@link Row} returned by {@link
   * Transaction#getTransactionState(Engine)}
   *
   * @param transactionState
   * @return A list of supported writer features as strings.
   */
  public static List<String> getWriterFeatures(Row transactionState) {
    return VectorUtils.toJavaList(
        transactionState.getArray(COL_NAME_TO_ORDINAL.get("writerFeatures")));
  }

  /**
   * Get the clustering table feature enabled or not from the transaction state {@link Row} returned
   * by {@link Transaction#getTransactionState(Engine)}
   *
   * @param transactionState Transaction state state {@link Row}
   * @return true if clustering is enabled (i.e., "clustering" is present in writerFeatures),
   *     otherwise false
   */
  public static boolean isClusteredTableSupported(Row transactionState) {
    return getWriterFeatures(transactionState).contains(CLUSTERING_W_FEATURE.featureName());
  }

  /**
   * Get the list of partition column names from the write state {@link Row} returned by {@link
   * Transaction#getTransactionState(Engine)}
   *
   * @param transactionState Transaction state state {@link Row}
   * @return List of partition column names according to the scan state.
   */
  public static List<String> getPartitionColumnsList(Row transactionState) {
    return VectorUtils.toJavaList(
        transactionState.getArray(COL_NAME_TO_ORDINAL.get("partitionColumns")));
  }

  /**
   * Get the table path from scan state {@link Row} returned by {@link
   * Transaction#getTransactionState(Engine)}
   *
   * @param transactionState Transaction state state {@link Row}
   * @return Fully qualified path to the location of the table.
   */
  public static String getTablePath(Row transactionState) {
    return transactionState.getString(COL_NAME_TO_ORDINAL.get("tablePath"));
  }
}
