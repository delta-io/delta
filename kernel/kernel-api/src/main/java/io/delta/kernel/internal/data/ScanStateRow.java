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

import static java.util.stream.Collectors.toMap;

import io.delta.kernel.Scan;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.types.DataTypeJsonSerDe;
import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.internal.util.ColumnMapping.ColumnMappingMode;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.*;
import java.net.URI;
import java.util.*;
import java.util.stream.IntStream;

/** Encapsulate the scan state (common info for all scan files) as a {@link Row} */
public class ScanStateRow extends GenericRow {
  private static final StructType SCHEMA =
      new StructType()
          .add("configuration", new MapType(StringType.STRING, StringType.STRING, false))
          .add("logicalSchemaJson", StringType.STRING)
          .add("physicalSchemaJson", StringType.STRING)
          .add("partitionColumns", new ArrayType(StringType.STRING, false))
          .add("minReaderVersion", IntegerType.INTEGER)
          .add("minWriterVersion", IntegerType.INTEGER)
          .add("tablePath", StringType.STRING);

  private static final Map<String, Integer> COL_NAME_TO_ORDINAL =
      IntStream.range(0, SCHEMA.length())
          .boxed()
          .collect(toMap(i -> SCHEMA.at(i).getName(), i -> i));

  public static ScanStateRow of(
      Metadata metadata,
      Protocol protocol,
      String logicalSchemaJson,
      String physicalSchemaJson,
      String tablePath) {
    HashMap<Integer, Object> valueMap = new HashMap<>();
    valueMap.put(COL_NAME_TO_ORDINAL.get("configuration"), metadata.getConfigurationMapValue());
    valueMap.put(COL_NAME_TO_ORDINAL.get("logicalSchemaJson"), logicalSchemaJson);
    valueMap.put(COL_NAME_TO_ORDINAL.get("physicalSchemaJson"), physicalSchemaJson);
    valueMap.put(COL_NAME_TO_ORDINAL.get("partitionColumns"), metadata.getPartitionColumns());
    valueMap.put(COL_NAME_TO_ORDINAL.get("minReaderVersion"), protocol.getMinReaderVersion());
    valueMap.put(COL_NAME_TO_ORDINAL.get("minWriterVersion"), protocol.getMinWriterVersion());
    valueMap.put(COL_NAME_TO_ORDINAL.get("tablePath"), tablePath);
    return new ScanStateRow(valueMap);
  }

  public ScanStateRow(HashMap<Integer, Object> valueMap) {
    super(SCHEMA, valueMap);
  }

  /**
   * Utility method to get the configuration map from the scan state {@link Row} returned by {@link
   * Scan#getScanState(Engine)}.
   *
   * @param scanState Scan state {@link Row}
   * @return Map of configuration key-value pairs.
   */
  public static Map<String, String> getConfiguration(Row scanState) {
    return VectorUtils.toJavaMap(scanState.getMap(COL_NAME_TO_ORDINAL.get("configuration")));
  }

  /**
   * Utility method to get the logical schema from the scan state {@link Row} returned by {@link
   * Scan#getScanState(Engine)}.
   *
   * @param scanState Scan state {@link Row}
   * @return Logical schema to read from the data files.
   */
  public static StructType getLogicalSchema(Row scanState) {
    String serializedSchema = scanState.getString(COL_NAME_TO_ORDINAL.get("logicalSchemaJson"));
    return DataTypeJsonSerDe.deserializeStructType(serializedSchema);
  }

  /**
   * Utility method to get the physical schema from the scan state {@link Row} returned by {@link
   * Scan#getScanState(Engine)}. This schema is used to request data from the scan files for the
   * query.
   *
   * @param scanState Scan state {@link Row}
   * @return Physical schema to read from the data files.
   */
  public static StructType getPhysicalDataReadSchema(Row scanState) {
    String serializedSchema = scanState.getString(COL_NAME_TO_ORDINAL.get("physicalSchemaJson"));
    return DataTypeJsonSerDe.deserializeStructType(serializedSchema);
  }

  /**
   * Get the list of partition column names from the scan state {@link Row} returned by {@link
   * Scan#getScanState(Engine)}.
   *
   * @param scanState Scan state {@link Row}
   * @return List of partition column names according to the scan state.
   */
  public static List<String> getPartitionColumns(Row scanState) {
    return VectorUtils.toJavaList(scanState.getArray(COL_NAME_TO_ORDINAL.get("partitionColumns")));
  }

  /**
   * Get the column mapping mode from the scan state {@link Row} returned by {@link
   * Scan#getScanState(Engine)}.
   */
  public static ColumnMappingMode getColumnMappingMode(Row scanState) {
    return ColumnMapping.getColumnMappingMode(getConfiguration(scanState));
  }

  /**
   * Get the table root from scan state {@link Row} returned by {@link Scan#getScanState(Engine)}
   *
   * @param scanState Scan state {@link Row}
   * @return Fully qualified path to the location of the table.
   */
  public static Path getTableRoot(Row scanState) {
    return new Path(URI.create(scanState.getString(COL_NAME_TO_ORDINAL.get("tablePath"))));
  }
}
