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

package io.delta.flink.kernel;

import io.delta.kernel.internal.util.ColumnMapping;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

/** Column-mapping helpers for the Flink connector. */
public final class ColumnMappingUtils {

  private ColumnMappingUtils() {}

  /**
   * Convert a (logical) schema into its physical schema by renaming every field to its physical
   * name.
   *
   * <p>Several Kernel APIs — {@code DataFileStatistics.serializeAsJson}/{@code deserializeFromJson}
   * and the Parquet read schema expect the names to already be physical. This method produces such
   * a schema: each field is renamed to its physical name when the metadata is present, recursing
   * into nested struct/array/map types.
   *
   * <p>For tables without column mapping (mode {@code NONE}) the fields carry no physical-name
   * metadata, so the schema is returned effectively unchanged (logical name == physical name).
   *
   * @param schema the schema whose fields carry column-mapping metadata (e.g. {@code
   *     table.getSchema()})
   * @return a schema whose field names are the physical names
   */
  public static StructType toPhysicalSchema(StructType schema) {
    StructType physical = new StructType();
    for (StructField field : schema.fields()) {
      physical =
          physical.add(
              new StructField(
                  physicalName(field),
                  toPhysicalType(field.getDataType()),
                  field.isNullable(),
                  field.getMetadata()));
    }
    return physical;
  }

  private static DataType toPhysicalType(DataType dataType) {
    if (dataType instanceof StructType) {
      return toPhysicalSchema((StructType) dataType);
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      return new ArrayType(toPhysicalType(arrayType.getElementType()), arrayType.containsNull());
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      return new MapType(
          toPhysicalType(mapType.getKeyType()),
          toPhysicalType(mapType.getValueType()),
          mapType.isValueContainsNull());
    }
    return dataType;
  }

  private static String physicalName(StructField field) {
    if (field.getMetadata().contains(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
      return field.getMetadata().getString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY);
    }
    return field.getName();
  }
}
