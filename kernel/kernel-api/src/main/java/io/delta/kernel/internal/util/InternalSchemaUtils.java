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
package io.delta.kernel.internal.util;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

public class InternalSchemaUtils
{
    private InternalSchemaUtils() {}

    /**
     * Helper method that converts the logical schema (requested by the connector) to physical
     * schema of the data stored in data files.
     * @param logicalSchema Logical schema of the scan
     * @param physicalSchema Physical schema of the scan
     * @param columnMappingMode Column mapping mode
     */
    public static StructType convertToPhysicalSchema(
        StructType logicalSchema,
        StructType physicalSchema,
        String columnMappingMode)
    {
        switch (columnMappingMode) {
            case "none":
                return logicalSchema;
            case "id":
                throw new UnsupportedOperationException(
                    "Column mapping `id` mode is not yet supported");
            case "name": {
                StructType newSchema = new StructType();
                for (StructField field : logicalSchema.fields()) {
                    DataType oldType = field.getDataType();
                    StructField fieldFromMetadata = physicalSchema.get(field.getName());
                    DataType newType;
                    if (oldType instanceof StructType) {
                        newType = convertToPhysicalSchema(
                            (StructType) field.getDataType(),
                            (StructType) fieldFromMetadata.getDataType(),
                            columnMappingMode);
                    }
                    else if (oldType instanceof ArrayType) {
                        throw new UnsupportedOperationException("NYI");
                    }
                    else if (oldType instanceof MapType) {
                        throw new UnsupportedOperationException("NYI");
                    }
                    else {
                        newType = oldType;
                    }
                    String physicalName = fieldFromMetadata
                        .getMetadata()
                        .get("delta.columnMapping.physicalName");
                    newSchema = newSchema.add(physicalName, newType);
                }
                return newSchema;
            }
            default:
                throw new UnsupportedOperationException(
                    "Unsupported column mapping mode: " + columnMappingMode);
        }
    }
}
