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
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Schema visitor to convert Iceberg types to Delta kernel types Most code and behavior is taken
 * from {@link org.apache.iceberg.spark.TypeToSparkType} with modifications for copying field IDs to
 * FieldMetadata
 */
class IcebergTypeToStructType extends TypeUtil.SchemaVisitor<DataType> {
  @Override
  public DataType schema(Schema schema, DataType structType) {
    return structType;
  }

  public DataType list(Types.ListType list, DataType elementResult) {
    return new ArrayType(elementResult, list.isElementOptional());
  }

  @Override
  public DataType map(Types.MapType map, DataType keyResult, DataType valueResult) {
    return new MapType(keyResult, valueResult, map.isValueOptional());
  }

  @Override
  public DataType struct(Types.StructType struct, List<DataType> fieldResults) {
    List<Types.NestedField> fields = struct.fields();

    List<StructField> structFields = Lists.newArrayListWithExpectedSize(fieldResults.size());
    for (int i = 0; i < fields.size(); i++) {
      Types.NestedField field = fields.get(i);
      DataType type = fieldResults.get(i);
      StructField structField =
          new StructField(
              field.name(),
              type,
              field.isOptional(),
              FieldMetadata.builder()
                  .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, field.fieldId())
                  .build());
      // ToDo: No concept of field comments in Delta StructType?
      structFields.add(structField);
    }

    return new StructType(structFields);
  }

  @Override
  public DataType field(Types.NestedField field, DataType fieldResult) {
    return fieldResult;
  }

  @Override
  public DataType primitive(Type.PrimitiveType primitive) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case INTEGER:
        return IntegerType.INTEGER;
      case LONG:
        return LongType.LONG;
      case FLOAT:
        return FloatType.FLOAT;
      case DOUBLE:
        return DoubleType.DOUBLE;
      case DATE:
        return DateType.DATE;
      case TIME:
        throw new UnsupportedOperationException("Spark does not support time fields");
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) primitive;
        if (ts.shouldAdjustToUTC()) {
          return TimestampType.TIMESTAMP;
        } else {
          return TimestampNTZType.TIMESTAMP_NTZ;
        }
      case STRING:
        return StringType.STRING;
      case UUID:
        // use String
        return StringType.STRING;
      case FIXED:
        return BinaryType.BINARY;
      case BINARY:
        return BinaryType.BINARY;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        return new DecimalType(decimal.precision(), decimal.scale());
      default:
        throw new UnsupportedOperationException(
            "Cannot convert unknown type to Spark: " + primitive);
    }
  }
}
