/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.flink.sink.internal;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import io.delta.standalone.types.*;

/**
 * This is a utility class to convert from Flink's specific {@link RowType} into
 * DeltaLake's specific {@link StructType} which is used for schema-matching comparisons
 * during {@link io.delta.standalone.DeltaLog} commits.
 */
public class SchemaConverter {

    /**
     * Main method for converting from {@link RowType} into {@link StructType}
     *
     * @param rowType Flink's logical type of stream's events
     * @return DeltaLake's specific type of stream's events
     */
    public static StructType toDeltaDataType(RowType rowType) {
        StructField[] fields = rowType.getFields()
            .stream()
            .map(rowField -> {
                DataType rowFieldType = toDeltaDataType(rowField.getType());
                return new StructField(
                    rowField.getName(),
                    rowFieldType,
                    rowField.getType().isNullable());
            })
            .toArray(StructField[]::new);

        return new StructType(fields);
    }

    /**
     * Method containing the actual mapping between Flink's and DeltaLake's types.
     *
     * @param flinkType Flink's logical type
     * @return DeltaLake's data type
     */
    public static DataType toDeltaDataType(LogicalType flinkType) {
        switch (flinkType.getTypeRoot()) {
            case ARRAY:
                org.apache.flink.table.types.logical.ArrayType arrayType =
                    (org.apache.flink.table.types.logical.ArrayType) flinkType;
                LogicalType flinkElementType = arrayType.getElementType();
                DataType deltaElementType = toDeltaDataType(flinkElementType);
                return new ArrayType(deltaElementType, flinkElementType.isNullable());
            case BIGINT:
                return new LongType();
            case BINARY:
            case VARBINARY:
                return new BinaryType();
            case BOOLEAN:
                return new BooleanType();
            case DATE:
                return new DateType();
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimalType =
                    (org.apache.flink.table.types.logical.DecimalType) flinkType;
                return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
            case DOUBLE:
                return new DoubleType();
            case FLOAT:
                return new FloatType();
            case INTEGER:
                return new IntegerType();
            case MAP:
                org.apache.flink.table.types.logical.MapType mapType =
                    (org.apache.flink.table.types.logical.MapType) flinkType;
                DataType keyType = toDeltaDataType(mapType.getKeyType());
                DataType valueType = toDeltaDataType(mapType.getValueType());
                boolean valueCanContainNull = mapType.getValueType().isNullable();
                return new MapType(keyType, valueType, valueCanContainNull);
            case NULL:
                return new NullType();
            case SMALLINT:
                return new ShortType();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampType();
            case TINYINT:
                return new ByteType();
            case CHAR:
            case VARCHAR:
                return new StringType();
            case ROW:
                return toDeltaDataType((RowType) flinkType);
            default:
                throw new UnsupportedOperationException(
                    "Type not supported: " + flinkType);
        }
    }
}
