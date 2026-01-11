/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.flink.sink;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Provide conversions between Flink and Delta data structures. This now includes:
 *
 * <ul>
 *   <li>Schema/Data type conversion
 *   <li>RowData to Kernel Literal conversion
 * </ul>
 */
public class Conversions {
  public static class FlinkToDelta {
    public static StructType schema(RowType rowType) {
      List<StructField> fields =
          rowType.getFields().stream()
              .map(
                  rowField -> {
                    DataType rowFieldType = dataType(rowField.getType());
                    return new StructField(
                        rowField.getName(), rowFieldType, rowField.getType().isNullable());
                  })
              .collect(Collectors.toList());

      return new StructType(fields);
    }

    public static DataType dataType(LogicalType flinkType) {
      final LogicalTypeRoot typeRoot = flinkType.getTypeRoot();
      switch (typeRoot) {
        case BOOLEAN:
          return io.delta.kernel.types.BooleanType.BOOLEAN;
        case INTEGER:
          return io.delta.kernel.types.IntegerType.INTEGER;
        case VARCHAR:
        case CHAR:
          return io.delta.kernel.types.StringType.STRING;
        case BINARY:
        case VARBINARY:
          return io.delta.kernel.types.BinaryType.BINARY;
        case BIGINT:
          return io.delta.kernel.types.LongType.LONG;
        case DOUBLE:
          return io.delta.kernel.types.DoubleType.DOUBLE;
        case FLOAT:
          return io.delta.kernel.types.FloatType.FLOAT;
        case DECIMAL:
          DecimalType decimalType = (DecimalType) flinkType;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          return new io.delta.kernel.types.DecimalType(precision, scale);
        case DATE:
          return io.delta.kernel.types.DateType.DATE;
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return io.delta.kernel.types.TimestampType.TIMESTAMP;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          return io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ;
        case ROW:
          RowType rowType = (RowType) flinkType;
          return new StructType(
              rowType.getFields().stream()
                  .map(
                      field ->
                          new StructField(
                              field.getName(),
                              dataType(field.getType()),
                              field.getType().isNullable()))
                  .collect(Collectors.toList()));
        case ARRAY:
          ArrayType arrayType = (ArrayType) flinkType;
          return new io.delta.kernel.types.ArrayType(
              dataType(arrayType.getElementType()), arrayType.getElementType().isNullable());
        case MAP:
          MapType mapType = (MapType) flinkType;
          return new io.delta.kernel.types.MapType(
              dataType(mapType.getKeyType()),
              dataType(mapType.getValueType()),
              mapType.getValueType().isNullable());
        default:
          throw new UnsupportedOperationException(
              String.format("Type not supported: %s: %s", flinkType, flinkType.getTypeRoot()));
      }
    }

    public static Map<String, Literal> partitionValues(
        StructType rowType, Collection<String> partitionColNames, RowData rowData) {
      if (rowType == null) {
        throw new RuntimeException();
      }
      return partitionColNames.stream()
          .map(
              name -> {
                final int partitionValueColIdx = rowType.indexOf(name);
                return new Object[] {
                  name, Conversions.FlinkToDelta.data(rowType, rowData, partitionValueColIdx)
                };
              })
          .collect(Collectors.toMap(o -> (String) o[0], o -> (Literal) o[1]));
    }

    public static Literal data(StructType rowType, RowData rowData, int colIdx) {
      final StructField field = rowType.at(colIdx);
      final DataType dataType = field.getDataType();
      if (dataType.equivalent(io.delta.kernel.types.IntegerType.INTEGER)) {
        return Literal.ofInt(rowData.getInt(colIdx));
      } else if (dataType.equivalent(io.delta.kernel.types.LongType.LONG)) {
        return Literal.ofLong(rowData.getLong(colIdx));
      } else if (dataType.equivalent(io.delta.kernel.types.StringType.STRING)) {
        return Literal.ofString(rowData.getString(colIdx).toString());
      } else if (dataType.equivalent(io.delta.kernel.types.DoubleType.DOUBLE)) {
        return Literal.ofDouble(rowData.getDouble(colIdx));
      } else if (dataType.equivalent(io.delta.kernel.types.FloatType.FLOAT)) {
        return Literal.ofFloat(rowData.getFloat(colIdx));
      } else if (dataType instanceof io.delta.kernel.types.DecimalType) {
        io.delta.kernel.types.DecimalType decimalType =
            (io.delta.kernel.types.DecimalType) dataType;
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        return Literal.ofDecimal(
            rowData.getDecimal(colIdx, precision, scale).toBigDecimal(), precision, scale);
      } else if (dataType.equivalent(io.delta.kernel.types.DateType.DATE)) {
        return Literal.ofDate(rowData.getInt(colIdx));
      } else if (dataType.equivalent(io.delta.kernel.types.TimestampType.TIMESTAMP)) {
        return Literal.ofTimestamp(rowData.getLong(colIdx));
      } else if (dataType.equivalent(io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ)) {
        return Literal.ofTimestampNtz(rowData.getLong(colIdx));
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
      }
    }

    /**
     * Convert a Flink Timestamp Data to Kernel timestamp ( microseconds )
     *
     * @param input
     * @return the microseconds
     */
    public static long timestamp(TimestampData input) {
      return input.getMillisecond() * 1000 + input.getNanoOfMillisecond() / 1000;
    }
  }

  public static class FlinkToJava {
    public static Map<String, Object> partitionValues(
        RowType rowType, Collection<String> partitionColNames, RowData rowData) {
      return partitionColNames.stream()
          .map(
              name -> {
                final int partitionValueColIdx = rowType.getFieldIndex(name);
                return new Object[] {
                  name, Conversions.FlinkToJava.data(rowType, rowData, partitionValueColIdx)
                };
              })
          .collect(Collectors.toMap(o -> (String) o[0], o -> o[1]));
    }

    public static Object data(RowType rowType, RowData rowData, int colIdx) {
      RowType.RowField field = rowType.getFields().get(colIdx);

      switch (field.getType().getTypeRoot()) {
        case BOOLEAN:
          return rowData.getBoolean(colIdx);
        case BINARY:
        case VARBINARY:
          return new String(
              Base64.getEncoder().encode(rowData.getBinary(colIdx)), StandardCharsets.UTF_8);
        case INTEGER:
        case DATE:
          return rowData.getInt(colIdx);
        case BIGINT:
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          return rowData.getLong(colIdx);
        case CHAR:
        case VARCHAR:
          return rowData.getString(colIdx).toString();
        case DOUBLE:
          return rowData.getDouble(colIdx);
        case FLOAT:
          return rowData.getFloat(colIdx);
        case DECIMAL:
          DecimalType decimalType = (DecimalType) field.getType();
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          return rowData.getDecimal(colIdx, precision, scale);
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + field.getType());
      }
    }
  }
}
