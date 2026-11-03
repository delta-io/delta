/*
 *  Copyright (2026) The Delta Lake Project Authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.delta.flink.inttest;

import io.delta.kernel.types.*;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IntTestUtils {

  public static String schemaToDDL(StructType schema) {
    return schema.fields().stream()
        .map(field -> String.format("%s %s", field.getName(), typeToDDL(field.getDataType())))
        .collect(Collectors.joining(","));
  }

  public static String typeToDDL(DataType type) {
    if (type instanceof BasePrimitiveType) {
      return type.toString();
    }
    if (type instanceof DecimalType) {
      DecimalType dtype = (DecimalType) type;
      return String.format("decimal(%d, %d)", dtype.getPrecision(), dtype.getScale());
    }
    if (type instanceof ArrayType) {
      return String.format("array<%s>", typeToDDL(((ArrayType) type).getElementType()));
    }
    if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      return String.format(
          "map<%s, %s>", typeToDDL(mapType.getKeyType()), typeToDDL(mapType.getValueType()));
    }
    if (type instanceof StructType) {
      StructType structType = (StructType) type;
      return String.format(
          "struct<%s>",
          structType.fields().stream()
              .map(field -> String.format("%s:%s", field.getName(), typeToDDL(field.getDataType())))
              .collect(Collectors.joining(",")));
    }
    throw new RuntimeException("Unrecognized type " + type.getClass());
  }

  public static final StructType SCHEMA_WITH_ALL_TYPES = createSchemaWithAllTypes();

  static StructType createSchemaWithAllTypes() {
    StructType nestedStruct =
        new StructType()
            .add("nested_int", IntegerType.INTEGER)
            .add("nested_str", StringType.STRING);
    ArrayType arrayOfStrings = new ArrayType(StringType.STRING, /* containsNull = */ true);
    MapType mapStrToLong =
        new MapType(StringType.STRING, LongType.LONG, /* valueContainsNull = */ true);

    // Decimal: either use USER_DEFAULT or pick an explicit (p,s)
    DecimalType decimal38_18 = new DecimalType(38, 18);

    return new StructType()
        // primitives
        .add("t_boolean", BooleanType.BOOLEAN)
        .add("t_byte", ByteType.BYTE)
        .add("t_short", ShortType.SHORT)
        .add("t_int", IntegerType.INTEGER)
        .add("t_long", LongType.LONG)
        .add("t_float", FloatType.FLOAT)
        .add("t_double", DoubleType.DOUBLE)
        .add("t_binary", BinaryType.BINARY)
        .add("t_string", StringType.STRING)
        .add("t_date", DateType.DATE)
        .add("t_timestamp", TimestampType.TIMESTAMP)
        .add("t_timestamp_ntz", TimestampNTZType.TIMESTAMP_NTZ)

        // logical/special
        .add("t_decimal_38_18", decimal38_18)
        //        .add("t_variant", VariantType.VARIANT) Not supported now

        // complex
        .add("t_array_string", arrayOfStrings)
        .add("t_map_string_long", mapStrToLong)
        .add("t_struct", nestedStruct);
  }

  static List<List<?>> dummyData(StructType schema, int numRows) {
    return IntStream.range(0, numRows)
        .mapToObj(
            rowIndex -> {
              List<Object> buffer = new ArrayList<>();

              for (StructField field : schema.fields()) {
                DataType dt = field.getDataType();

                if (dt == BooleanType.BOOLEAN) {
                  buffer.add(rowIndex % 2 == 0);
                } else if (dt == ByteType.BYTE) {
                  buffer.add((byte) rowIndex);
                } else if (dt == ShortType.SHORT) {
                  buffer.add((short) rowIndex);
                } else if (dt == IntegerType.INTEGER) {
                  buffer.add(rowIndex);
                } else if (dt == LongType.LONG) {
                  buffer.add((long) rowIndex);
                } else if (dt == FloatType.FLOAT) {
                  buffer.add((float) rowIndex + 0.1f);
                } else if (dt == DoubleType.DOUBLE) {
                  buffer.add((double) rowIndex + 0.01);
                } else if (dt == StringType.STRING) {
                  buffer.add("str_" + rowIndex);
                } else if (dt == BinaryType.BINARY) {
                  buffer.add(("bin_" + rowIndex).getBytes());
                } else if (dt == DateType.DATE) {
                  // days since epoch
                  buffer.add(rowIndex);
                } else if (dt == TimestampType.TIMESTAMP) {
                  // microseconds since epoch
                  buffer.add((long) rowIndex * 1_000_000L);
                } else if (dt == TimestampNTZType.TIMESTAMP_NTZ) {
                  buffer.add((long) rowIndex * 1_000_000L);
                } else if (dt instanceof DecimalType) {
                  DecimalType dec = (DecimalType) dt;
                  buffer.add(BigDecimal.valueOf(rowIndex, dec.getScale()));
                } else if (dt instanceof StructType) {
                  buffer.add(dummyData((StructType) dt, 1).get(0));
                } else if (dt instanceof ArrayType) {
                  ArrayType arrayType = (ArrayType) dt;
                  List<List<?>> subDummy =
                      dummyData(new StructType().add("element", arrayType.getElementType()), 5);
                  buffer.add(subDummy);
                } else if (dt instanceof MapType) {
                  MapType mapType = (MapType) dt;
                  List<List<?>> subDummy =
                      dummyData(
                          new StructType()
                              .add("key", mapType.getKeyType())
                              .add("value", mapType.getValueType()),
                          5);
                  buffer.add(subDummy);
                } else {
                  throw new UnsupportedOperationException("Unsupported data type: " + dt);
                }
              }
              return buffer;
            })
        .collect(Collectors.toList());
  }
}
