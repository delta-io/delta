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
package io.delta.kernel.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.*;

public final class VectorUtils {

    private VectorUtils() {}

    /**
     * Converts an {@link ArrayValue} to a Java list. Any nested complex types are also converted
     * to their Java type.
     */
    public static <T> List<T> toJavaList(ArrayValue arrayValue) {
        ColumnVector elementVector = arrayValue.getElements();
        DataType dataType = elementVector.getDataType();

        List<T> elements = new ArrayList<>();
        for (int i = 0; i < arrayValue.getSize(); i ++) {
            elements.add((T) getValueAsObject(elementVector, dataType, i));
        }
        return elements;
    }

    /**
     * Converts a {@link MapValue} to a Java map. Any nested complex types are also converted
     * to their Java type.
     *
     * Please note not all key types override hashCode/equals. Be careful when using with keys of:
     * - Struct type at any nesting level (i.e. ArrayType(StructType) does not)
     * - Binary type
     */
    public static <K, V> Map<K, V> toJavaMap(MapValue mapValue) {
        ColumnVector keyVector = mapValue.getKeys();
        DataType keyDataType = keyVector.getDataType();
        ColumnVector valueVector = mapValue.getValues();
        DataType valueDataType = valueVector.getDataType();

        Map<K, V> values = new HashMap<>();

        for (int i = 0; i < mapValue.getSize(); i ++) {
            Object key = getValueAsObject(keyVector, keyDataType, i);
            Object value = getValueAsObject(valueVector, valueDataType, i);
            values.put((K) key, (V) value);
        }
        return values;
    }

    /**
     * Gets the value at {@code rowId} from the column vector. The type of the Object returned
     * depends on the data type of the column vector. For complex types array and map, returns
     * the value as Java list or Java map.
     */
    private static Object getValueAsObject(
            ColumnVector columnVector, DataType dataType, int rowId) {
        if (columnVector.isNullAt(rowId)) {
            return null;
        }
        if (dataType instanceof BooleanType) {
            return columnVector.getBoolean(rowId);
        } else if (dataType instanceof ByteType) {
            return columnVector.getByte(rowId);
        } else if (dataType instanceof ShortType) {
            return columnVector.getShort(rowId);
        } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
            // DateType data is stored internally as the number of days since 1970-01-01
            return columnVector.getInt(rowId);
        } else if (dataType instanceof LongType || dataType instanceof TimestampType) {
            // TimestampType data is stored internally as the number of microseconds since the unix
            // epoch
            return columnVector.getLong(rowId);
        } else if (dataType instanceof FloatType) {
            return columnVector.getFloat(rowId);
        } else if (dataType instanceof DoubleType) {
            return columnVector.getDouble(rowId);
        } else if (dataType instanceof StringType) {
            return columnVector.getString(rowId);
        } else if (dataType instanceof BinaryType) {
            return columnVector.getBinary(rowId);
        } else if (dataType instanceof StructType) {
            return columnVector.getStruct(rowId);
        } else if (dataType instanceof DecimalType) {
            return columnVector.getDecimal(rowId);
        } else if (dataType instanceof ArrayType) {
            return toJavaList(columnVector.getArray(rowId));
        } else if (dataType instanceof MapType) {
            return toJavaMap(columnVector.getMap(rowId));
        } else {
            throw new UnsupportedOperationException("unsupported data type");
        }
    }
}
