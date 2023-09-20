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
     * Which types are supported??
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

    private static Object getValueAsObject(
            ColumnVector columnVector, DataType dataType, int rowId) {
        // TODO support more types
        // TODO combine with other utils?
        if (columnVector.isNullAt(rowId)) {
            return null;
        } else if (dataType instanceof StringType) {
            return columnVector.getString(rowId);
        } else if (dataType instanceof StructType) {
            return columnVector.getStruct(rowId);
        } else {
            throw new UnsupportedOperationException("unsupported data type");
        }
    }
}
