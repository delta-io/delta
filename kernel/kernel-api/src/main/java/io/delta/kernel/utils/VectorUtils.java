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
import java.util.List;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
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

            // TODO factor out and support more types
            if (elementVector.isNullAt(i)) {
                elements.add(null);
            } else if (dataType instanceof StringType) {
                elements.add((T) elementVector.getString(i));
            } else if (dataType instanceof StructType) {
                elements.add((T) elementVector.getStruct(i));
            } else {
                throw new UnsupportedOperationException("unsupported data type");
            }
        }
        return elements;
    }
}
