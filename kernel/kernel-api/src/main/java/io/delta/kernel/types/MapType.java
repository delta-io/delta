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

package io.delta.kernel.types;

public class MapType extends DataType {

    public static final MapType EMPTY_INSTANCE = new MapType(null, null, false);

    private final DataType keyType;
    private final DataType valueType;
    private final boolean valueContainsNull;

    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.valueContainsNull = valueContainsNull;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    public boolean isValueContainsNull() {
        return valueContainsNull;
    }
}
