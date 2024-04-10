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

import io.delta.kernel.annotation.Evolving;

/**
 * Base class for all data types.
 *
 * @since 3.0.0
 */
@Evolving
public abstract class DataType {
    /**
     * Convert the data type to Delta protocol specified serialization format.
     *
     * @return Data type serialized in JSON format.
     */
    public abstract String toJson();

    /**
     * Are the data types same? The metadata or column names could be different.
     *
     * @param dataType
     * @return
     */
    public boolean equivalent(DataType dataType) {
        return equals(dataType);
    }

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    // TODO: Implement proper parsing.
    public static DataType fromSQL(String json) {
        switch (json.toLowerCase()) {
            case "byte":
                return ByteType.BYTE;
            case "short":
                return ShortType.SHORT;
            case "integer":
                return IntegerType.INTEGER;
            case "long":
                return LongType.LONG;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + json.toLowerCase());
        }
    }
}

