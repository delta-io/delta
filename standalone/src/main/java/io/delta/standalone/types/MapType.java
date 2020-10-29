/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.types;

/**
 * The data type for Maps. Keys in a map are not allowed to have {@code null} values.
 */
public final class MapType extends DataType {
    private final DataType keyType;
    private final DataType valueType;
    private final boolean valueContainsNull;

    /**
     * @param keyType  the data type of map keys
     * @param valueType  the data type of map values
     * @param valueContainsNull  indicates if map values have {@code null} values
     */
    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.valueContainsNull = valueContainsNull;
    }

    /**
     * @return the data type of map keys
     */
    public DataType getKeyType() {
        return keyType;
    }

    /**
     * @return the data type of map values
     */
    public DataType getValueType() {
        return valueType;
    }

    /**
     * @return {@code true} if this map has null values, else {@code false}
     */
    public boolean valueContainsNull() {
        return valueContainsNull;
    }

    /**
     * Builds a readable {@code String} representation of this {@code MapType}.
     */
    public void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- key: %s\n", prefix, keyType.getTypeName()));
        DataType.buildFormattedString(keyType, nextPrefix, builder);
        builder.append(String.format("%s-- value: %s (valueContainsNull = %b)\n", prefix, valueType.getTypeName(), valueContainsNull));
    }
}
