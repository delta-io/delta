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
 * The data type for collections of multiple values.
 */
public final class ArrayType extends DataType {
    private final DataType elementType;
    private final boolean containsNull;

    /**
     * @param elementType  the data type of values
     * @param containsNull  indicates if values have {@code null} value
     */
    public ArrayType(DataType elementType, boolean containsNull) {
        this.elementType = elementType;
        this.containsNull = containsNull;
    }

    /**
     * @return the type of array elements
     */
    public DataType getElementType() {
        return elementType;
    }

    /**
     * @return {@code true} if the array has {@code null} values, else {@code false}
     */
    public boolean containsNull() {
        return containsNull;
    }

    /**
     * Builds a readable {@code String} representation of this {@code ArrayType}.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- element: %s (containsNull = %b)\n", prefix, elementType.getTypeName(), containsNull));
        DataType.buildFormattedString(elementType, nextPrefix, builder);
    }
}
