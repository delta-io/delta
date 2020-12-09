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

import java.util.Objects;

/**
 * A field inside a {@link StructType}.
 */
public final class StructField {
    private final String name;
    private final DataType dataType;
    private final boolean nullable;

    /**
     * @param name  the name of this field
     * @param dataType  the data type of this field
     * @param nullable  indicates if values of this field can be {@code null} values
     */
    public StructField(String name, DataType dataType, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
    }

    /**
     * Constructor with default {@code nullable = true}.
     *
     * @param name  the name of this field
     * @param dataType  the data type of this field
     */
    public StructField(String name, DataType dataType) {
        this(name, dataType, true);
    }

    /**
     * @return the name of this field
     */
    public String getName() {
        return name;
    }

    /**
     * @return the data type of this field
     */
    public DataType getDataType() {
        return dataType;
    }

    /**
     * @return whether this field allows to have a {@code null} value.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * Builds a readable {@code String} representation of this {@code StructField}.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
        final String nextPrefix = prefix + "    |";
        builder.append(String.format("%s-- %s: %s (nullable = %b)\n", prefix, name, dataType.getTypeName(), nullable));
        DataType.buildFormattedString(dataType, nextPrefix, builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructField that = (StructField) o;
        return name.equals(that.name) && dataType.equals(that.dataType) && nullable == that.nullable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, nullable);
    }
}
