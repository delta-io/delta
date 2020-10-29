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

import java.util.Arrays;
import java.util.HashMap;

/**
 * The data type representing a table's schema, consisting of a collection of
 * fields (that is, {@code fieldName} to {@code fieldDataType} pairs).
 *
 * @see StructField StructField
 */
public final class StructType extends DataType {
    private final StructField[] fields;
    private final HashMap<String, StructField> nameToField;

    public StructType(StructField[] fields) {
        this.fields = fields;

        // generate name -> field map
        this.nameToField = new HashMap<>();
        Arrays.stream(fields).forEach(field -> nameToField.put(field.getName(), field));
    }

    /**
     * @return array of fields
     */
    public StructField[] getFields() {
        return fields.clone();
    }

    /**
     * @return array of field names
     */
    public String[] getFieldNames() {
        return Arrays.stream(fields).map(StructField::getName).toArray(String[]::new);
    }

    /**
     * @param fieldName  the name of the desired {@code StructField}, not null
     * @return the {@code StructField} with the given name, not null
     * @throws IllegalArgumentException if a field with the given name does not exist
     */
    public StructField get(String fieldName) {
        if (!nameToField.containsKey(fieldName)) {
            throw new IllegalArgumentException(
                String.format(
                        "Field \"%s\" does not exist. Available fields: %s",
                        fieldName,
                        Arrays.toString(getFieldNames()))
                );
        }

        return nameToField.get(fieldName);
    }

    /**
     * @return a readable indented tree representation of this {@code StructType}
     *         and all of its nested elements
     */
    public String getTreeString() {
        final String prefix = " |";
        StringBuilder builder = new StringBuilder();
        builder.append("root\n");
        Arrays.stream(fields).forEach(field -> field.buildFormattedString(prefix, builder));
        return builder.toString();
    }

    /**
     * Builds a readable {@code String} representation of this {@code StructType}
     * and all of its nested elements.
     */
    protected void buildFormattedString(String prefix, StringBuilder builder) {
        Arrays.stream(fields).forEach(field -> field.buildFormattedString(prefix, builder));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StructType that = (StructType) o;
        return Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }
}
