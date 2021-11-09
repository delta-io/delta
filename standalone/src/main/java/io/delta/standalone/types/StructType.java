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
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import io.delta.standalone.expressions.Column;

/**
 * The data type representing a table's schema, consisting of a collection of
 * fields (that is, {@code fieldName} to {@code dataType} pairs).
 *
 * @see StructField StructField
 */
public final class StructType extends DataType {
    private final StructField[] fields;
    private final HashMap<String, StructField> nameToField;

    public StructType() {
        this(new StructField[0]);
    }

    public StructType(StructField[] fields) {
        this.fields = fields;
        this.nameToField = new HashMap<>();
        Arrays.stream(fields).forEach(field -> nameToField.put(field.getName(), field));
    }

    /**
     * Creates a new {@link StructType} by adding a new field.
     *
     * <pre>{@code
     * StructType schema = new StructType()
     *     .add(new StructField("a", new IntegerType(), true))
     *     .add(new StructField("b", new LongType(), false))
     *     .add(new StructField("c", new StringType(), true))
     * }</pre>
     * @param field  The new field to add.
     * @return The new {@link StructType}.
     */
    public StructType add(StructField field) {
        StructField[] newFields = Arrays.copyOf(fields, fields.length + 1);
        newFields[newFields.length - 1] = field;
        return new StructType(newFields);
    }

    /**
     * Creates a new {@link StructType} by adding a new nullable field with no metadata.
     *
     * <pre>{@code
     * StructType schema = new StructType()
     *     .add("a", new IntegerType())
     *     .add("b", new LongType())
     *     .add("c", new StringType())
     * }</pre>
     * @param fieldName  The name of the new field.
     * @param dataType  The datatype for the new field.
     * @return The new {@link StructType}.
     */
    public StructType add(String fieldName, DataType dataType) {
        StructField newField = new StructField(fieldName, dataType, true);
        return add(newField);
    }

    /**
     * Creates a new {@link StructType} by adding a new field with no metadata.
     *
     * <pre>{@code
     * StructType schema = new StructType()
     *     .add("a", new IntegerType(), true)
     *     .add("b", new LongType(), false)
     *     .add("c", new StringType(), true)
     * }</pre>
     * @param fieldName  The name of the new field.
     * @param dataType  The datatype for the new field.
     * @param nullable  Whether or not the new field is nullable.
     * @return The new {@link StructType}.
     */
    public StructType add(String fieldName, DataType dataType, boolean nullable) {
        StructField newField = new StructField(fieldName, dataType, nullable);
        return add(newField);
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
     * @return the number of fields
     */
    public int length() {
        return fields.length;
    }

    /**
     * @param fieldName  the name of the desired {@link StructField}, not null
     * @return the {@code link} with the given name, not null
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

    public Column column(String fieldName) {
        StructField field = nameToField.get(fieldName);
        return new Column(fieldName, field.getDataType());
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
