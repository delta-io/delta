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

import java.util.*;
import java.util.stream.Collectors;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.utils.Tuple2;

public final class StructType extends DataType {

    public static StructType EMPTY_INSTANCE = new StructType();

    public static StructType fromRow(Row row) {
        final List<Row> fields = row.getList(0);
        return new StructType(
            fields
                .stream()
                .map(StructField::fromRow)
                .collect(Collectors.toList())
        );
    }

    // TODO: docs
    public static StructType READ_SCHEMA = new StructType()
        .add("fields", new ArrayType(StructField.READ_SCHEMA, false /* contains null */ ));

    private final Map<String, Tuple2<StructField, Integer>> nameToFieldAndOrdinal;
    private final List<StructField> fields;
    private final List<String> fieldNames;

    public StructType() {
        this(new ArrayList<>());
    }

    public StructType(List<StructField> fields) {
        this.fields = fields;
        this.fieldNames = fields.stream().map(f -> f.getName()).collect(Collectors.toList());

        this.nameToFieldAndOrdinal = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            nameToFieldAndOrdinal.put(fields.get(i).getName(), new Tuple2<>(fields.get(i), i));
        }
    }

    public StructType add(StructField field) {
        final List<StructField> fieldsCopy = new ArrayList<>(fields);
        fieldsCopy.add(field);

        return new StructType(fieldsCopy);
    }

    public StructType add(String name, DataType dataType) {
        return add(new StructField(name, dataType, true /* nullable */,
            new HashMap<String, String>()));
    }

    public StructType add(String name, DataType dataType, Map<String, String> metadata) {
        return add(new StructField(name, dataType, true /* nullable */, metadata));
    }

    /**
     * @return array of fields
     */
    public List<StructField> fields() {
        return Collections.unmodifiableList(fields);
    }

    /**
     * @return array of field names
     */
    public List<String> fieldNames() {
        return fieldNames;
    }

    /**
     * @return the number of fields
     */
    public int length() {
        return fields.size();
    }

    public int indexOf(String fieldName) {
        return fieldNames.indexOf(fieldName);
    }

    public StructField get(String fieldName) {
        return nameToFieldAndOrdinal.get(fieldName)._1;
    }

    public StructField at(int index) {
        return fields.get(index);
    }

    /**
     * Creates a {@link Column} expression for the field at the given {@code ordinal}
     *
     * @param ordinal the ordinal of the {@link StructField} to create a column for
     * @return a {@link Column} expression for the {@link StructField} with ordinal {@code ordinal}
     */
    public Column column(int ordinal) {
        final StructField field = at(ordinal);
        return new Column(ordinal, field.getName(), field.getDataType());
    }

    /**
     * Creates a {@link Column} expression for the field with the given {@code fieldName}.
     *
     * @param fieldName the name of the {@link StructField} to create a column for
     * @return a {@link Column} expression for the {@link StructField} with name {@code fieldName}
     */
    public Column column(String fieldName) {
        Tuple2<StructField, Integer> fieldAndOrdinal = nameToFieldAndOrdinal.get(fieldName);
        System.out.println("Created column " + fieldName + " with ordinal " + fieldAndOrdinal._2);
        return new Column(fieldAndOrdinal._2, fieldName, fieldAndOrdinal._1.getDataType());
    }

    @Override
    public String toString() {
        return String.format(
            "%s(%s)",
            getClass().getSimpleName(),
            fields.stream().map(StructField::toString).collect(Collectors.joining(", "))
        );
    }

    /**
     * @return a readable indented tree representation of this {@code StructType}
     *         and all of its nested elements
     */
    public String treeString() {
        return "TODO";
    }
}
