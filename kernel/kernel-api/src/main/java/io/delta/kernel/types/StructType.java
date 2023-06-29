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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.utils.Tuple2;

/**
 * Struct type which contains one or more columns.
 */
public final class StructType extends DataType
{

    private final Map<String, Tuple2<StructField, Integer>> nameToFieldAndOrdinal;
    private final List<StructField> fields;
    private final List<String> fieldNames;

    public StructType()
    {
        this(new ArrayList<>());
    }

    public StructType(List<StructField> fields)
    {
        this.fields = fields;
        this.fieldNames = fields.stream().map(f -> f.getName()).collect(Collectors.toList());

        this.nameToFieldAndOrdinal = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            nameToFieldAndOrdinal.put(fields.get(i).getName(), new Tuple2<>(fields.get(i), i));
        }
    }

    public StructType add(StructField field)
    {
        final List<StructField> fieldsCopy = new ArrayList<>(fields);
        fieldsCopy.add(field);

        return new StructType(fieldsCopy);
    }

    public StructType add(String name, DataType dataType)
    {
        return add(new StructField(name, dataType, true /* nullable */, new HashMap<>()));
    }

    public StructType add(String name, DataType dataType, boolean nullable)
    {
        return add(new StructField(name, dataType, nullable, new HashMap<>()));
    }

    public StructType add(String name, DataType dataType, Map<String, String> metadata)
    {
        return add(new StructField(name, dataType, true /* nullable */, metadata));
    }

    /**
     * @return array of fields
     */
    public List<StructField> fields()
    {
        return Collections.unmodifiableList(fields);
    }

    /**
     * @return array of field names
     */
    public List<String> fieldNames()
    {
        return fieldNames;
    }

    /**
     * @return the number of fields
     */
    public int length()
    {
        return fields.size();
    }

    public int indexOf(String fieldName)
    {
        return fieldNames.indexOf(fieldName);
    }

    public StructField get(String fieldName)
    {
        return nameToFieldAndOrdinal.get(fieldName)._1;
    }

    public StructField at(int index)
    {
        return fields.get(index);
    }

    /**
     * Creates a {@link Column} expression for the field at the given {@code ordinal}
     *
     * @param ordinal the ordinal of the {@link StructField} to create a column for
     * @return a {@link Column} expression for the {@link StructField} with ordinal {@code ordinal}
     */
    public Column column(int ordinal)
    {
        final StructField field = at(ordinal);
        return new Column(ordinal, field.getName(), field.getDataType());
    }

    /**
     * Creates a {@link Column} expression for the field with the given {@code fieldName}.
     *
     * @param fieldName the name of the {@link StructField} to create a column for
     * @return a {@link Column} expression for the {@link StructField} with name {@code fieldName}
     */
    public Column column(String fieldName)
    {
        Tuple2<StructField, Integer> fieldAndOrdinal = nameToFieldAndOrdinal.get(fieldName);
        System.out.println("Created column " + fieldName + " with ordinal " + fieldAndOrdinal._2);
        return new Column(fieldAndOrdinal._2, fieldName, fieldAndOrdinal._1.getDataType());
    }

    @Override
    public boolean equivalent(DataType dataType)
    {
        if (!(dataType instanceof StructType)) {
            return false;
        }

        StructType otherType = ((StructType) dataType);
        return otherType.length() == length() &&
            IntStream.range(0, length())
                .mapToObj(i ->
                    otherType.at(i).getDataType().equivalent(at(i).getDataType()))
                .allMatch(result -> result);
    }

    @Override
    public String toString()
    {
        return String.format(
            "%s(%s)",
            getClass().getSimpleName(),
            fields.stream().map(StructField::toString).collect(Collectors.joining(", "))
        );
    }

    @Override
    public String toJson()
    {
        String fieldsAsJson = fields.stream()
            .map(e -> e.toJson())
            .collect(Collectors.joining(",\n"));

        return String.format(
            "{\n" +
                "  \"type\" : \"struct\",\n" +
                "  \"fields\" : [ %s ]\n" +
                "}",
            fieldsAsJson);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StructType that = (StructType) o;
        return nameToFieldAndOrdinal.equals(that.nameToFieldAndOrdinal) &&
            fields.equals(that.fields) &&
            fieldNames.equals(that.fieldNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nameToFieldAndOrdinal, fields, fieldNames);
    }
}
