package io.delta.kernel.types;

import java.util.*;
import java.util.stream.Collectors;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.utils.Tuple2;

public final class StructType extends DataType {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

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

    public static StructType READ_SCHEMA = new StructType()
        .add("fields", new ArrayType(StructField.READ_SCHEMA, false /* contains null */ ));

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

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
        return add(new StructField(name, dataType, true /* nullable */, new HashMap<String, String>()));
    }

    public StructType add(String name, DataType dataType, Map<String, String> metadata) {
        return add(new StructField(name, dataType, true /* nullable */, metadata));
    }

    public List<StructField> fields() {
        return Collections.unmodifiableList(fields);
    }

    public List<String> fieldNames() {
        return fieldNames;
    }

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
     * Creates a {@link Column} expression for the field with the given
     * {@code fieldName}.
     *
     * @param ordinal the ordinal of the {@link StructField} to create a column for
     * @return a {@link Column} expression for the {@link StructField} with name {@code fieldName}
     */
    public Column column(int ordinal) {
        final StructField field = at(ordinal);
        return new Column(ordinal, field.getName(), field.getDataType());
    }

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

    public String treeString() {
        return "TODO";
    }
}
