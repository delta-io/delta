package io.delta.kernel.types;

import io.delta.kernel.data.Row;

import java.util.Map;

public class StructField {

    ////////////////////////////////////////////////////////////////////////////////
    // Static Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    public static StructField fromRow(Row row) {
        final String name = row.getString(0);
        final DataType type = UnresolvedDataType.fromRow(row, 1);
        final boolean nullable = row.getBoolean(2);
        final Map<String, String> metadata = row.getMap(3);
        return new StructField(name, type, nullable, metadata);
    }

    public static final StructType READ_SCHEMA = new StructType()
        .add("name", StringType.INSTANCE)
        .add("type", UnresolvedDataType.INSTANCE)
        .add("nullable", BooleanType.INSTANCE)
        .add("metadata", new MapType(StringType.INSTANCE, StringType.INSTANCE, false));

    ////////////////////////////////////////////////////////////////////////////////
    // Instance Fields / Methods
    ////////////////////////////////////////////////////////////////////////////////

    private final String name;
    private final DataType dataType;
    private final boolean nullable;
    private final Map<String, String> metadata;
    // private final FieldMetadata metadata;

    public StructField(
            String name,
            DataType dataType,
            boolean nullable,
            Map<String, String> metadata) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        this.metadata = metadata;
    }

    public String getName() {
        return name;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public boolean isNullable() {
        return nullable;
    }

    @Override
    public String toString() {
        return String.format("StructField(name=%s,type=%s,nullable=%s,metadata=%s)",
                name, dataType, nullable, "empty(fix - this)");
    }
}
