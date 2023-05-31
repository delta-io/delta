package io.delta.kernel.types;

public class MapType extends DataType {

    public static final MapType EMPTY_INSTANCE = new MapType(null, null, false);

    private final DataType keyType;
    private final DataType valueType;
    private final boolean valueContainsNull;

    public MapType(DataType keyType, DataType valueType, boolean valueContainsNull) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.valueContainsNull = valueContainsNull;
    }

    public DataType getKeyType() {
        return keyType;
    }

    public DataType getValueType() {
        return valueType;
    }

    public boolean isValueContainsNull() {
        return valueContainsNull;
    }
}
