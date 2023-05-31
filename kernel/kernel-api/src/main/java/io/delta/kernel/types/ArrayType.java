package io.delta.kernel.types;

public class ArrayType extends DataType {
    public static ArrayType EMPTY_INSTANCE = new ArrayType(null, false);

    private final DataType elementType;
    private final boolean containsNull;

    public ArrayType(DataType elementType, boolean containsNull) {
        this.elementType = elementType;
        this.containsNull = containsNull;
    }

    public DataType getElementType() {
        return elementType;
    }

    public boolean containsNull() {
        return containsNull;
    }
}
