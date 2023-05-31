package io.delta.kernel.types;

import io.delta.kernel.data.Row;

/**
 * TODO: this needs to be removed, for now ignore this.
 */
public class UnresolvedDataType extends DataType {

    public static final UnresolvedDataType INSTANCE = new UnresolvedDataType();

    public static DataType fromRow(Row row, int ordinal) {
        try {
            // e.g. IntegerType -> {"name":"as_int","type":"integer","nullable":true,"metadata":{}
            // e.g. LongType -> {"name":"as_long","type":"long","nullable":true,"metadata":{}}
            final String typeName = row.getString(ordinal);
            return DataType.createPrimitive(typeName);
        } catch (RuntimeException ex) {
            throw new RuntimeException("Failed to parse UnresolvedDataType");
        }
    }

    private UnresolvedDataType() { }
}
