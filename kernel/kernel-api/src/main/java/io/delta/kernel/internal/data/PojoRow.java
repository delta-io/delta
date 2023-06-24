package io.delta.kernel.internal.data;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

// TODO: check for unsafe access
/**
 * Exposes a POJO object as a {@link Row}
 */
public class PojoRow<POJO_TYPE> implements Row {
    private final POJO_TYPE pojoObject;
    private final StructType schema;
    private final Map<Integer, Function<POJO_TYPE, Object>> ordinalToAccessor;

    public PojoRow(
            POJO_TYPE pojoObject,
            StructType schema,
            Map<Integer, Function<POJO_TYPE, Object>> ordinalToAccessor) {
        this.pojoObject = requireNonNull(pojoObject, "pojoObjects is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.ordinalToAccessor = requireNonNull(ordinalToAccessor, "ordinalToAccessor is null");
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return getValue(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return (boolean) getValue(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
        return (byte) getValue(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
        return (short) getValue(ordinal);
    }

    @Override
    public int getInt(int ordinal)
    {
        return (int) getValue(ordinal);
    }

    @Override
    public long getLong(int ordinal)
    {
        return (long) getValue(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
        return (float) getValue(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
        return (double) getValue(ordinal);
    }

    @Override
    public String getString(int ordinal)
    {
        return (String) getValue(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
        return (byte[]) getValue(ordinal);
    }

    @Override
    public Row getStruct(int ordinal)
    {
        return (Row) getValue(ordinal);
    }

    @Override
    public <T> List<T> getArray(int ordinal)
    {
        return (List<T>) getValue(ordinal);
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        return (Map<K, V>) getValue(ordinal);
    }

    private Object getValue(int ordinal) {
        return ordinalToAccessor.get(ordinal).apply(pojoObject);
    }
}
