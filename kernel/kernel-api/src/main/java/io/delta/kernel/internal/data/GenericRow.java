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

package io.delta.kernel.internal.data;

import java.math.BigDecimal;
import java.util.Map;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.*;

/**
 * Exposes a given map of values as a {@link Row}
 */
public class GenericRow implements Row {
    private final StructType schema;
    private final Map<Integer, Object> ordinalToValue;


    /**
     * @param schema the schema of the row
     * @param ordinalToValue a mapping of column ordinal to objects; for each column the object
     *                       must be of the return type corresponding to the data type's getter
     *                       method in the Row interface
     */
    public GenericRow(StructType schema, Map<Integer, Object> ordinalToValue) {
        this.schema = requireNonNull(schema, "schema is null");
        this.ordinalToValue = requireNonNull(ordinalToValue, "ordinalToValue is null");
    }

    @Override
    public StructType getSchema() {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal) {
        return getValue(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal) {
        throwIfUnsafeAccess(ordinal, BooleanType.class, "boolean");
        return (boolean) getValue(ordinal);
    }

    @Override
    public byte getByte(int ordinal) {
        throwIfUnsafeAccess(ordinal, ByteType.class, "byte");
        return (byte) getValue(ordinal);
    }

    @Override
    public short getShort(int ordinal) {
        throwIfUnsafeAccess(ordinal, ShortType.class, "short");
        return (short) getValue(ordinal);
    }

    @Override
    public int getInt(int ordinal) {
        throwIfUnsafeAccess(ordinal, IntegerType.class, "integer");
        return (int) getValue(ordinal);
    }

    @Override
    public long getLong(int ordinal) {
        throwIfUnsafeAccess(ordinal, LongType.class, "long");
        return (long) getValue(ordinal);
    }

    @Override
    public float getFloat(int ordinal) {
        throwIfUnsafeAccess(ordinal, FloatType.class, "float");
        return (float) getValue(ordinal);
    }

    @Override
    public double getDouble(int ordinal) {
        throwIfUnsafeAccess(ordinal, DoubleType.class, "double");
        return (double) getValue(ordinal);
    }

    @Override
    public String getString(int ordinal) {
        throwIfUnsafeAccess(ordinal, StringType.class, "string");
        return (String) getValue(ordinal);
    }

    @Override
    public BigDecimal getDecimal(int ordinal) {
        throwIfUnsafeAccess(ordinal, DecimalType.class, "decimal");
        return (BigDecimal) getValue(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal) {
        throwIfUnsafeAccess(ordinal, BinaryType.class, "binary");
        return (byte[]) getValue(ordinal);
    }

    @Override
    public Row getStruct(int ordinal) {
        throwIfUnsafeAccess(ordinal, StructType.class, "struct");
        return (Row) getValue(ordinal);
    }

    @Override
    public ArrayValue getArray(int ordinal) {
        // TODO: not sufficient check, also need to check the element type
        throwIfUnsafeAccess(ordinal, ArrayType.class, "array");
        return (ArrayValue) getValue(ordinal);
    }

    @Override
    public MapValue getMap(int ordinal) {
        // TODO: not sufficient check, also need to check the element types
        throwIfUnsafeAccess(ordinal, MapType.class, "map");
        return (MapValue) getValue(ordinal);
    }

    @Override
    public VariantValue getVariant(int ordinal) {
        // TODO(r.chen): test this path somehow?
        throwIfUnsafeAccess(ordinal, VariantType.class, "variant");
        return (VariantValue) getValue(ordinal);
    }

    private Object getValue(int ordinal) {
        return ordinalToValue.get(ordinal);
    }

    private void throwIfUnsafeAccess(
        int ordinal, Class<? extends DataType> expDataType, String accessType) {

        DataType actualDataType = dataType(ordinal);
        if (!expDataType.isAssignableFrom(actualDataType.getClass())) {
            String msg = String.format(
                "Trying to access a `%s` value from vector of type `%s`",
                accessType,
                actualDataType);
            throw new UnsupportedOperationException(msg);
        }
    }

    private DataType dataType(int ordinal) {
        if (schema.length() <= ordinal) {
            throw new IllegalArgumentException("invalid ordinal: " + ordinal);
        }

        return schema.at(ordinal).getDataType();
    }
}
