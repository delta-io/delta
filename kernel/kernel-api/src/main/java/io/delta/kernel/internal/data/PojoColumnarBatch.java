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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import static java.util.Objects.requireNonNull;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.*;

/**
 * Exposes a given a list of POJO objects and their schema as a {@link ColumnarBatch}.
 */
public class PojoColumnarBatch<POJO_TYPE>
        implements ColumnarBatch
{
    private final List<POJO_TYPE> pojoObjects;
    private final StructType schema;
    private final Map<Integer, Function<POJO_TYPE, Object>> ordinalToAccessor;
    private final Map<Integer, String> ordinalToColName;

    public PojoColumnarBatch(
            List<POJO_TYPE> pojoObjects,
            StructType schema,
            Map<Integer, Function<POJO_TYPE, Object>> ordinalToAccessor,
            Map<Integer, String> ordinalToColName)
    {
        this.pojoObjects = requireNonNull(pojoObjects, "pojoObjects is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.ordinalToAccessor = requireNonNull(ordinalToAccessor, "ordinalToAccessor is null");
        this.ordinalToColName = requireNonNull(ordinalToColName, "ordinalToColName is null");
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        return new ColumnVector()
        {
            // this will be accessed frequently, fetch and store locally.
            private DataType dataType = getDataType();

            @Override
            public DataType getDataType()
            {
                return getSchema().at(ordinal).getDataType();
            }

            @Override
            public int getSize()
            {
                return pojoObjects.size();
            }

            @Override
            public void close() { /* Nothing to close */ }

            @Override
            public boolean isNullAt(int rowId)
            {
                return getValue(rowId) == null;
            }

            @Override
            public boolean getBoolean(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof BooleanType, "boolean");
                return (boolean) getValue(rowId);
            }

            @Override
            public byte getByte(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof ByteType, "byte");
                return (Byte) getValue(rowId);
            }

            @Override
            public short getShort(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof ShortType, "short");
                return (short) getValue(rowId);
            }

            @Override
            public int getInt(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof IntegerType, "integer");
                return (int) getValue(rowId);
            }

            @Override
            public long getLong(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof LongType, "long");
                return (long) getValue(rowId);
            }

            @Override
            public float getFloat(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof FloatType, "float");
                return (float) getValue(rowId);
            }

            @Override
            public double getDouble(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof DoubleType, "double");
                return (double) getValue(rowId);
            }

            @Override
            public byte[] getBinary(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof BinaryType, "binary");
                return (byte[]) getValue(rowId);
            }

            @Override
            public String getString(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof StringType, "string");
                return (String) getValue(rowId);
            }

            @Override
            public <K, V> Map<K, V> getMap(int rowId)
            {
                // TODO: this isn't the sufficient typecheck. Need to check the map element types.
                throwIfUnsafeAccess(dataType instanceof MapType, "map");
                return (Map<K, V>) ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public Row getStruct(int rowId)
            {
                throwIfUnsafeAccess(dataType instanceof StructType, "struct");
                return (Row) ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public <T> List<T> getArray(int rowId)
            {
                // TODO: this isn't the sufficient typecheck. Need to check the array element types.
                throwIfUnsafeAccess(dataType instanceof ArrayType, "array");
                return (List<T>) ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            private Object getValue(int rowId)
            {
                return ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            private void throwIfUnsafeAccess(boolean typeSafe, String accessType) {
                if (!typeSafe) {
                    String msg = String.format(
                        "Trying to access a `%s` value from vector of type `%s`",
                        accessType,
                        dataType);
                    throw new UnsupportedOperationException(msg);
                }
            }
        };
    }

    @Override
    public int getSize()
    {
        return pojoObjects.size();
    }
}
