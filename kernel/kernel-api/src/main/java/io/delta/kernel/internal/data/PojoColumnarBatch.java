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

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

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
        return new ColumnVector() {
            @Override
            public DataType getDataType()
            {
                // TODO: null safe
                return getSchema().get(ordinalToColName.get(ordinal)).getDataType();
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
                // TODO: safe typecast
                return (boolean) getValue(rowId);
            }

            @Override
            public byte getByte(int rowId)
            {
                // TODO: safe typecast
                return (Byte) getValue(rowId);
            }

            @Override
            public short getShort(int rowId)
            {
                // TODO: safe typecast
                return (short) getValue(rowId);
            }

            @Override
            public int getInt(int rowId)
            {
                // TODO: safe typecast
                return (int) getValue(rowId);
            }

            @Override
            public long getLong(int rowId)
            {
                // TODO: safe typecast
                return (long) getValue(rowId);
            }

            @Override
            public float getFloat(int rowId)
            {
                // TODO: safe typecast
                return (float) getValue(rowId);
            }

            @Override
            public double getDouble(int rowId)
            {
                // TODO: safe typecast
                return (double) getValue(rowId);
            }

            @Override
            public byte[] getBinary(int rowId)
            {
                // TODO: safe typecast
                return (byte[]) getValue(rowId);
            }

            @Override
            public String getString(int rowId)
            {
                // TODO: safe typecast
                return (String) getValue(rowId);
            }

            private Object getValue(int rowId)
            {
                return ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public <K, V>  Map<K, V> getMap(int rowId)
            {
                return (Map<K, V>) ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public Row getStruct(int rowId)
            {
                return (Row) ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }

            @Override
            public <T> List<T>  getArray(int rowId)
            {
                return (List<T>) ordinalToAccessor.get(ordinal).apply(pojoObjects.get(rowId));
            }
        };
    }

    @Override
    public int getSize()
    {
        return pojoObjects.size();
    }
}
