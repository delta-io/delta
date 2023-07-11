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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.types.StructType;

/**
 * The type of Row that will be evaluated against {@link Column}s.
 * <p>
 * These Columns must be partition columns, and will have ordinals matching the latest snapshot
 * schema.
 */
public class PartitionRow
    implements Row
{

    private final StructType schema;
    private final Map<Integer, String> ordinalToValue;

    public PartitionRow(
        StructType schema,
        Map<String, Integer> partitionOrdinals,
        Map<String, String> partitionValuesMap)
    {
        this.ordinalToValue = new HashMap<>();
        for (Map.Entry<String, Integer> entry : partitionOrdinals.entrySet()) {
            final String partitionColumnName = entry.getKey();
            final int partitionColumnOrdinal = entry.getValue();
            final String partitionColumnValue = partitionValuesMap.get(partitionColumnName);
            ordinalToValue.put(partitionColumnOrdinal, partitionColumnValue);
        }
        this.schema = schema;
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return ordinalToValue.get(ordinal) == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return Boolean.parseBoolean(ordinalToValue.get(ordinal));
    }

    @Override
    public byte getByte(int ordinal)
    {
        return Byte.parseByte(ordinalToValue.get(ordinal));
    }

    @Override
    public short getShort(int ordinal)
    {
        return Short.parseShort(ordinalToValue.get(ordinal));
    }

    @Override
    public int getInt(int ordinal)
    {
        return Integer.parseInt(ordinalToValue.get(ordinal));
    }

    @Override
    public long getLong(int ordinal)
    {
        return Long.parseLong(ordinalToValue.get(ordinal));
    }

    @Override
    public float getFloat(int ordinal)
    {
        return Float.parseFloat(ordinalToValue.get(ordinal));
    }

    @Override
    public double getDouble(int ordinal)
    {
        return Double.parseDouble(ordinalToValue.get(ordinal));
    }

    @Override
    public String getString(int ordinal)
    {
        return ordinalToValue.get(ordinal);
    }

    @Override
    public byte[] getBinary(int ordinal)
    {
        // TODO: verify if this is the correct way.
        return ordinalToValue.get(ordinal).getBytes();
    }

    @Override
    public Row getStruct(int ordinal)
    {
        throw new UnsupportedOperationException("Partition values can't be StructTypes");
    }

    @Override
    public <T> List<T> getArray(int ordinal)
    {
        throw new UnsupportedOperationException("Partition values can't be ArrayType");
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        throw new UnsupportedOperationException("Partition values can't be MapType");
    }
}
