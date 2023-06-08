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

package io.delta.kernel.data;

import io.delta.kernel.types.StructType;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ParquetRowRecord implements Row
{
    private final StructType schema;
    private final Object[] values;

    public ParquetRowRecord(StructType schema, Object[] values) {
        this.schema = requireNonNull(schema, "schema is null");
        this.values = requireNonNull(values, "values is null");
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public boolean isNullAt(int ordinal)
    {
        return values[ordinal] == null;
    }

    @Override
    public boolean getBoolean(int ordinal)
    {
        return (boolean) values[ordinal];
    }

    @Override
    public int getInt(int ordinal)
    {
        return (int) values[ordinal];
    }

    @Override
    public long getLong(int ordinal)
    {
        return (long) values[ordinal];
    }

    @Override
    public String getString(int ordinal)
    {
        return (String) values[ordinal];
    }

    @Override
    public Row getRecord(int ordinal)
    {
        return (Row) values[ordinal];
    }

    @Override
    public <T> List<T> getList(int ordinal)
    {
        return (List<T>) values[ordinal];
    }

    @Override
    public <K, V> Map<K, V> getMap(int ordinal)
    {
        return (Map<K, V>) values[ordinal];
    }
}
