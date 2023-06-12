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

import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ColumnarBatch} wrapper around list of {@link Row} objects.
 */
public class DefaultRowBasedColumnarBatch
        implements ColumnarBatch
{
    private final StructType schema;
    private final List<Row> rows;
    private final Map<Integer, String> columnIndexToNameMap;

    public DefaultRowBasedColumnarBatch(StructType schema, List<Row> rows) {
        this.schema = schema;
        this.rows = rows;
        this.columnIndexToNameMap = constructColumnIndexMap(schema);
        // TODO: do a validation to make sure the values match with the schema
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public int getSize()
    {
        return rows.size();
    }

    @Override
    public ColumnarBatch slice(int start, int end)
    {
        return null;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        String columnName = columnIndexToNameMap.get(ordinal);
        StructField field = schema.get(columnName);
        return new DefaultColumnVector(field.getDataType(), rows, ordinal);
    }

    private static Map<Integer, String> constructColumnIndexMap(StructType schema) {
        // TODO: explore Java's zipWithIndex if available
        Map<Integer, String> columnIndexToNameMap = new HashMap<>();
        int index = 0;
        for (StructField field : schema.fields()) {
            columnIndexToNameMap.put(index, field.getName());
            index++;
        }
        return columnIndexToNameMap;
    }
}
