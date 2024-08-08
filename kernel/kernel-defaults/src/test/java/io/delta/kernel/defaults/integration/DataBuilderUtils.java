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
package io.delta.kernel.defaults.integration;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.data.VariantValue;
import io.delta.kernel.types.StructType;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;

import io.delta.kernel.defaults.internal.data.DefaultRowBasedColumnarBatch;

public class DataBuilderUtils {
    public static TestColumnBatchBuilder builder(StructType schema) {
        return new TestColumnBatchBuilder(schema);
    }

    public static Row row(StructType structType, Object... values) {
        return new TestRow(structType, values);
    }

    public static Row row(StructType structType) {
        return new TestRow(structType);
    }

    public static class TestColumnBatchBuilder {
        private StructType schema;
        private List<Row> rows = new ArrayList<>();

        private TestColumnBatchBuilder(StructType schema) {
            this.schema = schema;
        }

        public TestColumnBatchBuilder addRow(Object... values) {
            checkArgument(values.length == schema.length(), "Invalid columns length");
            rows.add(row(schema, values));

            return this;
        }

        public TestColumnBatchBuilder addAllNullsRow() {
            rows.add(row(schema));
            return this;
        }

        public ColumnarBatch build() {
            return new DefaultRowBasedColumnarBatch(schema, rows);
        }
    }

    private static class TestRow implements Row {
        private final StructType schema;
        private final Map<Integer, Object> values;

        private TestRow(StructType schema, Object... values) {
            this.schema = schema;
            this.values = new HashMap<>();
            for (int i = 0; i < values.length; i++) {
                // lamdas + streams don't work well with null values
                this.values.put(i, values[i]);
            }
        }

        private TestRow(StructType schema) {
            Map<Integer, Object> values = new HashMap<>();
            IntStream.range(0, schema.length()).forEach(idx -> values.put(idx, null));
            this.schema = schema;
            this.values = values;
        }

        @Override
        public StructType getSchema() {
            return schema;
        }

        @Override
        public boolean isNullAt(int ordinal) {
            return values.get(ordinal) == null;
        }

        @Override
        public boolean getBoolean(int ordinal) {
            return (boolean) values.get(ordinal);
        }

        @Override
        public byte getByte(int ordinal) {
            return (byte) values.get(ordinal);
        }

        @Override
        public short getShort(int ordinal) {
            return (short) values.get(ordinal);
        }

        @Override
        public int getInt(int ordinal) {
            return (int) values.get(ordinal);
        }

        @Override
        public long getLong(int ordinal) {
            return (long) values.get(ordinal);
        }

        @Override
        public float getFloat(int ordinal) {
            return (float) values.get(ordinal);
        }

        @Override
        public double getDouble(int ordinal) {
            return (double) values.get(ordinal);
        }

        @Override
        public String getString(int ordinal) {
            return (String) values.get(ordinal);
        }

        @Override
        public BigDecimal getDecimal(int ordinal) {
            return (BigDecimal) values.get(ordinal);
        }

        @Override
        public byte[] getBinary(int ordinal) {
            return (byte[]) values.get(ordinal);
        }

        @Override
        public Row getStruct(int ordinal) {
            return (Row) values.get(ordinal);
        }

        @Override
        public ArrayValue getArray(int ordinal) {
            throw new UnsupportedOperationException(
                    "array type unsupported for TestColumnBatchBuilder; use scala test utilities");
        }

        @Override
        public MapValue getMap(int ordinal) {
            throw new UnsupportedOperationException(
                    "map type unsupported for TestColumnBatchBuilder; use scala test utilities");
        }

        @Override
        public VariantValue getVariant(int ordinal) {
            return (VariantValue) values.get(ordinal);
        }
    }
}
