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

package io.delta.kernel.expressions;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import io.delta.kernel.data.Row;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

/**
 * A column whose row-value will be computed based on the data in a {@link Row}.
 * <p>
 * It is recommended that you instantiate using an existing table schema
 * {@link StructType} with {@link StructType#column(int)}.
 * <p>
 * Only supports primitive data types, see
 * <a href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#primitive-types">Delta Transaction Log Protocol: Primitive Types</a>.
 */
public final class Column extends LeafExpression {
    private final int ordinal;
    private final String name;
    private final DataType dataType;
    private final RowEvaluator evaluator;

    public Column(int ordinal, String name, DataType dataType) {
        this.ordinal = ordinal;
        this.name = name;
        this.dataType = dataType;

        if (dataType instanceof IntegerType) {
            evaluator = (row -> row.getInt(ordinal));
        } else if (dataType instanceof BooleanType) {
            evaluator = (row -> row.getBoolean(ordinal));
        } else if (dataType instanceof LongType) {
            evaluator = (row -> row.getLong(ordinal));
        } else if (dataType instanceof StringType) {
            evaluator = (row -> row.getString(ordinal));
        } else {
            throw new UnsupportedOperationException(
                String.format(
                    "The data type %s of column %s at ordinal %s is not supported",
                    dataType,
                    name,
                    ordinal)
            );
        }
    }

    public String name() {
        return name;
    }

    @Override
    public Object eval(Row row) {
        return row.isNullAt(ordinal) ? null : evaluator.nullSafeEval(row);
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "Column(" + name + ")";
    }

    @Override
    public Set<String> references() {
        return Collections.singleton(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Objects.equals(ordinal, column.ordinal) &&
            Objects.equals(name, column.name) &&
            Objects.equals(dataType, column.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType);
    }

    @FunctionalInterface
    private interface RowEvaluator {
        Object nullSafeEval(Row row);
    }
}
