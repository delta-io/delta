package io.delta.standalone.expressions;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

/**
 * A column whose row-value will be computed based on the data in a [[RowRecord]].
 *
 * Usage: new Column(columnName, columnDataType).
 *
 * It is recommended that you instantiate using a table schema (StructType).
 * e.g. schema.column(columnName)
 */
public final class Column extends LeafExpression {
    private final String name;
    private final DataType dataType;
    private final RowRecordEvaluator evaluator;

    public Column(String name, DataType dataType) {
        this.name = name;
        this.dataType = dataType;

        if (dataType instanceof IntegerType) {
            evaluator = (record -> record.getInt(name));
        } else if (dataType instanceof BooleanType) {
            evaluator = (record -> record.getBoolean(name));
        } else if (dataType instanceof StringType) {
            evaluator = (record -> record.getString(name));
        } else {
            throw new RuntimeException("Couldn't find matching rowRecord DataType for column: " + name);
        }
    }

    public String name() {
        return name;
    }

    @Override
    public Object eval(RowRecord record) {
        return record.isNullAt(name) ? null : evaluator.nullSafeEval(record);
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
        return Objects.equals(name, column.name) &&
            Objects.equals(dataType, column.dataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType);
    }

    @FunctionalInterface
    private interface RowRecordEvaluator {
        Object nullSafeEval(RowRecord record);
    }
}
