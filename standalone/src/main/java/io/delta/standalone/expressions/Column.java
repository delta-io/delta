package io.delta.standalone.expressions;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.*;

/**
 * A column whose row-value will be computed based on the data in a {@link RowRecord}.
 * <p>
 * Usage: {@code new Column(columnName, columnDataType)}.
 * <p>
 * It is recommended that you instantiate using an existing table schema ({@link StructType}).
 * e.g. {@code mySchema.column(columnName)}
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
        } else if (dataType instanceof LongType) {
            evaluator = (record -> record.getLong(name));
        } else if (dataType instanceof ByteType) {
            evaluator = (record -> record.getByte(name));
        } else if (dataType instanceof ShortType) {
            evaluator = (record -> record.getShort(name));
        } else if (dataType instanceof BooleanType) {
            evaluator = (record -> record.getBoolean(name));
        } else if (dataType instanceof FloatType) {
            evaluator = (record -> record.getFloat(name));
        } else if (dataType instanceof DoubleType) {
            evaluator = (record -> record.getDouble(name));
        } else if (dataType instanceof StringType) {
            evaluator = (record -> record.getString(name));
        } else if (dataType instanceof BinaryType) {
            evaluator = (record -> record.getBinary(name));
        } else if (dataType instanceof DecimalType) {
            evaluator = (record -> record.getBigDecimal(name));
        } else if (dataType instanceof TimestampType) {
            evaluator = (record -> record.getTimestamp(name));
        } else if (dataType instanceof DateType) {
            evaluator = (record -> record.getDate(name));
        } else {
            throw new UnsupportedOperationException("The data type of column " + name +
                    " is " + dataType.getTypeName() + ". This is not supported yet.");
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
