package org.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

public class ValueVisitor implements LogicalTypeVisitor<Object> {

    private final RowData row;

    private final int index;

    public ValueVisitor(RowData row, int index) {
        this.row = row;
        this.index = index;
    }


    @Override
    public String visit(CharType charType) {
        return row.getString(index).toString();
    }

    @Override
    public String visit(VarCharType varCharType) {
        return row.getString(index).toString();
    }

    @Override
    public Boolean visit(BooleanType booleanType) {
        return row.getBoolean(index);
    }

    @Override
    public byte[] visit(BinaryType binaryType) {
        return row.getBinary(index);
    }

    @Override
    public byte[] visit(VarBinaryType varBinaryType) {
        return row.getBinary(index);
    }

    @Override
    public DecimalData visit(DecimalType decimalType) {
        return row.getDecimal(index, decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public Byte visit(TinyIntType tinyIntType) {
        return row.getByte(index);
    }

    @Override
    public Short visit(SmallIntType smallIntType) {
        return row.getShort(index);
    }

    @Override
    public Integer visit(IntType intType) {
        return row.getInt(index);
    }

    @Override
    public Long visit(BigIntType bigIntType) {
        return row.getLong(index);
    }

    @Override
    public Float visit(FloatType floatType) {
        return row.getFloat(index);
    }

    @Override
    public Double visit(DoubleType doubleType) {
        return row.getDouble(index);
    }

    @Override
    public Object visit(DateType dateType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public TimestampData visit(TimeType timeType) {
        return row.getTimestamp(index, timeType.getPrecision());
    }

    @Override
    public TimestampData visit(TimestampType timestampType) {
        return row.getTimestamp(index, timestampType.getPrecision());
    }

    @Override
    public TimestampData visit(ZonedTimestampType zonedTimestampType) {
        return row.getTimestamp(index, zonedTimestampType.getPrecision());
    }

    @Override
    public TimestampData visit(LocalZonedTimestampType localZonedTimestampType) {
        return row.getTimestamp(index, localZonedTimestampType.getPrecision());
    }

    @Override
    public TimestampData visit(YearMonthIntervalType yearMonthIntervalType) {
        return row.getTimestamp(index, yearMonthIntervalType.getYearPrecision());
    }

    @Override
    public TimestampData visit(DayTimeIntervalType dayTimeIntervalType) {
        return row.getTimestamp(index, dayTimeIntervalType.getDayPrecision());
    }

    @Override
    public ArrayData visit(ArrayType arrayType) {
        return row.getArray(index);
    }

    @Override
    public Object visit(MultisetType multisetType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public MapData visit(MapType mapType) {
        return row.getMap(index);
    }

    @Override
    public RowData visit(RowType rowType) {
        return row.getRow(index, rowType.getFieldCount());
    }

    @Override
    public Object visit(DistinctType distinctType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object visit(StructuredType structuredType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object visit(NullType nullType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object visit(LogicalType other) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object visit(SymbolType symbolType) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public RawValueData<Object> visit(RawType rawType) {
        return row.getRawValue(index);
    }
}
