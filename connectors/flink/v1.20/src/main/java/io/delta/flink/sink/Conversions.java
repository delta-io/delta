package io.delta.flink.sink;

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** */
public class Conversions {
  public static class FlinkToDelta {
    public static StructType schema(RowType rowType) {
      List<StructField> fields =
          rowType.getFields().stream()
              .map(
                  rowField -> {
                    DataType rowFieldType = dataType(rowField.getType());
                    return new StructField(
                        rowField.getName(), rowFieldType, rowField.getType().isNullable());
                  })
              .collect(Collectors.toList());

      return new StructType(fields);
    }

    public static DataType dataType(LogicalType flinkType) {
      final LogicalTypeRoot typeRoot = flinkType.getTypeRoot();
      switch (typeRoot) {
        case INTEGER:
          return IntegerType.INTEGER;
        case VARCHAR:
        case CHAR:
          return StringType.STRING;
        case BIGINT:
          return LongType.LONG;
        case DOUBLE:
          return DoubleType.DOUBLE;
        case FLOAT:
          return FloatType.FLOAT;
        case DECIMAL:
          DecimalType decimalType = (DecimalType) flinkType;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          return new io.delta.kernel.types.DecimalType(precision, scale);
        case DATE:
          return DateType.DATE;
        case TIMESTAMP_WITH_TIME_ZONE:
          return TimestampType.TIMESTAMP;
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          return TimestampNTZType.TIMESTAMP_NTZ;
        default:
          throw new UnsupportedOperationException(
              String.format("Type not supported: %s", flinkType));
      }
    }

    public static Map<String, Literal> partitionValues(
        StructType rowType, Collection<String> partitionColNames, RowData rowData) {
      return partitionColNames.stream()
          .map(
              name -> {
                final int partitionValueColIdx = rowType.indexOf(name);
                return new Object[] {
                  name, Conversions.FlinkToDelta.data(rowType, rowData, partitionValueColIdx)
                };
              })
          .collect(Collectors.toMap(o -> (String) o[0], o -> (Literal) o[1]));
    }

    public static Literal data(StructType rowType, RowData rowData, int colIdx) {
      final StructField field = rowType.at(colIdx);
      final DataType dataType = field.getDataType();
      if (dataType.equivalent(IntegerType.INTEGER)) {
        return Literal.ofInt(rowData.getInt(colIdx));
      } else if (dataType.equivalent(LongType.LONG)) {
        return Literal.ofLong(rowData.getLong(colIdx));
      } else if (dataType.equivalent(StringType.STRING)) {
        return Literal.ofString(rowData.getString(colIdx).toString());
      } else if (dataType.equivalent(DoubleType.DOUBLE)) {
        return Literal.ofDouble(rowData.getDouble(colIdx));
      } else if (dataType.equivalent(FloatType.FLOAT)) {
        return Literal.ofFloat(rowData.getFloat(colIdx));
      } else if (dataType instanceof io.delta.kernel.types.DecimalType) {
        io.delta.kernel.types.DecimalType decimalType =
            (io.delta.kernel.types.DecimalType) dataType;
        int precision = decimalType.getPrecision();
        int scale = decimalType.getScale();
        return Literal.ofDecimal(
            rowData.getDecimal(colIdx, precision, scale).toBigDecimal(), precision, scale);
      } else if (dataType.equivalent(DateType.DATE)) {
        return Literal.ofDate(rowData.getInt(colIdx));
      } else if (dataType.equivalent(TimestampType.TIMESTAMP)) {
        return Literal.ofTimestamp(rowData.getLong(colIdx));
      } else if (dataType.equivalent(TimestampNTZType.TIMESTAMP_NTZ)) {
        return Literal.ofTimestampNtz(rowData.getLong(colIdx));
      } else {
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
      }
    }
  }
}
