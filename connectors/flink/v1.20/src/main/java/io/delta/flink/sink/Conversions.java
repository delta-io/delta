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
        RowType rowType, RowData rowData, Collection<String> partitionColNames) {
      return partitionColNames.stream()
          .map(
              name -> {
                final int partitionValueColIdx = rowType.getFieldIndex(name);
                return new Object[] {
                  name, Conversions.FlinkToDelta.data(rowType, rowData, partitionValueColIdx)
                };
              })
          .collect(Collectors.toMap(o -> (String) o[0], o -> (Literal) o[1]));
    }

    public static Literal data(RowType rowType, RowData rowData, int colIdx) {
      final LogicalType flinkType = rowType.getTypeAt(colIdx);
      final LogicalTypeRoot typeRoot = flinkType.getTypeRoot();
      switch (typeRoot) {
        case INTEGER:
          return Literal.ofInt(rowData.getInt(colIdx));
        case BIGINT:
          return Literal.ofLong(rowData.getLong(colIdx));
        case VARCHAR:
        case CHAR:
          return Literal.ofString(rowData.getString(colIdx).toString());
        case DOUBLE:
          return Literal.ofDouble(rowData.getDouble(colIdx));
        case FLOAT:
          return Literal.ofFloat(rowData.getFloat(colIdx));
        case DECIMAL:
          DecimalType decimalType = (DecimalType) flinkType;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          return Literal.ofDecimal(
              rowData.getDecimal(colIdx, precision, scale).toBigDecimal(), precision, scale);
        case DATE:
          return Literal.ofDate(rowData.getInt(colIdx));
        case TIMESTAMP_WITH_TIME_ZONE:
          return Literal.ofTimestamp(rowData.getLong(colIdx));
        case TIMESTAMP_WITHOUT_TIME_ZONE:
          return Literal.ofTimestampNtz(rowData.getLong(colIdx));
        default:
          throw new UnsupportedOperationException(
              String.format("Type not supported: %s", flinkType));
      }
    }
  }
}
