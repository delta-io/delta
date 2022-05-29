package io.delta.flink.source.internal;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * A utility class to convert Delta's {@link DataType} objects to Flink's {@link LogicalType}
 * equivalent object.
 */
public class SchemaConverter {

    /**
     * Converts Delta's {@link StructType} to Flink's {@link RowType}
     */
    private static RowType toRowType(StructType deltaRow, boolean nullable) {

        StructField[] deltaFields = deltaRow.getFields();
        String[] fieldNames = new String[deltaFields.length];
        LogicalType[] fieldTypes = new LogicalType[deltaFields.length];

        for (int i = 0; i < deltaFields.length; i++) {
            StructField deltaField = deltaFields[i];
            fieldNames[i] = deltaField.getName();
            fieldTypes[i] = toFlinkDataType(deltaField.getDataType(), deltaField.isNullable());
        }

        return RowType.of(nullable, fieldTypes, fieldNames);
    }

    /**
     * Converts Delta's {@link DataType} to Flink's {@link LogicalType}
     */
    public static LogicalType toFlinkDataType(DataType deltaType, boolean nullable) {

        DeltaDataType deltaDataType = DeltaDataType.instanceFrom(deltaType.getClass());
        switch (deltaDataType) {
            case ARRAY:
                boolean arrayContainsNull =
                    ((io.delta.standalone.types.ArrayType) deltaType).containsNull();
                LogicalType elementType = toFlinkDataType(
                    ((io.delta.standalone.types.ArrayType) deltaType).getElementType(),
                    arrayContainsNull);
                return
                    new ArrayType(nullable, elementType);
            case BIGINT:
                return new BigIntType(nullable);
            case BINARY:
                return new BinaryType(nullable, BinaryType.DEFAULT_LENGTH);
            case BOOLEAN:
                return new BooleanType(nullable);
            case BYTE:
            case TINYINT:
                return new TinyIntType(nullable);
            case DATE:
                return new DateType(nullable);
            case DECIMAL:
                int precision = ((io.delta.standalone.types.DecimalType) deltaType).getPrecision();
                int scale = ((io.delta.standalone.types.DecimalType) deltaType).getScale();
                return new DecimalType(nullable, precision, scale);
            case DOUBLE:
                return new DoubleType(nullable);
            case FLOAT:
                return new FloatType(nullable);
            case INTEGER:
                return new IntType(nullable);
            case MAP:
                boolean mapContainsNull =
                    ((io.delta.standalone.types.MapType) deltaType).valueContainsNull();
                LogicalType keyType =
                    toFlinkDataType(((io.delta.standalone.types.MapType) deltaType).getKeyType(),
                        mapContainsNull);
                LogicalType valueType =
                    toFlinkDataType(((io.delta.standalone.types.MapType) deltaType).getValueType(),
                        mapContainsNull);
                return new MapType(nullable, keyType, valueType);
            case NULL:
                return new NullType();
            case SMALLINT:
                return new SmallIntType(nullable);
            case TIMESTAMP:
                return new TimestampType(nullable, TimestampType.DEFAULT_PRECISION);
            case STRING:
                return new VarCharType(nullable, VarCharType.DEFAULT_LENGTH);
            case STRUCT:
                return toRowType((StructType) deltaType, nullable);
            default:
                throw new UnsupportedOperationException(
                    "Type not supported: " + deltaDataType);
        }
    }
}
