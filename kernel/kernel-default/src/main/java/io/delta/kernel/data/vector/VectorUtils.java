package io.delta.kernel.data.vector;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

/**
 * Utility methods for {@link io.delta.kernel.data.ColumnVector} implementations.
 */
public class VectorUtils
{
    private VectorUtils() {}

    /**
     * Get the value at given {@code rowId} from the column vector. The type of the value object
     * depends on the data type of the {@code vector}.
     *
     * @param vector
     * @param rowId
     * @return
     */
    public static Object getValueAsObject(ColumnVector vector, int rowId) {
        // TODO: may be it is better to just provide a `getObject` on the `ColumnVector` to
        // avoid the nested if-else statements.
        final DataType dataType = vector.getDataType();

        if (vector.isNullAt(rowId)) {
            return null;
        }

        if (dataType instanceof BooleanType) {
            return vector.getBoolean(rowId);
        } else if (dataType instanceof ByteType) {
            return vector.getByte(rowId);
        } else if (dataType instanceof ShortType) {
            return vector.getShort(rowId);
        } else if (dataType instanceof IntegerType || dataType instanceof DateType) {
            return vector.getInt(rowId);
        } else if (dataType instanceof LongType || dataType instanceof TimestampType) {
            return vector.getLong(rowId);
        } else if (dataType instanceof FloatType) {
            return vector.getFloat(rowId);
        } else if (dataType instanceof DoubleType) {
            return vector.getDouble(rowId);
        } else if (dataType instanceof StringType) {
            return vector.getString(rowId);
        } else if (dataType instanceof BinaryType) {
            return vector.getBinary(rowId);
        } else if (dataType instanceof StructType) {
            return vector.getStruct(rowId);
        } else if (dataType instanceof MapType) {
            return vector.getMap(rowId);
        } else if (dataType instanceof ArrayType) {
            return vector.getArray(rowId);
        }

        throw new UnsupportedOperationException(dataType + " is not supported yet");
    }
}
