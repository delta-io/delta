package io.delta.flink.source.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.defaults.internal.data.vector.DefaultGenericVector;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.types.*;

public class KernelMetadataUtils {
    public static StructType getSchema() {
        FieldMetadata.Builder builder = FieldMetadata.builder();
        builder.putString("key1", "value1");
        builder.putString("key2", "value2");
        ArrayList<DataType> typesToTest = new ArrayList<DataType>();
        ArrayType arrayType = new ArrayType(IntegerType.INTEGER, false);
        typesToTest.add(arrayType);
        typesToTest.add(BinaryType.BINARY);
        typesToTest.add(BooleanType.BOOLEAN);
        typesToTest.add(ByteType.BYTE);
        typesToTest.add(DateType.DATE);
        typesToTest.add(DecimalType.USER_DEFAULT);
        typesToTest.add(DoubleType.DOUBLE);
        typesToTest.add(FloatType.FLOAT);
        typesToTest.add(IntegerType.INTEGER);
        typesToTest.add(LongType.LONG);
        typesToTest.add(new MapType(ShortType.SHORT, arrayType, false));
        typesToTest.add(ShortType.SHORT);
        typesToTest.add(StringType.STRING);
        typesToTest.add(TimestampType.TIMESTAMP);
        ArrayList<StructField> fields = new ArrayList<StructField>();
        int fnum = 1;
        for (DataType dt : typesToTest) {
            fields.add(
                new StructField(
                    "Field " + fnum,
                    dt,
                    false,
                    builder.build()
                    )
            );
            fnum++;
        }
        return new StructType(fields);
    }

    public static Metadata getKernelMetadata() {
        StructType schema = getSchema();
        return new io.delta.kernel.internal.actions.Metadata(
            "id",
            Optional.ofNullable("name"),
            Optional.ofNullable("description"),
            new io.delta.kernel.internal.actions.Format("parquet", new HashMap<String, String>()),
            "schemaString",
            schema,
            new ArrayValue() { // paritionColumns
                @Override
                public int getSize() {
                    return 2;
                }

                @Override
                public ColumnVector getElements() {
                    return new ColumnVector() {
                        @Override
                        public DataType getDataType() {
                            return null;
                        }

                        @Override
                        public int getSize() {
                            return 2;
                        }

                        @Override
                        public void close() {}

                        @Override
                        public boolean isNullAt(int rowId) {
                            return false;
                        }

                        @Override
                        public String getString(int rowId) {
                            return "Row " + rowId;
                        }
                    };
                }
            },
            Optional.ofNullable(1234L),
            new MapValue() { // conf
                @Override
                public int getSize() {
                    return 1;
                }
                @Override
                public ColumnVector getKeys() {
                    return DefaultGenericVector.fromArray(
                        IntegerType.INTEGER, new Integer[]{new Integer(1)}
                    );
                }
                @Override
                public ColumnVector getValues() {
                    return DefaultGenericVector.fromArray(
                        IntegerType.INTEGER, new Integer[]{new Integer(2)}
                    );
                }
            }
        );
    }
}
