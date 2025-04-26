package io.delta.table;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

abstract class DeltaTypeVisitor<T> {
    public static <T> T visit(DataType type, DeltaTypeVisitor<T> visitor) {
        if (type instanceof StructType) {
            List<StructField> fields = ((StructType) type).fields();
            List<T> fieldResults = Lists.newArrayListWithExpectedSize(fields.size());

            for (StructField field : fields) {
                visitor.beforeField(field);
                T fieldResult;
                try {
                    fieldResult = visitor.field(field, visit(field.getDataType(), visitor));
                } finally {
                    visitor.afterField(field);
                }
                fieldResults.add(fieldResult);
            }

            return visitor.struct((StructType) type, fieldResults);

        } else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            DataType keyType = mapType.getKeyType();
            DataType valueType = mapType.getValueType();
            StructField keyAsField = new StructField("key", keyType, false);
            StructField valueAsField = new StructField("value", valueType, mapType.isValueContainsNull());

            T keyResult;
            T valueResult;

            visitor.beforeMapKey(keyAsField);
            try {
                keyResult = visit(keyType, visitor);
            } finally {
                visitor.afterMapKey(keyAsField);
            }

            visitor.beforeMapValue(valueAsField);
            try {
                valueResult = visit(valueType, visitor);
            } finally {
                visitor.afterMapValue(valueAsField);
            }

            return visitor.map(mapType, keyResult, valueResult);

        } else if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            DataType elementType = arrayType.getElementType();
            StructField elementAsField =
                    new StructField("element", elementType, arrayType.containsNull());
            visitor.beforeListElement(elementAsField);

            T elementResult;
            try {
                elementResult = visit(elementType, visitor);
            } finally {
                visitor.afterListElement(elementAsField);
            }

            return visitor.array(arrayType, elementResult);

        } else {
            return visitor.atomic(type);
        }
    }

    public abstract T struct(StructType struct, List<T> fieldResults);

    public abstract T field(StructField field, T typeResult);

    public abstract T array(ArrayType array, T elementResult);

    public abstract T map(MapType map, T keyResult, T valueResult);

    public abstract T atomic(DataType atomic);

    public void beforeField(StructField field) {}

    public void afterField(StructField field) {}

    public void beforeListElement(StructField field) {
        beforeField(field);
    }

    public void afterListElement(StructField field) {
        afterField(field);
    }

    public void beforeMapKey(StructField field) {
        beforeField(field);
    }

    public void afterMapKey(StructField field) {
        afterField(field);
    }

    public void beforeMapValue(StructField field) {
        beforeField(field);
    }

    public void afterMapValue(StructField field) {
        afterField(field);
    }
}
