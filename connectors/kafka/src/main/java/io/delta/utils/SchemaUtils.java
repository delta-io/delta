package io.delta.utils;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_ID_KEY;
import static io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_NESTED_IDS_KEY;

/**
 * Utility methods for converting between Iceberg and Delta schemas.
 */
public class SchemaUtils
{
    private SchemaUtils()
    {
    }

    public static StructType fromIcebergSchema(Types.StructType icebergStructType)
    {
        StructType structType = new StructType();
        for (Types.NestedField field : icebergStructType.fields()) {
            structType = structType.add(fromIcebergType(field.name(), field));
        }

        return structType;
    }

    public static Types.StructType toIcebergSchema(StructType structType)
    {
        return Types.StructType.of(
                structType.fields().stream()
                        .map(field -> {
                            Long fieldIdLong = field.getMetadata().getLong(COLUMN_MAPPING_ID_KEY);
                            if (fieldIdLong == null) {
                                throw new IllegalArgumentException("Field metadata does not contain a column mapping ID");
                            }
                            int fieldId = fieldIdLong.intValue();
                            return Types.NestedField.optional(
                                    fieldId,
                                    field.getName(),
                                    toIcebergType(field.getName(), field));
                        })
                        .toArray(Types.NestedField[]::new));
    }

    private static StructField fromIcebergType(String path, Types.NestedField icebergNestedField)
    {
        FieldMetadata.Builder metadataBuilder = FieldMetadata.builder()
                .putLong(COLUMN_MAPPING_ID_KEY, icebergNestedField.fieldId());

        Type icebergType = icebergNestedField.type();
        DataType deltaType = null;
        switch (icebergType.typeId()) {
            case BOOLEAN:
                deltaType = BooleanType.BOOLEAN;
                break;
            case STRING:
                deltaType = StringType.STRING;
                break;
            case INTEGER:
                deltaType = IntegerType.INTEGER;
                break;
            case LONG:
                deltaType = LongType.LONG;
                break;
            case FLOAT:
                deltaType = FloatType.FLOAT;
                break;
            case DOUBLE:
                deltaType = DoubleType.DOUBLE;
                break;
            case DATE:
                deltaType = DateType.DATE;
                break;
            case TIMESTAMP:
                deltaType = TimestampType.TIMESTAMP;
                break;
            case STRUCT:
                deltaType = fromIcebergSchema(icebergType.asStructType());
                break;
            case MAP:
                Types.MapType mapType = icebergType.asMapType();
                FieldMetadata nestedFieldIds = FieldMetadata.builder()
                        .putLong(path + ".key", mapType.keyId())
                        .putLong(path + ".value", mapType.valueId())
                        .build();
                metadataBuilder.putFieldMetadata(COLUMN_MAPPING_NESTED_IDS_KEY, nestedFieldIds);
                deltaType = new MapType(
                        fromIcebergType(path + ".key", icebergType.asMapType().fields().get(0)).getDataType(),
                        fromIcebergType(path + ".value", icebergType.asMapType().fields().get(1)).getDataType(),
                        icebergType.asMapType().isValueOptional());
                break;
            case LIST:
                Types.ListType listType = icebergType.asListType();
                FieldMetadata nestedFieldIds2 = FieldMetadata.builder()
                        .putLong(path + ".element", listType.elementId())
                        .build();
                metadataBuilder.putFieldMetadata(COLUMN_MAPPING_NESTED_IDS_KEY, nestedFieldIds2);
                deltaType = new ArrayType(
                        fromIcebergType(path + ".element", icebergType.asListType().fields().get(0)).getDataType(),
                        icebergType.asListType().isElementOptional());
                break;
            default:
                throw new IllegalArgumentException("Unsupported Iceberg type: " + icebergType);
        }

        return new StructField(icebergNestedField.name(), deltaType, true /* nullable */, metadataBuilder.build());
    }

    private static Type toIcebergType(String path, StructField field)
    {
        DataType deltaType = field.getDataType();
        if (deltaType instanceof BooleanType) {
            return Types.BooleanType.get();
        }
        else if (deltaType instanceof StringType) {
            return Types.StringType.get();
        }
        else if (deltaType instanceof IntegerType) {
            return Types.IntegerType.get();
        }
        else if (deltaType instanceof LongType) {
            return Types.LongType.get();
        }
        else if (deltaType instanceof FloatType) {
            return Types.FloatType.get();
        }
        else if (deltaType instanceof DoubleType) {
            return Types.DoubleType.get();
        }
        else if (deltaType instanceof DateType) {
            return Types.DateType.get();
        }
        else if (deltaType instanceof TimestampType) {
            return Types.TimestampType.withoutZone();
        }
        else if (deltaType instanceof StructType) {
            return toIcebergSchema((StructType) deltaType);
        }
        else if (deltaType instanceof MapType) {
            MapType mapType = (MapType) deltaType;
            FieldMetadata nestedFieldIds = field.getMetadata().getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
            if (nestedFieldIds == null) {
                throw new IllegalArgumentException("Field metadata does not contain nested column mapping IDs");
            }
            Long keyId = nestedFieldIds.getLong(path + ".key");
            Long valueId = nestedFieldIds.getLong(path + ".value");
            if (keyId == null || valueId == null) {
                throw new IllegalArgumentException("Field metadata does not contain nested column mapping IDs");
            }
            return Types.MapType.ofOptional(
                    keyId.intValue(),
                    valueId.intValue(),
                    toIcebergType(field.getName() + ".key", mapType.getKeyField()),
                    toIcebergType(field.getName() + ".value", mapType.getValueField()));
        }
        else if (deltaType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) deltaType;
            FieldMetadata nestedFieldIds = field.getMetadata().getMetadata(COLUMN_MAPPING_NESTED_IDS_KEY);
            if (nestedFieldIds == null) {
                throw new IllegalArgumentException("Field metadata does not contain nested column mapping IDs");
            }
            Long keyId = nestedFieldIds.getLong(field.getName() + ".element");
            return Types.ListType.ofOptional(
                    keyId.intValue(),
                    toIcebergType(field.getName() + ".element", arrayType.getElementField()));
        }
        else {
            throw new IllegalArgumentException("Unsupported Delta type: " + deltaType);
        }
    }
}
