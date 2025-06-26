package io.delta.dsv2.utils;

import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;

public class SchemaUtils {

  //////////////////////
  // Kernel --> Spark //
  //////////////////////

  /** Converts a Delta Kernel schema to a Spark schema. */
  public static org.apache.spark.sql.types.StructType convertKernelSchemaToSparkSchema(
      StructType kernelSchema) {
    List<org.apache.spark.sql.types.StructField> fields = new ArrayList<>();

    for (StructField field : kernelSchema.fields()) {
      fields.add(
          new org.apache.spark.sql.types.StructField(
              field.getName(),
              convertKernelDataTypeToSparkDataType(field.getDataType()),
              field.isNullable(),
              Metadata.empty()));
    }

    return new org.apache.spark.sql.types.StructType(
        fields.toArray(new org.apache.spark.sql.types.StructField[0]));
  }

  /** Converts a Delta Kernel data type to a Spark data type. */
  public static org.apache.spark.sql.types.DataType convertKernelDataTypeToSparkDataType(
      DataType kernelDataType) {
    if (kernelDataType instanceof StringType) {
      return DataTypes.StringType;
    } else if (kernelDataType instanceof BooleanType) {
      return DataTypes.BooleanType;
    } else if (kernelDataType instanceof IntegerType) {
      return DataTypes.IntegerType;
    } else if (kernelDataType instanceof LongType) {
      return DataTypes.LongType;
    } else {
      throw new IllegalArgumentException("unsupported data type " + kernelDataType);
    }
  }

  //////////////////////
  // Spark --> Kernel //
  //////////////////////

  /** Converts a Spark schema to a Delta Kernel schema. */
  public static StructType convertSparkSchemaToKernelSchema(
      org.apache.spark.sql.types.StructType sparkSchema) {
    List<StructField> kernelFields = new ArrayList<>();

    for (org.apache.spark.sql.types.StructField field : sparkSchema.fields()) {
      kernelFields.add(
          new StructField(
              field.name(),
              convertSparkDataTypeToKernelDataType(field.dataType()),
              field.nullable()));
    }

    return new StructType(kernelFields);
  }

  /** Converts a Spark data type to a Delta Kernel data type. */
  public static DataType convertSparkDataTypeToKernelDataType(
      org.apache.spark.sql.types.DataType sparkDataType) {
    if (sparkDataType == DataTypes.StringType) {
      return StringType.STRING;
    } else if (sparkDataType == DataTypes.BooleanType) {
      return BooleanType.BOOLEAN;
    } else if (sparkDataType == DataTypes.IntegerType) {
      return IntegerType.INTEGER;
    } else if (sparkDataType == DataTypes.LongType) {
      return LongType.LONG;
    } else {
      throw new IllegalArgumentException("unsupported data type " + sparkDataType);
    }
  }
}
