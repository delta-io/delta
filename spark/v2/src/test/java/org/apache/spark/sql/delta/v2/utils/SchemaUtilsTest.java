/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta.v2.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for {@link SchemaUtils}. */
public class SchemaUtilsTest {

  @Test
  public void testPrimitiveTypes() {
    checkConversion(DataTypes.StringType, StringType.STRING);
    checkConversion(DataTypes.BooleanType, BooleanType.BOOLEAN);
    checkConversion(DataTypes.IntegerType, IntegerType.INTEGER);
    checkConversion(DataTypes.LongType, LongType.LONG);
    checkConversion(DataTypes.BinaryType, BinaryType.BINARY);
    checkConversion(DataTypes.ByteType, ByteType.BYTE);
    checkConversion(DataTypes.DateType, DateType.DATE);
    checkConversion(DataTypes.createDecimalType(10, 2), new DecimalType(10, 2));
    checkConversion(DataTypes.DoubleType, DoubleType.DOUBLE);
    checkConversion(DataTypes.FloatType, FloatType.FLOAT);
    checkConversion(DataTypes.ShortType, ShortType.SHORT);
    checkConversion(DataTypes.TimestampType, TimestampType.TIMESTAMP);
    checkConversion(DataTypes.TimestampNTZType, TimestampNTZType.TIMESTAMP_NTZ);
    checkConversion(DataTypes.VariantType, VariantType.VARIANT);
  }

  static Stream<Arguments> collations() {
    return Stream.of(
        Arguments.of("UTF8_BINARY", "SPARK.UTF8_BINARY"),
        Arguments.of("UTF8_LCASE", "SPARK.UTF8_LCASE"),
        Arguments.of("UTF8_BINARY_RTRIM", "SPARK.UTF8_BINARY_RTRIM"),
        Arguments.of("UNICODE_CI", "ICU.UNICODE_CI"),
        Arguments.of("UNICODE_CI_AI", "ICU.UNICODE_CI_AI"),
        Arguments.of("tr_CI", "ICU.TR_CI"));
  }

  @ParameterizedTest
  @MethodSource("collations")
  public void testCollatedStringToKernel(String sparkCollation, String kernelCollation) {
    assertEquals(
        new StringType(kernelCollation),
        SchemaUtils.convertSparkDataTypeToKernelDataType(
            StringType$.MODULE$.apply(sparkCollation)));
  }

  @ParameterizedTest
  @MethodSource("collations")
  public void testNestedCollatedStringsToKernel(String sparkCollation, String kernelCollation) {
    org.apache.spark.sql.types.StringType sparkString = StringType$.MODULE$.apply(sparkCollation);
    StringType kernelString = new StringType(kernelCollation);
    Metadata sparkMetadata = new MetadataBuilder().putString("comment", "collated value").build();
    FieldMetadata kernelMetadata =
        FieldMetadata.builder().putString("comment", "collated value").build();
    org.apache.spark.sql.types.StructType sparkSchema =
        new org.apache.spark.sql.types.StructType()
            .add("value", sparkString, false, sparkMetadata)
            .add("array", DataTypes.createArrayType(sparkString, false), true)
            .add("map", DataTypes.createMapType(DataTypes.StringType, sparkString, false), false);
    StructType kernelSchema =
        new StructType()
            .add("value", kernelString, false, kernelMetadata)
            .add("array", new ArrayType(kernelString, false), true)
            .add("map", new MapType(StringType.STRING, kernelString, false), false);

    assertEquals(kernelSchema, SchemaUtils.convertSparkDataTypeToKernelDataType(sparkSchema));
    assertEquals(
        new StructType().add("nested", kernelSchema, false),
        SchemaUtils.convertSparkDataTypeToKernelDataType(
            new org.apache.spark.sql.types.StructType().add("nested", sparkSchema, false)));
  }

  @ParameterizedTest
  @MethodSource("collations")
  public void testCollatedMapKeys(String sparkCollation, String kernelCollation) {
    org.apache.spark.sql.types.DataType sparkMap =
        DataTypes.createMapType(StringType$.MODULE$.apply(sparkCollation), DataTypes.IntegerType);
    if (sparkCollation.equals("UTF8_BINARY")) {
      checkConversion(sparkMap, new MapType(StringType.STRING, IntegerType.INTEGER, true));
    } else {
      AnalysisException error =
          assertThrows(
              AnalysisException.class,
              () -> SchemaUtils.convertSparkDataTypeToKernelDataType(sparkMap));
      assertEquals("UNSUPPORTED_FEATURE.COLLATIONS_IN_MAP_KEYS", error.getCondition());
    }
  }

  @Test
  public void testArrayType() {
    checkConversion(
        DataTypes.createArrayType(DataTypes.IntegerType, true /* containsNull */),
        new ArrayType(IntegerType.INTEGER, true /* containsNull */));
    checkConversion(
        DataTypes.createArrayType(DataTypes.StringType, false /* containsNull */),
        new ArrayType(StringType.STRING, false /* containsNull */));
  }

  @Test
  public void testMapType() {
    checkConversion(
        DataTypes.createMapType(
            DataTypes.StringType, DataTypes.IntegerType, true /* valueContainsNull */),
        new MapType(StringType.STRING, IntegerType.INTEGER, true /* valueContainsNull */));
    checkConversion(
        DataTypes.createMapType(
            DataTypes.LongType, DataTypes.BooleanType, false /* valueContainsNull */),
        new MapType(LongType.LONG, BooleanType.BOOLEAN, false /* valueContainsNull */));
  }

  @Test
  public void testStructType() {
    org.apache.spark.sql.types.StructType sparkStruct =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("a", DataTypes.IntegerType, true /* nullable */),
              DataTypes.createStructField("b", DataTypes.StringType, false /* nullable */)
            });
    StructType kernelStruct =
        new StructType()
            .add("a", IntegerType.INTEGER, true /* nullable */)
            .add("b", StringType.STRING, false /* nullable */);

    checkConversion(sparkStruct, kernelStruct);
  }

  @Test
  public void testNestedTypes() {
    org.apache.spark.sql.types.StructType sparkStruct =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField(
                  "a",
                  DataTypes.createArrayType(DataTypes.IntegerType, true /* containsNull */),
                  true /* nullable */),
              DataTypes.createStructField(
                  "b",
                  DataTypes.createMapType(
                      DataTypes.StringType, DataTypes.BooleanType, false /* valueContainsNull */),
                  false /* nullable */)
            });
    StructType kernelStruct =
        new StructType()
            .add(
                "a",
                new ArrayType(IntegerType.INTEGER, true /* containsNull */),
                true /* nullable */)
            .add(
                "b",
                new MapType(StringType.STRING, BooleanType.BOOLEAN, false /* valueContainsNull */),
                false /* nullable */);

    checkConversion(sparkStruct, kernelStruct);
  }

  @Test
  public void testConvertSparkSchemaToKernelSchema() {
    org.apache.spark.sql.types.StructType sparkSchema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("a", DataTypes.IntegerType, true),
              DataTypes.createStructField("b", DataTypes.StringType, false)
            });

    StructType expectedKernelSchema =
        new StructType().add("a", IntegerType.INTEGER, true).add("b", StringType.STRING, false);

    StructType actualKernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    assertEquals(expectedKernelSchema, actualKernelSchema);
  }

  @Test
  public void testConvertKernelSchemaToSparkSchema() {
    StructType kernelSchema =
        new StructType().add("a", IntegerType.INTEGER, true).add("b", StringType.STRING, false);

    org.apache.spark.sql.types.StructType expectedSparkSchema =
        DataTypes.createStructType(
            new org.apache.spark.sql.types.StructField[] {
              DataTypes.createStructField("a", DataTypes.IntegerType, true),
              DataTypes.createStructField("b", DataTypes.StringType, false)
            });

    org.apache.spark.sql.types.StructType actualSparkSchema =
        SchemaUtils.convertKernelSchemaToSparkSchema(kernelSchema);
    assertEquals(expectedSparkSchema, actualSparkSchema);
  }

  @ParameterizedTest
  @MethodSource("collations")
  public void testCollatedStringsToSparkAreRejected(String sparkCollation, String kernelCollation) {
    StringType kernelString = new StringType(kernelCollation);
    List<StructType> kernelSchemas = new ArrayList<>();
    kernelSchemas.add(new StructType().add("s", kernelString));
    kernelSchemas.add(new StructType().add("a", new ArrayType(kernelString, true)));
    kernelSchemas.add(new StructType().add("outer", new StructType().add("s", kernelString)));
    List<DataType> kernelTypes =
        Arrays.asList(kernelString, new MapType(StringType.STRING, kernelString, true));
    if (sparkCollation.equals("UTF8_BINARY")) {
      kernelSchemas.forEach(SchemaUtils::convertKernelSchemaToSparkSchema);
      kernelTypes.forEach(SchemaUtils::convertKernelDataTypeToSparkDataType);
      return;
    }
    // A schema can also record a collation only in field metadata, on a binary string.
    kernelSchemas.add(
        new StructType()
            .add(
                "s",
                StringType.STRING,
                FieldMetadata.builder()
                    .putFieldMetadata(
                        "__COLLATIONS",
                        FieldMetadata.builder().putString("s", kernelCollation).build())
                    .build()));
    for (StructType kernelSchema : kernelSchemas) {
      assertThrows(
          IllegalArgumentException.class,
          () -> SchemaUtils.convertKernelSchemaToSparkSchema(kernelSchema),
          kernelSchema.toString());
    }
    for (DataType kernelType : kernelTypes) {
      assertThrows(
          IllegalArgumentException.class,
          () -> SchemaUtils.convertKernelDataTypeToSparkDataType(kernelType),
          kernelType.toString());
    }
  }

  static Stream<Arguments> nullInPrimitiveArraysProvider() {
    return Stream.of(
        Arguments.of(
            "Long",
            FieldMetadata.builder().putLongArray("ids", new Long[] {1L, null, 3L}).build(),
            1),
        Arguments.of(
            "Double",
            FieldMetadata.builder().putDoubleArray("scores", new Double[] {1.1, 2.2, null}).build(),
            2),
        Arguments.of(
            "Boolean",
            FieldMetadata.builder()
                .putBooleanArray("flags", new Boolean[] {true, null, false})
                .build(),
            1));
  }

  @ParameterizedTest(name = "{0} array with null at index {2}")
  @MethodSource("nullInPrimitiveArraysProvider")
  public void testNullInPrimitiveArrays(
      String typeName, FieldMetadata kernelMetadata, int expectedNullIndex) {
    Exception ex =
        assertThrows(
            Exception.class,
            () -> SchemaUtils.convertKernelFieldMetadataToSparkMetadata(kernelMetadata));
    assertTrue(ex.getMessage().contains("Null element at index " + expectedNullIndex));
  }

  @Test
  public void testSchemaRoundTripWithFieldMetadata() {
    // Schema with field metadata (e.g., column mapping)
    org.apache.spark.sql.types.StructType sparkSchema =
        new org.apache.spark.sql.types.StructType()
            .add(
                "user_id",
                DataTypes.IntegerType,
                true,
                new MetadataBuilder()
                    .putLong("delta.columnMapping.id", 123L)
                    .putString("delta.columnMapping.physicalName", "col-abc-123")
                    .build());

    StructType expectedKernelSchema =
        new StructType()
            .add(
                "user_id",
                IntegerType.INTEGER,
                true,
                FieldMetadata.builder()
                    .putLong("delta.columnMapping.id", 123L)
                    .putString("delta.columnMapping.physicalName", "col-abc-123")
                    .build());

    // Verify Spark → Kernel conversion
    StructType actualKernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(sparkSchema);
    assertEquals(expectedKernelSchema, actualKernelSchema);

    // Verify Kernel → Spark conversion
    org.apache.spark.sql.types.StructType sparkSchema2 =
        SchemaUtils.convertKernelSchemaToSparkSchema(actualKernelSchema);
    assertEquals(sparkSchema, sparkSchema2);
  }

  private void checkConversion(
      org.apache.spark.sql.types.DataType sparkDataType, DataType kernelDataType) {
    DataType toKernel = SchemaUtils.convertSparkDataTypeToKernelDataType(sparkDataType);
    assertEquals(kernelDataType, toKernel);
    org.apache.spark.sql.types.DataType toSpark =
        SchemaUtils.convertKernelDataTypeToSparkDataType(kernelDataType);
    assertEquals(sparkDataType, toSpark);
  }

  ////////////////////////////////
  // Field Metadata Tests       //
  ////////////////////////////////

  static Stream<Arguments> metadataTypesProvider() {
    return Stream.of(
        // Empty
        Arguments.of("Empty", Metadata.empty(), FieldMetadata.empty()),
        // Primitives
        Arguments.of(
            "Long",
            new MetadataBuilder().putLong("id", 123L).build(),
            FieldMetadata.builder().putLong("id", 123L).build()),
        Arguments.of(
            "Double",
            new MetadataBuilder().putDouble("score", 3.14).build(),
            FieldMetadata.builder().putDouble("score", 3.14).build()),
        Arguments.of(
            "Boolean",
            new MetadataBuilder().putBoolean("flag", true).build(),
            FieldMetadata.builder().putBoolean("flag", true).build()),
        Arguments.of(
            "String",
            new MetadataBuilder().putString("name", "test").build(),
            FieldMetadata.builder().putString("name", "test").build()),
        Arguments.of(
            "Null",
            new MetadataBuilder().putNull("empty").build(),
            FieldMetadata.builder().putNull("empty").build()),
        // Arrays
        Arguments.of(
            "LongArray",
            new MetadataBuilder().putLongArray("ids", new long[] {1L, 2L, 3L}).build(),
            FieldMetadata.builder().putLongArray("ids", new Long[] {1L, 2L, 3L}).build()),
        Arguments.of(
            "DoubleArray",
            new MetadataBuilder().putDoubleArray("scores", new double[] {1.1, 2.2, 3.3}).build(),
            FieldMetadata.builder().putDoubleArray("scores", new Double[] {1.1, 2.2, 3.3}).build()),
        Arguments.of(
            "BooleanArray",
            new MetadataBuilder()
                .putBooleanArray("flags", new boolean[] {true, false, true})
                .build(),
            FieldMetadata.builder()
                .putBooleanArray("flags", new Boolean[] {true, false, true})
                .build()),
        Arguments.of(
            "StringArray",
            new MetadataBuilder().putStringArray("names", new String[] {"a", "b", "c"}).build(),
            FieldMetadata.builder().putStringArray("names", new String[] {"a", "b", "c"}).build()),
        // Nested
        Arguments.of(
            "NestedMetadata",
            new MetadataBuilder()
                .putMetadata("inner", new MetadataBuilder().putString("nested", "value").build())
                .build(),
            FieldMetadata.builder()
                .putFieldMetadata(
                    "inner", FieldMetadata.builder().putString("nested", "value").build())
                .build()),
        // Metadata Array
        Arguments.of(
            "MetadataArray",
            new MetadataBuilder()
                .putMetadataArray(
                    "items",
                    new Metadata[] {
                      new MetadataBuilder().putString("name", "first").build(),
                      new MetadataBuilder().putString("name", "second").build()
                    })
                .build(),
            FieldMetadata.builder()
                .putFieldMetadataArray(
                    "items",
                    new FieldMetadata[] {
                      FieldMetadata.builder().putString("name", "first").build(),
                      FieldMetadata.builder().putString("name", "second").build()
                    })
                .build()),
        // Complex (multiple types mixed)
        Arguments.of(
            "ComplexMetadata",
            new MetadataBuilder()
                .putLong("id", 123L)
                .putDouble("score", 3.14)
                .putBoolean("active", true)
                .putString("name", "test")
                .putLongArray("versions", new long[] {1L, 2L})
                .putDoubleArray("scores", new double[] {1.1, 2.2})
                .putBooleanArray("flags", new boolean[] {true, false})
                .putStringArray("tags", new String[] {"a", "b"})
                .putMetadata(
                    "nested",
                    new MetadataBuilder()
                        .putString("type", "nested")
                        .putLongArray("ids", new long[] {1L, 2L, 3L})
                        .build())
                .putNull("empty")
                .build(),
            FieldMetadata.builder()
                .putLong("id", 123L)
                .putDouble("score", 3.14)
                .putBoolean("active", true)
                .putString("name", "test")
                .putLongArray("versions", new Long[] {1L, 2L})
                .putDoubleArray("scores", new Double[] {1.1, 2.2})
                .putBooleanArray("flags", new Boolean[] {true, false})
                .putStringArray("tags", new String[] {"a", "b"})
                .putFieldMetadata(
                    "nested",
                    FieldMetadata.builder()
                        .putString("type", "nested")
                        .putLongArray("ids", new Long[] {1L, 2L, 3L})
                        .build())
                .putNull("empty")
                .build()));
  }

  @ParameterizedTest(name = "Metadata type: {0}")
  @MethodSource("metadataTypesProvider")
  public void testMetadataConversion(
      String typeName, Metadata sparkMetadata, FieldMetadata kernelMetadata) {
    assertEquals(
        kernelMetadata, SchemaUtils.convertSparkMetadataToKernelFieldMetadata(sparkMetadata));
    assertEquals(
        sparkMetadata, SchemaUtils.convertKernelFieldMetadataToSparkMetadata(kernelMetadata));
  }

  @Test
  public void isReadCompatibleAcceptsAdditiveColumn() {
    org.apache.spark.sql.types.StructType existing =
        new org.apache.spark.sql.types.StructType().add("id", DataTypes.LongType);
    org.apache.spark.sql.types.StructType end =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType);
    assertTrue(SchemaUtils.isReadCompatible(existing, end));
  }

  @Test
  public void isReadCompatibleRejectsDroppedColumn() {
    org.apache.spark.sql.types.StructType existing =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.LongType)
            .add("name", DataTypes.StringType);
    org.apache.spark.sql.types.StructType end =
        new org.apache.spark.sql.types.StructType().add("id", DataTypes.LongType);
    assertFalse(SchemaUtils.isReadCompatible(existing, end));
  }

  @Test
  public void isReadCompatibleRejectsTightenedNullability() {
    org.apache.spark.sql.types.StructType existing =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.LongType, /* nullable */ true);
    org.apache.spark.sql.types.StructType end =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.LongType, /* nullable */ false);
    assertFalse(SchemaUtils.isReadCompatible(existing, end));
  }

  @Test
  public void isReadCompatibleAcceptsRelaxedNullability() {
    org.apache.spark.sql.types.StructType existing =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.LongType, /* nullable */ false);
    org.apache.spark.sql.types.StructType end =
        new org.apache.spark.sql.types.StructType()
            .add("id", DataTypes.LongType, /* nullable */ true);
    assertTrue(SchemaUtils.isReadCompatible(existing, end));
  }
}
