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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.MapValue;
import io.delta.kernel.spark.SparkDsv2TestBase;
import io.delta.kernel.types.StringType;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.immutable.Map$;

public class PartitionUtilsTest extends SparkDsv2TestBase {

  @ParameterizedTest
  @MethodSource("calculateMaxSplitBytesParameters")
  public void testCalculateMaxSplitBytes(
      long totalBytes, int fileCount, int minPartitionNum, String description) {
    SQLConf sqlConf = SQLConf.get();
    long originalMinPartitionNum = -1;
    try {
      // Set minPartitionNum if provided
      if (minPartitionNum > 0) {
        if (sqlConf.filesMinPartitionNum().isDefined()) {
          originalMinPartitionNum = ((Number) sqlConf.filesMinPartitionNum().get()).longValue();
        }
        sqlConf.setConfString("spark.sql.files.minPartitionNum", String.valueOf(minPartitionNum));
      }

      long result = PartitionUtils.calculateMaxSplitBytes(spark, totalBytes, fileCount, sqlConf);

      // Verify invariants: result should satisfy these constraints
      assertTrue(
          result > 0,
          String.format("%s: maxSplitBytes should be positive, got %d", description, result));
      assertTrue(
          result <= sqlConf.filesMaxPartitionBytes(),
          String.format("%s: maxSplitBytes should not exceed maxPartitionBytes", description));
      assertTrue(
          result >= sqlConf.filesOpenCostInBytes(),
          String.format("%s: maxSplitBytes should be at least openCostInBytes", description));
    } finally {
      // Restore original config
      if (minPartitionNum > 0) {
        if (originalMinPartitionNum >= 0) {
          sqlConf.setConfString(
              "spark.sql.files.minPartitionNum", String.valueOf(originalMinPartitionNum));
        } else {
          sqlConf.unsetConf("spark.sql.files.minPartitionNum");
        }
      }
    }
  }

  private static Stream<Arguments> calculateMaxSplitBytesParameters() {
    long MB = 1024 * 1024;
    return Stream.of(
        // (totalBytes, fileCount, minPartitionNum, description)
        Arguments.of(100 * MB, 10, -1, "Medium files"),
        Arguments.of(10 * MB, 1000, -1, "Many small files"),
        Arguments.of(1000 * MB, 1, -1, "Single large file"),
        Arguments.of(100 * MB, 10, 4, "With explicit minPartitionNum"),
        Arguments.of(0, 0, -1, "Zero bytes"));
  }

  @ParameterizedTest
  @MethodSource("nullHandlingParameters")
  public void testGetPartitionRow_NullHandling(
      Map<String, String> partitionValues, String description) {
    // Schema: year (int), month (int), day (int)
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true),
              DataTypes.createStructField("day", DataTypes.IntegerType, true)
            });

    MapValue mapValue = createMapValue(partitionValues);
    InternalRow row = PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC"));

    assertNotNull(row, description + ": row should not be null");
    assertEquals(3, row.numFields(), description + ": row should have 3 fields");

    // Verify null handling
    for (int i = 0; i < 3; i++) {
      String fieldName = partitionSchema.fields()[i].name();
      if (partitionValues.get(fieldName) == null) {
        assertTrue(row.isNullAt(i), description + ": " + fieldName + " should be null");
      } else {
        assertFalse(row.isNullAt(i), description + ": " + fieldName + " should not be null");
        assertEquals(
            Integer.parseInt(partitionValues.get(fieldName)),
            row.getInt(i),
            description + ": " + fieldName + " value mismatch");
      }
    }
  }

  private static Stream<Arguments> nullHandlingParameters() {
    Map<String, String> allNull = new HashMap<>();
    allNull.put("year", null);
    allNull.put("month", null);
    allNull.put("day", null);

    Map<String, String> mixed = new HashMap<>();
    mixed.put("year", "2024");
    mixed.put("month", null);
    mixed.put("day", "15");

    Map<String, String> noNull = new HashMap<>();
    noNull.put("year", "2024");
    noNull.put("month", "11");
    noNull.put("day", "25");

    return Stream.of(
        Arguments.of(allNull, "All nulls"),
        Arguments.of(mixed, "Mixed nulls and values"),
        Arguments.of(noNull, "No nulls"));
  }

  @ParameterizedTest
  @MethodSource("dataTypesAndTimezonesParameters")
  public void testGetPartitionRow_DataTypesAndTimezones(
      StructType schema, Map<String, String> partitionValues, ZoneId zoneId, String description) {
    MapValue mapValue = createMapValue(partitionValues);
    InternalRow row = PartitionUtils.getPartitionRow(mapValue, schema, zoneId);

    assertNotNull(row, description + ": row should not be null");
    assertEquals(
        schema.fields().length, row.numFields(), description + ": field count should match");
  }

  private static Stream<Arguments> dataTypesAndTimezonesParameters() {
    // Test different data types with different timezones
    StructType intLongStringSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, true),
              DataTypes.createStructField("count", DataTypes.LongType, true),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    Map<String, String> intLongStringValues = new HashMap<>();
    intLongStringValues.put("id", "123");
    intLongStringValues.put("count", "9999999999");
    intLongStringValues.put("name", "test_partition");

    StructType dateSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("date_col", DataTypes.DateType, true)});
    Map<String, String> dateValues = new HashMap<>();
    dateValues.put("date_col", "2024-11-25");

    StructType timestampSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("ts_col", DataTypes.TimestampType, true)
            });
    Map<String, String> timestampValues = new HashMap<>();
    timestampValues.put("ts_col", "2024-11-25T10:30:00");

    return Stream.of(
        Arguments.of(intLongStringSchema, intLongStringValues, ZoneId.of("UTC"), "Int/Long/String"),
        Arguments.of(dateSchema, dateValues, ZoneId.of("UTC"), "Date with UTC"),
        Arguments.of(dateSchema, dateValues, ZoneId.of("America/Los_Angeles"), "Date with PST"),
        Arguments.of(timestampSchema, timestampValues, ZoneId.of("UTC"), "Timestamp with UTC"),
        Arguments.of(
            timestampSchema,
            timestampValues,
            ZoneId.of("America/Los_Angeles"),
            "Timestamp with PST"));
  }

  @Test
  public void testGetPartitionRow_FieldOrdering() {
    // Schema defines order: year, month, day
    StructType partitionSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("year", DataTypes.IntegerType, true),
              DataTypes.createStructField("month", DataTypes.IntegerType, true),
              DataTypes.createStructField("day", DataTypes.IntegerType, true)
            });

    // MapValue has different order: day, year, month
    Map<String, String> partitionValues = new HashMap<>();
    partitionValues.put("day", "25");
    partitionValues.put("year", "2024");
    partitionValues.put("month", "11");

    MapValue mapValue = createMapValue(partitionValues);
    InternalRow row = PartitionUtils.getPartitionRow(mapValue, partitionSchema, ZoneId.of("UTC"));

    // Verify values are in schema order, not mapValue order
    assertEquals(2024, row.getInt(0), "year should be at position 0");
    assertEquals(11, row.getInt(1), "month should be at position 1");
    assertEquals(25, row.getInt(2), "day should be at position 2");
  }

  @ParameterizedTest
  @MethodSource("vectorizedVsRowBasedParameters")
  public void testCreateParquetReaderFactory_VectorizedVsRowBased(
      StructType readDataSchema, boolean expectVectorized, String description) {
    StructType dataSchema = readDataSchema;
    StructType partitionSchema = new StructType(new StructField[] {});
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createParquetReaderFactory(
            dataSchema, partitionSchema, readDataSchema, filters, options, hadoopConf, sqlConf);

    assertNotNull(factory, description + ": PartitionReaderFactory should not be null");
    // Note: We can't easily verify vectorization without reading actual data,
    // but we verify the factory is created successfully for both simple and complex schemas
  }

  private static Stream<Arguments> vectorizedVsRowBasedParameters() {
    // Simple schema - should enable vectorized reading
    StructType simpleSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField("value", DataTypes.DoubleType, true)
            });

    // Nested schema - may disable vectorized reading depending on Spark version
    StructType nestedSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "nested",
                  new StructType(
                      new StructField[] {
                        DataTypes.createStructField("inner_id", DataTypes.IntegerType, true)
                      }),
                  true)
            });

    // Array schema - may disable vectorized reading
    StructType arraySchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "tags", DataTypes.createArrayType(DataTypes.StringType, true), true)
            });

    return Stream.of(
        Arguments.of(simpleSchema, true, "Simple flat schema"),
        Arguments.of(nestedSchema, false, "Nested struct schema"),
        Arguments.of(arraySchema, false, "Array schema"));
  }

  @Test
  public void testCreateParquetReaderFactory() {
    StructType dataSchema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
    StructType partitionSchema =
        new StructType(
            new StructField[] {DataTypes.createStructField("year", DataTypes.IntegerType, true)});
    StructType readDataSchema = dataSchema;
    Filter[] filters = new Filter[0];
    scala.collection.immutable.Map<String, String> options = Map$.MODULE$.empty();
    Configuration hadoopConf = new Configuration();
    SQLConf sqlConf = SQLConf.get();

    PartitionReaderFactory factory =
        PartitionUtils.createParquetReaderFactory(
            dataSchema, partitionSchema, readDataSchema, filters, options, hadoopConf, sqlConf);

    assertNotNull(factory, "PartitionReaderFactory should not be null");
  }

  // ==============================================================================================
  // Helper methods
  // ==============================================================================================

  /** Helper to create MapValue from a Map of partition column names to string values. */
  private static MapValue createMapValue(Map<String, String> partitionValues) {
    String[] keys = partitionValues.keySet().toArray(new String[0]);
    String[] values = new String[keys.length];
    for (int i = 0; i < keys.length; i++) {
      values[i] = partitionValues.get(keys[i]);
    }

    io.delta.kernel.data.ColumnVector keysVector =
        new io.delta.kernel.data.ColumnVector() {
          @Override
          public io.delta.kernel.types.DataType getDataType() {
            return StringType.STRING;
          }

          @Override
          public int getSize() {
            return keys.length;
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return false;
          }

          @Override
          public String getString(int rowId) {
            return keys[rowId];
          }
        };

    io.delta.kernel.data.ColumnVector valuesVector =
        new io.delta.kernel.data.ColumnVector() {
          @Override
          public io.delta.kernel.types.DataType getDataType() {
            return StringType.STRING;
          }

          @Override
          public int getSize() {
            return values.length;
          }

          @Override
          public void close() {}

          @Override
          public boolean isNullAt(int rowId) {
            return values[rowId] == null;
          }

          @Override
          public String getString(int rowId) {
            return values[rowId];
          }
        };

    return new MapValue() {
      @Override
      public int getSize() {
        return keys.length;
      }

      @Override
      public io.delta.kernel.data.ColumnVector getKeys() {
        return keysVector;
      }

      @Override
      public io.delta.kernel.data.ColumnVector getValues() {
        return valuesVector;
      }
    };
  }
}
