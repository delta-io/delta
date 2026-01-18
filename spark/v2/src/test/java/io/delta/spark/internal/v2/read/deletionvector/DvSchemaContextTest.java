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
package io.delta.spark.internal.v2.read.deletionvector;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class DvSchemaContextTest {

  // Common test schemas
  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType);

  @ParameterizedTest(name = "useMetadataRowIndex={0}")
  @CsvSource({"false, 3, 2", "true, 4, 3"})
  void testSchemaWithDvColumn(
      boolean useMetadataRowIndex, int expectedFieldCount, int expectedDvIndex) {
    DvSchemaContext context =
        new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA, useMetadataRowIndex);

    StructType schemaWithDv = context.getSchemaWithDvColumn();
    assertEquals(expectedFieldCount, schemaWithDv.fields().length);
    assertEquals("id", schemaWithDv.fields()[0].name());
    assertEquals("name", schemaWithDv.fields()[1].name());

    if (useMetadataRowIndex) {
      assertEquals(
          ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(), schemaWithDv.fields()[2].name());
    }
    assertEquals(
        DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME(),
        schemaWithDv.fields()[expectedDvIndex].name());
  }

  @ParameterizedTest(name = "useMetadataRowIndex={0}")
  @CsvSource({"false, 4", "true, 5"})
  void testInputColumnCount(boolean useMetadataRowIndex, int expectedCount) {
    DvSchemaContext context =
        new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA, useMetadataRowIndex);
    assertEquals(expectedCount, context.getInputColumnCount());
  }

  @ParameterizedTest(name = "useMetadataRowIndex={0}")
  @CsvSource({"false, '0,1,3'", "true, '0,1,4'"})
  void testOutputColumnOrdinals(boolean useMetadataRowIndex, String expectedOrdinalsStr) {
    DvSchemaContext context =
        new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA, useMetadataRowIndex);

    List<Integer> expected =
        Arrays.stream(expectedOrdinalsStr.split(","))
            .map(String::trim)
            .map(Integer::parseInt)
            .collect(Collectors.toList());
    assertEquals(expected, context.getOutputColumnOrdinals());
  }

  @Test
  void testOutputSchema() {
    DvSchemaContext context =
        new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA, /* useMetadataRowIndex= */ false);

    StructType outputSchema = context.getOutputSchema();
    assertEquals(3, outputSchema.fields().length);
    assertEquals("id", outputSchema.fields()[0].name());
    assertEquals("name", outputSchema.fields()[1].name());
    assertEquals("date", outputSchema.fields()[2].name());
  }

  @Test
  void testEmptyPartitionSchema() {
    StructType emptyPartitionSchema = new StructType();
    DvSchemaContext context =
        new DvSchemaContext(DATA_SCHEMA, emptyPartitionSchema, /* useMetadataRowIndex= */ false);

    assertEquals(3, context.getSchemaWithDvColumn().fields().length); // id + name + DV
    assertEquals(2, context.getDvColumnIndex());
    assertEquals(3, context.getInputColumnCount()); // id + name + DV
    assertEquals(2, context.getOutputSchema().fields().length); // id + name
  }
}
