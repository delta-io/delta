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

import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class DvSchemaContextTest {

  // Common test schemas
  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType);

  @Test
  void testSchemaWithDvColumn() {
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);

    StructType schemaWithDv = context.getSchemaWithDvColumn();
    assertEquals(3, schemaWithDv.fields().length);
    assertEquals("id", schemaWithDv.fields()[0].name());
    assertEquals("name", schemaWithDv.fields()[1].name());
    assertEquals(
        DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME(), schemaWithDv.fields()[2].name());
  }

  @Test
  void testDvColumnIndex() {
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, new StructType());
    assertEquals(2, context.getDvColumnIndex());
  }

  @Test
  void testInputColumnCount() {
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);
    // Input: 2 data + 1 DV + 1 partition = 4
    assertEquals(4, context.getInputColumnCount());
  }

  @Test
  void testOutputSchema() {
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);

    StructType outputSchema = context.getOutputSchema();
    assertEquals(3, outputSchema.fields().length);
    assertEquals("id", outputSchema.fields()[0].name());
    assertEquals("name", outputSchema.fields()[1].name());
    assertEquals("date", outputSchema.fields()[2].name());
  }

  @Test
  void testEmptyPartitionSchema() {
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, new StructType());

    assertEquals(3, context.getSchemaWithDvColumn().fields().length); // id + name + DV
    assertEquals(2, context.getDvColumnIndex());
    assertEquals(3, context.getInputColumnCount()); // id + name + DV
    assertEquals(2, context.getOutputSchema().fields().length); // id + name
  }
}
