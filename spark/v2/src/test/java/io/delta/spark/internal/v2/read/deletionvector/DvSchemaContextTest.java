/*
 * Copyright (2026) The Delta Lake Project Authors.
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

  // Common test schemas.
  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType);

  @Test
  void testWithFullSchemas() {
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);

    StructType expectedSchemaWithDv =
        DATA_SCHEMA.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    assertEquals(expectedSchemaWithDv, context.getSchemaWithDvColumn());
    assertEquals(2, context.getDvColumnIndex());
    // Input: 2 data + 1 DV + 1 partition = 4.
    assertEquals(4, context.getInputColumnCount());
    StructType expectedOutputSchema =
        DATA_SCHEMA.merge(PARTITION_SCHEMA, /* handleDuplicateColumns= */ false);
    assertEquals(expectedOutputSchema, context.getOutputSchema());
  }

  @Test
  void testEmptyPartitionSchema() {
    StructType emptyPartition = new StructType();
    DvSchemaContext context = new DvSchemaContext(DATA_SCHEMA, emptyPartition);

    StructType expectedSchemaWithDv =
        DATA_SCHEMA.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    assertEquals(expectedSchemaWithDv, context.getSchemaWithDvColumn());
    assertEquals(2, context.getDvColumnIndex());
    // Input: 2 data + 1 DV = 3.
    assertEquals(3, context.getInputColumnCount());
    assertEquals(DATA_SCHEMA, context.getOutputSchema());
  }

  @Test
  void testEmptyDataSchema() {
    StructType emptyData = new StructType();
    DvSchemaContext context = new DvSchemaContext(emptyData, PARTITION_SCHEMA);

    StructType expectedSchemaWithDv =
        emptyData.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    assertEquals(expectedSchemaWithDv, context.getSchemaWithDvColumn());
    assertEquals(0, context.getDvColumnIndex());
    // Input: 1 DV + 1 partition = 2.
    assertEquals(2, context.getInputColumnCount());
    assertEquals(PARTITION_SCHEMA, context.getOutputSchema());
  }

  @Test
  void testDuplicateDvColumnThrowsException() {
    // Schema that already contains the DV column.
    StructType schemaWithDv =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME(), DataTypes.ByteType);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new DvSchemaContext(schemaWithDv, new StructType()));

    assertTrue(
        exception.getMessage().contains(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME()));
  }
}
