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
package io.delta.spark.internal.v2.read.cdc;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class CDCSchemaContextTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType);

  @Test
  void testDataOnlySchema() {
    CDCSchemaContext ctx = new CDCSchemaContext(DATA_SCHEMA, new StructType(), DATA_SCHEMA);

    // Augmented schema: [id, name, _change_type, _commit_version, _commit_timestamp]
    StructType augmented = ctx.getReadDataSchemaWithCDC();
    assertEquals(5, augmented.fields().length);
    assertEquals("id", augmented.fields()[0].name());
    assertEquals("name", augmented.fields()[1].name());
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, augmented.fields()[2].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_VERSION, augmented.fields()[3].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, augmented.fields()[4].name());

    // CDC indices at dataColCount=2
    assertEquals(2, ctx.getChangeTypeInternalIndex());
    assertEquals(3, ctx.getCommitVersionInternalIndex());
    assertEquals(4, ctx.getCommitTimestampInternalIndex());

    assertEquals(2, ctx.getTableColCount());

    // Output mapping: [id->0, name->1, CDC->-1, -1, -1]
    int[] mapping = ctx.getOutputToInternalMapping();
    assertArrayEquals(new int[] {0, 1, -1, -1, -1}, mapping);
  }

  @Test
  void testWithPartitionColumns() {
    // tableSchema interleaves data + partition: [id, name, date]
    StructType tableSchema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType)
            .add("date", DataTypes.StringType);
    CDCSchemaContext ctx = new CDCSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA, tableSchema);

    // Internal layout: [id(0), name(1), CDC(2,3,4), date(5)]
    // Output layout:   [id, name, date, CDC(3)]
    assertEquals(3, ctx.getTableColCount());
    assertEquals(2, ctx.getChangeTypeInternalIndex());

    // date maps to internal 5 (after data + CDC), not 2
    assertArrayEquals(new int[] {0, 1, 5, -1, -1, -1}, ctx.getOutputToInternalMapping());
  }

  @Test
  void testColumnPruning() {
    // readDataSchema is a strict subset of tableSchema (only "id" selected)
    StructType prunedData = new StructType().add("id", DataTypes.IntegerType);
    StructType tableSchema =
        new StructType().add("id", DataTypes.IntegerType).add("date", DataTypes.StringType);

    CDCSchemaContext ctx = new CDCSchemaContext(prunedData, PARTITION_SCHEMA, tableSchema);

    // Internal layout: [id(0), CDC(1,2,3), date(4)]
    assertEquals(1, ctx.getChangeTypeInternalIndex()); // dataColCount = 1
    assertEquals(2, ctx.getTableColCount());

    // date maps to internal 4 (1 data col + 3 CDC cols)
    assertArrayEquals(new int[] {0, 4, -1, -1, -1}, ctx.getOutputToInternalMapping());
  }

  @Test
  void testColumnNotFoundThrows() {
    StructType tableSchema =
        new StructType().add("id", DataTypes.IntegerType).add("unknown", DataTypes.StringType);
    StructType dataSchema = new StructType().add("id", DataTypes.IntegerType);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> new CDCSchemaContext(dataSchema, new StructType(), tableSchema));
    assertTrue(e.getMessage().contains("unknown"));
  }

  @Test
  void testIsCDCColumn() {
    assertTrue(CDCSchemaContext.isCDCColumn(CDCSchemaContext.CDC_TYPE_COLUMN));
    assertTrue(CDCSchemaContext.isCDCColumn(CDCSchemaContext.CDC_COMMIT_VERSION));
    assertTrue(CDCSchemaContext.isCDCColumn(CDCSchemaContext.CDC_COMMIT_TIMESTAMP));
    assertFalse(CDCSchemaContext.isCDCColumn("id"));
    assertFalse(CDCSchemaContext.isCDCColumn("_change_type_extra"));
  }

  @Test
  void testCdcFieldsDefensiveCopy() {
    StructField[] fields1 = CDCSchemaContext.cdcFields();
    StructField[] fields2 = CDCSchemaContext.cdcFields();

    assertEquals(3, fields1.length);
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, fields1[0].name());
    assertEquals(DataTypes.StringType, fields1[0].dataType());
    assertEquals(CDCSchemaContext.CDC_COMMIT_VERSION, fields1[1].name());
    assertEquals(DataTypes.LongType, fields1[1].dataType());
    assertEquals(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, fields1[2].name());
    assertEquals(DataTypes.TimestampType, fields1[2].dataType());

    assertNotSame(fields1, fields2);
  }
}
