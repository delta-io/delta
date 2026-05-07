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

import java.util.OptionalInt;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class CDCSchemaContextTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType);

  @Test
  void testNoCDCColumnsRequested() {
    CDCSchemaContext ctx = new CDCSchemaContext(DATA_SCHEMA, new StructType());
    assertEquals(OptionalInt.empty(), ctx.changeTypeIndex());
    assertEquals(OptionalInt.empty(), ctx.commitVersionIndex());
    assertEquals(OptionalInt.empty(), ctx.commitTimestampIndex());

    StructType full = ctx.getFullRowSchema();
    assertEquals(2, full.fields().length);
    assertEquals("id", full.fields()[0].name());
    assertEquals("name", full.fields()[1].name());
  }

  @Test
  void testAllCDCColumnsAtTail() {
    StructType readDataSchema = CDCSchemaContext.appendCDCColumns(DATA_SCHEMA);
    CDCSchemaContext ctx = new CDCSchemaContext(readDataSchema, PARTITION_SCHEMA);

    // [id(0), name(1), _change_type(2), _commit_version(3), _commit_timestamp(4)] then date(5)
    assertEquals(OptionalInt.of(2), ctx.changeTypeIndex());
    assertEquals(OptionalInt.of(3), ctx.commitVersionIndex());
    assertEquals(OptionalInt.of(4), ctx.commitTimestampIndex());

    StructType full = ctx.getFullRowSchema();
    assertEquals(6, full.fields().length);
    assertEquals("id", full.fields()[0].name());
    assertEquals("name", full.fields()[1].name());
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, full.fields()[2].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_VERSION, full.fields()[3].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, full.fields()[4].name());
    assertEquals("date", full.fields()[5].name());
  }

  @Test
  void testCDCColumnsInterleaved() {
    StructType readDataSchema =
        new StructType()
            .add("id", DataTypes.IntegerType)
            .add(CDCSchemaContext.CDC_TYPE_COLUMN, DataTypes.StringType)
            .add("name", DataTypes.StringType)
            .add(CDCSchemaContext.CDC_COMMIT_VERSION, DataTypes.LongType);
    CDCSchemaContext ctx = new CDCSchemaContext(readDataSchema, new StructType());

    assertEquals(OptionalInt.of(1), ctx.changeTypeIndex());
    assertEquals(OptionalInt.of(3), ctx.commitVersionIndex());
    assertEquals(OptionalInt.empty(), ctx.commitTimestampIndex());
  }

  @Test
  void testCDCColumnAtHead() {
    StructType readDataSchema =
        new StructType()
            .add(CDCSchemaContext.CDC_COMMIT_VERSION, DataTypes.LongType)
            .add("id", DataTypes.IntegerType)
            .add("name", DataTypes.StringType);
    CDCSchemaContext ctx = new CDCSchemaContext(readDataSchema, new StructType());

    assertEquals(OptionalInt.empty(), ctx.changeTypeIndex());
    assertEquals(OptionalInt.of(0), ctx.commitVersionIndex());
    assertEquals(OptionalInt.empty(), ctx.commitTimestampIndex());
  }

  @Test
  void testCaseInsensitiveLookup() {
    StructType readDataSchema =
        new StructType().add("id", DataTypes.IntegerType).add("_Change_Type", DataTypes.StringType);
    CDCSchemaContext ctx = new CDCSchemaContext(readDataSchema, new StructType());
    assertEquals(OptionalInt.of(1), ctx.changeTypeIndex());
  }

  @Test
  void testAppendCDCColumns() {
    StructType augmented = CDCSchemaContext.appendCDCColumns(DATA_SCHEMA);
    assertEquals(5, augmented.fields().length);
    assertEquals("id", augmented.fields()[0].name());
    assertEquals("name", augmented.fields()[1].name());
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, augmented.fields()[2].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_VERSION, augmented.fields()[3].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, augmented.fields()[4].name());
  }
}
