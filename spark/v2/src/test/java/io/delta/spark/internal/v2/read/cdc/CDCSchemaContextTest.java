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

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;

public class CDCSchemaContextTest {

  private static final StructType DATA_SCHEMA =
      new StructType().add("id", DataTypes.IntegerType).add("name", DataTypes.StringType);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType);

  @Test
  void testDataOnlySchema() {
    CDCSchemaContext ctx = new CDCSchemaContext(DATA_SCHEMA, new StructType());

    // Augmented schema (Parquet input): [id, name, _change_type, _commit_version,
    // _commit_timestamp]
    StructType augmented = ctx.getReadDataSchemaWithCDC();
    assertEquals(5, augmented.fields().length);
    assertEquals("id", augmented.fields()[0].name());
    assertEquals("name", augmented.fields()[1].name());
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, augmented.fields()[2].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_VERSION, augmented.fields()[3].name());
    assertEquals(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, augmented.fields()[4].name());

    assertEquals(2, ctx.getChangeTypeInternalIndex());

    // Ordinals: [id->0, name->1] (no partition, no CDC sentinels).
    assertEquals(List.of(0, 1), toJavaList(ctx.getDataAndPartitionOrdinals()));
  }

  @Test
  void testWithPartitionColumns() {
    CDCSchemaContext ctx = new CDCSchemaContext(DATA_SCHEMA, PARTITION_SCHEMA);

    // Internal layout: [id(0), name(1), CDC(2,3,4), date(5)]
    // Output (non-CDC): [id, name, date]; date maps to internal 5 (after data + CDC), not 2.
    assertEquals(2, ctx.getChangeTypeInternalIndex());
    assertEquals(List.of(0, 1, 5), toJavaList(ctx.getDataAndPartitionOrdinals()));

    // dataAndPartitionSchema is [id, name, date].
    StructType dap = ctx.getDataAndPartitionSchema();
    assertEquals(3, dap.fields().length);
    assertEquals("id", dap.fields()[0].name());
    assertEquals("name", dap.fields()[1].name());
    assertEquals("date", dap.fields()[2].name());
  }

  @Test
  void testDataColumnPruning() {
    // readDataSchema is a strict subset of dataSchema (only "id" selected; "name" pruned).
    StructType prunedData = new StructType().add("id", DataTypes.IntegerType);

    CDCSchemaContext ctx = new CDCSchemaContext(prunedData, PARTITION_SCHEMA);

    // Internal layout: [id(0), CDC(1,2,3), date(4)]
    // Output (non-CDC): [id, date]
    assertEquals(1, ctx.getChangeTypeInternalIndex()); // dataColCount = 1
    assertEquals(List.of(0, 4), toJavaList(ctx.getDataAndPartitionOrdinals()));

    // Output schema honors pruning: pruned data column ("name") is NOT in the output.
    StructType dap = ctx.getDataAndPartitionSchema();
    assertEquals(2, dap.fields().length);
    assertEquals("id", dap.fields()[0].name());
    assertEquals("date", dap.fields()[1].name());
  }

  @Test
  void testIsCDCColumn() {
    assertTrue(CDCSchemaContext.isCDCColumn(CDCSchemaContext.CDC_TYPE_COLUMN));
    assertTrue(CDCSchemaContext.isCDCColumn(CDCSchemaContext.CDC_COMMIT_VERSION));
    assertTrue(CDCSchemaContext.isCDCColumn(CDCSchemaContext.CDC_COMMIT_TIMESTAMP));
    assertTrue(CDCSchemaContext.isCDCColumn("_Change_Type"));
    assertTrue(CDCSchemaContext.isCDCColumn("_COMMIT_VERSION"));
    assertFalse(CDCSchemaContext.isCDCColumn("id"));
    assertFalse(CDCSchemaContext.isCDCColumn("_change_type_extra"));
  }

  @Test
  void testCdcFieldsDefensiveCopy() {
    var fields1 = CDCSchemaContext.cdcFields();
    var fields2 = CDCSchemaContext.cdcFields();

    assertEquals(3, fields1.length);
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, fields1[0].name());
    assertEquals(DataTypes.StringType, fields1[0].dataType());
    assertEquals(CDCSchemaContext.CDC_COMMIT_VERSION, fields1[1].name());
    assertEquals(DataTypes.LongType, fields1[1].dataType());
    assertEquals(CDCSchemaContext.CDC_COMMIT_TIMESTAMP, fields1[2].name());
    assertEquals(DataTypes.TimestampType, fields1[2].dataType());

    assertNotSame(fields1, fields2);
  }

  @Test
  void testIndexConstantsMatchCDCFieldsOrder() {
    var fields = CDCSchemaContext.cdcFields();
    assertEquals(CDCSchemaContext.CDC_TYPE_COLUMN, fields[CDCSchemaContext.CHANGE_TYPE_IDX].name());
    assertEquals(
        CDCSchemaContext.CDC_COMMIT_VERSION, fields[CDCSchemaContext.COMMIT_VERSION_IDX].name());
    assertEquals(
        CDCSchemaContext.CDC_COMMIT_TIMESTAMP,
        fields[CDCSchemaContext.COMMIT_TIMESTAMP_IDX].name());
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

  private static List<Integer> toJavaList(Seq<Object> seq) {
    List<Integer> result = new ArrayList<>(seq.size());
    for (int i = 0; i < seq.size(); i++) {
      result.add((Integer) seq.apply(i));
    }
    return result;
  }
}
