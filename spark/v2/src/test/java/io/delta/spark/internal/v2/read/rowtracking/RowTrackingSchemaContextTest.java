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
package io.delta.spark.internal.v2.read.rowtracking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.delta.RowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

public class RowTrackingSchemaContextTest {

  private static final String MATERIALIZED_ROW_ID_COLUMN = "__row_id";
  private static final String MATERIALIZED_ROW_COMMIT_VERSION_COLUMN = "__row_commit_version";
  private static final StructType BASE_TABLE_SCHEMA =
      new StructType().add("id", DataTypes.LongType, false).add("name", DataTypes.StringType, true);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType, true);

  @Test
  public void testNoRequestedRowTrackingFieldsKeepBaseProjectionState() {
    RowTrackingSchemaContext context =
        new RowTrackingSchemaContext(readSchemaWithMetadata(), createMetadata(), PARTITION_SCHEMA);

    assertEquals(false, context.areRowTrackingMetadataFieldsRequested());
    assertEquals(false, context.isRowIdRequested());
    assertEquals(false, context.isRowCommitVersionRequested());
    assertEquals(-1, context.getMaterializedRowIdIndex());
    assertEquals(-1, context.getRowIndexColumnIndex());
    assertEquals(-1, context.getMaterializedRowCommitVersionIndex());

    assertEquals(BASE_TABLE_SCHEMA, context.getSchemaWithRowTrackingColumns());
    assertEquals(BASE_TABLE_SCHEMA, context.getDataSchema());
    assertEquals(PARTITION_SCHEMA, context.getPartitionSchema());
    assertEquals(List.of(0, 1), toJavaOrdinals(context.getDataColumnsOrdinals()));
    assertEquals(List.of(2), toJavaOrdinals(context.getPartitionColumnsOrdinals()));
  }

  @Test
  public void testRowIdRequestAddsMaterializedRowIdAndRowIndexColumns() {
    RowTrackingSchemaContext context =
        new RowTrackingSchemaContext(
            readSchemaWithMetadata(RowId$.MODULE$.ROW_ID()), createMetadata(), PARTITION_SCHEMA);

    StructType expectedPhysicalReadSchema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("name", DataTypes.StringType, true)
            .add(MATERIALIZED_ROW_ID_COLUMN, DataTypes.LongType, true)
            .add(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(), DataTypes.LongType, true);

    assertEquals(true, context.areRowTrackingMetadataFieldsRequested());
    assertEquals(true, context.isRowIdRequested());
    assertEquals(false, context.isRowCommitVersionRequested());
    assertEquals(2, context.getMaterializedRowIdIndex());
    assertEquals(3, context.getRowIndexColumnIndex());
    assertEquals(-1, context.getMaterializedRowCommitVersionIndex());

    assertEquals(expectedPhysicalReadSchema, context.getSchemaWithRowTrackingColumns());
    assertEquals(BASE_TABLE_SCHEMA, context.getDataSchema());
    assertEquals(PARTITION_SCHEMA, context.getPartitionSchema());
    assertEquals(List.of(0, 1), toJavaOrdinals(context.getDataColumnsOrdinals()));
    assertEquals(List.of(4), toJavaOrdinals(context.getPartitionColumnsOrdinals()));
  }

  @Test
  public void testRowCommitVersionRequestOnlyAddsCommitVersionColumn() {
    RowTrackingSchemaContext context =
        new RowTrackingSchemaContext(
            readSchemaWithMetadata(RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME()),
            createMetadata(),
            PARTITION_SCHEMA);

    StructType expectedPhysicalReadSchema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("name", DataTypes.StringType, true)
            .add(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN, DataTypes.LongType, true);

    assertEquals(true, context.areRowTrackingMetadataFieldsRequested());
    assertEquals(false, context.isRowIdRequested());
    assertEquals(true, context.isRowCommitVersionRequested());
    assertEquals(-1, context.getMaterializedRowIdIndex());
    assertEquals(-1, context.getRowIndexColumnIndex());
    assertEquals(2, context.getMaterializedRowCommitVersionIndex());

    assertEquals(expectedPhysicalReadSchema, context.getSchemaWithRowTrackingColumns());
    assertEquals(BASE_TABLE_SCHEMA, context.getDataSchema());
    assertEquals(PARTITION_SCHEMA, context.getPartitionSchema());
    assertEquals(List.of(0, 1), toJavaOrdinals(context.getDataColumnsOrdinals()));
    assertEquals(List.of(3), toJavaOrdinals(context.getPartitionColumnsOrdinals()));
  }

  @Test
  public void testBothRequestsAddAllInternalColumnsInReadOrder() {
    RowTrackingSchemaContext context =
        new RowTrackingSchemaContext(
            readSchemaWithMetadata(
                RowId$.MODULE$.ROW_ID(), RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME()),
            createMetadata(),
            PARTITION_SCHEMA);

    StructType expectedPhysicalReadSchema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("name", DataTypes.StringType, true)
            .add(MATERIALIZED_ROW_ID_COLUMN, DataTypes.LongType, true)
            .add(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(), DataTypes.LongType, true)
            .add(MATERIALIZED_ROW_COMMIT_VERSION_COLUMN, DataTypes.LongType, true);

    assertEquals(true, context.areRowTrackingMetadataFieldsRequested());
    assertEquals(true, context.isRowIdRequested());
    assertEquals(true, context.isRowCommitVersionRequested());
    assertEquals(2, context.getMaterializedRowIdIndex());
    assertEquals(3, context.getRowIndexColumnIndex());
    assertEquals(4, context.getMaterializedRowCommitVersionIndex());

    assertEquals(expectedPhysicalReadSchema, context.getSchemaWithRowTrackingColumns());
    assertEquals(BASE_TABLE_SCHEMA, context.getDataSchema());
    assertEquals(PARTITION_SCHEMA, context.getPartitionSchema());
    assertEquals(List.of(0, 1), toJavaOrdinals(context.getDataColumnsOrdinals()));
    assertEquals(List.of(5), toJavaOrdinals(context.getPartitionColumnsOrdinals()));
  }

  private static StructType readSchemaWithMetadata(String... metadataFieldNames) {
    StructType metadataSchema = new StructType();
    for (String metadataFieldName : metadataFieldNames) {
      metadataSchema = metadataSchema.add(metadataFieldName, DataTypes.LongType, true);
    }
    return BASE_TABLE_SCHEMA.add(FileFormat$.MODULE$.METADATA_NAME(), metadataSchema, false);
  }

  private static List<Integer> toJavaOrdinals(Seq<Object> ordinals) {
    return CollectionConverters.asJava(ordinals).stream()
        .map(value -> ((Number) value).intValue())
        .collect(Collectors.toList());
  }

  private static Metadata createMetadata() {
    return createMetadata(
        Map.of(
            MaterializedRowTrackingColumn.MATERIALIZED_ROW_ID.getMaterializedColumnNameProperty(),
            MATERIALIZED_ROW_ID_COLUMN,
            MaterializedRowTrackingColumn.MATERIALIZED_ROW_COMMIT_VERSION
                .getMaterializedColumnNameProperty(),
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN));
  }

  private static Metadata createMetadata(Map<String, String> configuration) {
    io.delta.kernel.types.StructType kernelSchema =
        new io.delta.kernel.types.StructType()
            .add("id", io.delta.kernel.types.LongType.LONG)
            .add("name", io.delta.kernel.types.StringType.STRING);
    ArrayValue emptyPartitionColumns =
        new ArrayValue() {
          @Override
          public int getSize() {
            return 0;
          }

          @Override
          public ColumnVector getElements() {
            return null;
          }
        };
    return new Metadata(
        "id",
        java.util.Optional.empty() /* name */,
        java.util.Optional.empty() /* description */,
        new Format(),
        kernelSchema.toJson(),
        kernelSchema,
        emptyPartitionColumns,
        java.util.Optional.empty() /* createdTime */,
        io.delta.kernel.internal.util.VectorUtils.stringStringMapValue(configuration));
  }
}
