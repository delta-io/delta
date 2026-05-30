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
package io.delta.spark.internal.v2.read.metadata;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

public class MetadataStructSchemaContextTest {

  private static final String MATERIALIZED_ROW_ID_COLUMN = "__row_id";
  private static final String MATERIALIZED_ROW_COMMIT_VERSION_COLUMN = "__row_commit_version";

  private static final StructType BASE_TABLE_SCHEMA =
      new StructType().add("id", DataTypes.LongType, false).add("name", DataTypes.StringType, true);
  private static final StructType PARTITION_SCHEMA =
      new StructType().add("date", DataTypes.StringType, true);
  private static final StructType EMPTY_PARTITION_SCHEMA = new StructType();

  @Test
  public void testForSchemaReturnsEmptyWhenNoMetadataColumn() {
    Optional<MetadataStructSchemaContext> ctx =
        MetadataStructSchemaContext.forSchemaWithExtractors(
            BASE_TABLE_SCHEMA,
            PARTITION_SCHEMA,
            FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS(),
            createMetadata());
    assertFalse(ctx.isPresent(), "no _metadata struct should yield Optional.empty()");
  }

  @Test
  public void testFilePathOnlyPrunedStruct() {
    StructType readSchema = readSchemaWithMetadata(FileFormat$.MODULE$.FILE_PATH());
    MetadataStructSchemaContext ctx = forSchema(readSchema, EMPTY_PARTITION_SCHEMA);

    assertArrayEquals(
        new String[] {FileFormat$.MODULE$.FILE_PATH()}, ctx.getPrunedMetadataStruct().fieldNames());
    // No row tracking helper columns when only base fields are requested.
    assertEquals(BASE_TABLE_SCHEMA, ctx.getParquetReadSchema());
    assertEquals(BASE_TABLE_SCHEMA, ctx.getDataSchema());
    assertEquals(List.of(0, 1), toJavaOrdinals(ctx.getDataColumnsOrdinals()));
    assertEquals(0, ctx.getPartitionColumnsOrdinals().size());
    assertEquals(1, ctx.getValueSetterBuilders().length);
    assertTrue(
        ctx.getValueSetterBuilders()[0] instanceof FileConstantValueSetterBuilder,
        "file_path should be a FileConstantValueSetterBuilder");
  }

  @Test
  public void testRowIdOnlyAddsTwoHelperColumns() {
    StructType readSchema = readSchemaWithMetadata(RowId$.MODULE$.ROW_ID());
    MetadataStructSchemaContext ctx = forSchema(readSchema, PARTITION_SCHEMA);

    // Parquet read schema = base data + materialized row id + parquet row index.
    assertEquals(4, ctx.getParquetReadSchema().fields().length);
    assertEquals(MATERIALIZED_ROW_ID_COLUMN, ctx.getParquetReadSchema().fields()[2].name());
    assertEquals(
        ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(),
        ctx.getParquetReadSchema().fields()[3].name());

    // Data ordinals unchanged; partition ordinals shift past helpers.
    assertEquals(BASE_TABLE_SCHEMA, ctx.getDataSchema());
    assertEquals(List.of(0, 1), toJavaOrdinals(ctx.getDataColumnsOrdinals()));
    assertEquals(List.of(4), toJavaOrdinals(ctx.getPartitionColumnsOrdinals()));

    assertEquals(1, ctx.getValueSetterBuilders().length);
    assertTrue(ctx.getValueSetterBuilders()[0] instanceof RowIdValueSetterBuilder);
  }

  @Test
  public void testRowCommitVersionOnlyAddsOneHelperColumn() {
    StructType readSchema =
        readSchemaWithMetadata(RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME());
    MetadataStructSchemaContext ctx = forSchema(readSchema, EMPTY_PARTITION_SCHEMA);

    // Parquet read schema = base data + materialized row commit version (no row index column for
    // commit version alone).
    assertEquals(3, ctx.getParquetReadSchema().fields().length);
    assertEquals(
        MATERIALIZED_ROW_COMMIT_VERSION_COLUMN, ctx.getParquetReadSchema().fields()[2].name());
    assertEquals(1, ctx.getValueSetterBuilders().length);
    assertTrue(ctx.getValueSetterBuilders()[0] instanceof RowCommitVersionValueSetterBuilder);
  }

  @Test
  public void testBaseFieldsAndRowTrackingFieldsMixed() {
    StructType readSchema =
        readSchemaWithMetadata(
            FileFormat$.MODULE$.FILE_PATH(),
            RowId$.MODULE$.ROW_ID(),
            RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME());
    MetadataStructSchemaContext ctx = forSchema(readSchema, EMPTY_PARTITION_SCHEMA);

    // Parquet schema: base data (2) + row_id helpers (2) + row_commit_version helper (1) = 5.
    assertEquals(5, ctx.getParquetReadSchema().fields().length);
    assertEquals(3, ctx.getValueSetterBuilders().length);
    assertTrue(ctx.getValueSetterBuilders()[0] instanceof FileConstantValueSetterBuilder);
    assertTrue(ctx.getValueSetterBuilders()[1] instanceof RowIdValueSetterBuilder);
    assertTrue(ctx.getValueSetterBuilders()[2] instanceof RowCommitVersionValueSetterBuilder);
  }

  @Test
  public void testUnsupportedSubfieldThrowsAtContextConstruction() {
    StructType readSchema = readSchemaWithMetadata("not_a_known_metadata_field");
    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> forSchema(readSchema, EMPTY_PARTITION_SCHEMA));
    assertTrue(ex.getMessage().contains("not_a_known_metadata_field"));
    assertTrue(
        ex.getMessage().contains("row-tracking"),
        "Error message should reference row-tracking fallback");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static MetadataStructSchemaContext forSchema(
      StructType readSchema, StructType partitionSchema) {
    return MetadataStructSchemaContext.forSchemaWithExtractors(
            readSchema,
            partitionSchema,
            FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS(),
            createMetadata())
        .orElseThrow(() -> new AssertionError("expected non-empty context"));
  }

  private static StructType readSchemaWithMetadata(String... metadataFieldNames) {
    StructType metadataSchema = new StructType();
    for (String name : metadataFieldNames) {
      metadataSchema =
          metadataSchema.add(
              name,
              FileFormat$.MODULE$.FILE_PATH().equals(name)
                  ? DataTypes.StringType
                  : DataTypes.LongType,
              true);
    }
    return BASE_TABLE_SCHEMA.add(FileFormat$.MODULE$.METADATA_NAME(), metadataSchema, false);
  }

  private static List<Integer> toJavaOrdinals(Seq<Object> ordinals) {
    return CollectionConverters.asJava(ordinals).stream()
        .map(v -> ((Number) v).intValue())
        .collect(Collectors.toList());
  }

  private static Metadata createMetadata() {
    Map<String, String> configuration =
        Map.of(
            MaterializedRowTrackingColumn.MATERIALIZED_ROW_ID.getMaterializedColumnNameProperty(),
            MATERIALIZED_ROW_ID_COLUMN,
            MaterializedRowTrackingColumn.MATERIALIZED_ROW_COMMIT_VERSION
                .getMaterializedColumnNameProperty(),
            MATERIALIZED_ROW_COMMIT_VERSION_COLUMN);
    io.delta.kernel.types.StructType kernelSchema =
        new io.delta.kernel.types.StructType().add("id", io.delta.kernel.types.LongType.LONG);
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
        java.util.Optional.empty(),
        java.util.Optional.empty(),
        new Format(),
        kernelSchema.toJson(),
        kernelSchema,
        emptyPartitionColumns,
        java.util.Optional.empty(),
        io.delta.kernel.internal.util.VectorUtils.stringStringMapValue(configuration));
  }
}
