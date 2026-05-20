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

import static io.delta.spark.internal.v2.InternalRowTestUtils.collectRows;
import static io.delta.spark.internal.v2.InternalRowTestUtils.mockReader;
import static io.delta.spark.internal.v2.InternalRowTestUtils.row;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.kernel.data.ArrayValue;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.internal.actions.Format;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;
import java.util.List;
import java.util.Map;
import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.delta.RowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.collection.immutable.Map$;
import scala.jdk.javaapi.CollectionConverters;

/**
 * End-to-end tests for {@link MetadataStructReadFunction} covering Spark base fields and the
 * row-tracking fields, all driven through {@link MetadataValueSetterBuilder} via the unified {@link
 * MetadataStructSchemaContext}.
 */
public class MetadataStructReadFunctionTest {

  private static final String MATERIALIZED_ROW_ID_COLUMN = "__row_id";
  private static final String MATERIALIZED_ROW_COMMIT_VERSION_COLUMN = "__row_commit_version";
  private static final StructType EMPTY_PARTITION_SCHEMA = new StructType();

  // ---------------------------------------------------------------------------
  // Base Spark file-source metadata fields
  // ---------------------------------------------------------------------------

  @Test
  public void testFilePathAndFileSizeMatchPartitionedFileAndDisplayPath() {
    StructType metadataSchema =
        new StructType()
            .add(FileFormat$.MODULE$.FILE_PATH(), DataTypes.StringType, false)
            .add(FileFormat$.MODULE$.FILE_SIZE(), DataTypes.LongType, false);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    PartitionedFile file =
        new PartitionedFile(
            row(),
            SparkPath.fromUrlString("file:///tmp/metadata-test.parquet"),
            5L,
            100L,
            new String[0],
            1234L,
            9999L,
            Map$.MODULE$.empty());

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(1L))), context(readSchema, EMPTY_PARTITION_SCHEMA));

    List<InternalRow> result = collectRows(readFunction.apply(file));

    assertEquals(1, result.size());
    InternalRow out = result.get(0);
    assertEquals(2, out.numFields());
    InternalRow metadata = out.getStruct(1, 2);
    assertEquals(
        FileFormat$.MODULE$.getDisplayPath(file.filePath(), true),
        metadata.getUTF8String(0).toString());
    assertEquals(9999L, metadata.getLong(1));
  }

  /**
   * All six Spark base {@code _metadata} fields must match {@link
   * org.apache.spark.sql.execution.datasources.FileFormat#BASE_METADATA_EXTRACTORS} for the same
   * {@link PartitionedFile} (distinct start/length/size/mtime so values cannot be confused).
   */
  @Test
  public void testAllSixBaseMetadataFieldsMatchFileFormatExtractors() {
    StructType metadataSchema = new StructType();
    for (StructField f : CollectionConverters.asJava(FileFormat$.MODULE$.BASE_METADATA_FIELDS())) {
      metadataSchema = metadataSchema.add(f);
    }
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    PartitionedFile file =
        new PartitionedFile(
            row(),
            SparkPath.fromUrlString("file:///tmp/all-six-meta-test-unique.parquet"),
            7L,
            42L,
            new String[0],
            87_654_321L,
            999L,
            Map$.MODULE$.empty());

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(1L))), context(readSchema, EMPTY_PARTITION_SCHEMA));

    List<InternalRow> result = collectRows(readFunction.apply(file));
    assertEquals(1, result.size());
    InternalRow metadata = result.get(0).getStruct(1, 6);

    assertEquals(
        FileFormat$.MODULE$.getDisplayPath(file.filePath(), true),
        metadata.getUTF8String(0).toString());
    assertEquals(
        FileFormat$.MODULE$.getDisplayName(file.filePath()), metadata.getUTF8String(1).toString());
    assertEquals(999L, metadata.getLong(2));
    assertEquals(7L, metadata.getLong(3));
    assertEquals(42L, metadata.getLong(4));
    assertEquals(87_654_321L * 1000L, metadata.getLong(5));
  }

  @Test
  public void testUnsupportedMetadataSubfieldThrowsAtContextConstruction() {
    StructType metadataSchema =
        new StructType()
            .add("not_a_supported_metadata_field", DataTypes.StringType, false)
            .add(FileFormat$.MODULE$.FILE_PATH(), DataTypes.StringType, false);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    IllegalStateException ex =
        assertThrows(
            IllegalStateException.class, () -> context(readSchema, EMPTY_PARTITION_SCHEMA));
    assertTrue(ex.getMessage().contains("not_a_supported_metadata_field"));
    assertTrue(
        ex.getMessage().contains("row-tracking"),
        "Error should mention the row-tracking fallback so callers know what's allowed");
  }

  @Test
  public void testMetadataWithOnlyBaseFieldsHasNoRowTrackingSlots() {
    StructType metadataSchema =
        new StructType().add(FileFormat$.MODULE$.FILE_PATH(), DataTypes.StringType, false);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(1L))), context(readSchema, EMPTY_PARTITION_SCHEMA));
    List<InternalRow> result = collectRows(readFunction.apply(simplePartitionedFile(null, null)));

    assertEquals(1, result.size());
    InternalRow metadata = result.get(0).getStruct(1, 1);
    assertEquals(1, metadata.numFields(), "Only file_path slot should exist");
  }

  // ---------------------------------------------------------------------------
  // Row-tracking integration
  // ---------------------------------------------------------------------------

  @Test
  public void testRowIdProjectionUsesMaterializedRowIdWhenPresent() {
    StructType metadataSchema =
        new StructType().add(RowId$.MODULE$.ROW_ID(), DataTypes.LongType, true);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(1L, 101L, 0L))), context(readSchema, EMPTY_PARTITION_SCHEMA));

    List<InternalRow> result = collectRows(readFunction.apply(simplePartitionedFile(50L, null)));

    assertEquals(1, result.size());
    assertRowIdResult(result.get(0), 1L, 101L);
  }

  @Test
  public void testRowIdProjectionFallsBackToBaseRowIdPlusPhysicalRowIndex() {
    StructType metadataSchema =
        new StructType().add(RowId$.MODULE$.ROW_ID(), DataTypes.LongType, true);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(2L, null, 2L))), context(readSchema, EMPTY_PARTITION_SCHEMA));

    List<InternalRow> result = collectRows(readFunction.apply(simplePartitionedFile(50L, null)));

    assertEquals(1, result.size());
    assertRowIdResult(result.get(0), 2L, 52L);
  }

  @Test
  public void testRowCommitVersionProjectionUsesMaterializedCommitVersionWhenPresent() {
    StructType metadataSchema =
        new StructType()
            .add(RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME(), DataTypes.LongType, true);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(1L, 7L))), context(readSchema, EMPTY_PARTITION_SCHEMA));

    List<InternalRow> result = collectRows(readFunction.apply(simplePartitionedFile(null, 9L)));

    assertEquals(1, result.size());
    assertCommitVersionResult(result.get(0), 1L, 7L);
  }

  @Test
  public void testRowCommitVersionProjectionFallsBackToDefaultCommitVersion() {
    StructType metadataSchema =
        new StructType()
            .add(RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME(), DataTypes.LongType, true);
    StructType readSchema = readSchemaWithMetadata(metadataSchema);

    MetadataStructReadFunction readFunction =
        MetadataStructReadFunction.wrap(
            mockReader(List.of(row(2L, null))), context(readSchema, EMPTY_PARTITION_SCHEMA));

    List<InternalRow> result = collectRows(readFunction.apply(simplePartitionedFile(null, 9L)));

    assertEquals(1, result.size());
    assertCommitVersionResult(result.get(0), 2L, 9L);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void assertRowIdResult(InternalRow row, long expectedId, long expectedRowId) {
    assertEquals(2, row.numFields());
    assertEquals(expectedId, row.getLong(0));
    InternalRow metadata = row.getStruct(1, 1);
    assertEquals(expectedRowId, metadata.getLong(0));
  }

  private static void assertCommitVersionResult(
      InternalRow row, long expectedId, long expectedCommitVersion) {
    assertEquals(2, row.numFields());
    assertEquals(expectedId, row.getLong(0));
    InternalRow metadata = row.getStruct(1, 1);
    assertEquals(expectedCommitVersion, metadata.getLong(0));
  }

  private static StructType readSchemaWithMetadata(StructType metadataSchema) {
    return new StructType()
        .add("id", DataTypes.LongType, false)
        .add(FileFormat$.MODULE$.METADATA_NAME(), metadataSchema, false);
  }

  private static MetadataStructSchemaContext context(
      StructType readSchema, StructType partitionSchema) {
    return MetadataStructSchemaContext.forSchemaWithExtractors(
            readSchema,
            partitionSchema,
            FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS(),
            createMetadata())
        .orElseThrow(() -> new AssertionError("expected non-empty context"));
  }

  private static PartitionedFile simplePartitionedFile(
      Long baseRowId, Long defaultRowCommitVersion) {
    scala.collection.immutable.Map<String, Object> otherMetadata = Map$.MODULE$.empty();
    if (baseRowId != null) {
      otherMetadata = otherMetadata.$plus(new Tuple2<>(RowId$.MODULE$.BASE_ROW_ID(), baseRowId));
    }
    if (defaultRowCommitVersion != null) {
      otherMetadata =
          otherMetadata.$plus(
              new Tuple2<>(
                  DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME(),
                  defaultRowCommitVersion));
    }
    return new PartitionedFile(
        row(),
        SparkPath.fromUrlString("file:///tmp/metadata-test.parquet"),
        0L,
        1L,
        new String[0],
        0L,
        1L,
        otherMetadata);
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
