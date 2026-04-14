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

import static io.delta.spark.internal.v2.InternalRowTestUtils.collectRows;
import static io.delta.spark.internal.v2.InternalRowTestUtils.mockReader;
import static io.delta.spark.internal.v2.InternalRowTestUtils.row;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.collection.immutable.Map$;

public class RowTrackingReadFunctionTest {

  private static final String MATERIALIZED_ROW_ID_COLUMN = "__row_id";
  private static final String MATERIALIZED_ROW_COMMIT_VERSION_COLUMN = "__row_commit_version";
  private static final StructType EMPTY_PARTITION_SCHEMA = new StructType();

  @Test
  public void testRowIdProjectionUsesMaterializedRowIdWhenPresent() {
    RowTrackingSchemaContext context =
        createContext(readSchemaWithMetadata(RowId$.MODULE$.ROW_ID()), EMPTY_PARTITION_SCHEMA);

    List<InternalRow> inputRows = List.of(row(1L, 101L, 0L));
    RowTrackingReadFunction readFunction =
        RowTrackingReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunction.apply(createPartitionedFile(50L, null)));

    assertEquals(1, result.size());
    assertRowIdResult(result.get(0), 1L, 101L);
  }

  @Test
  public void testRowIdProjectionFallsBackToBaseRowIdPlusPhysicalRowIndex() {
    RowTrackingSchemaContext context =
        createContext(readSchemaWithMetadata(RowId$.MODULE$.ROW_ID()), EMPTY_PARTITION_SCHEMA);

    List<InternalRow> inputRows = List.of(row(2L, null, 2L));
    RowTrackingReadFunction readFunction =
        RowTrackingReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunction.apply(createPartitionedFile(50L, null)));

    assertEquals(1, result.size());
    assertRowIdResult(result.get(0), 2L, 52L);
  }

  @Test
  public void testRowCommitVersionProjectionUsesMaterializedCommitVersionWhenPresent() {
    RowTrackingSchemaContext context =
        createContext(
            readSchemaWithMetadata(RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME()),
            EMPTY_PARTITION_SCHEMA);

    List<InternalRow> inputRows = List.of(row(1L, 7L));
    RowTrackingReadFunction readFunction =
        RowTrackingReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunction.apply(createPartitionedFile(null, 9L)));

    assertEquals(1, result.size());
    assertCommitVersionResult(result.get(0), 1L, 7L);
  }

  @Test
  public void testRowCommitVersionProjectionFallsBackToDefaultCommitVersion() {
    RowTrackingSchemaContext context =
        createContext(
            readSchemaWithMetadata(RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME()),
            EMPTY_PARTITION_SCHEMA);

    List<InternalRow> inputRows = List.of(row(2L, null));
    RowTrackingReadFunction readFunction =
        RowTrackingReadFunction.wrap(mockReader(inputRows), context);

    List<InternalRow> result = collectRows(readFunction.apply(createPartitionedFile(null, 9L)));

    assertEquals(1, result.size());
    assertCommitVersionResult(result.get(0), 2L, 9L);
  }

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

  private static RowTrackingSchemaContext createContext(
      StructType readSchema, StructType partitionSchema) {
    return new RowTrackingSchemaContext(readSchema, createMetadata(), partitionSchema);
  }

  private static StructType readSchemaWithMetadata(String... metadataFieldNames) {
    StructType metadataSchema = new StructType();
    for (String metadataFieldName : metadataFieldNames) {
      metadataSchema = metadataSchema.add(metadataFieldName, DataTypes.LongType, true);
    }
    return new StructType()
        .add("id", DataTypes.LongType, false)
        .add(FileFormat$.MODULE$.METADATA_NAME(), metadataSchema, false);
  }

  private static PartitionedFile createPartitionedFile(
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
        SparkPath.fromUrlString("file:///tmp/row-tracking-test.parquet"),
        0L,
        1L,
        new String[0],
        0L,
        1L,
        otherMetadata);
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
