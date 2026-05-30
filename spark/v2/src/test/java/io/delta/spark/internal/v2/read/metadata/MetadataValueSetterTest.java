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

import static io.delta.spark.internal.v2.InternalRowTestUtils.row;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.paths.SparkPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.junit.jupiter.api.Test;
import scala.Tuple2;
import scala.collection.immutable.Map$;

/**
 * Unit tests for the three {@link MetadataValueSetterBuilder} implementations.
 *
 * <p>Each setter is tested independently of the larger schema-context wiring: bind to a synthetic
 * {@link PartitionedFile}, run {@link BoundMetadataValueSetter#setValue} against a known inner row,
 * and verify the metadata-row ordinal.
 */
public class MetadataValueSetterTest {

  // ---------------------------------------------------------------------------
  // FileConstantValueSetterBuilder
  // ---------------------------------------------------------------------------

  @Test
  public void testFileConstantValueSetterWritesFilePath() {
    FileConstantValueSetterBuilder setter =
        new FileConstantValueSetterBuilder(
            FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS().apply(FileFormat$.MODULE$.FILE_PATH()));

    PartitionedFile file = simplePartitionedFile();
    BoundMetadataValueSetter bound = setter.buildWithFile(file);

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    bound.setValue(metadataRow, 0, row(1L));

    assertEquals(
        FileFormat$.MODULE$
            .BASE_METADATA_EXTRACTORS()
            .apply(FileFormat$.MODULE$.FILE_PATH())
            .apply(file),
        metadataRow.get(0, null));
  }

  @Test
  public void testFileConstantValueSetterWritesFileSize() {
    FileConstantValueSetterBuilder setter =
        new FileConstantValueSetterBuilder(
            FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS().apply(FileFormat$.MODULE$.FILE_SIZE()));

    PartitionedFile file = partitionedFileWithSize(9999L);
    BoundMetadataValueSetter bound = setter.buildWithFile(file);

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    bound.setValue(metadataRow, 0, row(1L));

    assertEquals(9999L, metadataRow.getLong(0));
  }

  // ---------------------------------------------------------------------------
  // RowIdValueSetterBuilder
  // ---------------------------------------------------------------------------

  @Test
  public void testRowIdSetterUsesMaterializedValueWhenPresent() {
    // Inner row layout: [data | materialized_row_id | row_index]
    int materializedIdx = 1;
    int rowIndexIdx = 2;
    RowIdValueSetterBuilder setter = new RowIdValueSetterBuilder(materializedIdx, rowIndexIdx);

    BoundMetadataValueSetter bound = setter.buildWithFile(partitionedFileWithBaseRowId(50L));

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    InternalRow innerRow = row(1L, 101L, 0L);
    bound.setValue(metadataRow, 0, innerRow);

    assertEquals(101L, metadataRow.getLong(0), "materialized row_id should win when present");
  }

  @Test
  public void testRowIdSetterFallsBackToBaseRowIdPlusRowIndex() {
    int materializedIdx = 1;
    int rowIndexIdx = 2;
    RowIdValueSetterBuilder setter = new RowIdValueSetterBuilder(materializedIdx, rowIndexIdx);

    BoundMetadataValueSetter bound = setter.buildWithFile(partitionedFileWithBaseRowId(50L));

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    InternalRow innerRow = row(2L, null, 2L);
    bound.setValue(metadataRow, 0, innerRow);

    assertEquals(52L, metadataRow.getLong(0), "row_id should fall back to baseRowId + rowIndex");
  }

  // ---------------------------------------------------------------------------
  // RowCommitVersionValueSetterBuilder
  // ---------------------------------------------------------------------------

  @Test
  public void testRowCommitVersionSetterUsesMaterializedValueWhenPresent() {
    int materializedIdx = 1;
    RowCommitVersionValueSetterBuilder setter =
        new RowCommitVersionValueSetterBuilder(materializedIdx);

    BoundMetadataValueSetter bound =
        setter.buildWithFile(partitionedFileWithDefaultCommitVersion(9L));

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    InternalRow innerRow = row(1L, 7L);
    bound.setValue(metadataRow, 0, innerRow);

    assertEquals(7L, metadataRow.getLong(0));
  }

  @Test
  public void testRowCommitVersionSetterFallsBackToDefault() {
    int materializedIdx = 1;
    RowCommitVersionValueSetterBuilder setter =
        new RowCommitVersionValueSetterBuilder(materializedIdx);

    BoundMetadataValueSetter bound =
        setter.buildWithFile(partitionedFileWithDefaultCommitVersion(9L));

    GenericInternalRow metadataRow = new GenericInternalRow(1);
    InternalRow innerRow = row(2L, null);
    bound.setValue(metadataRow, 0, innerRow);

    assertEquals(9L, metadataRow.getLong(0));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static PartitionedFile simplePartitionedFile() {
    return new PartitionedFile(
        row(),
        SparkPath.fromUrlString("file:///tmp/setter-test.parquet"),
        0L,
        1L,
        new String[0],
        0L,
        1L,
        Map$.MODULE$.empty());
  }

  private static PartitionedFile partitionedFileWithSize(long size) {
    return new PartitionedFile(
        row(),
        SparkPath.fromUrlString("file:///tmp/setter-test.parquet"),
        0L,
        size,
        new String[0],
        0L,
        size,
        Map$.MODULE$.empty());
  }

  private static PartitionedFile partitionedFileWithBaseRowId(long baseRowId) {
    scala.collection.immutable.Map<String, Object> otherMetadata =
        Map$.MODULE$
            .<String, Object>empty()
            .$plus(new Tuple2<>(RowId$.MODULE$.BASE_ROW_ID(), (Object) baseRowId));
    return new PartitionedFile(
        row(),
        SparkPath.fromUrlString("file:///tmp/setter-test.parquet"),
        0L,
        1L,
        new String[0],
        0L,
        1L,
        otherMetadata);
  }

  private static PartitionedFile partitionedFileWithDefaultCommitVersion(long version) {
    scala.collection.immutable.Map<String, Object> otherMetadata =
        Map$.MODULE$
            .<String, Object>empty()
            .$plus(
                new Tuple2<>(
                    DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME(),
                    (Object) version));
    return new PartitionedFile(
        row(),
        SparkPath.fromUrlString("file:///tmp/setter-test.parquet"),
        0L,
        1L,
        new String[0],
        0L,
        1L,
        otherMetadata);
  }
}
