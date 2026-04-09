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

import org.apache.spark.sql.delta.RowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Seq;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn;


/**
 * Schema context for row tracking in the V2 connector.
 *
 * <p>Parses requested {@code _metadata} row-tracking fields from the read schema, augments the
 * physical read schema with the required helper columns, and pre-computes indices/projections used
 * by {@link RowTrackingReadFunction}.
 *
 * <p>Helper columns are added only for requested fields:
 * <ul>
 *   <li>{@code row_id}: materialized row ID + temporary row-index column
 *   <li>{@code row_commit_version}: materialized row-commit-version column
 * </ul>
 */
public class RowTrackingSchemaContext implements Serializable {

  private static final String METADATA_COLUMN_NAME = FileFormat$.MODULE$.METADATA_NAME();
  private static final String ROW_ID_METADATA_FIELD_NAME = RowId$.MODULE$.ROW_ID();
  private static final String ROW_COMMIT_VERSION_METADATA_FIELD_NAME =
      RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME();

  private StructType schemaWithRowTrackingColumns;
  private int materializedRowIdIndex = -1;
  private int materializedRowCommitVersionIndex = -1;
  private int rowIndexColumnIndex = -1;
  private StructType dataSchema;
  private Seq<Object> dataColumnsOrdinals;
  private StructType partitionSchema;
  private Seq<Object> partitionColumnsOrdinals;

  public RowTrackingSchemaContext(
      StructType readDataSchema, Metadata metadata, StructType partitionSchema) {
    StructField metadataColumn =
      Arrays.stream(readDataSchema.fields())
        .filter(field -> METADATA_COLUMN_NAME.equals(field.name()))
        .findFirst()
        .orElse(null);
    if (metadataColumn == null || !(metadataColumn.dataType() instanceof StructType metadataType)) {
      return;
    }

    StructType baseSchemaWithoutMetadata = new StructType(
        Arrays.stream(readDataSchema.fields())
            .filter(f -> !METADATA_COLUMN_NAME.equals(f.name()))
            .toArray(StructField[]::new));

    this.schemaWithRowTrackingColumns = baseSchemaWithoutMetadata;

    int internalColumnsStartIndex = baseSchemaWithoutMetadata.fields().length;
    int index = internalColumnsStartIndex;
    int internalColumnsCount = 0;

    boolean rowIdRequested = containsRowTrackingMetadataField(metadataType, ROW_ID_METADATA_FIELD_NAME);
    boolean rowCommitVersionRequested =
        containsRowTrackingMetadataField(metadataType, ROW_COMMIT_VERSION_METADATA_FIELD_NAME);
    
    if (rowIdRequested) {
      String rowIdColumnName = MaterializedRowTrackingColumn.MATERIALIZED_ROW_ID.getPhysicalColumnName(
          metadata.getConfiguration());
      schemaWithRowTrackingColumns = schemaWithRowTrackingColumns.add(rowIdColumnName, DataTypes.LongType, true);
      materializedRowIdIndex = index++;
      internalColumnsCount++;

      schemaWithRowTrackingColumns = schemaWithRowTrackingColumns
          .add(ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(), DataTypes.LongType, true);
      rowIndexColumnIndex = index++;
      internalColumnsCount++;
    }

    if (rowCommitVersionRequested) {
      String rowCommitVersionColumnName = MaterializedRowTrackingColumn.MATERIALIZED_ROW_COMMIT_VERSION
          .getPhysicalColumnName(
              metadata.getConfiguration());
      schemaWithRowTrackingColumns = schemaWithRowTrackingColumns.add(rowCommitVersionColumnName, DataTypes.LongType,
          true);
      materializedRowCommitVersionIndex = index++;
      internalColumnsCount++;
    }
    
    this.dataSchema = baseSchemaWithoutMetadata;
    this.dataColumnsOrdinals = buildRangeOrdinals(0, internalColumnsStartIndex);
    this.partitionSchema = partitionSchema;
    this.partitionColumnsOrdinals = buildRangeOrdinals(internalColumnsStartIndex + internalColumnsCount, schemaWithRowTrackingColumns.fields().length + partitionSchema.fields().length);
  }
  
  public StructType getSchemaWithRowTrackingColumns() {
    return schemaWithRowTrackingColumns;
  }
  
  public int getMaterializedRowIdIndex() {
    return materializedRowIdIndex;
  }

  public int getMaterializedRowCommitVersionIndex() {
    return materializedRowCommitVersionIndex;
  }

  public int getRowIndexColumnIndex() {
    return rowIndexColumnIndex;
  }

  public boolean isRowIdRequested() {
    return materializedRowIdIndex != -1;
  }

  public boolean isRowCommitVersionRequested() {
    return materializedRowCommitVersionIndex != -1;
  }

  public boolean areRowTrackingMetadataFieldsRequested() {
    return isRowIdRequested() || isRowCommitVersionRequested();
  }

  private static Seq<Object> buildRangeOrdinals(int startInclusive, int endExclusive) {
    int len = Math.max(0, endExclusive - startInclusive);
    int[] ordinals = new int[len];
    for (int i = 0; i < len; i++) {
      ordinals[i] = startInclusive + i;
    }
    return scala.Predef.wrapIntArray(ordinals).toList();
  }

  public StructType getDataSchema() {
    return dataSchema;
  }

  public Seq<Object> getDataColumnsOrdinals() {
    return dataColumnsOrdinals;
  }

  public StructType getPartitionSchema() {
    return partitionSchema;
  }

  public Seq<Object> getPartitionColumnsOrdinals() {
    return partitionColumnsOrdinals; 
  }

  public boolean hasPartitionColumns() { 
    return partitionSchema.fields().length > 0;
  }

  private static boolean containsRowTrackingMetadataField(StructType metadataType, String metadataFieldName) {
    return Arrays.stream(metadataType.fields())
        .anyMatch(field -> metadataFieldName.equals(field.name()));
  }
}
