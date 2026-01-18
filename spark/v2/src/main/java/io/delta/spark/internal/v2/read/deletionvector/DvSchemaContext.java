/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.delta.DeltaParquetFileFormat;
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Schema context for deletion vector processing in the V2 connector.
 *
 * <p>Encapsulates schema with DV column and pre-computed indices needed for DV filtering.
 */
public class DvSchemaContext implements Serializable {

  private static final long serialVersionUID = 1L;

  private final StructType schemaWithDvColumn;
  private final int dvColumnIndex;
  private final int inputColumnCount;
  private final StructType outputSchema;
  private final int[] excludedColumnIndices;
  private final List<Integer> outputColumnOrdinals;

  /**
   * Create a DV schema context.
   *
   * @param readDataSchema original data schema without DV column
   * @param partitionSchema partition columns schema
   * @param useMetadataRowIndex whether to use _metadata.row_index for DV filtering
   */
  public DvSchemaContext(
      StructType readDataSchema, StructType partitionSchema, boolean useMetadataRowIndex) {
    // Build schema: add row_index column (if using metadata) then is_row_deleted column
    StructType schemaBuilder = readDataSchema;
    int rowIndexColumnIndex = -1;
    if (useMetadataRowIndex) {
      // Add temporary row index column that Parquet reader will populate from _metadata.row_index
      rowIndexColumnIndex = schemaBuilder.fields().length;
      schemaBuilder =
          schemaBuilder.add(
              ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME(), DataTypes.LongType);
    }
    this.schemaWithDvColumn =
        schemaBuilder.add(DeltaParquetFileFormat.IS_ROW_DELETED_STRUCT_FIELD());
    this.dvColumnIndex =
        schemaWithDvColumn.fieldIndex(DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME());
    this.inputColumnCount = schemaWithDvColumn.fields().length + partitionSchema.fields().length;
    this.outputSchema = readDataSchema.merge(partitionSchema, /* handleDuplicateColumns= */ false);

    // Columns to exclude from projection: row_index (if present) and is_row_deleted
    if (useMetadataRowIndex) {
      this.excludedColumnIndices = new int[] {rowIndexColumnIndex, dvColumnIndex};
    } else {
      this.excludedColumnIndices = new int[] {dvColumnIndex};
    }

    // Pre-compute output column ordinals: all indices except excluded columns
    Set<Integer> excludeSet = new HashSet<>();
    for (int idx : excludedColumnIndices) {
      excludeSet.add(idx);
    }
    List<Integer> ordinals = new ArrayList<>(inputColumnCount - excludedColumnIndices.length);
    for (int i = 0; i < inputColumnCount; i++) {
      if (!excludeSet.contains(i)) {
        ordinals.add(i);
      }
    }
    this.outputColumnOrdinals = ordinals;
  }

  /** Returns schema with the __delta_internal_is_row_deleted column added. */
  public StructType getSchemaWithDvColumn() {
    return schemaWithDvColumn;
  }

  public int getDvColumnIndex() {
    return dvColumnIndex;
  }

  public int getInputColumnCount() {
    return inputColumnCount;
  }

  public StructType getOutputSchema() {
    return outputSchema;
  }

  /** Returns indices of columns to exclude from output (row_index if present, is_row_deleted). */
  public int[] getExcludedColumnIndices() {
    return excludedColumnIndices;
  }

  /** Returns pre-computed output column ordinals (indices of columns to include in output). */
  public List<Integer> getOutputColumnOrdinals() {
    return outputColumnOrdinals;
  }
}
