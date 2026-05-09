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

import io.delta.spark.internal.v2.utils.CloseableIterator;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.delta.DefaultRowCommitVersion$;
import org.apache.spark.sql.delta.RowCommitVersion$;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.FileFormat$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructField;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * Read-function decorator that appends metadata values under the DSv2 {@code _metadata} struct.
 *
 * <p>This wrapper consumes rows that include internal helper columns introduced by {@link
 * RowTrackingSchemaContext}, computes requested metadata fields, and returns rows in logical output
 * order: {@code data columns + _metadata + partition columns}.
 */
public class RowTrackingReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {
  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final RowTrackingSchemaContext rowTrackingSchemaContext;

  private RowTrackingReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      RowTrackingSchemaContext rowTrackingSchemaContext) {
    this.baseReadFunc = baseReadFunc;
    this.rowTrackingSchemaContext = rowTrackingSchemaContext;
  }

  /**
   * Produces rows with a single {@code _metadata} struct column that contains requested metadata
   * values.
   *
   * <p>For each row, computes {@code row_id} as {@code COALESCE(materialized_row_id, base_row_id +
   * physical_row_index)} and computes {@code row_commit_version} as {@code
   * COALESCE(materialized_row_commit_version, default_row_commit_version)}.
   */
  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    final long baseRowId;
    if (rowTrackingSchemaContext.isRowIdRequested()) {
      baseRowId =
          ((Number) file.otherConstantMetadataColumnValues().apply(RowId$.MODULE$.BASE_ROW_ID()))
              .longValue();
    } else {
      baseRowId = 0L;
    }

    final long commitVersionId;
    if (rowTrackingSchemaContext.isRowCommitVersionRequested()) {
      commitVersionId =
          ((Number)
                  file.otherConstantMetadataColumnValues()
                      .apply(DefaultRowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME()))
              .longValue();
    } else {
      commitVersionId = 0L;
    }

    Iterator<InternalRow> baseIterator = baseReadFunc.apply(file);

    GenericInternalRow metadataStruct = new GenericInternalRow(1);
    GenericInternalRow metadataFields =
        new GenericInternalRow(rowTrackingSchemaContext.getMetadataSchema().fields().length);
    populateFileMetadataFields(file, metadataFields);

    ProjectingInternalRow dataProjection =
        ProjectingInternalRow.apply(
            rowTrackingSchemaContext.getDataSchema(),
            rowTrackingSchemaContext.getDataColumnsOrdinals());
    ProjectingInternalRow partitionProjection =
        ProjectingInternalRow.apply(
            rowTrackingSchemaContext.getPartitionSchema(),
            rowTrackingSchemaContext.getPartitionColumnsOrdinals());
    JoinedRow joinedDataAndMetadata = new JoinedRow();
    JoinedRow joinedWithPartitions = new JoinedRow();

    return CloseableIterator.wrap(baseIterator)
        .mapCloseable(
            row -> {
              populateRowTrackingMetadataFields(row, metadataFields, baseRowId, commitVersionId);
              dataProjection.project(row);
              partitionProjection.project(row);
              metadataStruct.update(0, metadataFields.copy());

              // Partition columns are appended after data columns in readSchema, so insert
              // `_metadata` between projected data columns and partition columns to preserve
              // output column order.
              // Assuming that metadata column is always inserted after all data columns in
              // readSchema.
              // Needs to be revisited if the _metadata struct position can be arbitrary.
              InternalRow dataWithMetadata =
                  (InternalRow) joinedDataAndMetadata.apply(dataProjection, metadataStruct);
              if (rowTrackingSchemaContext.hasPartitionColumns()) {
                return (InternalRow)
                    joinedWithPartitions.apply(dataWithMetadata, partitionProjection);
              }
              return dataWithMetadata;
            });
  }

  private void populateRowTrackingMetadataFields(
      InternalRow row, GenericInternalRow metadataFields, long baseRowId, long commitVersionId) {
    StructField[] fields = rowTrackingSchemaContext.getMetadataSchema().fields();
    for (int i = 0; i < fields.length; i++) {
      String name = fields[i].name();
      if (RowId$.MODULE$.ROW_ID().equals(name)) {
        int materializedRowIdIndex = rowTrackingSchemaContext.getMaterializedRowIdIndex();
        int rowIndexColumnIndex = rowTrackingSchemaContext.getRowIndexColumnIndex();
        long physicalRowIndex = row.getLong(rowIndexColumnIndex);
        // When reading tables with f.e. mixed file history, the materialized RowIds can be absent
        // so materializedRowIdIndex can be beyond the row's width. Treat this case like null and
        // fall back to baseRowId + physicalRowIndex.
        long rowId =
            (row.numFields() <= materializedRowIdIndex || row.isNullAt(materializedRowIdIndex))
                ? baseRowId + physicalRowIndex
                : row.getLong(materializedRowIdIndex);
        metadataFields.setLong(i, rowId);
      } else if (RowCommitVersion$.MODULE$.METADATA_STRUCT_FIELD_NAME().equals(name)) {
        int materializedCommitVersionIndex =
            rowTrackingSchemaContext.getMaterializedRowCommitVersionIndex();
        long rowCommitVersion =
            row.isNullAt(materializedCommitVersionIndex)
                ? commitVersionId
                : row.getLong(materializedCommitVersionIndex);
        metadataFields.setLong(i, rowCommitVersion);
      }
    }
  }

  private void populateFileMetadataFields(PartitionedFile file, GenericInternalRow metadataFields) {
    StructField[] fields = rowTrackingSchemaContext.getMetadataSchema().fields();
    for (int i = 0; i < fields.length; i++) {
      String name = fields[i].name();
      if (FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS().contains(name)) {
        Literal literal =
            FileFormat$.MODULE$.getFileConstantMetadataColumnValue(
                name, file, FileFormat$.MODULE$.BASE_METADATA_EXTRACTORS());
        if (literal.value() == null) {
          metadataFields.setNullAt(i);
        } else {
          metadataFields.update(i, literal.value());
        }
      }
    }
  }

  /** Creates a row-tracking read-function wrapper around a base Parquet read function. */
  public static RowTrackingReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      RowTrackingSchemaContext context) {
    return new RowTrackingReadFunction(baseReadFunc, context);
  }
}
