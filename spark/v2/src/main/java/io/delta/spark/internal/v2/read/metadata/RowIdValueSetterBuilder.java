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

import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.delta.RowId$;
import org.apache.spark.sql.execution.datasources.PartitionedFile;

/**
 * Builder for {@code _metadata.row_id}. Per-row coalesce against the materialised row-id helper
 * column: if present and non-null in the inner row, use it; otherwise fall back to {@code baseRowId
 * + physicalRowIndex}.
 */
public final class RowIdValueSetterBuilder implements MetadataValueSetterBuilder {

  private final int materializedRowIdIdx;
  private final int rowIndexIdx;

  public RowIdValueSetterBuilder(int materializedRowIdIdx, int rowIndexIdx) {
    this.materializedRowIdIdx = materializedRowIdIdx;
    this.rowIndexIdx = rowIndexIdx;
  }

  @Override
  public BoundMetadataValueSetter buildWithFile(PartitionedFile file) {
    // base_row_id is required whenever row tracking is enabled on the file; if it's not present
    // (would yield null via Scala's `apply` throwing NoSuchElementException, or returning null
    // from a Java-shaped map), fail fast with an actionable error rather than NPE on the
    // subsequent .longValue() call.
    Object rawBaseRowId =
        file.otherConstantMetadataColumnValues()
            .get(RowId$.MODULE$.BASE_ROW_ID())
            .getOrElse(() -> null);
    if (rawBaseRowId == null) {
      throw new IllegalStateException(
          "Missing '"
              + RowId$.MODULE$.BASE_ROW_ID()
              + "' on PartitionedFile '"
              + file.filePath()
              + "' for row tracking — every Delta file with row tracking "
              + "enabled must carry a base_row_id constant.");
    }
    long baseRowId = ((Number) rawBaseRowId).longValue();
    return new Bound(baseRowId, materializedRowIdIdx, rowIndexIdx);
  }

  private static final class Bound implements BoundMetadataValueSetter, Serializable {
    private final long baseRowId;
    private final int materializedRowIdIdx;
    private final int rowIndexIdx;

    Bound(long baseRowId, int materializedRowIdIdx, int rowIndexIdx) {
      this.baseRowId = baseRowId;
      this.materializedRowIdIdx = materializedRowIdIdx;
      this.rowIndexIdx = rowIndexIdx;
    }

    @Override
    public void setValue(GenericInternalRow metadataRow, int ordinal, InternalRow innerRow) {
      long rowId;
      // Materialized row IDs are written only for rows rewritten by UPDATE/MERGE; for plain
      // INSERTs the column is null and we fall back to baseRowId + physicalRowIndex. We don't
      // need to guard against the column being absent from the parquet file — the reader produces
      // nulls for any requested column missing from the file, which is the same contract V1's
      // Coalesce in GenerateRowIDs.scala relies on.
      if (innerRow.isNullAt(materializedRowIdIdx)) {
        rowId = baseRowId + innerRow.getLong(rowIndexIdx);
      } else {
        rowId = innerRow.getLong(materializedRowIdIdx);
      }
      metadataRow.setLong(ordinal, rowId);
    }
  }
}
