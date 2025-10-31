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
package io.delta.kernel.spark.utils;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static io.delta.kernel.internal.util.Preconditions.checkState;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.data.StructRow;
import java.util.Optional;
import org.apache.spark.annotation.Experimental;

/**
 * Helper class providing utilities for working with Delta table data in streaming scenarios.
 *
 * <p>This class provides static utility methods for extracting information from Delta table
 * batches, such as version numbers and data change actions.
 */
@Experimental
public class StreamingHelper {

  /**
   * Returns the index of the field with the given name in the schema of the batch. Throws an {@link
   * IllegalArgumentException} if the field is not found.
   */
  private static int getFieldIndex(ColumnarBatch batch, String fieldName) {
    int index = batch.getSchema().indexOf(fieldName);
    checkArgument(index >= 0, "Field '%s' not found in schema: %s", fieldName, batch.getSchema());
    return index;
  }

  /**
   * Get the version from a batch. Assumes all rows in the batch have the same version, so it reads
   * from the first row (rowId=0).
   */
  public static long getVersion(ColumnarBatch batch) {
    int versionColIdx = getFieldIndex(batch, "version");
    return batch.getColumnVector(versionColIdx).getLong(0);
  }

  /** Get AddFile action from a batch at the specified row, if present and has dataChange=true. */
  public static Optional<AddFile> getDataChangeAdd(ColumnarBatch batch, int rowId) {
    int addIdx = getFieldIndex(batch, "add");
    ColumnVector addVector = batch.getColumnVector(addIdx);
    if (addVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row addFileRow = StructRow.fromStructVector(addVector, rowId);
    checkState(
        addFileRow != null,
        String.format("Failed to extract AddFile struct from batch at rowId=%d.", rowId));

    AddFile addFile = new AddFile(addFileRow);
    return addFile.getDataChange() ? Optional.of(addFile) : Optional.empty();
  }

  /**
   * Get RemoveFile action from a batch at the specified row, if present and has dataChange=true.
   */
  public static Optional<RemoveFile> getDataChangeRemove(ColumnarBatch batch, int rowId) {
    int removeIdx = getFieldIndex(batch, "remove");
    ColumnVector removeVector = batch.getColumnVector(removeIdx);
    if (removeVector.isNullAt(rowId)) {
      return Optional.empty();
    }

    Row removeFileRow = StructRow.fromStructVector(removeVector, rowId);
    checkState(
        removeFileRow != null,
        String.format("Failed to extract RemoveFile struct from batch at rowId=%d.", rowId));

    RemoveFile removeFile = new RemoveFile(removeFileRow);
    return removeFile.getDataChange() ? Optional.of(removeFile) : Optional.empty();
  }

  /** Private constructor to prevent instantiation of this utility class. */
  private StreamingHelper() {}
}
