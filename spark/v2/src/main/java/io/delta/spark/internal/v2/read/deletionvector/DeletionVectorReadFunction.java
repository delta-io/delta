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
package io.delta.spark.internal.v2.read.deletionvector;

import io.delta.spark.internal.v2.utils.CloseableIterator;
import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * Wraps a Parquet reader function to apply deletion vector filtering.
 *
 * <p>This function:
 *
 * <ol>
 *   <li>Reads rows from the base Parquet reader (which includes the is_row_deleted column)
 *   <li>Filters out deleted rows (where is_row_deleted != 0)
 *   <li>Projects out the is_row_deleted column from the output
 * </ol>
 *
 * <p>The returned iterator implements {@link java.io.Closeable} to ensure proper resource cleanup
 * of the underlying Parquet reader, even when the iterator is not fully consumed.
 */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Byte value in the DV column indicating the row is NOT deleted (row should be kept). */
  private static final byte ROW_NOT_DELETED = 0;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final DeletionVectorSchemaContext dvSchemaContext;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      DeletionVectorSchemaContext dvSchemaContext) {
    this.baseReadFunc = baseReadFunc;
    this.dvSchemaContext = dvSchemaContext;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    int dvColumnIndex = dvSchemaContext.getDvColumnIndex();
    // Use pre-computed ordinals from DeletionVectorSchemaContext.
    ProjectingInternalRow projection =
        ProjectingInternalRow.apply(
            dvSchemaContext.getOutputSchema(), dvSchemaContext.getOutputColumnOrdinals());

    // Wrap the base iterator as CloseableIterator to preserve close() through filter/map.
    // This ensures proper resource cleanup even when the iterator is not fully consumed.
    Iterator<InternalRow> baseIterator = baseReadFunc.apply(file);

    return CloseableIterator.wrap(baseIterator)
        .filterCloseable(row -> row.getByte(dvColumnIndex) == ROW_NOT_DELETED)
        .mapCloseable(
            row -> {
              projection.project(row);
              return (InternalRow) projection;
            });
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      DeletionVectorSchemaContext dvSchemaContext) {
    return new DeletionVectorReadFunction(baseReadFunc, dvSchemaContext);
  }
}
