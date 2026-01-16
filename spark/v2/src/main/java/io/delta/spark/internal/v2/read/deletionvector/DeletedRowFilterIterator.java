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

import java.io.Closeable;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.spark.sql.catalyst.InternalRow;
import scala.collection.Iterator;

/**
 * Iterator that filters deleted rows and projects out the DV column.
 *
 * <p>Uses {@link ProjectedInternalRow} to avoid data copy (inspired by Iceberg).
 *
 * <p>This implementation handles row-based reading only. For vectorized reading support, see PR3.
 */
public class DeletedRowFilterIterator implements Iterator<InternalRow>, Closeable {

  private final Iterator<InternalRow> baseIter;
  private final int dvColumnIndex;
  private InternalRow nextRow = null;

  public DeletedRowFilterIterator(
      Iterator<InternalRow> baseIter, int dvColumnIndex, int totalColumns) {
    this.baseIter = baseIter;
    this.dvColumnIndex = dvColumnIndex;
    // totalColumns not used in row-based filtering; kept for API consistency with PR3
  }

  @Override
  public void close() throws IOException {
    if (baseIter instanceof Closeable) {
      ((Closeable) baseIter).close();
    }
  }

  @Override
  public boolean hasNext() {
    while (nextRow == null && baseIter.hasNext()) {
      InternalRow row = baseIter.next();
      if (row.getByte(dvColumnIndex) == 0) {
        nextRow = projectRow(row);
        return true;
      }
    }
    return nextRow != null;
  }

  @Override
  public InternalRow next() {
    if (nextRow == null && !hasNext()) {
      throw new NoSuchElementException();
    }
    InternalRow result = nextRow;
    nextRow = null;
    return result;
  }

  /** Project out DV column from row without copying data. */
  private InternalRow projectRow(InternalRow row) {
    return new ProjectedInternalRow(row, dvColumnIndex);
  }
}
