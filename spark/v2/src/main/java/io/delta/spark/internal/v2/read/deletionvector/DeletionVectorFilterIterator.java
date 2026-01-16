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
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import scala.collection.Iterator;

/**
 * Iterator that filters deleted rows and projects out the DV column.
 *
 * <p>This implementation handles row-based reading only. For vectorized reading support, see PR3.
 */
public class DeletionVectorFilterIterator implements Iterator<InternalRow>, Closeable {

  private final Iterator<InternalRow> baseIter;
  private final int dvColumnIndex;
  private final int outputColumnCount;
  private InternalRow nextRow = null;

  public DeletionVectorFilterIterator(
      Iterator<InternalRow> baseIter, int dvColumnIndex, int totalColumns) {
    this.baseIter = baseIter;
    this.dvColumnIndex = dvColumnIndex;
    this.outputColumnCount = totalColumns - 1;
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

  /**
   * Project out DV column from row.
   *
   * <p>Note: row.get(i, null) with null dataType returns the raw object without type conversion.
   * This is safe here since we're just copying values to a new row.
   */
  private InternalRow projectRow(InternalRow row) {
    Object[] values = new Object[outputColumnCount];
    int outIdx = 0;
    for (int i = 0; i <= outputColumnCount; i++) {
      if (i != dvColumnIndex) {
        values[outIdx++] = row.get(i, null);
      }
    }
    return new GenericInternalRow(values);
  }
}
