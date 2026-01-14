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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/**
 * Wraps a base reader function to apply deletion vector filtering.
 *
 * <p>Returns a {@link DeletionVectorFilterIterator} that filters deleted rows and removes the DV
 * column from output. Supports both ColumnarBatch and InternalRow.
 */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final int dvColumnIndex;
  private final int totalColumns;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      int dvColumnIndex,
      int totalColumns) {
    this.baseReadFunc = baseReadFunc;
    this.dvColumnIndex = dvColumnIndex;
    this.totalColumns = totalColumns;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<InternalRow> apply(PartitionedFile file) {
    return (Iterator<InternalRow>)
        (Iterator<?>)
            new DeletionVectorFilterIterator(baseReadFunc.apply(file), dvColumnIndex, totalColumns);
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      int dvColumnIndex,
      int totalColumns) {
    return new DeletionVectorReadFunction(baseReadFunc, dvColumnIndex, totalColumns);
  }
}
