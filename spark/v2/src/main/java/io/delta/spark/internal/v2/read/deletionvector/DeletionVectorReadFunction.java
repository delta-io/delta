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

import java.io.Serializable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;

/** Extends original reader function for handling dv related business logic. */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final DvSchemaContext dvSchemaContext;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      DvSchemaContext dvSchemaContext) {
    this.baseReadFunc = baseReadFunc;
    this.dvSchemaContext = dvSchemaContext;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    int dvColumnIndex = dvSchemaContext.getDvColumnIndex();
    // Use pre-computed ordinals from DvSchemaContext.
    ProjectingInternalRow projection =
        ProjectingInternalRow.apply(
            dvSchemaContext.getOutputSchema(), dvSchemaContext.getOutputColumnOrdinals());

    // Use Scala's filter/map since Spark returns scala.collection.Iterator (not
    // java.util.Iterator).
    // filter: keep non-deleted rows (DV column == 0)
    // map: project out DV column using Spark's ProjectingInternalRow
    return baseReadFunc
        .apply(file)
        .filter(
            // Must use Object: Java generics are invariant, so AbstractFunction1<_, Boolean>
            // cannot be passed to Scala's filter() which expects Function1[_, Boolean] (erased to
            // Object)
            new AbstractFunction1<InternalRow, Object>() {
              @Override
              public Object apply(InternalRow row) {
                return row.getByte(dvColumnIndex) == 0;
              }
            })
        .map(
            new AbstractFunction1<InternalRow, InternalRow>() {
              @Override
              public InternalRow apply(InternalRow row) {
                projection.project(row);
                return projection;
              }
            });
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      DvSchemaContext dvSchemaContext) {
    return new DeletionVectorReadFunction(baseReadFunc, dvSchemaContext);
  }
}
