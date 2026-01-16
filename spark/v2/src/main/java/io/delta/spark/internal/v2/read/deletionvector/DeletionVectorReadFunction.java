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
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;

/**
 * Wraps a base reader function to apply deletion vector filtering.
 *
 * <p>Filters deleted rows using Scala Iterator's filter() and projects out the DV column using
 * map() with Spark's ProjectingInternalRow.
 *
 * <p>This implementation handles row-based reading only. For vectorized reading support, see PR3.
 */
public class DeletionVectorReadFunction
    extends AbstractFunction1<PartitionedFile, Iterator<InternalRow>> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc;
  private final int dvColumnIndex;
  private final StructType inputSchema;

  private DeletionVectorReadFunction(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      int dvColumnIndex,
      StructType inputSchema) {
    this.baseReadFunc = baseReadFunc;
    this.dvColumnIndex = dvColumnIndex;
    this.inputSchema = inputSchema;
  }

  @Override
  public Iterator<InternalRow> apply(PartitionedFile file) {
    ProjectingInternalRow projection = buildProjection(inputSchema, dvColumnIndex);

    // filter: keep non-deleted rows (DV column == 0)
    // map: project out DV column using Spark's ProjectingInternalRow
    return baseReadFunc
        .apply(file)
        .filter(
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

  /** Build ProjectingInternalRow that skips the DV column. */
  private static ProjectingInternalRow buildProjection(StructType inputSchema, int excludeIndex) {
    List<StructField> outputFields = new ArrayList<>();
    List<Integer> colOrdinals = new ArrayList<>();

    for (int i = 0; i < inputSchema.fields().length; i++) {
      if (i != excludeIndex) {
        outputFields.add(inputSchema.fields()[i]);
        colOrdinals.add(i);
      }
    }

    StructType outputSchema = new StructType(outputFields.toArray(new StructField[0]));
    return ProjectingInternalRow.apply(
        outputSchema, CollectionConverters.asScala(colOrdinals).toSeq());
  }

  /** Factory method to wrap a reader function with DV filtering. */
  public static DeletionVectorReadFunction wrap(
      Function1<PartitionedFile, Iterator<InternalRow>> baseReadFunc,
      int dvColumnIndex,
      StructType inputSchema) {
    return new DeletionVectorReadFunction(baseReadFunc, dvColumnIndex, inputSchema);
  }
}
