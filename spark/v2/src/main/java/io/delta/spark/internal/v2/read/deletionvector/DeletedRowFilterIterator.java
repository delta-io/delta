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

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;

/**
 * Utility to create deletion vector filtering iterator using Scala Iterator's filter() and map().
 *
 * <p>This implementation handles row-based reading only. For vectorized reading support, see PR3.
 */
public class DeletedRowFilterIterator {

  private DeletedRowFilterIterator() {}

  /**
   * Create a filtering iterator that removes deleted rows and projects out the DV column.
   *
   * <p>Uses Scala Iterator's filter() and map() for clean functional composition.
   */
  public static Iterator<InternalRow> create(
      Iterator<InternalRow> baseIter, int dvColumnIndex, int totalColumns, StructType inputSchema) {

    ProjectingInternalRow projection = buildProjection(inputSchema, dvColumnIndex);

    // filter: keep non-deleted rows (DV column == 0)
    // map: project out DV column
    return baseIter
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
}
