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
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.ProjectingInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Iterator that filters deleted rows and projects out the DV column.
 *
 * <p>Uses Spark's {@link ProjectingInternalRow} to avoid data copy.
 *
 * <p>This implementation handles row-based reading only. For vectorized reading support, see PR3.
 */
public class DeletedRowFilterIterator implements Iterator<InternalRow>, Closeable {

  private final Iterator<InternalRow> baseIter;
  private final int dvColumnIndex;
  private final ProjectingInternalRow projection;
  private InternalRow nextRow = null;

  public DeletedRowFilterIterator(
      Iterator<InternalRow> baseIter, int dvColumnIndex, int totalColumns, StructType inputSchema) {
    this.baseIter = baseIter;
    this.dvColumnIndex = dvColumnIndex;
    this.projection = buildProjection(inputSchema, dvColumnIndex);
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
        projection.project(row);
        nextRow = projection;
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
}
