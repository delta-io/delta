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
package io.delta.spark.internal.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;

/** Test helper utilities for InternalRow operations. */
public class InternalRowTestUtils {

  private InternalRowTestUtils() {}

  /** Create a mock base reader that returns the given rows. */
  public static Function1<PartitionedFile, Iterator<InternalRow>> mockReader(
      List<InternalRow> rows) {
    return new AbstractFunction1<PartitionedFile, Iterator<InternalRow>>() {
      @Override
      public Iterator<InternalRow> apply(PartitionedFile file) {
        return CollectionConverters.asScala(rows.iterator());
      }
    };
  }

  /**
   * Create a mock reader that returns ColumnarBatch objects through Iterator&lt;InternalRow&gt;.
   *
   * <p>Mimics Spark vectorized mode where ColumnarBatch is passed via type erasure.
   */
  @SuppressWarnings("unchecked")
  public static Function1<PartitionedFile, Iterator<InternalRow>> mockBatchReader(
      List<ColumnarBatch> batches) {
    return new AbstractFunction1<PartitionedFile, Iterator<InternalRow>>() {
      @Override
      public Iterator<InternalRow> apply(PartitionedFile file) {
        return (Iterator<InternalRow>)
            (Iterator<?>) CollectionConverters.asScala(batches.iterator());
      }
    };
  }

  /** Collect all rows from iterator into a list, copying each row. */
  public static List<InternalRow> collectRows(Iterator<InternalRow> iter) {
    List<InternalRow> result = new ArrayList<>();
    while (iter.hasNext()) {
      result.add(iter.next().copy());
    }
    return result;
  }

  /** Collect all ColumnarBatch objects from an iterator (for vectorized mode testing). */
  @SuppressWarnings("unchecked")
  public static List<ColumnarBatch> collectBatches(Iterator<InternalRow> iter) {
    Iterator<Object> objectIter = (Iterator<Object>) (Iterator<?>) iter;
    List<ColumnarBatch> result = new ArrayList<>();
    while (objectIter.hasNext()) {
      result.add((ColumnarBatch) objectIter.next());
    }
    return result;
  }

  /** Create an InternalRow from values. Strings are converted to UTF8String. */
  public static InternalRow row(Object... values) {
    Object[] converted = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      converted[i] =
          values[i] instanceof String ? UTF8String.fromString((String) values[i]) : values[i];
    }
    return new GenericInternalRow(converted);
  }

  /** Assert that actual InternalRows match expected rows. Converts to Row for comparison. */
  public static void assertRowsEquals(List<InternalRow> actual, List<InternalRow> expected) {
    assertEquals(toRows(expected), toRows(actual));
  }

  /** Convert InternalRows to Rows, converting UTF8String back to String. */
  private static List<Row> toRows(List<InternalRow> rows) {
    return rows.stream()
        .map(
            r ->
                RowFactory.create(
                    IntStream.range(0, r.numFields())
                        .mapToObj(
                            i ->
                                r.get(i, null) instanceof UTF8String
                                    ? r.get(i, null).toString()
                                    : r.get(i, null))
                        .toArray()))
        .collect(Collectors.toList());
  }
}
