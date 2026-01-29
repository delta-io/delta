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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Function1;
import scala.collection.Iterator;
import scala.jdk.javaapi.CollectionConverters;
import scala.runtime.AbstractFunction1;

/** Test helper utilities for InternalRow operations. */
public class TestHelper {

  private TestHelper() {}

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

  /** Collect all rows from iterator into a list, copying each row. */
  public static List<InternalRow> collectRows(Iterator<InternalRow> iter) {
    List<InternalRow> result = new ArrayList<>();
    while (iter.hasNext()) {
      result.add(iter.next().copy());
    }
    return result;
  }

  /** Assert that actual InternalRows match expected rows. */
  public static void assertRowsEquals(List<InternalRow> actual, List<List<Object>> expected) {
    List<InternalRow> expectedRows =
        expected.stream().map(TestHelper::toInternalRow).collect(Collectors.toList());
    assertEquals(expectedRows.size(), actual.size(), "Row count mismatch");
    for (int i = 0; i < actual.size(); i++) {
      assertRowEquals(actual.get(i), expectedRows.get(i), i);
    }
  }

  /** Convert List<Object> to InternalRow. Strings are converted to UTF8String. */
  private static InternalRow toInternalRow(List<Object> values) {
    Object[] converted = new Object[values.size()];
    for (int i = 0; i < values.size(); i++) {
      Object val = values.get(i);
      converted[i] = val instanceof String ? UTF8String.fromString((String) val) : val;
    }
    return new GenericInternalRow(converted);
  }

  /** Assert two InternalRows are equal field by field. */
  private static void assertRowEquals(InternalRow actual, InternalRow expected, int rowIndex) {
    assertEquals(
        expected.numFields(), actual.numFields(), "Row " + rowIndex + " field count mismatch");
    for (int i = 0; i < actual.numFields(); i++) {
      Object actualVal = actual.get(i, null);
      Object expectedVal = expected.get(i, null);
      assertEquals(expectedVal, actualVal, "Row " + rowIndex + " field " + i + " mismatch");
    }
  }
}
