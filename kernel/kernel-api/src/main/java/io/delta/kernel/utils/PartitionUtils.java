/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.utils;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;

public class PartitionUtils {

  private PartitionUtils() {}

  /**
   * Check if a partition exists (i.e. actually has data) in the given {@link Snapshot} based on the
   * given {@link Predicate}.
   *
   * @param engine the {@link Engine} to use for scanning the partition.
   * @param snapshot the {@link Snapshot} to scan.
   * @param partitionPredicate the {@link Predicate} to use for filtering the partition.
   * @return true if the partition exists, false otherwise.
   * @throws IllegalArgumentException if the predicate does not reference any partition columns or
   *     if it references any data columns
   */
  public static boolean partitionExists(
      Engine engine, Snapshot snapshot, Predicate partitionPredicate) {
    requireNonNull(engine, "engine is null");
    requireNonNull(snapshot, "snapshot is null");
    requireNonNull(partitionPredicate, "partitionPredicate is null");

    final Set<String> snapshotPartColNames = new HashSet<>(snapshot.getPartitionColumnNames());

    io.delta.kernel.internal.util.PartitionUtils.validatePredicateOnlyOnPartitionColumns(
        partitionPredicate, snapshotPartColNames);

    final Scan scan = snapshot.getScanBuilder().withFilter(partitionPredicate).build();

    try (CloseableIterator<FilteredColumnarBatch> columnarBatchIter = scan.getScanFiles(engine)) {
      while (columnarBatchIter.hasNext()) {
        try (CloseableIterator<Row> selectedRowsIter = columnarBatchIter.next().getRows()) {
          if (selectedRowsIter.hasNext()) {
            return true;
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return false;
  }
}
