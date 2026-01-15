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
package io.delta.spark.internal.v2.read;

import java.io.Closeable;
import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;

public class SparkPartitionReader<T> implements PartitionReader<T> {
  // Function that produces an Iterator for a given file.
  // May return RecordReaderIterator (standard) or DeletionVectorFilterIterator (with DV).
  private final Function1<PartitionedFile, Iterator<InternalRow>> readFunc;
  private final FilePartition partition;

  // Index of the next file to read within the partition.
  private int currentFileIndex = 0;

  // Current iterator - may be RecordReaderIterator or DeletionVectorFilterIterator.
  private Iterator<T> currentIterator = null;

  public SparkPartitionReader(
      Function1<PartitionedFile, Iterator<InternalRow>> readFunc, FilePartition partition) {
    this.readFunc = java.util.Objects.requireNonNull(readFunc, "readFunc");
    this.partition = java.util.Objects.requireNonNull(partition, "partition");
  }

  @Override
  public boolean next() throws IOException {
    // Advance to the next available record, opening readers as needed and closing exhausted ones.
    while (true) {
      if (currentIterator != null && currentIterator.hasNext()) {
        return true;
      }

      closeCurrentIterator();

      if (currentFileIndex >= partition.files().length) {
        return false;
      }

      final PartitionedFile file = partition.files()[currentFileIndex++];
      @SuppressWarnings("unchecked")
      Iterator<T> it = (Iterator<T>) readFunc.apply(file);
      currentIterator = it;
    }
  }

  @Override
  public T get() {
    if (currentIterator == null) {
      throw new IllegalStateException("No current record. Call next() before get().");
    }
    return currentIterator.next();
  }

  @Override
  public void close() throws IOException {
    closeCurrentIterator();
  }

  private void closeCurrentIterator() throws IOException {
    if (currentIterator != null) {
      if (currentIterator instanceof Closeable) {
        ((Closeable) currentIterator).close();
      }
      currentIterator = null;
    }
  }
}
