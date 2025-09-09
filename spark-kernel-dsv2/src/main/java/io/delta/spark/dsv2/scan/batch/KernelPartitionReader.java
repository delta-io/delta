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
package io.delta.spark.dsv2.scan.batch;

import io.delta.spark.dsv2.utils.SerializableReaderFunction;
import java.util.Collections;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;

/**
 * Partition reader for Delta Kernel-based Spark scans.
 *
 * <p>Reads data from a Delta table partition using the Delta Kernel interface.
 *
 * @param <T> The type of data returned by the reader (either InternalRow or ColumnarBatch)
 */
class KernelPartitionReader<T> implements PartitionReader<T> {

  private final Function1<PartitionedFile, Iterator<InternalRow>> readFunc;
  private final FilePartition partition;

  private Iterator<T> currentIterator;
  private T currentValue;
  private boolean isInitialized = false;

  /**
   * Creates a new KernelPartitionReader.
   *
   * @param readFunc Function to read parquet files
   * @param partition The input partition to read
   */
  public KernelPartitionReader(SerializableReaderFunction readFunc, FilePartition partition) {
    this.readFunc = readFunc;
    this.partition = partition;
    this.currentIterator =
        scala.collection.JavaConverters.asScalaIterator(Collections.<T>emptyIterator());
  }

  @Override
  public boolean next() {
    if (!isInitialized) {
      initialize();
    }
    if (!currentIterator.hasNext()) {
      return false;
    }
    currentValue = currentIterator.next();
    return true;
  }

  @Override
  public T get() {
    if (!isInitialized) {
      throw new IllegalArgumentException();
    }
    return currentValue;
  }

  /** Initialize the reader. */
  @SuppressWarnings("unchecked")
  private void initialize() {
    if (isInitialized) {
      return;
    }
    // Get the scan file row
    PartitionedFile partitionedFile = partition.files()[0];
    // Apply the read function
    currentIterator = (Iterator<T>) readFunc.apply(partitionedFile);
    isInitialized = true;
  }

  @Override
  public void close() {
    // Nothing to close in this implementation
  }
}
