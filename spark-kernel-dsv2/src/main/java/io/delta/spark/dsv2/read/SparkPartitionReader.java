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
package io.delta.spark.dsv2.read;

import java.io.IOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.Iterator$;

public class SparkPartitionReader<T> implements PartitionReader<T> {
  private final Function1<PartitionedFile, Iterator<InternalRow>> readFunc;
  private final FilePartition partition;
  private int currentFileIndex = 0;
  private Iterator<T> currentIterator = (Iterator<T>) Iterator$.MODULE$.empty();

  public SparkPartitionReader(
      Function1<PartitionedFile, Iterator<InternalRow>> readFunc, FilePartition partition) {
    this.readFunc = readFunc;
    this.partition = partition;
    ;
  }

  @Override
  public boolean next() {
    while (!currentIterator.hasNext() && currentFileIndex < partition.files().length) {
      PartitionedFile file = partition.files()[currentFileIndex];
      currentIterator = (Iterator<T>) readFunc.apply(file);
      currentFileIndex++;
    }
    return currentIterator.hasNext();
  }

  @Override
  public T get() {
    return currentIterator.next();
  }

  @Override
  public void close() throws IOException {}
}
