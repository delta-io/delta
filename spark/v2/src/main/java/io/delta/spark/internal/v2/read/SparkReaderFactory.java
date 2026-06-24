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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function1;
import scala.collection.Iterator;

public class SparkReaderFactory implements PartitionReaderFactory {
  private Function1<PartitionedFile, Iterator<InternalRow>> readFunc;
  private boolean supportsColumnar;

  public SparkReaderFactory(
      Function1<PartitionedFile, Iterator<InternalRow>> readFunc, boolean supportsColumnar) {
    this.readFunc = readFunc;
    this.supportsColumnar = supportsColumnar;
  }

  @Override
  public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    return new SparkPartitionReader<ColumnarBatch>(readFunc, (FilePartition) partition);
  }

  @Override
  public boolean supportColumnarReads(InputPartition partition) {
    return supportsColumnar;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new SparkPartitionReader<InternalRow>(readFunc, (FilePartition) partition);
  }
}
