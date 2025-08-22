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

import static java.util.Objects.requireNonNull;

import io.delta.spark.dsv2.scan.KernelSparkScanContext;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

/** Spark Batch implementation backed by Delta Kernel Scan. */
public class KernelSparkBatchScan implements Batch {

  private final KernelSparkScanContext sharedContext;

  public KernelSparkBatchScan(KernelSparkScanContext sharedContext) {
    this.sharedContext = requireNonNull(sharedContext, "sharedContext is null");
  }

  @Override
  public InputPartition[] planInputPartitions() {
    return sharedContext.planPartitions();
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    throw new UnsupportedOperationException("reader factory is not implemented");
  }
}
