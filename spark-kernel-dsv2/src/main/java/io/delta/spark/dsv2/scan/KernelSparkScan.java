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
package io.delta.spark.dsv2.scan;

import static java.util.Objects.requireNonNull;

import io.delta.kernel.Scan;
import io.delta.kernel.engine.Engine;
import io.delta.spark.dsv2.scan.batch.KernelSparkBatchScan;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark Scan implementation that wraps Delta Kernel's Scan. This allows Spark to use Delta Kernel
 * for reading Delta tables.
 */
public class KernelSparkScan implements org.apache.spark.sql.connector.read.Scan {

  private final StructType sparkReadSchema;
  private final KernelSparkScanContext kernelSparkScanContext;

  public KernelSparkScan(Scan kernelScan, StructType sparkReadSchema, Engine engine) {
    this.sparkReadSchema = requireNonNull(sparkReadSchema, "sparkReadSchema is null");
    this.kernelSparkScanContext =
        new KernelSparkScanContext(requireNonNull(kernelScan, "kernelScan is null"), engine);
  }

  @Override
  public StructType readSchema() {
    return sparkReadSchema;
  }

  @Override
  public Batch toBatch() {
    return new KernelSparkBatchScan(kernelSparkScanContext);
  }
}
