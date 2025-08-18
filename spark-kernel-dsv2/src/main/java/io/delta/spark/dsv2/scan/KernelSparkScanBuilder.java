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

import io.delta.kernel.ScanBuilder;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.spark.dsv2.utils.SchemaUtils;
import org.apache.spark.sql.types.StructType;

/**
 * A Spark ScanBuilder implementation that wraps Delta Kernel's ScanBuilder. This allows Spark to
 * use Delta Kernel for reading Delta tables.
 */
public class KernelSparkScanBuilder implements org.apache.spark.sql.connector.read.ScanBuilder {

  private final ScanBuilder kernelScanBuilder;
  private final StructType sparkReadSchema;

  public KernelSparkScanBuilder(SnapshotImpl snapshot) {
    requireNonNull(snapshot, "snapshot is null");

    this.kernelScanBuilder = snapshot.getScanBuilder();

    this.sparkReadSchema = SchemaUtils.convertKernelSchemaToSparkSchema(snapshot.getSchema());
  }

  @Override
  public org.apache.spark.sql.connector.read.Scan build() {
    return new KernelSparkScan(kernelScanBuilder.build(), sparkReadSchema);
  }
}
