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

import org.apache.spark.sql.types.StructType;

/**
 * A Spark Scan implementation that wraps Delta Kernel's Scan. This allows Spark to use Delta Kernel
 * for reading Delta tables.
 */
public class DeltaKernelScan implements org.apache.spark.sql.connector.read.Scan {

  private final StructType readSchema;

  public DeltaKernelScan(StructType readSchema) {
    this.readSchema = readSchema;
  }

  @Override
  public StructType readSchema() {
    return readSchema;
  }
}
