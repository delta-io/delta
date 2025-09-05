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
package io.delta.spark.dsv2.read.batch;

import io.delta.spark.dsv2.utils.SerializableKernelRowWrapper;
import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Spark InputPartition implementation that holds serialized Delta Kernel scan information.
 *
 * <p>This class is created on the Driver during partition planning and serialized to Executors
 * where it's used to create PartitionReaders. Contains both scan state and scan file metadata
 * needed to read a specific portion of files of the Delta table.
 */
public final class KernelSparkInputPartition implements InputPartition, Serializable {

  private final SerializableKernelRowWrapper serializedScanState;

  // TODO: [delta-io/delta#5109] implement the logic to group files into partition based on file
  //       size.
  /** Serialized representation of one scan file row from kernel scan */
  private final SerializableKernelRowWrapper serializedScanFileRow;

  public KernelSparkInputPartition(
      SerializableKernelRowWrapper serializedScanState,
      SerializableKernelRowWrapper serializedScanFileRow) {
    this.serializedScanState = Objects.requireNonNull(serializedScanState, "serializedScanState");
    this.serializedScanFileRow =
        Objects.requireNonNull(serializedScanFileRow, "serializedScanFileRow");
  }

  public SerializableKernelRowWrapper getSerializedScanState() {
    return serializedScanState;
  }

  public SerializableKernelRowWrapper getSerializedScanFileRow() {
    return serializedScanFileRow;
  }
}
