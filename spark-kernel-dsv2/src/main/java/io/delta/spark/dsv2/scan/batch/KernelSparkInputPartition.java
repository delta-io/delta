package io.delta.spark.dsv2.scan.batch;

import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Spark InputPartition implementation that holds serialized Delta Kernel scan information. Contains
 * both scan state and add files.
 */
public final class KernelSparkInputPartition implements InputPartition, Serializable {

  private final String serializedScanState;
  // TODO: [delta-io/delta#5109] implement the logic to group files in to partition based on file size.
  // Json representation of one add file in kernel
  private final String serializedScanFileRow;

  public KernelSparkInputPartition(String serializedScanState, String serializedScanFileRow) {
    this.serializedScanState = Objects.requireNonNull(serializedScanState, "serializedScanState");
    this.serializedScanFileRow =
        Objects.requireNonNull(serializedScanFileRow, "serializedScanFileRow");
  }

  public String getSerializedScanState() {
    return serializedScanState;
  }

  public String getSerializedScanFileRow() {
    return serializedScanFileRow;
  }
}
