package io.delta.spark.dsv2.scan.batch;

import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.connector.read.InputPartition;

/**
 * Spark InputPartition implementation that holds serialized Delta Kernel scan information. Contains
 * both scan state and individual file metadata needed for distributed processing.
 */
public final class KernelSparkInputPartition implements InputPartition, Serializable {

  private final String serializedScanState;
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
