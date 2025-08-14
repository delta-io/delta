package io.delta.spark.dsv2.scan.batch;

import java.io.Serializable;
import java.util.Objects;
import org.apache.spark.sql.connector.read.InputPartition;

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


