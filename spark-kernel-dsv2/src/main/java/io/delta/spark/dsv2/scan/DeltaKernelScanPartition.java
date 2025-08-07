package io.delta.spark.dsv2.scan;

import org.apache.spark.sql.connector.read.InputPartition;

public class DeltaKernelScanPartition implements InputPartition, java.io.Serializable {

  private final String serializedScanFileRow;
  private final String serializedScanState;

  public DeltaKernelScanPartition(String serializedScanFileRow, String serializedScanState) {
    this.serializedScanFileRow = serializedScanFileRow;
    this.serializedScanState = serializedScanState;
  }

  public String getSerializedScanFileRow() {
    return serializedScanFileRow;
  }

  public String getSerializedScanState() {
    return serializedScanState;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DeltaKernelScanPartition that = (DeltaKernelScanPartition) o;

    if (!serializedScanFileRow.equals(that.serializedScanFileRow)) return false;
    return serializedScanState.equals(that.serializedScanState);
  }

  @Override
  public int hashCode() {
    int result = serializedScanFileRow.hashCode();
    result = 31 * result + serializedScanState.hashCode();
    return result;
  }
}
