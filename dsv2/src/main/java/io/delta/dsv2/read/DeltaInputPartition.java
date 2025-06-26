package io.delta.dsv2.read;

import org.apache.spark.sql.connector.read.InputPartition;

/** Serialized and sent from the Driver to the Executors */
class DeltaInputPartition implements InputPartition, java.io.Serializable {
  private static final long serialVersionUID = 1L;

  private final String serializedScanFileRow;
  private final String serializedScanState;

  public DeltaInputPartition(String serializedScanFileRow, String serializedScanState) {
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

    DeltaInputPartition that = (DeltaInputPartition) o;

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
