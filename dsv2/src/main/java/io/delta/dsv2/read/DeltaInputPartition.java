package io.delta.dsv2.read;

import org.apache.spark.sql.connector.read.InputPartition;

/** Serialized and sent from the Driver to the Executors */
class DeltaInputPartition implements InputPartition, java.io.Serializable {

  private final String serializedScanFileRow;
  private final String serializedScanState;
  // hack fields for AWS credentials
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;

  public DeltaInputPartition(
      String serializedScanFileRow,
      String serializedScanState,
      String accessKey,
      String secretKey,
      String sessionToken) {
    this.serializedScanFileRow = serializedScanFileRow;
    this.serializedScanState = serializedScanState;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
  }

  // Add getters for the new fields
  public String getAccessKey() {
    return accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public String getSessionToken() {
    return sessionToken;
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
