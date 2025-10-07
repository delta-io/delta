package io.delta.kernel.defaults.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.defaults.benchmarks.workloadrunners.SnapshotConstructionRunner;
import io.delta.kernel.defaults.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.engine.Engine;

public class SnapshotConstructionSpec extends WorkloadSpec {

  /** The snapshot version to read. If null, the latest version will be read. From spec file. */
  @JsonProperty("version")
  private Long version;

  /** Expected data file for validating the read data result. From spec file. */
  @JsonProperty("expected_protocol_and_metadata")
  private String expectedProtocolAndMetadata;

  // Default constructor for Jackson
  public SnapshotConstructionSpec() {
    super("snapshot_construction");
  }

  // Copy constructor
  public SnapshotConstructionSpec(
      TableInfo tableInfo, String caseName, Long version, String expectedProtocolAndMetadata) {
    super("snapshot_construction");
    this.tableInfo = tableInfo;
    this.version = version;
    this.caseName = caseName;
    this.expectedProtocolAndMetadata = expectedProtocolAndMetadata;
  }

  /** @return the snapshot version to read, or null if the latest version should be read. */
  public Long getVersion() {
    return version;
  }

  @Override
  public WorkloadRunner getRunner(Engine engine) {
    return new SnapshotConstructionRunner(this, engine);
  }
}
