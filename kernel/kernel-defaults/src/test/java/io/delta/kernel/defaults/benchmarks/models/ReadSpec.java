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

package io.delta.kernel.defaults.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.defaults.benchmarks.workloadRunners.ReadMetadataRunner;
import io.delta.kernel.defaults.benchmarks.workloadRunners.WorkloadRunner;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.List;

/**
 * Workload specification for read benchmarks. Defines test cases for reading Delta tables at
 * specific versions or latest, with different operation types.
 *
 * <h2>Overview</h2>
 *
 * <p>A ReadSpec represents a single test case that can generate multiple benchmark variants:
 *
 * <ul>
 *   <li><b>read_metadata</b>: Scans table metadata (add/remove files) at the specified version
 *   <li><b>read_data</b>: Reads actual data and validates against expected results (future)
 * </ul>
 *
 * <h2>Spec File Format</h2>
 *
 * <p>On-disk spec files contain only test parameters (minimal):
 *
 * <pre>{@code
 * {
 *   "type": "read",
 *   "version": 5             // optional: specific version, omit for latest
 * }
 * }</pre>
 *
 * <h2>Runtime Enrichment</h2>
 *
 * <p>During loading, the spec is enriched with runtime metadata:
 *
 * <ul>
 *   <li><b>tableInfo</b>: Added from table_info.json in parent directory
 *   <li><b>caseName</b>: Added from the spec directory name
 * </ul>
 *
 * <h2>Variant Generation</h2>
 *
 * <p>When getWorkloadVariants() is called, this spec generates variants:
 *
 * <pre>{@code
 * ReadSpec(type="read", version=5)
 *   → ReadSpec(type="read", version=5, operationType="read_metadata")
 *   → ReadSpec(type="read", version=5, operationType="read_data")  // future
 * }</pre>
 *
 * <h2>Fields</h2>
 *
 * <ul>
 *   <li><b>version</b>: Snapshot version to read (null = latest). From spec file.
 *   <li><b>expected_data</b>: Expected data file for validation (future). From spec file.
 *   <li><b>operationType</b>: Operation variant ("read_metadata", "read_data"). Set during variant
 *       generation, not in spec file.
 * </ul>
 *
 * <h2>Usage</h2>
 *
 * <p>To run this workload, use {@link WorkloadSpec#getRunner(Engine)} to get the appropriate {@link
 * ReadMetadataRunner} or ReadDataRunner based on the operation type.
 *
 * @see ReadMetadataRunner
 */
public class ReadSpec extends WorkloadSpec {

  /** The snapshot version to read. If null, the latest version will be read. From spec file. */
  @JsonProperty("version")
  private Long version;

  /** Expected data file for validating the read data result. From spec file. */
  @JsonProperty("expected_data")
  private String expectedData;

  /**
   * The operation type for this variant ("read_metadata" or "read_data"). Set during variant
   * generation via getWorkloadVariants(), not present in spec files.
   */
  @JsonProperty("operation_type")
  private String operationType;

  // Default constructor for Jackson
  public ReadSpec() {
    super("read");
  }

  // Copy constructor
  public ReadSpec(
      TableInfo tableInfo,
      String caseName,
      Long version,
      String expectedData,
      String operationType) {
    super("read");
    this.tableInfo = tableInfo;
    this.version = version;
    this.caseName = caseName;
    this.expectedData = expectedData;
    this.operationType = operationType;
  }

  /** @return the snapshot version to read, or null if the latest version should be read. */
  public Long getVersion() {
    return version;
  }

  /**
   * @return the full name of this workload, derived from table name, case name, and operation type.
   */
  @Override
  public String getFullName() {
    return this.tableInfo.name + "/" + this.caseName + "/read/" + this.operationType;
  }

  @Override
  public WorkloadRunner getRunner(Engine engine) {
    if (operationType.equals("read_metadata")) {
      return new ReadMetadataRunner(this, engine);
    } else {
      throw new IllegalArgumentException("Unsupported operation for ReadSpec: " + operationType);
    }
  }

  /**
   * Generates workload variants from this test case specification.
   *
   * <p>A single ReadSpec test case can generate multiple workload variants, one for each operation
   * type. Each variant is a complete ReadSpec with the operation type set.
   *
   * <p>Currently generates:
   *
   * <ul>
   *   <li>read_metadata variant - scans table metadata
   * </ul>
   *
   * <p>Future: will also generate read_data variant when implemented.
   *
   * @return list of ReadSpec variants, each representing a separate workload execution
   */
  @Override
  public List<WorkloadSpec> getWorkloadVariants() {
    // TODO: In the future, we will support the read_data operation as well.
    String[] operationTypes = {"read_metadata"};
    List<WorkloadSpec> out = new ArrayList<>();
    for (String opType : operationTypes) {
      ReadSpec specVariant =
          new ReadSpec(this.tableInfo, this.caseName, this.version, this.expectedData, opType);
      out.add(specVariant);
    }
    return out;
  }

  /**
   * Gets the operation type for this read variant.
   *
   * @return the operation type ("read_metadata" or "read_data")
   */
  public String getOperationType() {
    return operationType;
  }

  @Override
  public String toString() {
    return String.format(
        "Read{caseName='%s', operationType='%s', snapshotVersion=%s, tableInfo='%s'}",
        caseName, operationType, version, tableInfo);
  }
}
