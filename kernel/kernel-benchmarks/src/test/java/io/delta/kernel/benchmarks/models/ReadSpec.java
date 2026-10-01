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

package io.delta.kernel.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.benchmarks.workloadrunners.ReadMetadataRunner;
import io.delta.kernel.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.List;

/**
 * Workload specification for read benchmarks. Defines test cases for reading Delta tables at
 * specific versions or latest, with different operation types.
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
  public ReadSpec(TableInfo tableInfo, String caseName, Long version, String operationType) {
    super("read");
    this.tableInfo = tableInfo;
    this.version = version;
    this.caseName = caseName;
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
    return this.tableInfo.name + "/" + caseName + "/read/" + operationType;
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
   * @return list of ReadSpec variants, each representing a separate workload execution
   */
  @Override
  public List<WorkloadSpec> getWorkloadVariants() {
    // TODO: In the future, we will support the read_data operation as well.
    String[] operationTypes = {"read_metadata"};
    List<WorkloadSpec> out = new ArrayList<>();
    for (String opType : operationTypes) {
      ReadSpec specVariant = new ReadSpec(tableInfo, caseName, version, opType);
      out.add(specVariant);
    }
    return out;
  }

  /** @return the operation type ("read_metadata" or "read_data") */
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
