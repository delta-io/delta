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

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.engine.Engine;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Base class for all workload specifications. Workload specifications define test cases and their
 * parameters that can be executed as benchmarks using the corresponding {@link WorkloadRunner}.
 *
 * <p>This class uses Jackson annotations to support polymorphic deserialization based on the "type"
 * field in the JSON.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ReadSpec.class, name = "read"),
  @JsonSubTypes.Type(value = SnapshotConstructionSpec.class, name = "snapshot_construction"),
  @JsonSubTypes.Type(value = WriteSpec.class, name = "write")
})
public abstract class WorkloadSpec {
  /**
   * The type of workload (e.g., "read"). This is used by Jackson's polymorphic deserialization to
   * automatically instantiate the correct subclass based on the "type" field in the JSON.
   */
  protected String type;

  @JsonProperty("table_info")
  protected TableInfo tableInfo;

  @JsonProperty("case_name")
  protected String caseName;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /** Default constructor for Jackson */
  protected WorkloadSpec() {}

  protected WorkloadSpec(String type) {
    this.type = type;
  }

  /** @return the type of this workload. */
  @JsonIgnore
  public String getType() {
    return type;
  }

  /** @return the case name of this workload. */
  public String getCaseName() {
    return caseName;
  }

  @JsonProperty(value = "full_name", access = JsonProperty.Access.READ_ONLY)
  public String getFullName() {
    return tableInfo.name + "/" + caseName + "/" + type;
  }

  public void setCaseName(String caseName) {
    this.caseName = caseName;
  }

  public TableInfo getTableInfo() {
    return tableInfo;
  }

  @JsonIgnore
  public String getSpecDirectoryPath() {
    return tableInfo.getTableInfoPath() + "/specs/" + caseName;
  }

  /**
   * Sets the table information for this workload specification.
   *
   * @param tableInfo the table information containing name, description, and root path
   */
  public void setTableInfo(TableInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  /**
   * Creates a WorkloadRunner for this workload specification.
   *
   * @param engine The engine to use for executing the workload.
   * @return the WorkloadRunner instance for this workload specification.
   */
  public abstract WorkloadRunner getRunner(Engine engine);

  /**
   * Loads a WorkloadSpec from the given JSON file path.
   *
   * @param workloadPath the path to the JSON file containing the workload specification.
   * @param caseName the name of the test case for this workload
   * @param tableInfo the table information to associate with this workload
   * @return the WorkloadSpec instance parsed from the JSON file.
   * @throws IOException if there is an error reading or parsing the file.
   */
  public static WorkloadSpec fromJsonPath(String workloadPath, String caseName, TableInfo tableInfo)
      throws IOException {

    WorkloadSpec spec = objectMapper.readValue(new File(workloadPath), WorkloadSpec.class);
    spec.setTableInfo(tableInfo);
    spec.setCaseName(caseName);
    return spec;
  }

  /**
   * Generates workload variants from this test case specification.
   *
   * <p>A single WorkloadSpec can generate multiple workload variants. For example, a read spec
   * might generate both read_metadata and read_data variants. Each variant can be executed as a
   * benchmark or test.
   *
   * <p>The default implementation returns a single variant (itself). Subclasses should override to
   * generate multiple variants if needed.
   *
   * @return list of WorkloadSpec variants, each representing a separate workload execution
   */
  @JsonIgnore
  public List<WorkloadSpec> getWorkloadVariants() {
    return Collections.singletonList(this);
  }

  /**
   * Loads a WorkloadSpec from the given JSON string.
   *
   * @param json the JSON string containing the workload specification.
   * @return the WorkloadSpec instance parsed from the JSON string.
   * @throws IOException if there is an error parsing the JSON.
   */
  public static WorkloadSpec fromJsonString(String json) throws IOException {
    return objectMapper.readValue(json, WorkloadSpec.class);
  }

  /**
   * Serializes this WorkloadSpec to a pretty-printed JSON string.
   *
   * @return the JSON string representation of this WorkloadSpec.
   */
  public String toJsonString() {
    try {
      return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize WorkloadSpec to JSON", e);
    }
  }
}
