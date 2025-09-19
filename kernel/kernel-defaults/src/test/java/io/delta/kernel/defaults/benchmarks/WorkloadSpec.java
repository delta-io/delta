/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.benchmarks;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.engine.Engine;
import java.io.File;
import java.io.IOException;

/**
 * Base class for all workload specifications. Workload specifications define workloads and their
 * parameters. These workloads can then be executed as benchmarks using the corresponding
 * [[WorkloadRunner]].
 *
 * <p>This class uses Jackson annotations to support polymorphic deserialization based on the "type"
 * field in the JSON.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = ReadMetadataSpec.class, name = "read")})
public abstract class WorkloadSpec {
  /**
   * The type of workload (e.g., "read_metadata") This is used by Jackson's polymorphic
   * deserialization to automatically instantiate the correct subclass based on the "type" field in
   * the JSON.
   */
  private String type;

  /** The name of the workload */
  @JsonProperty("name")
  public String name;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  /** Default constructor for Jackson */
  protected WorkloadSpec() {}

  protected WorkloadSpec(String type) {
    this.type = type;
  }

  /** @return the type of this workload. */
  public String getType() {
    return type;
  }

  /**
   * Creates a WorkloadRunner for this workload specification.
   *
   * @param baseWorkloadDirPath The base directory containing workload tables. This is used to
   *     resolve relative table paths if present.
   * @param engine The engine to use for executing the workload.
   * @return the WorkloadRunner instance for this workload specification.
   */
  public abstract WorkloadRunner getRunner(String baseWorkloadDirPath, Engine engine);

  // JSON Serialization/Deserialization

  /**
   * Loads a WorkloadSpec from the given JSON file path.
   *
   * @param workloadPath the path to the JSON file containing the workload specification.
   * @return the WorkloadSpec instance parsed from the JSON file.
   * @throws IOException if there is an error reading or parsing the file.
   */
  public static WorkloadSpec fromJsonPath(String workloadPath) throws IOException {
    return objectMapper.readValue(new File(workloadPath), WorkloadSpec.class);
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
