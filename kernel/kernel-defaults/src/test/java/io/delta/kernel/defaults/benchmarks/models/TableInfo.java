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
import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Represents metadata about a Delta table used in benchmark workloads.
 *
 * <p>This class contains information about a Delta table that is used by benchmark workloads to
 * locate and access the table data. It includes the table name, description, the root path where
 * the table is stored, and optional engine information.
 *
 * <p>TableInfo instances are typically loaded from JSON files in the workload specifications
 * directory structure. Each table directory should contain a {@code table_info.json} file with the
 * table metadata and a {@code delta} subdirectory containing the actual table data. The table root
 * path is the absolute path to the root of the table and is provided separately in {@link
 * WorkloadSpec#fromJsonPath(String, String, TableInfo)}.
 *
 * <p>Example JSON structure:
 *
 * <pre>{@code
 * {
 *   "name": "large-table",
 *   "description": "A large Delta table with multi-part checkpoints for performance testing",
 *   "engineInfo": "Apache-Spark/3.5.1 Delta-Lake/3.1.0"
 * }
 * }</pre>
 */
public class TableInfo {
  /** The name of the table, used for identification in benchmark reports. */
  @JsonProperty("name")
  public String name;

  /** A human-readable description of the table and its purpose. */
  @JsonProperty("description")
  public String description;

  /**
   * Information about the engine used to create this table (e.g., "Apache-Spark/3.5.1
   * Delta-Lake/3.1.0"). Optional field to track which engine/version created the table data.
   */
  @JsonProperty("engineInfo")
  public String engineInfo;

  /** The root path where the Delta table is stored. */
  @JsonProperty("table_root")
  public String tableRoot;

  /**
   * Gets the absolute path to the root of the Delta table.
   *
   * @return the absolute path to the root of the table
   */
  public String getTableRoot() {
    return tableRoot;
  }

  /**
   * Sets the root path of the Delta table.
   *
   * @param tableRoot the absolute path to the root of the table
   */
  public void setTableRoot(String tableRoot) {
    this.tableRoot = tableRoot;
  }

  /**
   * Default constructor for Jackson deserialization.
   *
   * <p>This constructor is required for Jackson to deserialize JSON into TableInfo objects. All
   * fields should be set via Jackson annotations or setter methods.
   */
  public TableInfo() {}

  /**
   * Creates a TableInfo instance by reading from a JSON file at the specified path.
   *
   * <p>This method loads table metadata from a JSON file and sets the table root path. The JSON
   * file should contain the table name and description, while the table root path is provided
   * separately with the absolute path.
   *
   * @param jsonPath the path to the JSON file containing the TableInfo metadata
   * @param tableRoot the absolute path to the root of the Delta table
   * @return a TableInfo instance populated from the JSON file and table root path
   * @throws RuntimeException if there is an error reading or parsing the JSON file
   */
  public static TableInfo fromJsonPath(String jsonPath, String tableRoot) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      TableInfo info = mapper.readValue(new File(jsonPath), TableInfo.class);
      info.setTableRoot(tableRoot);
      return info;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read TableInfo from JSON file: " + jsonPath, e);
    }
  }

  /**
   * Returns a string representation of this TableInfo.
   *
   * <p>The string includes the table name, description, and engine info, but excludes the table
   * root path for security reasons (as it may contain sensitive path information).
   *
   * @return a string representation of this TableInfo
   */
  @Override
  public String toString() {
    return "TableInfo{name='"
        + name
        + "', description='"
        + description
        + "', engineInfo='"
        + engineInfo
        + "'}";
  }
}
