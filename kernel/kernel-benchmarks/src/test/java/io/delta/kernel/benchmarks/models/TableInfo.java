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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Optional;

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
  @JsonProperty("engine_info")
  public String engineInfo;

  /** The path to the table_info directory */
  @JsonProperty("table_info_path")
  private String tableInfoPath;

  /**
   * Whether this table is a Unity Catalog managed table. If true, the UC Catalog info is loaded
   * from a fixed path: catalog_managed_info.json in the same directory as table_info.json.
   */
  @JsonProperty("is_catalog_managed")
  private boolean isCatalogManaged;

  /**
   * Lazily loaded Unity Catalog information. This is populated when {@link #getUcCatalogInfo()} is
   * called for the first time.
   */
  @JsonIgnore private Optional<UcCatalogInfo> ucCatalogInfo = Optional.empty();

  /**
   * Default constructor for Jackson deserialization.
   *
   * <p>This constructor is required for Jackson to deserialize JSON into TableInfo objects. All
   * fields should be set via Jackson annotations or setter methods.
   */
  public TableInfo() {}

  /** Resolves the table root path based on the table type and location configuration. */
  @JsonIgnore
  public String getResolvedTableRoot() {
    return Paths.get(tableInfoPath, "delta").toAbsolutePath().toString();
  }

  public String getTableInfoPath() {
    return tableInfoPath;
  }

  public void setTableInfoPath(String tableInfoDirectory) {
    this.tableInfoPath = tableInfoDirectory;
  }

  /**
   * Checks if this table is a Unity Catalog managed table.
   *
   * @return true if is_catalog_managed is true, false otherwise
   */
  @JsonIgnore
  public boolean isCatalogManaged() {
    return isCatalogManaged;
  }

  /**
   * Gets the Unity Catalog information for this table. Lazily loads the UcCatalogInfo from
   * catalog_managed_info.json in the same directory as table_info.json if not already loaded.
   *
   * @return the UcCatalogInfo for this table
   * @throws IllegalStateException if this is not a Unity Catalog managed table
   * @throws RuntimeException if there is an error loading the UcCatalogInfo
   */
  @JsonIgnore
  public UcCatalogInfo getUcCatalogInfo() {
    if (!isCatalogManaged()) {
      throw new IllegalStateException(
          "This is not a Unity Catalog managed table. is_catalog_managed is not set to true in table_info.json");
    }

    // If ucCatalogInfo is not cached, load it from catalog_managed_info.json
    if (!ucCatalogInfo.isPresent()) {
      String catalogManagedInfoFullPath =
          Paths.get(tableInfoPath, "catalog_managed_info.json").toString();
      try {
        ucCatalogInfo = Optional.of(UcCatalogInfo.fromJsonPath(catalogManagedInfoFullPath));
      } catch (java.io.IOException e) {
        throw new RuntimeException(
            "Failed to load UcCatalogInfo from: " + catalogManagedInfoFullPath, e);
      }
    }

    return ucCatalogInfo.get();
  }

  /**
   * Creates a TableInfo instance by reading from a JSON file at the specified path.
   *
   * <p>This method loads table metadata from a JSON file and sets the table root path. The JSON
   * file should contain the table name and description, while the table root path is provided
   * separately with the absolute path.
   *
   * @param jsonPath the path to the JSON file containing the TableInfo metadata
   * @param tableInfoPath the directory containing the table_info.json file
   * @return a TableInfo instance populated from the JSON file and table root path
   * @throws RuntimeException if there is an error reading or parsing the JSON file
   */
  public static TableInfo fromJsonPath(String jsonPath, String tableInfoPath) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      TableInfo info = mapper.readValue(new File(jsonPath), TableInfo.class);
      info.setTableInfoPath(tableInfoPath);
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
