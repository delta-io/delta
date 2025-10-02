package io.delta.kernel.defaults.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Represents metadata about a Delta table used in benchmark workloads.
 *
 * <p>This class contains information about a Delta table that is used by benchmark workloads to
 * locate and access the table data. It includes the table name, description, and the root path
 * where the table is stored.
 *
 * <p>TableInfo instances are typically loaded from JSON files in the workload specifications
 * directory structure. Each table directory should contain a {@code table_info.json} file with the
 * table metadata and a {@code delta} subdirectory containing the actual table data.
 *
 * <p>Example JSON structure:
 *
 * <pre>{@code
 * {
 *   "name": "large-table",
 *   "description": "A large Delta table with multi-part checkpoints for performance testing",
 *   "table_root": "/path/to/table/delta"
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
   * <p>The string includes the table name and description, but excludes the table root path for
   * security reasons (as it may contain sensitive path information).
   *
   * @return a string representation of this TableInfo
   */
  @Override
  public String toString() {
    return "TableInfo{name='" + name + "', description='" + description + "'}";
  }
}
