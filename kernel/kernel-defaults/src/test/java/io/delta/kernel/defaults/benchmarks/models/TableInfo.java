package io.delta.kernel.defaults.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

public class TableInfo {
  @JsonProperty("name")
  public String name;

  @JsonProperty("description")
  public String description;

  @JsonProperty("table_root")
  public String tableRoot;

  /** @return the absolute path to the root of the table. */
  public String getTableRoot() {
    return tableRoot;
  }

  public void setTableRoot(String tableRoot) {
    this.tableRoot = tableRoot;
  }

  // Default constructor for Jackson
  public TableInfo() {}

  /**
   * Creates a TableInfo instance by reading from a JSON file at the specified path.
   *
   * @param jsonPath Path to the JSON file containing the TableInfo.
   * @param tableRoot The root path of the table to set in the TableInfo.
   * @return The TableInfo instance populated from the JSON file and tableRoot.
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

  public String toString() {
    return "TableInfo{name='" + name + "', description='" + description + "'}";
  }
}
