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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.defaults.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.defaults.benchmarks.workloadrunners.WriteRunner;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.DataFileStatus;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Workload specification for write benchmarks. Defines test cases for writing to Delta tables with
 * one or more commits containing add/remove actions.
 *
 * <h2>Usage</h2>
 *
 * <p>To run this workload, use {@link WorkloadSpec#getRunner(Engine)} to get the appropriate {@link
 * WriteRunner}.
 *
 * @see WriteRunner
 */
public class WriteSpec extends WorkloadSpec {

  /**
   * Container for data file actions (adds) from a commit specification file.
   *
   * <p>This class is used to deserialize the JSON structure containing the list of files to add in
   * a commit.
   */
  private static class DataFiles {
    @JsonProperty("adds")
    private ArrayList<DataFilesStatusSerde> adds;

    /** @return the list of added data files in this commit. */
    @JsonIgnore
    public ArrayList<DataFilesStatusSerde> getAdds() {
      return adds;
    }
  }

  /**
   * Serialization/deserialization wrapper for {@link DataFileStatus}.
   *
   * <p>This class represents the JSON structure for data file metadata, including path, size,
   * modification time, and optional statistics. It can be converted to a {@link DataFileStatus}
   * instance for use in Delta Kernel APIs.
   */
  private static class DataFilesStatusSerde {
    @JsonProperty("path")
    private String path;

    @JsonProperty("size")
    private long size;

    @JsonProperty("modification_time")
    private long modificationTime;

    @JsonProperty("stats")
    private String stats;

    /**
     * Converts this serialization object to a {@link DataFileStatus} instance.
     *
     * @param schema the table schema used to parse statistics
     * @return a DataFileStatus instance with the file metadata
     */
    public DataFileStatus toDataFileStatus(StructType schema) {
      Optional<DataFileStatistics> parsedStats = Optional.empty();
      if (stats != null) {
        parsedStats = DataFileStatistics.deserializeFromJson(stats, schema);
      }
      return new DataFileStatus(path, size, modificationTime, parsedStats);
    }
  }

  /**
   * Container for a single commit's configuration.
   *
   * <p>Each commit references a file containing the Delta log JSON actions (add/remove files) to be
   * committed.
   */
  public static class CommitSpec {
    /**
     * Path to the commit file containing Delta log JSON actions. The path is relative to the spec
     * directory (where spec.json is located).
     *
     * <p>Example: "commit_a.json"
     */
    @JsonProperty("data_files_path")
    private String dataFilesPath;

    /** Default constructor for Jackson. */
    public CommitSpec() {}

    public String getDataFilesPath() {
      return dataFilesPath;
    }

    /**
     * Parses the data_files file and returns the list of added and removed data files.
     *
     * @param specPath the base path where the commit file is located
     * @throws IOException if there's an error reading or parsing the file
     */
    public List<DataFileStatus> readDataFiles(String specPath, StructType schema)
        throws IOException {
      ObjectMapper mapper = new ObjectMapper();
      String commitFilePath = new Path(specPath, getDataFilesPath()).toString();

      WriteSpec.DataFiles dataFiles =
          mapper.readValue(new File(commitFilePath), WriteSpec.DataFiles.class);
      return dataFiles.adds.stream()
          .map(file -> file.toDataFileStatus(schema))
          .collect(Collectors.toList());
    }
  }

  /**
   * List of commits to execute in sequence. Each commit contains a reference to a file with Delta
   * log JSON actions. All commits are executed as part of the timed benchmark.
   */
  @JsonProperty("commits")
  private List<CommitSpec> commits;

  // Default constructor for Jackson
  public WriteSpec() {
    super("write");
  }

  /**
   * Gets the list of commits to execute.
   *
   * @return list of commit specifications
   */
  public List<CommitSpec> getCommits() {
    return commits != null ? commits : Collections.emptyList();
  }

  /** @return the full name of this workload, derived from table name, case name, and type. */
  @Override
  public String getFullName() {
    return this.tableInfo.name + "/" + caseName + "/write";
  }

  @Override
  public WorkloadRunner getRunner(Engine engine) {
    return new WriteRunner(this, engine);
  }

  /**
   * Generates workload variants from this test case specification.
   *
   * <p>Currently, WriteSpec generates a single variant (itself). In the future, this could be
   * extended to generate variants for different write patterns or configurations.
   *
   * @return list of WriteSpec variants, each representing a separate workload execution
   */
  @Override
  public List<WorkloadSpec> getWorkloadVariants() {
    return Collections.singletonList(this);
  }

  @Override
  public String toString() {
    return String.format(
        "Write{caseName='%s', commits=%d, tableInfo='%s'}",
        caseName, getCommits().size(), tableInfo);
  }
}
