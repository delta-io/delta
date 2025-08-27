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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Utility class for reading workload specifications from JSON files. */
public class WorkloadReader {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final String WORKLOADS_DIR =
      "/Users/oussama.saoudi/projects/code/delta-kernel-rs/kernel/benches/workloads";

  /**
   * Scans the workloads directory and loads all JSON workload specifications.
   *
   * @return List of loaded workload specifications
   * @throws RuntimeException if the workloads directory doesn't exist
   */
  public static List<String> loadAllWorkloads() {
    List<WorkloadSpec> workloads = new ArrayList<>();

    Path workloadsPath = Paths.get(WORKLOADS_DIR);
    if (!Files.exists(workloadsPath)) {
      throw new RuntimeException("Workloads directory does not exist: " + WORKLOADS_DIR);
    }

    try {

      return Files.list(workloadsPath)
          .filter(path -> path.toString().endsWith(".json"))
          .map(path -> path.toString())
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Failed to scan workloads directory", e);
    }
  }

  /**
   * Loads a single workload specification from a JSON file.
   *
   * @param filePath Path to the JSON file
   * @return Parsed WorkloadSpec instance
   * @throws IOException if the file cannot be read or parsed
   */
  public static WorkloadSpec loadWorkloadFromFile(String filePath) throws IOException {
    return objectMapper.readValue(new File(filePath), WorkloadSpec.class);
  }
}
