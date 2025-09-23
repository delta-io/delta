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

package io.delta.kernel.defaults.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;

/** Useful utilities and values for benchmarks. */
public class BenchmarkUtils {
  public static final Path RESOURCES_DIR =
      Paths.get(System.getProperty("user.dir") + "/src/test/resources");
  public static final Path WORKLOAD_SPECS_DIR = RESOURCES_DIR.resolve("workload_specs");

  /**
   * Scans the workloads directory and loads all JSON workload specifications.
   *
   * @return List of loaded workload specifications
   * @throws RuntimeException if the workloads directory doesn't exist
   */
  public static List<WorkloadSpec> loadAllWorkloads(Path specDirPath) {
    List<WorkloadSpec> workloadSpecs = new ArrayList<>();
    try (Stream<Path> files = Files.list(specDirPath)) {
      List<Path> tablePaths =
          files.collect(Collectors.toList()).stream()
              .filter(Files::isDirectory)
              .collect(Collectors.toList());
      if (tablePaths.isEmpty()) {
        throw new RuntimeException("No tables found in " + specDirPath);
      }
      // Check that the delta and specs directories exist
      Path deltaDir = tablePaths.get(0).resolve("delta");
      Path specsDir = tablePaths.get(0).resolve("specs");
      if (!Files.exists(deltaDir) || !Files.isDirectory(deltaDir)) {
        throw new RuntimeException("Delta directory not found in " + tablePaths.get(0));
      }

      // List directories under specs dir and return their names

      try (Stream<Path> specDirs = Files.list(specsDir)) {
        @NotNull
        List<Path> specCases = specDirs.filter(Files::isDirectory).collect(Collectors.toList());
        if (specCases.isEmpty()) {
          throw new RuntimeException("No spec cases found in " + specsDir);
        }

        // Read the spec.json file in each spec case directory
        for (Path specCase : specCases) {
          Path specFile = specCase.resolve("spec.json");
          if (!Files.exists(specFile) || !Files.isRegularFile(specFile)) {
            throw new RuntimeException("spec.json not found in " + specCase);
          }
          // Read the workloadSpec, and inject the table root = deltaDir
          WorkloadSpec workloadSpec =
              WorkloadSpec.fromJsonPath(
                  specFile.toString(), deltaDir.toString(), specCase.getFileName().toString());
          System.out.println("Loaded workload spec: " + workloadSpec);
          workloadSpecs.add(workloadSpec);
        }
      }

    } catch (IOException e) {
      throw new RuntimeException("Failed to scan workloads directory", e);
    }
    return workloadSpecs;
  }
}
