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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  public static List<String> loadAllWorkloads(Path specDirPath) {

    try (Stream<Path> files = Files.list(specDirPath)) {
      return files
          .map(Path::toString)
          .filter(string -> string.endsWith(".json"))
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Failed to scan workloads directory", e);
    }
  }
}
