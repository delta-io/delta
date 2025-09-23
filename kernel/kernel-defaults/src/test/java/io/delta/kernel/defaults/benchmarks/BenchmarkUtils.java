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

import io.delta.kernel.defaults.benchmarks.models.TableInfo;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Useful utilities and values for benchmarks. */
public class BenchmarkUtils {

  public static final Path RESOURCES_DIR =
      Paths.get(System.getProperty("user.dir") + "/src/test/resources");
  public static final Path WORKLOAD_SPECS_DIR = RESOURCES_DIR.resolve("workload_specs");

  private static final String DELTA_DIR_NAME = "delta";
  private static final String SPECS_DIR_NAME = "specs";
  private static final String SPEC_FILE_NAME = "spec.json";
  private static final String TABLE_INFO_FILE_NAME = "table_info.json";

  /**
   * Scans the workloads directory and loads all JSON workload specifications.
   *
   * @param specDirPath Path to the directory containing workload specifications
   * @return List of loaded workload specifications
   * @throws WorkloadLoadException if workloads cannot be loaded
   */
  public static List<WorkloadSpec> loadAllWorkloads(Path specDirPath) {
    validateWorkloadDirectory(specDirPath);

    List<Path> tableDirectories = findTableDirectories(specDirPath);

    return tableDirectories.stream()
        .flatMap(tableDir -> loadSpecsFromTable(tableDir).stream())
        .collect(Collectors.toList());
  }

  /** Validates that the workload directory exists and is accessible. */
  private static void validateWorkloadDirectory(Path specDirPath) {
    if (!Files.exists(specDirPath)) {
      throw new WorkloadLoadException("Workload directory does not exist: " + specDirPath);
    }

    if (!Files.isDirectory(specDirPath)) {
      throw new WorkloadLoadException("Path is not a directory: " + specDirPath);
    }

    if (!Files.isReadable(specDirPath)) {
      throw new WorkloadLoadException("Cannot read workload directory: " + specDirPath);
    }
  }

  /** Finds all table directories within the workload specifications directory. */
  private static List<Path> findTableDirectories(Path specDirPath) {
    try (Stream<Path> files = Files.list(specDirPath)) {
      List<Path> tableDirectories = files.filter(Files::isDirectory).collect(Collectors.toList());

      if (tableDirectories.isEmpty()) {
        throw new WorkloadLoadException("No table directories found in " + specDirPath);
      }

      return tableDirectories;
    } catch (IOException e) {
      throw new WorkloadLoadException("Failed to list table directories in " + specDirPath, e);
    }
  }

  /** Loads all workload specifications from a single table directory. */
  private static List<WorkloadSpec> loadSpecsFromTable(Path tableDir) {
    validateTableStructure(tableDir);

    Path tableInfoPath = tableDir.resolve(TABLE_INFO_FILE_NAME);
    Path deltaDir = tableDir.resolve(DELTA_DIR_NAME);
    Path specsDir = tableDir.resolve(SPECS_DIR_NAME);

    TableInfo tableInfo =
        TableInfo.fromJsonPath(tableInfoPath.toString(), deltaDir.toAbsolutePath().toString());
    System.out.println("Loading specs for table: " + tableInfo);

    return findSpecDirectories(specsDir).stream()
        .map(specDir -> loadSingleSpec(specDir, tableInfo))
        .collect(Collectors.toList());
  }

  /** Validates that a table directory has the required structure. */
  private static void validateTableStructure(Path tableDir) {
    Path deltaDir = tableDir.resolve(DELTA_DIR_NAME);
    Path specsDir = tableDir.resolve(SPECS_DIR_NAME);

    if (!Files.exists(deltaDir) || !Files.isDirectory(deltaDir)) {
      throw new WorkloadLoadException("Delta directory not found: " + deltaDir);
    }

    if (!Files.exists(specsDir) || !Files.isDirectory(specsDir)) {
      throw new WorkloadLoadException("Specs directory not found: " + specsDir);
    }
  }

  /** Finds all specification directories within the specs directory. */
  private static List<Path> findSpecDirectories(Path specsDir) {
    try (Stream<Path> specDirs = Files.list(specsDir)) {
      List<Path> specDirectories = specDirs.filter(Files::isDirectory).collect(Collectors.toList());

      if (specDirectories.isEmpty()) {
        throw new WorkloadLoadException("No spec directories found in " + specsDir);
      }

      return specDirectories;
    } catch (IOException e) {
      throw new WorkloadLoadException("Failed to list spec directories in " + specsDir, e);
    }
  }

  /** Loads a single workload specification from a spec directory. */
  private static WorkloadSpec loadSingleSpec(Path specDir, TableInfo tableInfo) {
    Path specFile = specDir.resolve(SPEC_FILE_NAME);

    if (!Files.exists(specFile) || !Files.isRegularFile(specFile)) {
      throw new WorkloadLoadException("Spec file not found: " + specFile);
    }

    try {
      String specName = specDir.getFileName().toString();
      WorkloadSpec workloadSpec =
          WorkloadSpec.fromJsonPath(specFile.toString(), specName, tableInfo);

      System.out.println("Loaded workload spec: " + workloadSpec);
      return workloadSpec;

    } catch (Exception e) {
      throw new WorkloadLoadException("Failed to parse spec file: " + specFile, e);
    }
  }

  /** Custom exception for workload loading errors. */
  public static class WorkloadLoadException extends RuntimeException {
    public WorkloadLoadException(String message) {
      super(message);
    }

    public WorkloadLoadException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
