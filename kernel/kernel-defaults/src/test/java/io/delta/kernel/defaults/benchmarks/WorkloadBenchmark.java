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

import static io.delta.kernel.defaults.benchmarks.BenchmarkUtils.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Generic JMH benchmark for all workload types. Automatically loads and runs benchmarks based on
 * JSON workload specifications.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WorkloadBenchmark {

  /**
   * Dynamic benchmark state that reads the workload specification and sets up the workload runner
   */
  @State(Scope.Thread)
  public static class BenchmarkState {
    // This parameter will be set dynamically in the main method to include all workload spec paths.
    @Param({})
    private String workloadPath;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private WorkloadSpec workload;
    private WorkloadRunner runner;

    /**
     * Setup method that runs before each benchmark invocation. Reads the workload specification
     * from the given path and initializes the corresponding workload runner.
     *
     * @throws Exception If any error occurs during setup.
     */
    @Setup(Level.Invocation)
    public void setup() throws Exception {
      workload = objectMapper.readValue(new File(workloadPath), WorkloadSpec.class);
      runner = workload.getRunner(RESOURCES_DIR);
    }

    /** @return The workload specification for this benchmark invocation. */
    public WorkloadSpec getWorkloadSpecification() {
      return workload;
    }

    /** @return The workload runner initialized for this benchmark invocation. */
    public WorkloadRunner getRunner() {
      return runner;
    }
  }

  /**
   * Benchmark method that executes the workload specified in the state.
   *
   * @param state The benchmark state containing the workload specification and runner.
   * @param blackhole The Blackhole to consume results and prevent dead code elimination.
   * @throws Exception If any error occurs during workload execution.
   */
  @Benchmark
  public void benchmarkWorkload(BenchmarkState state, Blackhole blackhole) throws Exception {
    WorkloadRunner workload = state.getRunner();
    workload.executeAsBenchmark(blackhole);
  }

  public static void main(String[] args) throws RunnerException {
    // Get workload specs from the workloads directory
    List<String> workloadSpecPaths = BenchmarkUtils.loadAllWorkloads(WORKLOAD_SPECS_DIR);
    if (workloadSpecPaths.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Convert paths into a String array for JMH. JMH requires that parameters be of type String[].
    String[] paths = workloadSpecPaths.toArray(new String[0]);

    // Create and run the benchmark
    Options opt =
        new OptionsBuilder()
            .include(WorkloadBenchmark.class.getSimpleName())
            .param("workloadPath", paths)
            .forks(1)
            .warmupIterations(3)
            .measurementIterations(5)
            .warmupTime(TimeValue.seconds(1))
            .measurementTime(TimeValue.seconds(1))
            .resultFormat(ResultFormatType.JSON)
            .build();

    new Runner(opt).run();
  }
}
