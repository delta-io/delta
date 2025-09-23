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

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
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
    /**
     * The json representation of the workload specification. Note: This parameter will be set
     * dynamically by JMH. The value is set in the main method.
     */
    @Param({})
    private String workloadSpecJson;

    /**
     * The base directory containing workload tables. This is used to resolve relative table paths.
     * Note: This parameter will be set dynamically by JMH. The value is set in the main method.
     */
    @Param({})
    private String baseWorkloadDir;

    /**
     * The engine to use for this benchmark. Note: This parameter will be set dynamically by JMH.
     * The value is set in the main method.
     */
    @Param({})
    private String engineName;

    /** The workload runner initialized for this benchmark invocation. */
    private WorkloadRunner runner;

    /**
     * Setup method that runs before each benchmark invocation. Reads the workload specification
     * from the given path and initializes the corresponding workload runner.
     *
     * @throws Exception If any error occurs during setup.
     */
    @Setup(Level.Invocation)
    public void setup() throws Exception {
      WorkloadSpec spec = WorkloadSpec.fromJsonString(workloadSpecJson);
      Engine engine;
      if (engineName.equals("default")) {
        engine = DefaultEngine.create(new Configuration());
      } else {
        throw new IllegalArgumentException("Unsupported engine: " + engineName);
      }
      runner = spec.getRunner(baseWorkloadDir, engine);
      runner.setup();
    }

    /** @return The workload specification for this benchmark invocation. */
    public WorkloadSpec getWorkloadSpecification() {
      return getRunner().getWorkloadSpec();
    }

    /** @return The workload runner initialized for this benchmark invocation. */
    public WorkloadRunner getRunner() {
      return runner;
    }
  }

  /**
   * Benchmark method that executes the workload runner specified in the state as a benchmark.
   *
   * @param state The benchmark state containing the workload specification and runner.
   * @param blackhole The Blackhole provided by JMH to consume results and prevent dead code
   *     elimination.
   * @throws Exception If any error occurs during workload execution.
   */
  @Benchmark
  public void benchmarkWorkload(BenchmarkState state, Blackhole blackhole) throws Exception {
    WorkloadRunner runner = state.getRunner();
    runner.executeAsBenchmark(blackhole);
  }

  public static void main(String[] args) throws RunnerException, IOException {
    // Get workload specs from the workloads directory
    List<WorkloadSpec> workloadSpecs = BenchmarkUtils.loadAllWorkloads(WORKLOAD_SPECS_DIR);
    if (workloadSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Parse the Json specs from the json paths
    List<WorkloadSpec> specs = new ArrayList<>();
    for (WorkloadSpec spec : workloadSpecs) {
      // TODO: In the future, we can filter specific workloads using command line args here.
      specs.addAll(spec.getBenchmarkVariants());
    }

    // Convert paths into a String array for JMH. JMH requires that parameters be of type String[].
    String[] workloadSpecsArray =
        specs.stream().map(WorkloadSpec::toJsonString).toArray(String[]::new);

    // Configure and run JMH benchmark with the loaded workload specs
    Options opt =
        new OptionsBuilder()
            .include(WorkloadBenchmark.class.getSimpleName())
            .param("workloadSpecJson", workloadSpecsArray)
            .param("baseWorkloadDir", RESOURCES_DIR.toString())
            // In the future, this can be extended to support multiple engines.
            .param("engineName", "default")
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
