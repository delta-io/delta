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

package io.delta.kernel.benchmarks;

import static io.delta.kernel.benchmarks.BenchmarkUtils.*;

import io.delta.kernel.benchmarks.models.WorkloadSpec;
import io.delta.kernel.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Generic JMH benchmark for all workload types. Automatically loads and runs benchmarks based on
 * JSON workload specifications.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WorkloadBenchmark<T> {

  /** Default implementation of BenchmarkState that supports only the "default" engine. */
  public static class DefaultBenchmarkState extends AbstractBenchmarkState {
    @Override
    protected Engine getEngine(String engineName) {
      if (engineName.equals("default")) {
        return DefaultEngine.create(new Configuration());
      } else {
        throw new IllegalArgumentException("Unsupported engine: " + engineName);
      }
    }
  }

  /**
   * Benchmark method that executes the workload runner specified in the state as a benchmark.
   *
   * @param state The benchmark state containing the workload runner to execute.
   * @param blackhole The Blackhole provided by JMH to consume results and prevent dead code
   *     elimination.
   * @throws Exception If any error occurs during workload execution.
   */
  @Benchmark
  public void benchmarkWorkload(DefaultBenchmarkState state, Blackhole blackhole) throws Exception {
    WorkloadRunner runner = state.getRunner();
    runner.executeAsBenchmark(blackhole);
  }

  /**
   * Command line argument configuration for the benchmark.
   */
  private static class BenchmarkConfig {
    // Filter for benchmark specs (substring match)
    String includeTest = null;
    // Benchmark configuration parameters with defaults matching current behavior
    int warmupIterations = 3;
    int measurementIterations = 5;
    int warmupTimeSeconds = 1;
    int measurementTimeSeconds = 1;
    int forks = 1;

    static BenchmarkConfig parseArgs(String[] args) {
      BenchmarkConfig config = new BenchmarkConfig();
      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--include-test":
            if (i + 1 < args.length) {
              config.includeTest = args[++i];
            }
            break;
          case "--warmup-iterations":
            if (i + 1 < args.length) {
              config.warmupIterations = Integer.parseInt(args[++i]);
            }
            break;
          case "--measurement-iterations":
            if (i + 1 < args.length) {
              config.measurementIterations = Integer.parseInt(args[++i]);
            }
            break;
          case "--warmup-time":
            if (i + 1 < args.length) {
              config.warmupTimeSeconds = Integer.parseInt(args[++i]);
            }
            break;
          case "--measurement-time":
            if (i + 1 < args.length) {
              config.measurementTimeSeconds = Integer.parseInt(args[++i]);
            }
            break;
          case "--forks":
            if (i + 1 < args.length) {
              config.forks = Integer.parseInt(args[++i]);
            }
            break;
          case "--help":
            printHelp();
            System.exit(0);
            break;
          default:
            System.err.println("Unknown option: " + args[i]);
            printHelp();
            System.exit(1);
        }
      }
      return config;
    }

    static void printHelp() {
      System.out.println("WorkloadBenchmark - JMH Benchmark for Delta Kernel Workloads\n");
      System.out.println("Options:");
      System.out.println("  --include-test <pattern>       Filter benchmark specs by substring match");
      System.out.println("  --warmup-iterations <n>        Number of warmup iterations (default: 3)");
      System.out.println("  --measurement-iterations <n>   Number of measurement iterations (default: 5)");
      System.out.println("  --warmup-time <seconds>        Warmup time per iteration in seconds (default: 1)");
      System.out.println("  --measurement-time <seconds>   Measurement time per iteration in seconds (default: 1)");
      System.out.println("  --forks <n>                    Number of forks (default: 1)");
      System.out.println("  --help                         Print this help message");
    }
  }

  /**
   * TODO: In the future, this can be extracted so that new benchmarks with custom BenchmarkStates
   * can be easily constructed.
   */
  public static void main(String[] args) throws RunnerException, IOException {
    // Parse command line arguments
    BenchmarkConfig config = BenchmarkConfig.parseArgs(args);

    // Get workload specs from the workloads directory
    List<WorkloadSpec> workloadSpecs = BenchmarkUtils.loadAllWorkloads(WORKLOAD_SPECS_DIR);
    if (workloadSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Parse the Json specs from the json paths and filter if specified
    List<WorkloadSpec> filteredSpecs = new ArrayList<>();
    for (WorkloadSpec spec : workloadSpecs) {
      for (WorkloadSpec variant : spec.getWorkloadVariants()) {
        // Filter by test name if specified
        if (config.includeTest == null || variant.getFullName().contains(config.includeTest)) {
          filteredSpecs.add(variant);
        }
      }
    }

    if (filteredSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads matched the filter: " + config.includeTest);
    }

    // Convert paths into a String array for JMH. JMH requires that parameters be of type String[].
    String[] workloadSpecsArray =
        filteredSpecs.stream().map(WorkloadSpec::toJsonString).toArray(String[]::new);

    // Configure and run JMH benchmark with the loaded workload specs
    Options opt =
        new OptionsBuilder()
            .include(WorkloadBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .param("workloadSpecJson", workloadSpecsArray)
            // TODO: In the future, this can be extended to support multiple engines.
            .param("engineName", "default")
            .forks(config.forks)
            .warmupIterations(config.warmupIterations)
            .measurementIterations(config.measurementIterations)
            .warmupTime(TimeValue.seconds(config.warmupTimeSeconds))
            .measurementTime(TimeValue.seconds(config.measurementTimeSeconds))
            .addProfiler(KernelMetricsProfiler.class)
            .build();

    new Runner(opt, new WorkloadOutputFormat()).run();
  }
}
