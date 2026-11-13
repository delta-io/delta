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
   * TODO: In the future, this can be extracted so that new benchmarks with custom BenchmarkStates
   * can be easily constructed.
   */
  public static void main(String[] args) throws RunnerException, IOException {
    // Parse command line arguments
    CommandLineArgs cliArgs = CommandLineArgs.parse(args);

    // Get workload specs from the workloads directory
    List<WorkloadSpec> workloadSpecs = BenchmarkUtils.loadAllWorkloads(WORKLOAD_SPECS_DIR);
    if (workloadSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Parse the Json specs from the json paths and apply optional filtering.
    List<WorkloadSpec> filteredSpecs = new ArrayList<>();
    for (WorkloadSpec spec : workloadSpecs) {
      for (WorkloadSpec variant : spec.getWorkloadVariants()) {
        if (cliArgs.shouldInclude(variant.getFullName())) {
          filteredSpecs.add(variant);
        }
      }
    }
    if (filteredSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads matched filters. Try removing --include-test or using a broader substring.");
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
            .forks(cliArgs.forks)
            .warmupIterations(cliArgs.warmupIterations)
            .measurementIterations(cliArgs.measurementIterations)
            .warmupTime(TimeValue.seconds(cliArgs.warmupTimeSeconds))
            .measurementTime(TimeValue.seconds(cliArgs.measurementTimeSeconds))
            .addProfiler(KernelMetricsProfiler.class)
            .build();

    new Runner(opt, new WorkloadOutputFormat()).run();
  }

  /**
   * Minimal command-line parser for configuring benchmark runs.
   *
   * <p>See delta-io/delta#5420: https://github.com/delta-io/delta/issues/5420
   *
   * <p>Supported options:
   *
   * <ul>
   *   <li>{@code --include-test SUBSTRING} (repeatable): include only workload variants whose
   *       {@code full_name} contains {@code SUBSTRING}.
   *   <li>{@code --forks <int>}: number of forks (default: 1)
   *   <li>{@code --warmup-iterations <int>} (alias: {@code --warum-iterations}): warmup iterations
   *       (default: 3)
   *   <li>{@code --measurement-iterations <int>}: measurement iterations (default: 5)
   *   <li>{@code --warmup-time-seconds <int>}: warmup time per iteration, seconds (default: 1)
   *   <li>{@code --measurement-time-seconds <int>}: measurement time per iteration, seconds
   *       (default: 1)
   * </ul>
   */
  static class CommandLineArgs {
    final List<String> includeTestSubstrings;
    final int forks;
    final int warmupIterations;
    final int measurementIterations;
    final int warmupTimeSeconds;
    final int measurementTimeSeconds;

    private CommandLineArgs(
        List<String> includeTestSubstrings,
        int forks,
        int warmupIterations,
        int measurementIterations,
        int warmupTimeSeconds,
        int measurementTimeSeconds) {
      this.includeTestSubstrings = includeTestSubstrings;
      this.forks = forks;
      this.warmupIterations = warmupIterations;
      this.measurementIterations = measurementIterations;
      this.warmupTimeSeconds = warmupTimeSeconds;
      this.measurementTimeSeconds = measurementTimeSeconds;
    }

    static CommandLineArgs parse(String[] args) {
      List<String> include = new ArrayList<>();

      // Defaults match existing hard-coded values.
      int forks = 1;
      int warmupIterations = 3;
      int measurementIterations = 5;
      int warmupTimeSeconds = 1;
      int measurementTimeSeconds = 1;

      for (int i = 0; i < args.length; i++) {
        String arg = args[i];

        if ("--help".equals(arg) || "-h".equals(arg)) {
          throw new IllegalArgumentException(usage());
        }

        switch (arg) {
          case "--include-test":
            include.add(requireValue(args, ++i, arg));
            break;
          case "--forks":
            forks = parsePositiveInt(requireValue(args, ++i, arg), arg);
            break;
          case "--warmup-iterations":
          case "--warum-iterations": // tolerate typo from issue description
            warmupIterations = parseNonNegativeInt(requireValue(args, ++i, arg), arg);
            break;
          case "--measurement-iterations":
            measurementIterations = parseNonNegativeInt(requireValue(args, ++i, arg), arg);
            break;
          case "--warmup-time-seconds":
            warmupTimeSeconds = parsePositiveInt(requireValue(args, ++i, arg), arg);
            break;
          case "--measurement-time-seconds":
            measurementTimeSeconds = parsePositiveInt(requireValue(args, ++i, arg), arg);
            break;
          default:
            throw new IllegalArgumentException("Unknown argument: " + arg + "\n\n" + usage());
        }
      }

      return new CommandLineArgs(
          include,
          forks,
          warmupIterations,
          measurementIterations,
          warmupTimeSeconds,
          measurementTimeSeconds);
    }

    boolean shouldInclude(String fullName) {
      if (includeTestSubstrings.isEmpty()) {
        return true;
      }
      for (String s : includeTestSubstrings) {
        if (fullName.contains(s)) {
          return true;
        }
      }
      return false;
    }

    private static String requireValue(String[] args, int idx, String flag) {
      if (idx >= args.length) {
        throw new IllegalArgumentException("Missing value for " + flag + "\n\n" + usage());
      }
      return args[idx];
    }

    private static int parsePositiveInt(String raw, String flag) {
      int v = parseInt(raw, flag);
      if (v <= 0) {
        throw new IllegalArgumentException(flag + " must be > 0, got: " + raw + "\n\n" + usage());
      }
      return v;
    }

    private static int parseNonNegativeInt(String raw, String flag) {
      int v = parseInt(raw, flag);
      if (v < 0) {
        throw new IllegalArgumentException(flag + " must be >= 0, got: " + raw + "\n\n" + usage());
      }
      return v;
    }

    private static int parseInt(String raw, String flag) {
      try {
        return Integer.parseInt(raw);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid integer for " + flag + ": " + raw + "\n\n" + usage(), e);
      }
    }

    private static String usage() {
      return "WorkloadBenchmark options:\n"
          + "  --include-test SUBSTRING               (repeatable) filter workload variants by full name\n"
          + "  --forks <int>                          default: 1\n"
          + "  --warmup-iterations <int>              default: 3\n"
          + "  --measurement-iterations <int>         default: 5\n"
          + "  --warmup-time-seconds <int>            default: 1\n"
          + "  --measurement-time-seconds <int>       default: 1\n"
          + "  --help | -h\n";
    }
  }
}
