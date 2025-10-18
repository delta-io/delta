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

import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import io.delta.kernel.defaults.benchmarks.workloadrunners.WorkloadRunner;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger log = LoggerFactory.getLogger(WorkloadBenchmark.class);

  /** Default implementation of BenchmarkState that supports only the "default" engine. */
  public static class DefaultBenchmarkState extends AbstractBenchmarkState {
    @Override
    protected Engine getEngine(String engineName) {
      if (engineName.equals("default")) {
        return DefaultEngine.create(
            new Configuration() {
              {
                // Set the batch sizes to small so that we get to test the multiple batch/file
                // scenarios.
                set("delta.kernel.default.parquet.reader.batch-size", "20");
                set("delta.kernel.default.json.reader.batch-size", "20");
                set("delta.kernel.default.parquet.writer.targetMaxFileSize", "20");
              }
            });
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
    // Get workload specs from the workloads directory
    List<WorkloadSpec> workloadSpecs = BenchmarkUtils.loadAllWorkloads(WORKLOAD_SPECS_DIR);
    System.out.println("Loaded " + workloadSpecs.size() + " workload specs");
    if (workloadSpecs.isEmpty()) {
      throw new RunnerException(
          "No workloads found. Please add workload specs to the workloads directory.");
    }

    // Parse the Json specs from the json paths
    List<WorkloadSpec> filteredSpecs = new ArrayList<>();
    for (WorkloadSpec spec : workloadSpecs) {
      // TODO: In the future, we can filter specific workloads using command line args here.
      List<WorkloadSpec> variants = spec.getWorkloadVariants();
      for (WorkloadSpec variant : variants) {
        if (variant.getType().equals("write")) {
          filteredSpecs.add(variant);
        }
      }
    }

    // Convert paths into a String array for JMH. JMH requires that parameters be of type String[].
    String[] workloadSpecsArray =
        filteredSpecs.stream().map(WorkloadSpec::toJsonString).toArray(String[]::new);

    // Configure and run JMH benchmark with the loaded workload specs
    Options opt =
        new OptionsBuilder()
            .include(WorkloadBenchmark.class.getSimpleName())
            .param("workloadSpecJson", workloadSpecsArray)
            // TODO: In the future, this can be extended to support multiple engines.
            .param("engineName", "default")
            .forks(1)
            .warmupIterations(3)
            .measurementIterations(5)
            .warmupTime(TimeValue.seconds(1))
            .measurementTime(TimeValue.seconds(1))
            .addProfiler(KernelMetricsProfiler.class)
            .build();

    new Runner(opt).run();
  }
}
