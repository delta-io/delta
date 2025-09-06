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

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class WorkloadBenchmark {

  @Benchmark
  public void benchmarkWorkload(WorkloadBenchmarkState state, Blackhole blackhole)
      throws IOException {
    WorkloadSpec workload = state.getWorkload();

    if (workload instanceof ReadMetadata) {
      ReadMetadata readMetadataWorkload = (ReadMetadata) workload;

      // Run the actual metadata reading workload
      try (CloseableIterator<FilteredColumnarBatch> iterator =
          MetadataWorkloadRunner.runReadMetadata(readMetadataWorkload)) {

        // Consume the iterator to measure the actual work
        while (iterator.hasNext()) {
          FilteredColumnarBatch batch = iterator.next();
          blackhole.consume(batch);
        }
      }
    }
    // Future workload types can be added here:
    // else if (workload instanceof Append) { ... }
    else {
      throw new IllegalArgumentException(
          "Unsupported workload type: " + workload.getClass().getSimpleName());
    }
  }

  public static void main(String[] args) throws RunnerException {
    // Load all workloads
    List<String> workloadPaths = WorkloadReader.loadAllWorkloads();

    if (workloadPaths.isEmpty()) {
      System.err.println("No workloads found. Please add JSON files to the workloads directory.");
      return;
    }

    String[] paths = workloadPaths.toArray(new String[0]);

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
            .build();

    new Runner(opt).run();
  }
}
