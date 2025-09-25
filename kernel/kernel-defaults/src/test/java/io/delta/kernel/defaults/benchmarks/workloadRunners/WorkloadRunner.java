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

package io.delta.kernel.defaults.benchmarks.workloadRunners;

import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A runner that can execute a specific workload as a benchmark. A WorkloadRunner is created from a
 * {@link WorkloadSpec} and is responsible for setting up any state necessary to execute the
 * workload as a benchmark using {@link WorkloadRunner#setup()}, as well as executing the workload
 * itself using {@link WorkloadRunner#executeAsBenchmark(Blackhole)}.
 */
public abstract class WorkloadRunner {
  public WorkloadRunner() {}

  /** @return the name of this workload derived from the contents of the workload specification. */
  public abstract String getName();

  /** @return The workload specification used to create this runner. */
  public abstract WorkloadSpec getWorkloadSpec();

  /**
   * Sets up any state necessary to execute this workload as a benchmark. This method must be called
   * before {@link #executeAsBenchmark(Blackhole)}.
   *
   * @throws Exception if any error occurs during setup.
   */
  public abstract void setup() throws Exception;

  /**
   * Executes the workload as a benchmark, consuming any output via the provided Blackhole to
   * prevent dead code elimination by the JIT compiler. The {@link #setup()} method must be called
   * before invoking this method.
   *
   * @param blackhole the Blackhole provided by JMH to consume output.
   * @throws Exception if any error occurs during execution.
   */
  public abstract void executeAsBenchmark(Blackhole blackhole) throws Exception;
}
