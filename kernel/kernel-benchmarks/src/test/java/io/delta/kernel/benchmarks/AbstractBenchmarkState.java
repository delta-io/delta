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

import io.delta.kernel.benchmarks.models.WorkloadSpec;
import io.delta.kernel.benchmarks.workloadrunners.WorkloadRunner;
import io.delta.kernel.engine.*;
import org.openjdk.jmh.annotations.*;

/**
 * Base state class for all benchmark state. This class is responsible for setting up the {@link
 * WorkloadRunner} based on the {@link WorkloadSpec} and engine parameters provided by JMH.
 *
 * <p>To add support for a new engine, extend this class and implement the {@link
 * #getEngine(String)} method to return an instance of the desired engine based on the provided
 * engine name.
 *
 * @see WorkloadRunner
 * @see WorkloadSpec
 */
@State(Scope.Thread)
public abstract class AbstractBenchmarkState {

  /**
   * The json representation of the workload specification. Note: This parameter will be set
   * dynamically by JMH. The value is set in the main method.
   */
  @Param({})
  private String workloadSpecJson;

  /**
   * The engine to use for this benchmark. Note: This parameter will be set dynamically by JMH. The
   * value is set in the main method.
   */
  @Param({})
  private String engineName;

  /** The workload runner initialized for this benchmark invocation. */
  private WorkloadRunner runner;

  /**
   * Parses the workload specification from JSON and initializes the benchmarking engine. This also
   * sets up the workload runner.
   *
   * @throws Exception If any error occurs during setup.
   */
  @Setup(Level.Trial)
  public void setupTrial() throws Exception {
    WorkloadSpec spec = WorkloadSpec.fromJsonString(workloadSpecJson);
    Engine engine = KernelMetricsProfiler.BenchmarkingEngine.wrapEngine(getEngine(engineName));
    runner = spec.getRunner(engine);
  }

  /**
   * Setup method that runs before each benchmark invocation. This calls the {@link
   * WorkloadRunner#setup()} to set up the workload runner.
   *
   * @throws Exception If any error occurs during setup.
   */
  @Setup(Level.Invocation)
  public void setupInvocation() throws Exception {
    runner.setup();
  }

  /**
   * Teardown method that runs after each benchmark invocation. This calls the {@link
   * WorkloadRunner#cleanup()} to clean up any state created during execution.
   *
   * @throws Exception If any error occurs during cleanup.
   */
  @TearDown(Level.Invocation)
  public void teardownInvocation() throws Exception {
    runner.cleanup();
  }

  /**
   * Returns an instance of the desired engine based on the provided engine name.
   *
   * @param engineName The name of the engine to instantiate.
   * @return An instance of the specified engine.
   */
  protected abstract Engine getEngine(String engineName);

  /** @return The workload specification for this benchmark invocation. */
  public WorkloadSpec getWorkloadSpecification() {
    return getRunner().getWorkloadSpec();
  }

  /** @return The workload runner initialized for this benchmark invocation. */
  public WorkloadRunner getRunner() {
    return runner;
  }
}
