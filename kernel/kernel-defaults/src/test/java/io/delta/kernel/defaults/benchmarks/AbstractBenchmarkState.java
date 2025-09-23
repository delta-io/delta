package io.delta.kernel.defaults.benchmarks;

import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import io.delta.kernel.defaults.benchmarks.workloadRunners.WorkloadRunner;
import io.delta.kernel.engine.Engine;
import org.openjdk.jmh.annotations.*;

/**
 * Base state class for all benchmarkstate. This class is responsible for setting up the workload
 * runner based on the workload specification and engine parameters provided by JMH.
 *
 * <p>To add support for a new engine, extend this class and implement the {@link
 * #getEngine(String)} method to return an instance of the desired engine based on the provided
 * engine name.
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
   * Setup method that runs before each benchmark invocation. Reads the workload specification from
   * the given path and initializes the corresponding workload runner.
   *
   * @throws Exception If any error occurs during setup.
   */
  @Setup(Level.Invocation)
  public void setup() throws Exception {
    WorkloadSpec spec = WorkloadSpec.fromJsonString(workloadSpecJson);
    Engine engine = getEngine(engineName);
    runner = spec.getRunner(engine);
    runner.setup();
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
