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

package io.delta.kernel.defaults.benchmarks.workloadrunners;

import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.benchmarks.CCv2Context;
import io.delta.kernel.defaults.benchmarks.models.TableInfo;
import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import io.delta.kernel.engine.Engine;
import java.util.Optional;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A runner that can execute a specific workload as a benchmark or test. A WorkloadRunner is created
 * from a {@link WorkloadSpec} and is responsible for setting up any state necessary to execute the
 * workload using {@link WorkloadRunner#setup()}, as well as executing the workload itself.
 *
 * <h2>Execution Modes</h2>
 *
 * <ul>
 *   <li><b>Benchmark</b>: Execute via {@link #executeAsBenchmark(Blackhole)} for JMH performance
 *       measurements
 *   <li><b>Test</b>: Execute via executeAsTest() for correctness validation (future work)
 * </ul>
 *
 * <p>The {@link #setup()} method must be called before any execution method.
 */
public abstract class WorkloadRunner {

  public WorkloadRunner() {}

  /** @return the name of this workload derived from the contents of the workload specification. */
  public abstract String getName();

  /** @return The workload specification used to create this runner. */
  public abstract WorkloadSpec getWorkloadSpec();

  /**
   * Sets up any state necessary to execute this workload. This method must be called before
   * executing the workload as a benchmark or test.
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

  /**
   * Cleans up any state created during benchmark execution. For write workloads, this removes added
   * files and reverts table state. For read workloads, this is typically a no-op.
   *
   * <p>This method is called after each benchmark invocation to ensure a clean state for the next
   * run.
   *
   * @throws Exception if any error occurs during cleanup.
   */
  public void cleanup() throws Exception {
    // Default implementation is no-op
  }

  /**
   * Creates a CCv2Context from the given TableInfo if it's a CCv2 table.
   *
   * <p>This method checks if the table has CCv2 configuration and creates the necessary
   * infrastructure (InMemoryUCClient, coordinator, committer) for working with coordinated commits.
   *
   * @param tableInfo the table information
   * @return Optional containing CCv2Context if this is a CCv2 table, empty otherwise
   */
  protected Optional<CCv2Context> createCCv2Context(TableInfo tableInfo, Engine engine) {
    if (tableInfo.isCCv2Enabled()) {
      return Optional.of(CCv2Context.createFromTableInfo(tableInfo, engine));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Creates a SnapshotBuilder for the given table root, optionally configuring it for CCv2 tables.
   *
   * <p>If ccv2Context is present, the builder is configured with the CCv2 committer and parsed log
   * data to enable reading from and writing to coordinated commits.
   *
   * @param tableRoot the root path of the Delta table
   * @param ccv2Context optional CCv2 context for coordinated commits tables
   * @return a SnapshotBuilder configured appropriately for the table type
   */
  protected SnapshotBuilder getSnapshotBuilder(
      String tableRoot, Optional<CCv2Context> ccv2Context) {
    SnapshotBuilder builder = TableManager.loadSnapshot(tableRoot);
    if (ccv2Context.isPresent()) {
      CCv2Context context = ccv2Context.get();
      builder =
          builder.withCommitter(context.getCommitter()).withLogData(context.getParsedLogData());
    }
    return builder;
  }

  // TODO: Add executeAsTest() method for correctness validation
  // public abstract void executeAsTest() throws Exception;
}
