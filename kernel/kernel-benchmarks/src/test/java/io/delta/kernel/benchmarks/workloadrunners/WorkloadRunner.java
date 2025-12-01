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

package io.delta.kernel.benchmarks.workloadrunners;

import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.benchmarks.models.TableInfo;
import io.delta.kernel.benchmarks.models.UcCatalogInfo;
import io.delta.kernel.benchmarks.models.WorkloadSpec;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.unitycatalog.InMemoryUCClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import java.net.URI;
import java.nio.file.Paths;
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
  public abstract void cleanup() throws Exception;

  /**
   * Loads a snapshot for the table.
   *
   * <p>For Unity Catalog managed tables, uses {@link UCCatalogManagedClient} to handle staged
   * commits. For regular tables, uses {@link TableManager}.
   *
   * @param engine the engine to use
   * @param tableInfo the table information
   * @param versionOpt optional version to load (if empty, loads latest)
   * @return a Snapshot for the table
   * @throws Exception if there's an error loading the snapshot
   */
  protected Snapshot loadSnapshot(Engine engine, TableInfo tableInfo, Optional<Long> versionOpt)
      throws Exception {
    String tableRoot = tableInfo.getResolvedTableRoot();

    if (tableInfo.isCatalogManaged()) {
      UcCatalogInfo ucCatalogInfo = tableInfo.getUcCatalogInfo();
      InMemoryUCClient ucClient = ucCatalogInfo.createUCClient(engine, tableRoot);
      UCCatalogManagedClient ucCatalogManagedClient = new UCCatalogManagedClient(ucClient);

      // Use Paths.get().toUri() to get properly formatted file:// URI
      URI tableUri = Paths.get(tableRoot).toUri();
      return ucCatalogManagedClient.loadSnapshot(
          engine,
          ucCatalogInfo.getUcTableId(),
          tableUri.toString(),
          versionOpt,
          Optional.empty() /* timestampOpt */);
    } else {
      // Use direct TableManager for regular filesystem tables
      SnapshotBuilder builder = TableManager.loadSnapshot(tableRoot);
      if (versionOpt.isPresent()) {
        builder = builder.atVersion(versionOpt.get());
      }
      return builder.build(engine);
    }
  }

  // TODO: Add executeAsTest() method for correctness validation
  // public abstract void executeAsTest() throws Exception;
}
