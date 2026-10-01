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
import io.delta.kernel.benchmarks.models.SnapshotConstructionSpec;
import io.delta.kernel.benchmarks.models.WorkloadSpec;
import io.delta.kernel.engine.Engine;
import org.openjdk.jmh.infra.Blackhole;

public class SnapshotConstructionRunner extends WorkloadRunner {

  private final SnapshotConstructionSpec workloadSpec;
  private final Engine engine;

  /**
   * Construct a SnapshotConstructionRunner from the workload spec and engine.
   *
   * @param workloadSpec The snapshot_construction workload specification.
   * @param engine The engine to use for executing the workload.
   */
  public SnapshotConstructionRunner(SnapshotConstructionSpec workloadSpec, Engine engine) {
    this.workloadSpec = workloadSpec;
    this.engine = engine;
  }

  @Override
  public String getName() {
    return "snapshot_construction";
  }

  @Override
  public WorkloadSpec getWorkloadSpec() {
    return this.workloadSpec;
  }

  @Override
  public void setup() throws Exception {
    /* No setup needed for snapshot construction */
  }

  @Override
  public void executeAsBenchmark(Blackhole blackhole) throws Exception {
    blackhole.consume(this.execute());
  }

  @Override
  public void cleanup() throws Exception {
    /* No cleanup needed for snapshot construction */
  }

  private Snapshot execute() {
    String workloadTableRoot = workloadSpec.getTableInfo().getResolvedTableRoot();
    SnapshotBuilder builder = TableManager.loadSnapshot(workloadTableRoot);
    if (workloadSpec.getVersion() != null) {
      builder.atVersion(workloadSpec.getVersion());
    }
    return builder.build(engine);
  }
}
