package io.delta.kernel.defaults.benchmarks.workloadrunners;

import io.delta.kernel.Snapshot;
import io.delta.kernel.SnapshotBuilder;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.benchmarks.models.SnapshotConstructionSpec;
import io.delta.kernel.defaults.benchmarks.models.WorkloadSpec;
import io.delta.kernel.engine.Engine;
import org.openjdk.jmh.infra.Blackhole;

public class SnapshotConstructionRunner extends WorkloadRunner {

  private final SnapshotConstructionSpec workloadSpec;
  private final Engine engine;

  public SnapshotConstructionRunner(SnapshotConstructionSpec spec, Engine engine) {
    this.workloadSpec = spec;
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

  private Snapshot execute() {
    String workloadTableRoot = workloadSpec.getTableInfo().getTableRoot();
    SnapshotBuilder builder = TableManager.loadSnapshot(workloadTableRoot);
    if (workloadSpec.getVersion() != null) {
      builder.atVersion(workloadSpec.getVersion());
    }
    return builder.build(engine);
  }
}
