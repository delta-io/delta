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

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A WorkloadRunner that can execute the read_metadata workload as a benchmark. This runner is
 * created from a {@link ReadMetadataSpec}. The workload performs a scan of the table's metadata, at
 * the specified snapshot version (or latest if not specified).
 *
 * <p>If run as a benchmark using {@link #executeAsBenchmark(Blackhole)}, this measures the time to
 * perform the metadata scan and consume all results. It does not include the time to load the
 * snapshot or set up the scan, which is done in {@link #setup()}.
 */
public class ReadMetadataRunner implements WorkloadRunner {
  private Scan scan;
  private final Engine engine;
  private final Path baseWorkloadDirPath;
  private final ReadMetadataSpec workloadSpec;

  /**
   * Constructs the ReadMetadataRunner from the workload spec and the base workload directory
   * containing test tables.
   *
   * @param baseWorkloadDirPath The base directory containing workload tables.
   * @param workloadSpec The read_metadata workload specification.
   */
  public ReadMetadataRunner(String baseWorkloadDirPath, ReadMetadataSpec workloadSpec, Engine engine) {
    this.baseWorkloadDirPath = Paths.get(baseWorkloadDirPath);
    this.workloadSpec = workloadSpec;
    this.engine = engine;
  }

  @Override
  public void setup() {
    // 1) Resolve the table root using
    String workloadTableRoot = workloadSpec.getTableRoot();
    if (!workloadTableRoot.contains("://")) {
      // If this is not a URI, treat it as a relative path under the base workload directory
      // This uses a hard-coded base directory for workload tables. In the future, this could be
      // made configurable.
      workloadTableRoot = baseWorkloadDirPath.resolve(Paths.get(workloadTableRoot)).toString();
    }

    // 2) Load the snapshot, and build the scan
    SnapshotBuilder builder = TableManager.loadSnapshot(workloadTableRoot);
    if (workloadSpec.getSnapshotVersion() != null) {
      builder.atVersion(workloadSpec.getSnapshotVersion());
    }
    Snapshot snapshot = builder.build(engine);
    scan = snapshot.getScanBuilder().build();
  }

  /** @return the name of this workload derived from the workload specification. */
  @Override
  public String getName() {
    return "read_metadata/" + workloadSpec.name;
  }

  /** @return The workload specification used to create this runner. */
  @Override
  public WorkloadSpec getWorkloadSpec() {
    return workloadSpec;
  }

  /**
   * Executes the read_metadata workload as a benchmark, consuming results via the provided
   * Blackhole.
   *
   * @param blackhole The Blackhole to consume results and avoid dead code elimination.
   */
  @Override
  public void executeAsBenchmark(Blackhole blackhole) {
    // Run the actual metadata reading workload
    try (CloseableIterator<FilteredColumnarBatch> iterator = execute()) {
      // Consume the iterator to measure the actual work
      while (iterator.hasNext()) {
        FilteredColumnarBatch batch = iterator.next();
        blackhole.consume(batch);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error during benchmark execution", e);
    }
  }

  /**
   * Executes the read_metadata workload, returning an iterator over the results. This must be fully
   * consumed by the caller to ensure the workload is fully executed.
   *
   * @return Iterator of results from the read_metadata workload.
   */
  private CloseableIterator<FilteredColumnarBatch> execute() {
    if (scan == null) {
      throw new IllegalStateException(
              "ReadMetadataRunner not initialized. Call setup() before executing.");
    }
    return scan.getScanFiles(engine);
  }
}
