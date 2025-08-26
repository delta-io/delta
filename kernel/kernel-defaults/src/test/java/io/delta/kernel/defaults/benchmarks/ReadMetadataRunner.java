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

import io.delta.kernel.Scan;
import io.delta.kernel.ScanBuilder;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.openjdk.jmh.infra.Blackhole;

public class ReadMetadataRunner implements WorkloadRunner {
  private final Engine engine;
  private final Scan scan;

  /**
   * Constructs the ReadMetadataRunner from the workload spec and the base workload directory
   * containing test tables. This initializes any state necessary to execute this workload as a
   * benchmark.
   *
   * @param baseWorkloadDirPath The base directory containing workload tables.
   * @param workload The read_metadata workload specification.
   */
  public ReadMetadataRunner(Path baseWorkloadDirPath, ReadMetadata workload) {
    String resolvedTableRoot = getResolvedTableRoot(baseWorkloadDirPath, workload.getTableRoot());
    engine = DefaultEngine.create(new Configuration());
    scan = getScanFromWorkloadSpec(engine, workload, resolvedTableRoot);
  }

  /**
   * Resolves the table root path based on whether it's a URI or a relative path. If it is a
   * relative path, it is resolved based on the workload base directory.
   *
   * @param baseWorkloadDirPath The base directory containing workload tables.
   * @param workloadTableRoot The table root path from the workload spec.
   * @return The resolved table root path.
   */
  private static String getResolvedTableRoot(Path baseWorkloadDirPath, String workloadTableRoot) {
    String resolvedTableRoot = workloadTableRoot;
    // If this is not a URI, treat it as a relative path under the base workload directory
    if (!workloadTableRoot.contains("://")) {
      // This uses a hard-coded base directory for workload tables. In the future, this could be
      // made configurable.
      resolvedTableRoot = baseWorkloadDirPath.resolve(Paths.get(workloadTableRoot)).toString();
    }
    return resolvedTableRoot;
  }

  /**
   * Constructs the Scan from the workload specification for this runner.
   *
   * @param engine The kernel engine to use for table operations.
   * @param workload The read_metadata workload specification.
   * @param resolvedTableRoot The resolved table root path.
   * @return The Scan object used to execute the workload.
   */
  private static Scan getScanFromWorkloadSpec(
      Engine engine, ReadMetadata workload, String resolvedTableRoot) {
    Table table = Table.forPath(engine, resolvedTableRoot);
    // Get snapshot (use specified version or latest)
    Snapshot snapshot;
    if (workload.getSnapshotVersion() != null) {
      snapshot = table.getSnapshotAsOfVersion(engine, workload.getSnapshotVersion());
    } else {
      snapshot = table.getLatestSnapshot(engine);
    }
    // Build scan
    ScanBuilder scanBuilder = snapshot.getScanBuilder();
    // in the read_metadata workload spec
    return scanBuilder.build();
  }

  /**
   * Executes the read_metadata workload, returning an iterator over the results. This must be fully
   * consumed by the caller to ensure the workload is fully executed.
   *
   * @return Iterator of results from the read_metadata workload.
   */
  public CloseableIterator<FilteredColumnarBatch> execute() {
    return scan.getScanFiles(engine);
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
}
