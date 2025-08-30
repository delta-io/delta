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

import static io.delta.kernel.defaults.benchmarks.WorkloadReader.RESOURCES_DIR;

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;

/** Runner for executing metadata reading workloads using Delta Kernel. */
public class MetadataWorkloadRunner {

  /**
   * Runs a read metadata workload and returns an iterator of engine data.
   *
   * @param workload The ReadMetadata workload specification
   * @return Iterator of FilteredColumnarBatch containing the metadata
   */
  public static CloseableIterator<FilteredColumnarBatch> runReadMetadata(ReadMetadata workload) {
    Engine engine = DefaultEngine.create(new Configuration());

    try {

      // Resource path + workload root
      String tablePath = RESOURCES_DIR.resolve(Paths.get(workload.getTableRoot())).toString();
      // Create table instance

      Table table = Table.forPath(engine, tablePath);

      // Get snapshot (use specified version or latest)
      Snapshot snapshot;
      if (workload.getSnapshotVersion() != null) {
        snapshot = table.getSnapshotAsOfVersion(engine, workload.getSnapshotVersion());
      } else {
        snapshot = table.getLatestSnapshot(engine);
      }

      // Build scan
      ScanBuilder scanBuilder = snapshot.getScanBuilder();
      if (workload.getPredicate() != null) {
        scanBuilder = scanBuilder.withFilter(workload.getPredicate());
      }
      Scan scan = scanBuilder.build();

      // Get scan metadata (this is what we're actually benchmarking)

      return scan.getScanFiles(engine);

    } catch (Exception e) {
      throw new RuntimeException("Failed to run read metadata workload", e);
    }
  }
}
