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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;

/** Workload specification for read_metadata benchmarks. */
public class ReadMetadata extends WorkloadSpec {

  /**
   * @return The path to the root of the table. If it is a URI, this must be treated as an absolute
   *     path. Otherwise, the path is relative to the base workload directory.
   */
  @JsonProperty("table_root")
  private String tableRoot;

  /** @return The snapshot version to read. If null, the latest version must be read. */
  @JsonProperty("snapshot_version")
  private Long snapshotVersion;

  // Default constructor for Jackson
  public ReadMetadata() {
    super("read_metadata");
  }

  public String getTableRoot() {
    return tableRoot;
  }

  public Long getSnapshotVersion() {
    return snapshotVersion;
  }

  @Override
  public WorkloadRunner getRunner(Path baseWorkloadDirPath) {
    return new ReadMetadataRunner(baseWorkloadDirPath, this);
  }

  @Override
  public String toString() {
    return "ReadMetadata{"
        + "tableRoot='"
        + tableRoot
        + '\''
        + ", snapshotVersion="
        + snapshotVersion
        + '\''
        + '}';
  }
}
