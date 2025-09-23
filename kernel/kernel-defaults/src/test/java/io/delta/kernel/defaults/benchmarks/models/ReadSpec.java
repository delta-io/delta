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

package io.delta.kernel.defaults.benchmarks.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.defaults.benchmarks.ReadMetadataRunner;
import io.delta.kernel.defaults.benchmarks.WorkloadRunner;
import io.delta.kernel.defaults.benchmarks.WorkloadSpec;
import io.delta.kernel.engine.Engine;
import java.util.Arrays;
import java.util.List;

/**
 * Workload specification for read_metadata benchmarks. This workload reads the metadata of the
 * Delta table located at the provided table_root. If a snapshot_version is provided, the metadata
 * at that version is read; otherwise, the latest metadata is read.
 *
 * <p>To run this workload, you can construct a {@link ReadMetadataRunner} from this spec using
 * {@link WorkloadSpec#getRunner(String, Engine, String)}.
 *
 * <p>Sample JSON specification for local delta table at specific version: { "type":
 * "read_metadata", "name": "read-basic-checkpoint-at-v5", "table_root": "basic-checkpoint-table",
 * "snapshot_version": 5 }
 *
 * <p>Sample JSON specification for S3 delta table at latest version: { "type": "read_metadata",
 * "name": "read-basic-checkpoint-at-latest", "table_root":
 * "s3:///my-bucket/delta-tables/basic-checkpoint-table" }
 */
public class ReadSpec extends WorkloadSpec {

  /** The snapshot version to read. If null, the latest version must be read. */
  @JsonProperty("version")
  private Long version;

  @JsonProperty("expected_data")
  private String expectedData;

  // Default constructor for Jackson
  public ReadSpec() {
    super("read");
  }

  /** @return the table root path as specified in the workload spec. */
  public String getTableRoot() {
    return tableRoot;
  }

  /** @return the snapshot version to read, or null if the latest version should be read. */
  public Long getVersion() {
    return version;
  }

  @Override
  public WorkloadRunner getRunner(String baseWorkloadDirPath, Engine engine, String operation) {
    if (operation.equals("read_metadata")) {
      return new ReadMetadataRunner(baseWorkloadDirPath, this, engine);
    } else {
      throw new IllegalArgumentException("Unsupported operation for ReadSpec: " + operation);
    }
  }

  @Override
  public List<String> getBenchmarkOperations() {
    return Arrays.asList("read_metadata", "read_data");
  }

  @Override
  public String toString() {
    return String.format("ReadMetadata{tableRoot='%s', snapshotVersion=%s}", tableRoot, version);
  }
}
